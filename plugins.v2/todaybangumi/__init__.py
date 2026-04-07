import datetime
import importlib
import re
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.db.site_oper import SiteOper
from app.db.systemconfig_oper import SystemConfigOper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas.types import MediaType, NotificationType, SystemConfigKey
from app.utils.http import RequestUtils


class TodayBangumi(_PluginBase):
    plugin_name = "Bangumi每日放送"
    plugin_desc = "通过 Bangumi 每日放送生成海报界面，供用户挑选并添加订阅。"
    plugin_icon = "Bangumi_A.png"
    plugin_version = "1.0.1"
    plugin_author = "luanyi143"
    author_url = "https://github.com/luanyi143"
    plugin_config_prefix = "todaybangumi_"
    plugin_order = 14
    auth_level = 1

    _enabled = False
    _onlyonce = False
    _proxy = False
    _silent_mode = False
    _cron = "0 9 * * *"
    _days_ahead = 0
    _items_limit = 24
    _resolution_filters: List[str] = []
    _subscribe_sites: List[int] = []
    _subscribe_filter_groups: List[str] = []
    _custom_category: str = ""

    _calendar_api = "https://api.bgm.tv/calendar"
    _subject_api = "https://api.bgm.tv/v0/subjects/%s"
    _episodes_api = "https://api.bgm.tv/v0/episodes?subject_id=%s&type=0"
    _summary_image = "https://raw.githubusercontent.com/luanyi143/MoviePilot-Plugins/main/icons/post_message.jpeg"

    _scheduler = None
    subscribechain: Optional[SubscribeChain] = None

    _resolution_options = [
        {"title": "2160p / 4K", "value": "2160p|4k|uhd"},
        {"title": "1080p", "value": "1080p"},
    ]

    def init_plugin(self, config: dict = None):
        self.stop_service()
        self.subscribechain = SubscribeChain()

        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._proxy = bool(config.get("proxy", False))
        self._silent_mode = bool(config.get("silent_mode", False))
        self._cron = config.get("cron") or "0 9 * * *"
        self._days_ahead = self.__normalize_days_ahead(config.get("days_ahead"))
        self._items_limit = self.__normalize_items_limit(config.get("items_limit"))
        self._resolution_filters = config.get("resolution_filters") or []
        self._subscribe_sites = config.get("subscribe_sites") or []
        self._subscribe_filter_groups = self.__normalize_filter_groups(
            config.get("subscribe_filter_groups", config.get("subscribe_rule"))
        )
        self._custom_category = str(config.get("custom_category") or "").strip()

        previous_silent_mode = bool(self.get_data("silent_mode_enabled"))
        if self._silent_mode and not previous_silent_mode:
            self.save_data("silent_last_run_date", "")
            logger.info("[TodayBangumi] 检测到静默模式首次开启，已重置静默执行标记")
        elif not self._silent_mode and previous_silent_mode:
            self.save_data("silent_last_run_date", "")
            logger.info("[TodayBangumi] 检测到静默模式已关闭，已清理静默执行标记")
        self.save_data("silent_mode_enabled", self._silent_mode)

        if self._onlyonce:
            self.__schedule_once()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/manual_subscribe",
                "endpoint": self.__manual_subscribe,
                "methods": ["GET"],
                "summary": "添加 Bangumi 每日放送订阅",
                "description": "点击海报后通过标题识别媒体并添加电视剧订阅",
            },
            {
                "path": "/bulk_subscribe",
                "endpoint": self.__bulk_subscribe,
                "methods": ["GET"],
                "summary": "批量添加 Bangumi 每日放送订阅",
                "description": "将当前页面已获取到的每日放送条目批量添加为电视剧订阅",
            },
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if not self._enabled:
            return []

        services: List[Dict[str, Any]] = []

        if self._cron:
            services.append(
                {
                    "id": "todaybangumi_refresh",
                    "name": "Bangumi每日放送刷新服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__refresh_calendar,
                    "kwargs": {"trigger_source": "cron"},
                }
            )

        return services

    def get_form(self) -> Tuple[List[dict], Dict[str, Any]]:
        return [
            {
                "component": "VForm",
                "content": [
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "enabled", "label": "启用插件"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "proxy", "label": "使用代理"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "silent_mode", "label": "静默模式"},
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {"model": "onlyonce", "label": "立即刷新一次"},
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "刷新周期",
                                            "placeholder": "5位cron表达式",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "days_ahead",
                                            "label": "偏移天数",
                                            "type": "number",
                                            "min": 0,
                                            "max": 6,
                                            "placeholder": "0 表示今天，1 表示明天",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "items_limit",
                                            "label": "最多展示条数",
                                            "type": "number",
                                            "min": 1,
                                            "placeholder": "24",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "subscribe_sites",
                                            "label": "默认订阅站点",
                                            "items": self.__get_site_options(),
                                            "item-title": "title",
                                            "item-value": "value",
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "resolution_filters",
                                            "label": "默认订阅分辨率",
                                            "items": self._resolution_options,
                                            "item-title": "title",
                                            "item-value": "value",
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VCombobox",
                                        "props": {
                                            "model": "subscribe_filter_groups",
                                            "label": "规则",
                                            "items": self.__get_rule_options(),
                                            "item-title": "title",
                                            "item-value": "value",
                                            "multiple": True,
                                            "chips": True,
                                            "clearable": True,
                                            "closable-chips": True,
                                            "hint": "可直接输入规则组名称，回车后添加",
                                            "persistent-hint": True,
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 6},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "custom_category",
                                            "label": "自定义类别",
                                            "placeholder": "自定义类别",
                                        },
                                    }
                                ],
                            },
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "开启静默模式后，插件会在定时刷新时自动把当前获取到的条目逐个尝试添加为订阅，不再需要手动点击海报按钮。为避免对上游网站造成压力，静默模式下每天最多只会自动执行一次。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                    {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "info",
                                            "variant": "tonal",
                                            "text": "[订阅规则自动填充]插件会影响本插件的设置项，如无特殊要求建议全部保持为空，以[订阅规则自动填充]为准。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                                        {
                        "component": "VRow",
                        "content": [
                            {
                                "component": "VCol",
                                "props": {"cols": 12},
                                "content": [
                                    {
                                        "component": "VAlert",
                                        "props": {
                                            "type": "warning",
                                            "variant": "tonal",
                                            "text": "当季新番全部订阅成功后，建议关闭插件，或者将下次执行时间调整到下个季度。",
                                        },
                                    }
                                ],
                            }
                        ],
                    },
                ],
            }
        ], {
            "enabled": False,
            "proxy": False,
            "silent_mode": False,
            "onlyonce": False,
            "cron": "0 9 * * *",
            "days_ahead": 0,
            "items_limit": 24,
            "resolution_filters": [],
            "subscribe_sites": [],
            "subscribe_filter_groups": [],
            "custom_category": "",
        }

    def get_page(self) -> List[dict]:
        history: List[dict] = self.get_data("calendar_items") or []
        target_label = self.get_data("target_day_label") or "今日"
        updated_at = self.get_data("updated_at") or ""

        if not history:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "暂无每日放送数据，请先启用插件并执行一次刷新。",
                    },
                }
            ]

        cards = []
        for item in history:
            title = item.get("title") or "未知标题"
            title_origin = item.get("title_origin") or ""
            link = item.get("link") or ""
            poster = item.get("poster") or ""
            air_weekday = item.get("air_weekday") or ""
            air_date = item.get("air_date") or ""
            year = item.get("year") or ""
            summary = item.get("summary") or ""
            subject_id = item.get("subject_id") or ""
            bangumi_total_episodes = item.get("bangumi_total_episodes")

            cards.append(
                {
                    "component": "VCard",
                    "props": {
                        "class": "h-100",
                        "style": "height: 230px; border-radius: 14px; overflow: hidden;",
                    },
                    "content": [
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex flex-nowrap flex-row h-100",
                                "style": "height: 230px;",
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "props": {
                                        "style": "width: 120px; min-width: 120px; height: 180px; margin: 25px 0 25px 12px;",
                                    },
                                    "content": [
                                        {
                                            "component": "VImg",
                                            "props": {
                                                "src": poster,
                                                "height": 180,
                                                "width": 120,
                                                "aspect-ratio": "2/3",
                                                "class": "object-cover shadow ring-gray-500",
                                                "style": "border-radius: 10px;",
                                                "cover": True,
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "div",
                                    "props": {
                                        "class": "d-flex flex-column justify-space-between flex-grow-1",
                                        "style": "height: 230px; min-width: 0; padding: 8px 10px 8px 10px;",
                                    },
                                    "content": [
                                        {
                                            "component": "div",
                                            "content": [
                                                {
                                                    "component": "VCardSubtitle",
                                                    "props": {
                                                        "class": "pa-0 font-weight-bold",
                                                        "style": "display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; line-height: 1.15rem; min-height: 2.3rem;",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "a",
                                                            "props": {
                                                                "href": link,
                                                                "target": "_blank",
                                                                "style": "color: inherit; text-decoration: none;",
                                                            },
                                                            "text": title,
                                                        }
                                                    ],
                                                },
                                                {
                                                    "component": "VCardText",
                                                    "props": {
                                                        "class": "pa-0 text-caption text-medium-emphasis",
                                                        "style": "display: -webkit-box; -webkit-line-clamp: 1; -webkit-box-orient: vertical; overflow: hidden; min-height: 16px; line-height: 1rem;",
                                                    },
                                                    "text": f"原名：{title_origin}" if title_origin and title_origin != title else "原名：—",
                                                },
                                                {
                                                    "component": "VCardText",
                                                    "props": {
                                                        "class": "pa-0 text-body-2",
                                                        "style": "white-space: nowrap; overflow: hidden; text-overflow: ellipsis; line-height: 1rem;",
                                                    },
                                                    "text": f"放送：{air_weekday} {air_date}".strip(),
                                                },
                                                {
                                                    "component": "VCardText",
                                                    "props": {
                                                        "class": "pa-0 text-body-2",
                                                        "style": "white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-height: 16px; line-height: 1rem;",
                                                    },
                                                    "text": f"Bangumi ID：{subject_id}" if subject_id else "Bangumi ID：—",
                                                },
                                                {
                                                    "component": "VCardText",
                                                    "props": {
                                                        "class": "pa-0 pt-1 text-body-2",
                                                        "style": "white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-height: 20px;",
                                                    },
                                                    "text": f"Bangumi 总集数：{bangumi_total_episodes}" if bangumi_total_episodes else "Bangumi 总集数：—",
                                                },
                                                {
                                                    "component": "VCardText",
                                                    "props": {
                                                        "class": "pa-0 text-caption",
                                                        "style": "display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; line-height: 0.95rem; min-height: 1.9rem;",
                                                    },
                                                    "text": summary if summary else "暂无简介",
                                                },
                                            ],
                                        },
                                        {
                                            "component": "div",
                                            "content": [
                                                {
                                                    "component": "VCardText",
                                                    "props": {
                                                        "class": "pa-0 text-caption text-primary",
                                                        "style": "min-height: 16px; line-height: 1rem;",
                                                    },
                                                    "text": "点击下方按钮可尝试添加订阅",
                                                },
                                                {
                                                    "component": "VCardActions",
                                                    "props": {"class": "pa-0"},
                                                    "content": [
                                                        {
                                                            "component": "VBtn",
                                                            "props": {
                                                                "color": "primary",
                                                                "variant": "tonal",
                                                                "size": "small",
                                                            },
                                                            "text": "添加订阅",
                                                            "events": {
                                                                "click": {
                                                                    "api": "plugin/TodayBangumi/manual_subscribe",
                                                                    "method": "get",
                                                                    "params": {
                                                                        "apikey": settings.API_TOKEN,
                                                                        "title": title,
                                                                        "title_origin": title_origin,
                                                                        "year": year,
                                                                        "subject_id": subject_id,
                                                                    },
                                                                }
                                                            },
                                                        }
                                                    ],
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ],
                        }
                    ],
                }
            )

        return [
            {
                "component": "div",
                "props": {"class": "mb-3"},
                "content": [
                    {
                        "component": "VAlert",
                        "props": {
                            "type": "success",
                            "variant": "tonal",
                            "text": f"{target_label}每日放送，共 {len(history)} 条，最近刷新时间：{updated_at}",
                        },
                    }
                ],
            },
            {"component": "div", "props": {"class": "grid gap-3 grid-info-card"}, "content": cards},
            {
                "component": "div",
                "props": {"class": "d-flex justify-start mt-4 ml-2"},
                "content": [
                    {
                        "component": "VBtn",
                        "props": {
                            "color": "primary",
                            "variant": "elevated",
                            "size": "default",
                        },
                        "text": "全部订阅",
                        "events": {
                            "click": {
                                "api": "plugin/TodayBangumi/bulk_subscribe",
                                "method": "get",
                                "params": {"apikey": settings.API_TOKEN},
                            }
                        },
                    }
                ],
            },
        ]

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                self._scheduler.shutdown()
                self._scheduler = None
        except Exception as err:
            logger.error(f"[TodayBangumi] 停止一次性调度服务失败：{err}")

    def __schedule_once(self):
        self._scheduler = BackgroundScheduler(timezone=settings.TZ)
        self._scheduler.add_job(
            func=self.__refresh_calendar,
            trigger="date",
            run_date=self.__now() + datetime.timedelta(seconds=3),
            name="Bangumi每日放送立即刷新",
            kwargs={"trigger_source": "onlyonce"},
        )
        self._scheduler.start()
        logger.info("[TodayBangumi] 已注册一次性刷新任务")

    def __update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "onlyonce": False,
                "proxy": self._proxy,
                "silent_mode": self._silent_mode,
                "cron": self._cron,
                "days_ahead": self._days_ahead,
                "items_limit": self._items_limit,
                "resolution_filters": self._resolution_filters,
                "subscribe_sites": self._subscribe_sites,
                "subscribe_filter_groups": self._subscribe_filter_groups,
                "custom_category": self._custom_category,
            }
        )
        self._onlyonce = False

    def __refresh_calendar(self, trigger_source: str = "cron"):
        try:
            target_date = self.__now().date() + datetime.timedelta(days=self._days_ahead)
            target_weekday = target_date.strftime("%a")
            target_label = self.__build_target_day_label(target_date)

            logger.info(
                f"[TodayBangumi] 开始刷新 Bangumi 每日放送，目标日期：{target_date}，触发来源：{trigger_source}"
            )
            items = self.__fetch_bangumi_calendar(target_weekday=target_weekday, target_date=target_date)
            self.save_data("calendar_items", items)
            self.save_data("target_day_label", target_label)
            self.save_data("updated_at", self.__now().strftime("%Y-%m-%d %H:%M:%S"))

            if self._silent_mode:
                if self.__should_skip_silent_refresh(trigger_source=trigger_source):
                    logger.info("[TodayBangumi] 本次将仅刷新每日放送数据，跳过静默自动订阅")
                else:
                    silent_completed = False
                    try:
                        if items:
                            self.__auto_subscribe_items(items)
                        silent_completed = True
                    except Exception as err:
                        logger.error(f"[TodayBangumi] 静默模式自动订阅流程异常：{err}")

                    if silent_completed and trigger_source == "cron":
                        self.save_data("silent_last_run_date", self.__now().strftime("%Y-%m-%d"))

            if self._onlyonce:
                self.__update_config()

            logger.info(f"[TodayBangumi] Bangumi 每日放送刷新完成，共 {len(items)} 条")
        except Exception as err:
            logger.error(f"[TodayBangumi] 刷新 Bangumi 每日放送失败：{err}")

    def __auto_subscribe_items(self, items: List[dict]) -> None:
        success_count = 0
        fail_details: List[str] = []

        logger.info(f"[TodayBangumi] 静默模式已开启，开始自动处理 {len(items)} 条媒体订阅")

        for item in items:
            item_title = item.get("title") or item.get("title_origin") or "未知标题"
            try:
                success, message = self.__subscribe_item(
                    title=item.get("title") or "",
                    title_origin=item.get("title_origin") or "",
                    year=item.get("year") or "",
                    subject_id=item.get("subject_id"),
                    bangumi_total_episodes=item.get("bangumi_total_episodes"),
                    use_mp_notify=False,
                )
                if success:
                    success_count += 1
                else:
                    fail_details.append(f"{item_title}：{message}")
                    logger.warn(
                        f"[TodayBangumi] 静默模式自动订阅失败：title={item_title}，原因：{message}"
                    )
            except Exception as err:
                fail_details.append(f"{item_title}：{err}")
                logger.error(
                    f"[TodayBangumi] 静默模式处理条目异常：title={item_title}，错误：{err}"
                )

        fail_count = len(fail_details)
        self.__post_summary_message(
            user_name="静默模式",
            success_count=success_count,
            fail_details=fail_details,
        )
        logger.info(
            f"[TodayBangumi] 静默模式自动订阅完成：成功 {success_count} 条，失败 {fail_count} 条"
        )

    def __should_skip_silent_refresh(self, trigger_source: str = "cron") -> bool:
        if not self._silent_mode:
            return False

        if trigger_source != "cron":
            return False

        today_text = self.__now().strftime("%Y-%m-%d")
        last_run_date = str(self.get_data("silent_last_run_date") or "").strip()
        if last_run_date == today_text:
            logger.info(
                f"[TodayBangumi] 静默模式今日已完成一次定时自动订阅，跳过本次定时自动订阅：last_run_date={last_run_date}"
            )
            return True
        return False

    def __fetch_bangumi_calendar(self, target_weekday: str, target_date: datetime.date) -> List[dict]:
        try:
            response = RequestUtils(proxies=settings.PROXY).get_res(self._calendar_api) if self._proxy else RequestUtils().get_res(self._calendar_api)
            if not response:
                return []

            data = response.json()
            if not isinstance(data, list):
                return []

            results: List[dict] = []
            for day_block in data:
                weekday = day_block.get("weekday") or {}
                weekday_en = str(weekday.get("en") or "").strip()
                if weekday_en.lower() != target_weekday.lower():
                    continue

                for subject in day_block.get("items") or []:
                    item = self.__build_subject_item(subject=subject, target_date=target_date, weekday=weekday)
                    if item:
                        results.append(item)

            results = results[: self._items_limit]
            return results
        except Exception as err:
            logger.error(f"[TodayBangumi] 获取 Bangumi 每日放送失败：{err}")
            return []

    def __build_subject_item(self, subject: dict, target_date: datetime.date, weekday: dict) -> Optional[dict]:
        subject_id = subject.get("id")
        name = (subject.get("name_cn") or subject.get("name") or "").strip()
        title_origin = (subject.get("name") or "").strip()
        if not name:
            return None

        images = subject.get("images") or {}
        poster = self.__normalize_image_url(
            images.get("large")
            or images.get("common")
            or images.get("medium")
            or images.get("small")
            or ""
        )

        summary = re.sub(r"\s+", " ", str(subject.get("summary") or "")).strip()
        air_date = self.__extract_air_date(subject) or target_date.strftime("%Y-%m-%d")
        year = air_date[:4] if air_date and re.match(r"^\d{4}", air_date) else ""
        air_weekday = str(weekday.get("cn") or weekday.get("en") or "").strip()
        link = f"https://bgm.tv/subject/{subject_id}" if subject_id else "https://bgm.tv"
        bangumi_total_episodes = self.__fetch_bangumi_total_episodes(subject_id)

        return {
            "subject_id": subject_id,
            "title": name,
            "title_origin": title_origin,
            "poster": poster,
            "summary": summary,
            "air_date": air_date,
            "air_weekday": air_weekday,
            "year": year,
            "link": link,
            "bangumi_total_episodes": bangumi_total_episodes,
        }

    def __request_json(self, url: str) -> Any:
        try:
            response = RequestUtils(proxies=settings.PROXY).get_res(url) if self._proxy else RequestUtils().get_res(url)
            if not response:
                return None
            return response.json()
        except Exception as err:
            logger.warn(f"[TodayBangumi] 请求 Bangumi 接口失败：{url}，错误：{err}")
            return None

    def __fetch_bangumi_total_episodes(self, subject_id: Any) -> Optional[int]:
        if not subject_id:
            return None

        try:
            sid = int(subject_id)
        except (TypeError, ValueError):
            return None

        episodes_data = self.__request_json(self._episodes_api % sid)
        if isinstance(episodes_data, dict):
            total = episodes_data.get("total")
            try:
                total = int(total)
                if total > 0:
                    return total
            except (TypeError, ValueError):
                pass

        subject_data = self.__request_json(self._subject_api % sid)
        if isinstance(subject_data, dict):
            for field in ["total_episodes", "eps"]:
                value = subject_data.get(field)
                try:
                    value = int(value)
                    if value > 0:
                        return value
                except (TypeError, ValueError):
                    continue

        return None

    @staticmethod
    def __normalize_image_url(url: Any) -> str:
        raw_url = str(url or "").strip()
        if not raw_url:
            return ""
        if raw_url.startswith("//"):
            return f"https:{raw_url}"
        if raw_url.startswith("http://") or raw_url.startswith("https://"):
            return raw_url
        return ""

    @staticmethod
    def __extract_air_date(subject: dict) -> str:
        date_value = str(subject.get("date") or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", date_value):
            return date_value

        air_date = str(subject.get("air_date") or "").strip()
        if re.match(r"^\d{4}-\d{2}-\d{2}$", air_date):
            return air_date

        return ""

    @staticmethod
    def __normalize_days_ahead(raw_value: Any) -> int:
        try:
            value = int(raw_value)
            if value < 0:
                return 0
            if value > 6:
                return 6
            return value
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def __normalize_items_limit(raw_value: Any) -> int:
        try:
            value = int(raw_value)
            if value <= 0:
                return 24
            return min(value, 100)
        except (TypeError, ValueError):
            return 24

    @staticmethod
    def __get_site_options() -> List[Dict[str, Any]]:
        return [{"title": site.name, "value": site.id} for site in SiteOper().list_active()]

    @staticmethod
    def __normalize_filter_groups(raw_value: Any) -> List[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, list):
            return [str(item).strip() for item in raw_value if str(item).strip()]
        raw_text = str(raw_value).strip()
        if not raw_text:
            return []
        return [raw_text]

    def __get_rule_options(self) -> List[Dict[str, Any]]:
        options: List[Dict[str, Any]] = []
        seen_values = set()

        def append_option(value: Any, title: Any = None) -> None:
            option_value = str(value or "").strip()
            option_title = str(title or value or "").strip()
            if not option_value or option_value in seen_values:
                return
            seen_values.add(option_value)
            options.append({"title": option_title, "value": option_value})

        try:
            config_groups = SystemConfigOper().get(SystemConfigKey.SubscribeFilterRuleGroups) or []
            if isinstance(config_groups, list):
                for group in config_groups:
                    append_option(group)
            elif config_groups:
                append_option(config_groups)
        except Exception as err:
            logger.debug(f"[TodayBangumi] 读取系统配置规则组失败：err={err}")

        candidate_defs = [
            ("app.db.subscribefilter_oper", "SubscribeFilterOper", ["list", "list_all", "list_enabled"]),
            ("app.db.rule_oper", "RuleOper", ["list", "list_all", "list_enabled"]),
            ("app.db.filter_oper", "FilterOper", ["list", "list_all", "list_enabled"]),
        ]

        for module_name, class_name, method_names in candidate_defs:
            try:
                module = importlib.import_module(module_name)
                oper_cls = getattr(module, class_name, None)
                if not oper_cls:
                    continue

                oper = oper_cls()
                records = None
                for method_name in method_names:
                    method = getattr(oper, method_name, None)
                    if callable(method):
                        records = method()
                        if records:
                            break

                if not isinstance(records, list):
                    continue

                for record in records:
                    title = ""
                    value = ""

                    if isinstance(record, dict):
                        title = str(
                            record.get("group_name")
                            or record.get("name")
                            or record.get("title")
                            or record.get("rule_name")
                            or record.get("id")
                            or ""
                        ).strip()
                        value = str(
                            record.get("group_name")
                            or record.get("name")
                            or record.get("title")
                            or record.get("id")
                            or ""
                        ).strip()
                    else:
                        title = str(
                            getattr(record, "group_name", None)
                            or getattr(record, "name", None)
                            or getattr(record, "title", None)
                            or getattr(record, "rule_name", None)
                            or getattr(record, "id", None)
                            or ""
                        ).strip()
                        value = str(
                            getattr(record, "group_name", None)
                            or getattr(record, "name", None)
                            or getattr(record, "title", None)
                            or getattr(record, "id", None)
                            or ""
                        ).strip()

                    append_option(value, title)
            except Exception as err:
                logger.debug(f"[TodayBangumi] 读取规则组失败：module={module_name}, err={err}")

        for item in self._subscribe_filter_groups:
            append_option(item)

        return options

    def __inject_subscribe_preferences(self, subscribe_kwargs: Dict[str, Any]) -> Dict[str, Any]:
        if self._subscribe_filter_groups:
            subscribe_kwargs["filter_groups"] = self._subscribe_filter_groups

        if self._custom_category:
            subscribe_kwargs["media_category"] = self._custom_category

        return subscribe_kwargs

    def __build_resolution_rule(self) -> Optional[str]:
        if not self._resolution_filters:
            return None
        if len(self._resolution_filters) == 1:
            return self._resolution_filters[0]
        return "|".join([f"(?:{item})" for item in self._resolution_filters if item])

    @staticmethod
    def __now() -> datetime.datetime:
        return datetime.datetime.now(pytz.timezone(settings.TZ))

    @staticmethod
    def __build_target_day_label(target_date: datetime.date) -> str:
        today = datetime.datetime.now().date()
        if target_date == today:
            return "今日"
        if target_date == today + datetime.timedelta(days=1):
            return "明日"
        return target_date.strftime("%Y-%m-%d")

    @staticmethod
    def __format_fail_detail(fail_details: List[str], max_items: int = 10) -> str:
        if not fail_details:
            return ""

        visible_items = fail_details[:max_items]
        lines = [f"{index + 1}. {item}" for index, item in enumerate(visible_items)]
        remaining = len(fail_details) - len(visible_items)
        if remaining > 0:
            lines.append(f"... 其余 {remaining} 条未展开")
        return "\n".join(lines)

    def __post_summary_message(self, user_name: str, success_count: int, fail_details: List[str]) -> None:
        fail_count = len(fail_details)
        fail_detail_text = self.__format_fail_detail(fail_details)
        title = "📢🐱 Go修金sama！你的番剧订阅全部搞定啦🥳"
        text = f"✅ 成功订阅：{success_count} 部"

        if fail_count > 0:
            text += f" \n❌ 订阅失败：{fail_count} 部 \n\n⚠️ 失败的番剧列表：\n{fail_detail_text}"

        message_kwargs = {
            "mtype": NotificationType.Subscribe,
            "title": title,
            "text": text,
            "link": settings.MP_DOMAIN("#/subscribe/tv?tab=mysub"),
        }

        try:
            self.post_message(
                **message_kwargs,
                image=self._summary_image,
            )
        except Exception as err:
            logger.error(f"[TodayBangumi] 汇总通知发送失败：{err}")

    def __subscribe_item(
        self,
        title: str = "",
        title_origin: str = "",
        year: str = "",
        subject_id: Any = None,
        bangumi_total_episodes: Optional[int] = None,
        use_mp_notify: bool = False,
    ) -> Tuple[bool, str]:
        title = (title or "").strip()
        title_origin = (title_origin or "").strip()

        if not title and not title_origin:
            logger.warn("[TodayBangumi] 订阅失败：标题为空")
            return False, "标题为空"

        search_title = title or title_origin
        meta = MetaInfo(search_title)
        if year:
            meta.year = str(year)
        meta.type = MediaType.TV

        if bangumi_total_episodes is None:
            bangumi_total_episodes = self.__fetch_bangumi_total_episodes(subject_id)

        if bangumi_total_episodes:
            logger.info(f"[TodayBangumi] Bangumi 条目 {subject_id} 的正片总集数：{bangumi_total_episodes}")

        mediainfo: MediaInfo = self.chain.recognize_media(meta=meta, mtype=MediaType.TV)
        if not mediainfo and title_origin and title_origin != title:
            fallback_meta = MetaInfo(title_origin)
            if year:
                fallback_meta.year = str(year)
            fallback_meta.type = MediaType.TV
            mediainfo = self.chain.recognize_media(meta=fallback_meta, mtype=MediaType.TV)

        if not mediainfo:
            bangumi_hint = f"（Bangumi ID: {subject_id}）" if subject_id else ""
            logger.warn(f"[TodayBangumi] 订阅失败：未识别到媒体 {search_title} {bangumi_hint}")
            return False, f"未识别到对应媒体信息{bangumi_hint}"

        subscribe_kwargs = {
            "title": mediainfo.title or search_title,
            "year": mediainfo.year or year or "",
            "mtype": MediaType.TV,
            "tmdbid": mediainfo.tmdb_id,
            "doubanid": mediainfo.douban_id,
            "season": mediainfo.season or 1,
            "exist_ok": True,
            "username": self.plugin_name,
            "message": use_mp_notify,
        }

        if self._subscribe_sites:
            subscribe_kwargs["sites"] = self._subscribe_sites

        resolution_rule = self.__build_resolution_rule()
        if resolution_rule:
            subscribe_kwargs["resolution"] = resolution_rule

        subscribe_kwargs = self.__inject_subscribe_preferences(subscribe_kwargs)

        logger.info(
            f"[TodayBangumi] 本次订阅参数注入："
            f"sites={subscribe_kwargs.get('sites', '未注入')}，"
            f"resolution={subscribe_kwargs.get('resolution', '未注入')}，"
            f"filter_groups={subscribe_kwargs.get('filter_groups', '未注入')}，"
            f"media_category={subscribe_kwargs.get('media_category', '未注入')}"
        )

        target_season = mediainfo.season or 1
        if bangumi_total_episodes and target_season == 1:
            subscribe_kwargs.update(
                {
                    "total_episode": bangumi_total_episodes,
                    "start_episode": 1,
                    "lack_episode": bangumi_total_episodes,
                    "manual_total_episode": 1,
                }
            )
            logger.info(
                f"[TodayBangumi] 已向订阅注入 Bangumi 总集数：season={target_season}, total={bangumi_total_episodes}"
            )
        elif bangumi_total_episodes:
            logger.info(
                f"[TodayBangumi] 识别结果为第 {target_season} 季，跳过注入 Bangumi 总集数：{bangumi_total_episodes}"
            )

        sid, msg = self.subscribechain.add(**subscribe_kwargs)
        if not sid:
            logger.warn(f"[TodayBangumi] 订阅失败：{msg or '添加订阅失败'}")
            return False, msg or "添加订阅失败"

        subscribe_title = mediainfo.title_year or search_title

        logger.info(
            f"[TodayBangumi] 添加订阅成功：{subscribe_title}"
            f"{f'，Bangumi总集数：{bangumi_total_episodes}' if bangumi_total_episodes else ''}"
        )
        return True, (
            f"{subscribe_title} 已添加订阅"
            + (f"（Bangumi总集数：{bangumi_total_episodes}）" if bangumi_total_episodes else "")
        )

    def __bulk_subscribe(self, apikey: str = "") -> schemas.Response:
        logger.info("[TodayBangumi] 收到批量订阅请求")

        if apikey != settings.API_TOKEN:
            logger.warn("[TodayBangumi] 批量订阅请求认证失败")
            return schemas.Response(success=False, message="API密钥错误")

        items: List[dict] = self.get_data("calendar_items") or []
        if not items:
            logger.warn("[TodayBangumi] 批量订阅失败：当前暂无可订阅条目")
            return schemas.Response(success=False, message="暂无可订阅条目，请先刷新每日放送数据")

        success_count = 0
        fail_details: List[str] = []

        for item in items:
            item_title = item.get("title") or item.get("title_origin") or "未知标题"
            try:
                success, message = self.__subscribe_item(
                    title=item.get("title") or "",
                    title_origin=item.get("title_origin") or "",
                    year=item.get("year") or "",
                    subject_id=item.get("subject_id"),
                    bangumi_total_episodes=item.get("bangumi_total_episodes"),
                    use_mp_notify=False,
                )
                if success:
                    success_count += 1
                else:
                    fail_details.append(f"{item_title}：{message}")
                    logger.warn(
                        f"[TodayBangumi] 批量订阅单条失败：title={item_title}，原因：{message}"
                    )
            except Exception as err:
                fail_details.append(f"{item_title}：{err}")
                logger.error(
                    f"[TodayBangumi] 批量订阅单条异常：title={item_title}，错误：{err}"
                )

        fail_count = len(fail_details)
        self.__post_summary_message(
            user_name="全部订阅",
            success_count=success_count,
            fail_details=fail_details,
        )
        result_message = f"全部订阅完成：成功 {success_count} 条，失败 {fail_count} 条"
        logger.info(f"[TodayBangumi] {result_message}")
        return schemas.Response(success=success_count > 0, message=result_message)

    def __manual_subscribe(
        self,
        apikey: str = "",
        title: str = "",
        title_origin: str = "",
        year: str = "",
        subject_id: Any = None,
    ) -> schemas.Response:
        logger.info(
            f"[TodayBangumi] 收到手动订阅请求：title={title}, title_origin={title_origin}, year={year}, subject_id={subject_id}"
        )

        if apikey != settings.API_TOKEN:
            logger.warn("[TodayBangumi] 手动订阅请求认证失败")
            return schemas.Response(success=False, message="API密钥错误")

        success, message = self.__subscribe_item(
            title=title,
            title_origin=title_origin,
            year=year,
            subject_id=subject_id,
            use_mp_notify=True,
        )
        return schemas.Response(success=success, message=message)
