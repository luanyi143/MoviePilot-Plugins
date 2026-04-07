import datetime
import re
import xml.dom.minidom
from threading import Event
from typing import Tuple, List, Dict, Any, Optional

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.download import DownloadChain
from app.chain.media import MediaChain
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.context import MediaInfo
from app.core.metainfo import MetaInfo
from app.helper.mediaserver import MediaServerHelper
from app.log import logger
from app.plugins import _PluginBase
from app.schemas import MediaType
from app.utils.dom import DomUtils
from app.utils.http import RequestUtils


class DoubanComingNotice(_PluginBase):
    # 插件名称
    plugin_name = "豆瓣将映魔改版"
    # 插件描述
    plugin_desc = "监控豆瓣即将开播电视剧，想看人数达到阈值自动添加订阅，并在开播前指定时间发送提醒。"
    # 插件图标
    plugin_icon = "https://github.com/luanyi143/MoviePilot-Plugins/blob/main/icons/Douban_A.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "luanyi143"
    # 作者主页
    author_url = "https://github.com/luanyi143"
    # 插件配置项ID前缀
    plugin_config_prefix = "doubancomingnotice_"
    # 加载顺序
    plugin_order = 6
    # 可使用的用户级别
    auth_level = 1

    # 退出事件
    _event = Event()
    _scheduler: Optional[BackgroundScheduler] = None

    # 配置项
    _enabled = False
    _onlyonce = False
    _cron = ""
    _rsshub = "https://rsshub.ddsrem.com"
    _sort_by = "hot"
    _count = 10
    _wish_count_threshold = 5000
    _advance_days = 7
    _notify_before_air = True
    _notify_hours = 24
    _proxy = False
    _clear = False
    _clearflag = False

    def init_plugin(self, config: dict = None):
        if config:
            self._enabled = bool(config.get("enabled"))
            self._onlyonce = bool(config.get("onlyonce"))
            self._cron = config.get("cron") or ""
            self._rsshub = (config.get("rsshub") or "https://rsshub.ddsrem.com").strip() or "https://rsshub.ddsrem.com"
            self._sort_by = self.__normalize_sort_by(config.get("sort_by"))
            self._count = self.__safe_positive_int(config.get("count"), 10)
            self._wish_count_threshold = self.__safe_positive_int(
                config.get("wish_count_threshold", config.get("hot_threshold")), 5000
            )
            self._advance_days = self.__safe_positive_int(config.get("advance_days"), 7)
            self._notify_before_air = bool(config.get("notify_before_air", True))
            self._notify_hours = self.__safe_positive_int(config.get("notify_hours"), 24)
            self._proxy = bool(config.get("proxy"))
            self._clear = bool(config.get("clear"))

        # 停止现有任务
        self.stop_service()

        # 启动服务
        if self._enabled or self._onlyonce:
            if self._onlyonce:
                self._scheduler = BackgroundScheduler(timezone=settings.TZ)
                logger.info("豆瓣即将开播订阅服务启动，立即运行一次")
                self._scheduler.add_job(
                    func=self.__refresh_rss,
                    trigger="date",
                    run_date=datetime.datetime.now(tz=pytz.timezone(settings.TZ))
                    + datetime.timedelta(seconds=3),
                )
                if self._scheduler.get_jobs():
                    self._scheduler.print_jobs()
                    self._scheduler.start()

            if self._onlyonce or self._clear:
                self._onlyonce = False
                self._clearflag = self._clear
                self._clear = False
                self.__update_config()

    def get_state(self) -> bool:
        return self._enabled

    @staticmethod
    def get_command() -> List[Dict[str, Any]]:
        return []

    def get_api(self) -> List[Dict[str, Any]]:
        return [
            {
                "path": "/delete_history",
                "endpoint": self.delete_history,
                "methods": ["GET"],
                "summary": "删除豆瓣即将开播订阅历史记录",
            }
        ]

    def get_service(self) -> List[Dict[str, Any]]:
        if self._enabled and self._cron:
            return [
                {
                    "id": "DoubanComingNotice",
                    "name": "豆瓣即将开播订阅服务",
                    "trigger": CronTrigger.from_crontab(self._cron),
                    "func": self.__refresh_rss,
                    "kwargs": {},
                }
            ]
        elif self._enabled:
            return [
                {
                    "id": "DoubanComingNotice",
                    "name": "豆瓣即将开播订阅服务",
                    "trigger": CronTrigger.from_crontab("0 8 * * *"),
                    "func": self.__refresh_rss,
                    "kwargs": {},
                }
            ]
        return []

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
                                        "props": {
                                            "model": "enabled",
                                            "label": "启用插件",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "notify_before_air",
                                            "label": "开播前提醒",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "onlyonce",
                                            "label": "立即运行一次",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "proxy",
                                            "label": "使用代理服务器",
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
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "rsshub",
                                            "label": "RSSHub地址",
                                            "placeholder": "https://rsshub.ddsrem.com",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VSelect",
                                        "props": {
                                            "model": "sort_by",
                                            "label": "排序方式",
                                            "items": [
                                                {"title": "热度", "value": "hot"},
                                                {"title": "时间", "value": "time"},
                                            ],
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 4},
                                "content": [
                                    {
                                        "component": "VCronField",
                                        "props": {
                                            "model": "cron",
                                            "label": "执行周期",
                                            "placeholder": "5位cron表达式，留空默认每天8点",
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
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "count",
                                            "label": "请求数量",
                                            "placeholder": "正整数，默认10",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "wish_count_threshold",
                                            "label": "热度筛选",
                                            "placeholder": "想看人数大于等于该值才处理，默认5000",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "advance_days",
                                            "label": "提前订阅天数",
                                            "placeholder": "距离开播小于等于该值才订阅，默认7",
                                        },
                                    }
                                ],
                            },
                            {
                                "component": "VCol",
                                "props": {"cols": 12, "md": 3},
                                "content": [
                                    {
                                        "component": "VTextField",
                                        "props": {
                                            "model": "notify_hours",
                                            "label": "开播提醒提前小时数",
                                            "placeholder": "默认24",
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
                                        "component": "VSwitch",
                                        "props": {
                                            "model": "clear",
                                            "label": "清理历史记录",
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
                                            "text": "仅处理想看人数达到阈值的电视剧；支持提前订阅，并在开播前指定时间发送一次提醒。本插件基于honue大佬的思路魔改。",
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
            "notify_before_air": True,
            "proxy": False,
            "onlyonce": False,
            "cron": "",
            "rsshub": "https://rsshub.ddsrem.com",
            "sort_by": "hot",
            "count": 10,
            "wish_count_threshold": 5000,
            "advance_days": 7,
            "notify_hours": 24,
            "clear": False,
        }

    def get_page(self) -> List[dict]:
        historys = self.get_data("history")
        if not historys:
            return [
                {
                    "component": "div",
                    "text": "暂无数据",
                    "props": {
                        "class": "text-center text-medium-emphasis py-8",
                    },
                }
            ]

        historys = sorted(historys, key=lambda x: x.get("time"), reverse=True)
        contents = []

        for history in historys:
            title = history.get("title") or "-"
            poster = history.get("poster")
            mtype = history.get("type") or "电视剧"
            time_str = history.get("time") or "-"
            doubanid = history.get("doubanid")
            wish_count = history.get("wish_count") or history.get("hot") or "-"
            air_date = history.get("air_date") or "-"
            notified = bool(history.get("air_notify_sent"))
            subscribed = bool(history.get("subscribed"))
            genres_list = [genre for genre in (history.get("genres") or []) if genre]
            genres = " / ".join(genres_list) or "-"
            douban_url = f"https://movie.douban.com/subject/{doubanid}" if doubanid else None

            genre_text = " / ".join(genres_list[:3]) if genres_list else genres
            if len(genres_list) > 3:
                genre_text = f"{genre_text} / +{len(genres_list) - 3}"

            status_text = f"{'已订阅' if subscribed else '未订阅'} / {'已提醒' if notified else '未提醒'}"

            title_content = [
                {
                    "component": "span",
                    "props": {
                        "class": "text-body-2 font-weight-bold text-high-emphasis line-clamp-2",
                    },
                    "text": title,
                }
            ]
            if douban_url:
                title_content = [
                    {
                        "component": "a",
                        "props": {
                            "href": douban_url,
                            "target": "_blank",
                            "class": "text-body-2 font-weight-bold text-high-emphasis text-decoration-none line-clamp-2",
                        },
                        "text": title,
                    }
                ]

            contents.append(
                {
                    "component": "VCard",
                    "props": {
                        "class": "pa-2 rounded-lg position-relative overflow-hidden h-100",
                        "variant": "elevated",
                        "elevation": 2,
                        "style": "height: 250px;",
                    },
                    "content": [
                        {
                            "component": "VDialogCloseBtn",
                            "props": {
                                "innerClass": "absolute top-0 right-0 mt-1 me-1 opacity-60",
                            },
                            "events": {
                                "click": {
                                    "api": "plugin/DoubanComingNotice/delete_history",
                                    "method": "get",
                                    "params": {
                                        "key": history.get("unique"),
                                        "apikey": settings.API_TOKEN,
                                    },
                                }
                            },
                        },
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex align-start ga-2 h-100",
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "props": {
                                        "class": "d-flex flex-column align-start justify-start flex-shrink-0",
                                        "style": "width: 96px;",
                                    },
                                    "content": [
                                        {
                                            "component": "VImg",
                                            "props": {
                                                "src": poster,
                                                "height": 144,
                                                "width": 96,
                                                "aspect-ratio": "2/3",
                                                "class": "rounded-lg shadow object-cover bg-grey-lighten-3",
                                                "cover": True,
                                            },
                                        }
                                    ],
                                },
                                {
                                    "component": "div",
                                    "props": {
                                        "class": "d-flex flex-column flex-grow-1 min-w-0 h-100 ps-1 pt-1 pe-1",
                                    },
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {
                                                "class": "mb-1 lh-sm",
                                            },
                                            "content": title_content,
                                        },
                                        {
                                            "component": "div",
                                            "props": {
                                                "class": "d-grid ga-1 text-caption",
                                            },
                                            "content": [
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "d-flex align-start",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "span",
                                                            "props": {
                                                                "class": "text-caption text-high-emphasis font-weight-medium me-1 flex-shrink-0",
                                                                "style": "width: 36px;",
                                                            },
                                                            "text": "类型",
                                                        },
                                                        {
                                                            "component": "div",
                                                            "props": {
                                                                "class": "text-caption text-medium-emphasis flex-grow-1 min-w-0",
                                                                "style": "line-height: 1.35;",
                                                            },
                                                            "text": mtype,
                                                        },
                                                    ],
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "d-flex align-start",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "span",
                                                            "props": {
                                                                "class": "text-caption text-high-emphasis font-weight-medium me-1 flex-shrink-0",
                                                                "style": "width: 36px;",
                                                            },
                                                            "text": "分类",
                                                        },
                                                        {
                                                            "component": "div",
                                                            "props": {
                                                                "class": "text-caption text-medium-emphasis flex-grow-1 min-w-0",
                                                                "style": "line-height: 1.35;",
                                                            },
                                                            "text": genre_text,
                                                        },
                                                    ],
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "d-flex align-start",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "span",
                                                            "props": {
                                                                "class": "text-caption text-high-emphasis font-weight-medium me-1 flex-shrink-0",
                                                                "style": "width: 36px;",
                                                            },
                                                            "text": "想看",
                                                        },
                                                        {
                                                            "component": "div",
                                                            "props": {
                                                                "class": "text-caption text-medium-emphasis flex-grow-1 min-w-0",
                                                            },
                                                            "text": str(wish_count),
                                                        },
                                                    ],
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "d-flex align-start",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "span",
                                                            "props": {
                                                                "class": "text-caption text-high-emphasis font-weight-medium me-1 flex-shrink-0",
                                                                "style": "width: 36px;",
                                                            },
                                                            "text": "开播",
                                                        },
                                                        {
                                                            "component": "div",
                                                            "props": {
                                                                "class": "text-caption text-medium-emphasis flex-grow-1 min-w-0",
                                                            },
                                                            "text": air_date,
                                                        },
                                                    ],
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "d-flex align-start",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "span",
                                                            "props": {
                                                                "class": "text-caption text-high-emphasis font-weight-medium me-1 flex-shrink-0",
                                                                "style": "width: 36px;",
                                                            },
                                                            "text": "处理",
                                                        },
                                                        {
                                                            "component": "div",
                                                            "props": {
                                                                "class": "text-caption text-medium-emphasis flex-grow-1 min-w-0",
                                                                "style": "line-height: 1.35;",
                                                            },
                                                            "text": time_str,
                                                        },
                                                    ],
                                                },
                                                {
                                                    "component": "div",
                                                    "props": {
                                                        "class": "d-flex align-start",
                                                    },
                                                    "content": [
                                                        {
                                                            "component": "span",
                                                            "props": {
                                                                "class": "text-caption text-high-emphasis font-weight-medium me-1 flex-shrink-0",
                                                                "style": "width: 36px;",
                                                            },
                                                            "text": "状态",
                                                        },
                                                        {
                                                            "component": "div",
                                                            "props": {
                                                                "class": "text-caption text-medium-emphasis flex-grow-1 min-w-0",
                                                                "style": "line-height: 1.35;",
                                                            },
                                                            "text": status_text,
                                                        },
                                                    ],
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                }
            )

        return [
            {
                "component": "div",
                "props": {
                    "class": "grid gap-2 grid-info-card",
                    "style": "grid-template-columns: repeat(4, minmax(0, 1fr));",
                },
                "content": contents,
            }
        ]

    def stop_service(self):
        try:
            if self._scheduler:
                self._scheduler.remove_all_jobs()
                if self._scheduler.running:
                    self._event.set()
                    self._scheduler.shutdown()
                    self._event.clear()
                self._scheduler = None
        except Exception as err:
            logger.error(f"停止豆瓣即将开播订阅服务失败：{err}")

    def delete_history(self, key: str, apikey: str):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")

        historys = self.get_data("history")
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")

        historys = [h for h in historys if h.get("unique") != key]
        self.save_data("history", historys)
        return schemas.Response(success=True, message="删除成功")

    def __update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "notify_before_air": self._notify_before_air,
                "notify_hours": self._notify_hours,
                "proxy": self._proxy,
                "onlyonce": self._onlyonce,
                "cron": self._cron,
                "rsshub": self._rsshub,
                "sort_by": self._sort_by,
                "count": self._count,
                "wish_count_threshold": self._wish_count_threshold,
                "advance_days": self._advance_days,
                "clear": self._clear,
            }
        )

    @staticmethod
    def __normalize_sort_by(value: Any) -> str:
        if str(value).lower() in {"hot", "time"}:
            return str(value).lower()
        return "hot"

    @staticmethod
    def __safe_positive_int(value: Any, default: int) -> int:
        try:
            ivalue = int(value)
            if ivalue > 0:
                return ivalue
        except Exception:
            pass
        return default

    def __build_rss_url(self) -> str:
        base = (self._rsshub or "https://rsshub.ddsrem.com").rstrip("/")
        return f"{base}/douban/tv/coming/{self._sort_by}/{self._count}"

    @staticmethod
    def __extract_wish_count(text: str) -> int:
        if not text:
            return 0

        clean_text = text.replace(",", "").replace("，", "")
        patterns = [
            r"(?:想看人数|想看)\s*[:：]?\s*(\d+)",
            r"(\d+)\s*人想看",
        ]
        for pattern in patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except Exception:
                    continue
        return 0

    @staticmethod
    def __extract_air_date(text: str) -> Optional[datetime.date]:
        if not text:
            return None

        patterns = [
            r"(20\d{2})[-/年.](\d{1,2})[-/月.](\d{1,2})",
            r"(19\d{2})[-/年.](\d{1,2})[-/月.](\d{1,2})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if not match:
                continue
            try:
                year = int(match.group(1))
                month = int(match.group(2))
                day = int(match.group(3))
                return datetime.date(year, month, day)
            except Exception:
                continue
        return None

    @staticmethod
    def __extract_year(text: str) -> Optional[str]:
        if not text:
            return None
        match = re.search(r"\b(19\d{2}|20\d{2})\b", text)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def __chinese_to_int(text: str) -> Optional[int]:
        if not text:
            return None

        mapping = {
            "一": 1,
            "二": 2,
            "三": 3,
            "四": 4,
            "五": 5,
            "六": 6,
            "七": 7,
            "八": 8,
            "九": 9,
            "十": 10,
        }

        if text.isdigit():
            value = int(text)
            return value if value > 0 else None

        if text in mapping:
            return mapping[text]

        if text.startswith("十"):
            tail = text[1:]
            return 10 + (mapping.get(tail, 0) if tail else 0)

        if "十" in text:
            parts = text.split("十", 1)
            head = mapping.get(parts[0], 0) if parts[0] else 1
            tail = mapping.get(parts[1], 0) if parts[1] else 0
            value = head * 10 + tail
            return value if value > 0 else None

        return None

    def __extract_season_from_title(self, title: str) -> Optional[int]:
        if not title:
            return None

        patterns = [
            r"[第\s]*([0-9]{1,2})\s*季",
            r"第([一二三四五六七八九十]{1,3})季",
            r"\bS\s*([0-9]{1,2})\b",
            r"\bSeason\s*([0-9]{1,2})\b",
        ]

        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if not match:
                continue
            value = self.__chinese_to_int(match.group(1))
            if value and value > 0:
                return value

        return None

    def __fetch_tmdb_air_date(
        self,
        mediainfo: MediaInfo,
        current_season: Optional[int] = None,
    ) -> Optional[str]:
        if not mediainfo or not mediainfo.tmdb_id or not mediainfo.type:
            return None

        if mediainfo.type == MediaType.TV and current_season and current_season > 0:
            try:
                season_info = self.chain.tmdb_info(
                    tmdbid=mediainfo.tmdb_id,
                    mtype=mediainfo.type,
                    season=current_season,
                )
                season_air_date = season_info.get("air_date") if season_info else None
                parsed_season_air_date = self.__extract_air_date(str(season_air_date)) if season_air_date else None
                if parsed_season_air_date:
                    result = parsed_season_air_date.strftime("%Y-%m-%d")
                    logger.info(f"TMDB 当前季开播日期解析成功：tmdb={mediainfo.tmdb_id} S{current_season} -> {result}")
                    return result
            except Exception as err:
                logger.warning(f"获取 TMDB 当前季开播日期失败：tmdb={mediainfo.tmdb_id} S{current_season}，错误：{err}")

        try:
            tmdb_info = self.chain.tmdb_info(
                tmdbid=mediainfo.tmdb_id,
                mtype=mediainfo.type,
            )
            air_date = None
            if tmdb_info:
                air_date = tmdb_info.get("first_air_date") or tmdb_info.get("air_date") or tmdb_info.get("release_date")
            parsed_air_date = self.__extract_air_date(str(air_date)) if air_date else None
            if parsed_air_date:
                result = parsed_air_date.strftime("%Y-%m-%d")
                logger.info(f"TMDB 剧级开播日期解析成功：tmdb={mediainfo.tmdb_id} -> {result}")
                return result
        except Exception as err:
            logger.warning(f"获取 TMDB 剧级开播日期失败：tmdb={mediainfo.tmdb_id}，错误：{err}")

        return None

    def __get_rss_info(self, addr: str) -> List[dict]:
        try:
            if self._proxy:
                ret = RequestUtils(proxies=settings.PROXY).get_res(addr)
            else:
                ret = RequestUtils().get_res(addr)

            if not ret:
                return []

            ret_xml = ret.text
            ret_array: List[dict] = []

            dom_tree = xml.dom.minidom.parseString(ret_xml)
            root_node = dom_tree.documentElement
            items = root_node.getElementsByTagName("item")

            for item in items:
                try:
                    rss_info: Dict[str, Any] = {}

                    title = DomUtils.tag_value(item, "title", default="")
                    link = DomUtils.tag_value(item, "link", default="")
                    description = DomUtils.tag_value(item, "description", default="")

                    if not title and not link:
                        logger.warning("条目标题和链接均为空，无法处理")
                        continue

                    rss_info["title"] = title.strip()
                    rss_info["link"] = link.strip()
                    rss_info["description"] = description or ""

                    doubanid = re.findall(r"/(\d+)(?=/|$)", link)
                    if doubanid:
                        doubanid = doubanid[0]
                    else:
                        doubanid = None

                    if doubanid and not str(doubanid).isdigit():
                        logger.warning(f"解析的豆瓣ID格式不正确：{doubanid}")
                        continue

                    rss_info["doubanid"] = doubanid
                    rss_info["wish_count"] = self.__extract_wish_count(description)
                    rss_info["year"] = self.__extract_year(description)

                    ret_array.append(rss_info)
                except Exception as item_err:
                    logger.error(f"解析RSS条目失败：{item_err}")
                    continue

            return ret_array
        except Exception as err:
            logger.error(f"获取RSS失败：{err}")
            return []

    @staticmethod
    def __days_until_air(air_date_str: Optional[str]) -> Optional[int]:
        if not air_date_str:
            return None
        try:
            air_date = datetime.datetime.strptime(air_date_str, "%Y-%m-%d").date()
            today = datetime.datetime.now().date()
            return (air_date - today).days
        except Exception:
            return None

    def __hours_until_air(self, air_date_str: Optional[str]) -> Optional[float]:
        if not air_date_str:
            return None
        try:
            tz = pytz.timezone(settings.TZ)
            air_date = datetime.datetime.strptime(air_date_str, "%Y-%m-%d").date()
            air_datetime = tz.localize(datetime.datetime.combine(air_date, datetime.time.min))
            now = datetime.datetime.now(tz)
            return (air_datetime - now).total_seconds() / 3600
        except Exception:
            return None

    @staticmethod
    def __get_media_genres(mediainfo: MediaInfo) -> List[str]:
        genre_values: List[str] = []

        for attr in ["genres", "genre", "category", "categories"]:
            value = getattr(mediainfo, attr, None)
            if not value:
                continue
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        name = item.get("name")
                        if name:
                            genre_values.append(str(name).strip())
                    else:
                        genre_values.append(str(item).strip())
            elif isinstance(value, str):
                genre_values.extend([part.strip() for part in re.split(r"[/,，|]+", value) if part.strip()])

        deduped = []
        for item in genre_values:
            if item and item not in deduped:
                deduped.append(item)
        return deduped

    @staticmethod
    def __get_current_season(meta: MetaInfo, mediainfo: MediaInfo, title_season: Optional[int] = None) -> int:
        for value in [title_season, getattr(meta, "begin_season", None), getattr(mediainfo, "season", None)]:
            try:
                season = int(value)
                if season > 0:
                    return season
            except Exception:
                continue
        return 1

    def __get_active_media_service(self, server: str = None, server_type: str = None):
        services = MediaServerHelper().get_services(type_filter=server_type, name_filters=[server] if server else None)
        if not services:
            return None
        if server:
            return services.get(server)
        return next(iter(services.values()), None)

    def __get_media_server_iteminfo(self, server: str, server_type: str, itemid: str) -> dict:
        service = self.__get_active_media_service(server=server, server_type=server_type)
        if not service:
            logger.warning(f"未找到媒体服务器实例：server={server} type={server_type}")
            return {}

        try:
            if server_type == "emby":
                url = f"[HOST]emby/Users/[USER]/Items/{itemid}?Fields=ProviderIds,Path,RecursiveItemCount,ChildCount&api_key=[APIKEY]"
                res = service.instance.get_data(url=url)
                return res.json() if res else {}
            if server_type == "jellyfin":
                url = f"[HOST]Users/[USER]/Items/{itemid}?Fields=ProviderIds,Path,RecursiveItemCount,ChildCount&api_key=[APIKEY]"
                res = service.instance.get_data(url=url)
                return res.json() if res else {}
            plex = service.instance.get_plex()
            plexitem = plex.library.fetchItem(ekey=itemid)
            iteminfo = {
                "Id": plexitem.key,
                "Name": plexitem.title,
                "Type": "Series" if "show" in getattr(plexitem, "TYPE", "") else "Movie",
            }
            return iteminfo
        except Exception as err:
            logger.warning(f"获取媒体库条目详情失败：server={server} itemid={itemid} err={err}")
            return {}

    def __get_media_server_items(self, server: str, server_type: str, parentid: str, mtype: str = None) -> dict:
        service = self.__get_active_media_service(server=server, server_type=server_type)
        if not service:
            logger.warning(f"未找到媒体服务器实例：server={server} type={server_type}")
            return {}

        try:
            if server_type == "emby":
                url = f"[HOST]emby/Users/[USER]/Items?ParentId={parentid}&api_key=[APIKEY]"
                res = service.instance.get_data(url=url)
                data = res.json() if res else {}
            elif server_type == "jellyfin":
                url = f"[HOST]Users/[USER]/Items?ParentId={parentid}&api_key=[APIKEY]"
                res = service.instance.get_data(url=url)
                data = res.json() if res else {}
            else:
                plex = service.instance.get_plex()
                plexitem = plex.library.fetchItem(ekey=parentid)
                data = {"Items": []}
                if mtype and "Season" in mtype:
                    for season in plexitem.seasons():
                        data["Items"].append(
                            {
                                "Name": season.title,
                                "Id": season.key,
                                "IndexNumber": season.seasonNumber,
                                "Type": "Season",
                            }
                        )
                elif mtype and "Episode" in mtype:
                    for episode in plexitem.episodes():
                        data["Items"].append(
                            {
                                "Name": episode.title,
                                "Id": episode.key,
                                "IndexNumber": episode.episodeNumber,
                                "Type": "Episode",
                            }
                        )
                return data

            items = data.get("Items") or []
            if mtype:
                filtered = []
                for item in items:
                    item_type = str(item.get("Type") or "")
                    if item_type.lower() == mtype.lower():
                        filtered.append(item)
                data["Items"] = filtered
            return data
        except Exception as err:
            logger.warning(f"获取媒体库子项失败：server={server} parentid={parentid} mtype={mtype} err={err}")
            return {}

    def __collect_library_season_episode_map(self, mediainfo: MediaInfo) -> Dict[int, set]:
        season_episode_map: Dict[int, set] = {}

        try:
            existsinfo = self.chain.media_exists(mediainfo=mediainfo)
        except Exception as err:
            logger.warning(f"查询媒体库存在状态失败：{mediainfo.title_year} err={err}")
            return season_episode_map

        if not existsinfo or not getattr(existsinfo, "itemid", None):
            logger.info(f"{mediainfo.title_year} 在媒体库中不存在，无法获取季集信息")
            return season_episode_map

        server = getattr(existsinfo, "server", None)
        server_type = getattr(existsinfo, "server_type", None)
        itemid = getattr(existsinfo, "itemid", None)
        if not server or not server_type or not itemid:
            logger.warning(f"{mediainfo.title_year} 媒体库定位信息不完整：server={server} type={server_type} itemid={itemid}")
            return season_episode_map

        iteminfo = self.__get_media_server_iteminfo(server=server, server_type=server_type, itemid=itemid)
        item_type = str(iteminfo.get("Type") or "")
        if item_type and "Series" not in item_type and "show" not in item_type.lower():
            logger.info(f"{mediainfo.title_year} 媒体库条目不是剧集类型：{item_type}")
            return season_episode_map

        seasons = self.__get_media_server_items(server=server, server_type=server_type, parentid=itemid, mtype="Season")
        season_items = seasons.get("Items") or []
        for season in season_items:
            season_no = season.get("IndexNumber")
            try:
                season_no = int(season_no)
            except Exception:
                continue
            if season_no <= 0:
                continue

            season_episode_map.setdefault(season_no, set())
            season_id = season.get("Id")
            if not season_id:
                continue

            episodes = self.__get_media_server_items(
                server=server,
                server_type=server_type,
                parentid=season_id,
                mtype="Episode",
            )
            for episode in episodes.get("Items") or []:
                episode_no = episode.get("IndexNumber")
                try:
                    episode_no = int(episode_no)
                except Exception:
                    continue
                if episode_no > 0:
                    season_episode_map[season_no].add(episode_no)

        logger.info(
            f"{mediainfo.title_year} 媒体库季集信息："
            f"{ {season: sorted(list(episodes)) for season, episodes in season_episode_map.items()} }"
        )
        return season_episode_map

    def __get_tmdb_previous_season_map(self, mediainfo: MediaInfo, current_season: int) -> Dict[int, set]:
        tmdb_season_map: Dict[int, set] = {}

        for season in range(1, current_season):
            try:
                season_info = self.chain.tmdb_info(
                    tmdbid=mediainfo.tmdb_id,
                    mtype=mediainfo.type,
                    season=season,
                ) or {}
                episodes = season_info.get("episodes") or []
                episode_numbers = set()
                for episode in episodes:
                    episode_number = episode.get("episode_number") or episode.get("EpisodeNumber") or episode.get("IndexNumber")
                    try:
                        episode_number = int(episode_number)
                    except Exception:
                        continue
                    if episode_number > 0:
                        episode_numbers.add(episode_number)
                tmdb_season_map[season] = episode_numbers
            except Exception as err:
                logger.warning(f"获取 TMDB 季信息失败：tmdb={mediainfo.tmdb_id} season={season} err={err}")
                tmdb_season_map[season] = set()

        logger.info(
            f"{mediainfo.title_year} TMDB 往季信息："
            f"{ {season: sorted(list(episodes)) for season, episodes in tmdb_season_map.items()} }"
        )
        return tmdb_season_map

    def __get_previous_season_status(
        self,
        mediainfo: MediaInfo,
        current_season: int,
    ) -> Optional[str]:
        if current_season <= 1:
            return None

        library_map = self.__collect_library_season_episode_map(mediainfo=mediainfo)
        tmdb_map = self.__get_tmdb_previous_season_map(mediainfo=mediainfo, current_season=current_season)

        if not tmdb_map:
            return "未知"

        missing_parts: List[str] = []
        incomplete_parts: List[str] = []

        for season in range(1, current_season):
            tmdb_episodes = tmdb_map.get(season) or set()
            library_episodes = library_map.get(season) or set()

            if not library_episodes:
                missing_parts.append(f"第{season}季")
                continue

            if tmdb_episodes:
                missing_episodes = sorted(list(tmdb_episodes - library_episodes))
                if missing_episodes:
                    preview = "、".join([f"E{episode:02d}" for episode in missing_episodes[:5]])
                    suffix = "..." if len(missing_episodes) > 5 else ""
                    incomplete_parts.append(f"第{season}季缺{preview}{suffix}")

        if not missing_parts and not incomplete_parts:
            return "已入库"

        parts: List[str] = []
        if missing_parts:
            parts.append("缺少" + "、".join(missing_parts))
        if incomplete_parts:
            parts.append("；".join(incomplete_parts))
        return "；".join(parts)

    def __build_notify_unique_key(self, douban_id: Optional[str], title: str, air_date: Optional[str]) -> str:
        return f"air_notify:{douban_id or title}:{air_date or 'unknown'}"

    def __has_sent_air_notify(self, unique_key: str) -> bool:
        notify_history = self.get_data("notify_history") or []
        return any(item.get("unique") == unique_key for item in notify_history)

    def __save_air_notify_history(self, item: dict):
        notify_history = self.get_data("notify_history") or []
        notify_history.append(item)
        self.save_data("notify_history", notify_history)

    def __send_air_notify_message(
        self,
        title: str,
        mediainfo: MediaInfo,
        douban_id: Optional[str],
        wish_count: int,
        air_date: Optional[str],
        subscribed: bool,
        previous_season_status: Optional[str],
        poster: Optional[str],
    ):
        if not self._notify_before_air:
            return

        douban_link = f"https://movie.douban.com/subject/{douban_id}" if douban_id else "-"
        media_type = mediainfo.type.value if getattr(mediainfo, "type", None) else "电视剧"
        subscribed_text = "已订阅" if subscribed else "未订阅"

        lines = [
            f"🎬 名称：{title}",
            f"📂 类型：{media_type}",
            f"⏰ 开播时间：{air_date or '-'}",
            f"👀 想看人数：{wish_count}",
            f"🔔 订阅状态：{subscribed_text}",
        ]

        if previous_season_status:
            lines.append(f"📦 往季状态：{previous_season_status}")

        lines.extend(
            [
                f"📅 年份：{mediainfo.year or '-'}",
                f"🆔 TMDB ID：{mediainfo.tmdb_id or '-'}",
                f"🔗 豆瓣链接：{douban_link}",
            ]
        )

        text = "\n".join(lines)

        try:
            self.post_message(
                title="**📺 豆瓣开播提醒 ✨**",
                text=text,
                image=poster,
            )
        except Exception as err:
            logger.error(f"发送开播提醒失败：{err}")

    def __refresh_rss(self):
        logger.info("开始刷新豆瓣即将开播订阅 ...")
        rss_url = self.__build_rss_url()
        logger.info(f"请求地址：{rss_url}")

        if self._clearflag:
            history: List[dict] = []
            self.save_data("history", history)
            self.save_data("notify_history", [])
        else:
            history = self.get_data("history") or []

        rss_infos = self.__get_rss_info(rss_url)
        if not rss_infos:
            logger.warning(f"未从 RSS 获取到数据：{rss_url}")
            self._clearflag = False
            return

        logger.info(f"共获取到 {len(rss_infos)} 条 RSS 数据")
        added_count = 0

        for rss_info in rss_infos:
            try:
                if self._event.is_set():
                    logger.info("订阅服务停止")
                    return

                title = rss_info.get("title")
                douban_id = rss_info.get("doubanid")
                description = rss_info.get("description") or ""
                wish_count = self.__safe_positive_int(rss_info.get("wish_count"), 0)
                year = rss_info.get("year")

                if not title:
                    logger.warning("RSS 条目标题为空，跳过")
                    continue

                title_season = self.__extract_season_from_title(title)

                if title_season:
                    logger.info(f"{title} 从标题中解析到季号：S{title_season}")

                if wish_count < self._wish_count_threshold:
                    logger.info(f"{title} 想看人数 {wish_count} 低于阈值 {self._wish_count_threshold}，跳过")
                    continue

                meta = MetaInfo(title)
                if year:
                    meta.year = year
                meta.type = MediaType.TV

                mediainfo: Optional[MediaInfo] = None
                if douban_id:
                    if settings.RECOGNIZE_SOURCE == "themoviedb":
                        tmdbinfo = MediaChain().get_tmdbinfo_by_doubanid(doubanid=douban_id, mtype=meta.type)
                        if not tmdbinfo:
                            logger.warning(f"未能通过豆瓣ID {douban_id} 获取 TMDB 信息，标题：{title}")
                            continue
                        meta.type = tmdbinfo.get("media_type")
                        mediainfo = self.chain.recognize_media(meta=meta, tmdbid=tmdbinfo.get("id"))
                        if not mediainfo:
                            logger.warning(f"TMDBID {tmdbinfo.get('id')} 未识别到媒体信息")
                            continue
                    else:
                        mediainfo = self.chain.recognize_media(meta=meta, doubanid=douban_id)
                        if not mediainfo:
                            logger.warning(f"豆瓣ID {douban_id} 未识别到媒体信息")
                            continue
                else:
                    mediainfo = self.chain.recognize_media(meta=meta)
                    if not mediainfo:
                        logger.warning(f"未识别到媒体信息，标题：{title}")
                        continue

                if mediainfo.type != MediaType.TV:
                    logger.info(f"{mediainfo.title_year} 不是电视剧类型，跳过")
                    continue

                genres = self.__get_media_genres(mediainfo)
                current_season = self.__get_current_season(meta, mediainfo, title_season=title_season)

                if title_season and getattr(meta, "begin_season", None) != title_season:
                    meta.begin_season = title_season

                logger.info(f"{title} 开播日期直接使用 TMDB 获取，使用季号：S{current_season}")
                air_date = self.__fetch_tmdb_air_date(mediainfo=mediainfo, current_season=current_season)

                if not air_date:
                    logger.warning(f"{title} 未从 TMDB 获取到开播日期，后续将跳过依赖开播日期的逻辑")
                elif not year:
                    year = air_date[:4]
                    meta.year = year

                exist_flag, _ = DownloadChain().get_no_exists_info(meta=meta, mediainfo=mediainfo)
                in_library = bool(exist_flag)

                previous_season_status = None
                try:
                    previous_season_status = self.__get_previous_season_status(
                        mediainfo=mediainfo,
                        current_season=current_season,
                    )
                except Exception as err:
                    logger.warning(f"{title} 往季状态判断失败，不影响开播提醒继续发送：{err}")

                subscribe_chain = SubscribeChain()
                subscribed = bool(subscribe_chain.exists(mediainfo=mediainfo, meta=meta))

                unique_flag = f"doubancomingnotice: {title} (DB:{douban_id})"
                history_item = next((h for h in history if h.get("unique") == unique_flag), None)

                # 提前订阅逻辑：每次刷新都重新判断，避免首次出现过早时后续永远不订阅
                newly_subscribed = False
                days_until_air = self.__days_until_air(air_date)
                if days_until_air is None:
                    logger.info(f"{title} 未解析到开播日期，跳过提前订阅判断")
                elif days_until_air < 0:
                    logger.info(f"{title} 已开播 {abs(days_until_air)} 天，跳过订阅")
                elif days_until_air > self._advance_days:
                    logger.info(f"{title} 距开播还有 {days_until_air} 天，超过提前订阅阈值 {self._advance_days} 天")
                elif in_library:
                    logger.info(f"{mediainfo.title_year} 媒体库中已存在")
                elif subscribed:
                    logger.info(f"{mediainfo.title_year} 订阅已存在")
                else:
                    subscribe_chain.add(
                        title=mediainfo.title,
                        year=mediainfo.year,
                        mtype=mediainfo.type,
                        tmdbid=mediainfo.tmdb_id,
                        season=meta.begin_season,
                        exist_ok=True,
                        username="豆瓣即将开播",
                    )
                    subscribed = True
                    newly_subscribed = True
                    logger.info(f"已添加订阅：{mediainfo.title_year}")

                now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                if not history_item:
                    history_item = {
                        "title": title,
                        "type": mediainfo.type.value,
                        "year": mediainfo.year,
                        "poster": mediainfo.get_poster_image(),
                        "overview": mediainfo.overview or description,
                        "tmdbid": mediainfo.tmdb_id,
                        "doubanid": douban_id,
                        "wish_count": wish_count,
                        "air_date": air_date,
                        "genres": genres,
                        "subscribed": subscribed,
                        "air_notify_sent": False,
                        "time": now_str,
                        "unique": unique_flag,
                    }
                    history.append(history_item)
                else:
                    history_item.update(
                        {
                            "title": title,
                            "type": mediainfo.type.value,
                            "year": mediainfo.year,
                            "poster": mediainfo.get_poster_image(),
                            "overview": mediainfo.overview or description,
                            "tmdbid": mediainfo.tmdb_id,
                            "doubanid": douban_id,
                            "wish_count": wish_count,
                            "air_date": air_date or history_item.get("air_date"),
                            "genres": genres,
                            "subscribed": subscribed,
                            "time": now_str,
                        }
                    )

                if newly_subscribed:
                    added_count += 1

                # 开播前提醒逻辑
                if self._notify_before_air:
                    hours_until_air = self.__hours_until_air(air_date)
                    if hours_until_air is None:
                        logger.info(f"{title} 未解析到开播日期，跳过开播提醒")
                        continue

                    if hours_until_air < 0:
                        logger.info(f"{title} 已开播，跳过开播提醒")
                        continue

                    if hours_until_air > self._notify_hours:
                        logger.info(f"{title} 距开播还有 {hours_until_air:.2f} 小时，未进入提醒窗口")
                        continue

                    notify_unique = self.__build_notify_unique_key(douban_id, title, air_date)
                    if self.__has_sent_air_notify(notify_unique):
                        logger.info(f"{title} 开播提醒已发送过，跳过")
                        continue

                    self.__send_air_notify_message(
                        title=title,
                        mediainfo=mediainfo,
                        douban_id=douban_id,
                        wish_count=wish_count,
                        air_date=air_date,
                        subscribed=subscribed,
                        previous_season_status=previous_season_status,
                        poster=history_item.get("poster") or mediainfo.get_poster_image(),
                    )

                    self.__save_air_notify_history(
                        {
                            "unique": notify_unique,
                            "title": title,
                            "doubanid": douban_id,
                            "tmdbid": mediainfo.tmdb_id,
                            "type": mediainfo.type.value,
                            "genres": genres,
                            "wish_count": wish_count,
                            "air_date": air_date,
                            "subscribed": subscribed,
                            "in_library": in_library,
                            "notified_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        }
                    )

                    history_item["air_notify_sent"] = True
                    history_item["subscribed"] = subscribed

                    logger.info(f"{title} 已发送开播提醒")
            except Exception as err:
                logger.error(f"处理条目失败：{err}")

        self.save_data("history", history)
        self._clearflag = False
        logger.info(f"豆瓣即将开播订阅刷新完成，本次新增 {added_count} 条")
