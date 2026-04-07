import datetime
import importlib
import re
import xml.dom.minidom
from threading import Event
from typing import Any, Dict, List, Optional, Tuple

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app import schemas
from app.chain.subscribe import SubscribeChain
from app.core.config import settings
from app.core.metainfo import MetaInfo
from app.log import logger
from app.plugins import _PluginBase
from app.utils.http import RequestUtils

try:
    from app.schemas.types import MediaType
except Exception:
    from app.schemas import MediaType


class DoubanComingNotice(_PluginBase):
    # 插件名称
    plugin_name = "豆瓣将映魔改版"
    # 插件描述
    plugin_desc = "监控豆瓣即将开播电视剧，想看人数达到阈值自动添加订阅，并在开播前指定时间发送提醒。"
    # 插件图标
    plugin_icon = "Douban_A.png"
    # 插件版本
    plugin_version = "1.0"
    # 插件作者
    plugin_author = "luanyi143"
    # 作者主页
    author_url = "https://github.com/luanyi143"
    # 插件配置项 ID 前缀
    plugin_config_prefix = "doubancomingnotice_"
    # 加载顺序
    plugin_order = 6
    # 可使用的用户级别
    auth_level = 2

    _event = Event()
    _scheduler: Optional[BackgroundScheduler] = None
    subscribechain: Optional[SubscribeChain] = None

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

    def init_plugin(self, config: dict = None):
        self.stop_service()

        config = config or {}
        self._enabled = bool(config.get("enabled", False))
        self._onlyonce = bool(config.get("onlyonce", False))
        self._cron = str(config.get("cron") or "").strip()
        self._rsshub = (config.get("rsshub") or "https://rsshub.ddsrem.com").strip() or "https://rsshub.ddsrem.com"
        self._sort_by = self.__normalize_sort_by(config.get("sort_by"))
        self._count = self.__safe_positive_int(config.get("count"), 10)
        self._wish_count_threshold = self.__safe_positive_int(
            config.get("wish_count_threshold", config.get("hot_threshold")),
            5000,
        )
        self._advance_days = self.__safe_positive_int(config.get("advance_days", config.get("advance", "7day")), 7)
        self._notify_before_air = bool(config.get("notify_before_air", True))
        self._notify_hours = self.__safe_positive_int(config.get("notify_hours"), 24)
        self._proxy = bool(config.get("proxy", False))
        self._clear = bool(config.get("clear", False))

        try:
            self.subscribechain = SubscribeChain()
        except Exception as err:
            self.subscribechain = None
            logger.warning(f"豆瓣即将开播订阅：初始化订阅链失败，不影响插件加载，错误：{err}")

        if self._clear:
            self.save_data("history", [])
            self.save_data("notify_history", [])
            self._clear = False
            logger.info("豆瓣即将开播订阅：历史记录已清理")
            self.__update_config()

        if self._onlyonce:
            self.__schedule_once()
            self._onlyonce = False
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
        if not self._enabled:
            return []

        cron_text = self._cron or "0 8 * * *"
        try:
            trigger = CronTrigger.from_crontab(cron_text)
        except Exception:
            logger.warning(f"豆瓣即将开播订阅：Cron 表达式无效 {cron_text}，已回退到默认值 0 8 * * *")
            trigger = CronTrigger.from_crontab("0 8 * * *")

        return [
            {
                "id": "DoubanComingNotice",
                "name": "豆瓣即将开播订阅服务",
                "trigger": trigger,
                "func": self.__refresh_rss,
                "kwargs": {},
            }
        ]

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
                                        "component": "VTextField",
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
                                            "placeholder": "支持 7 或 7day，默认7",
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
                                            "text": "插件通过 RSSHub 的 /douban/tv/coming/:sortBy?/:count? 路由获取豆瓣将映电视剧数据。达到热度阈值且距离开播时间不超过设定天数时自动添加订阅；进入提醒窗口后只发送一次开播提醒。",
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
        historys = self.get_data("history") or []
        if not historys:
            return [
                {
                    "component": "VAlert",
                    "props": {
                        "type": "info",
                        "variant": "tonal",
                        "text": "暂无历史数据，请先启用插件并执行一次刷新。",
                    },
                }
            ]

        historys = sorted(historys, key=lambda x: x.get("time") or "", reverse=True)
        updated_at = historys[0].get("time") or "-"

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
                            "text": f"当前共有 {len(historys)} 条历史记录，最近处理时间：{updated_at}",
                        },
                    }
                ],
            },
            {
                "component": "div",
                "props": {"class": "grid gap-3 grid-info-card"},
                "content": [self.__build_history_card(history) for history in historys],
            },
        ]

    def stop_service(self):
        try:
            self._event.set()
            if self._scheduler:
                try:
                    self._scheduler.remove_all_jobs()
                except Exception:
                    pass
                try:
                    self._scheduler.shutdown(wait=False)
                except TypeError:
                    self._scheduler.shutdown()
                except Exception:
                    pass
                self._scheduler = None
            self._event.clear()
        except Exception as err:
            logger.error(f"停止豆瓣即将开播订阅服务失败：{err}")

    def delete_history(self, key: str = "", apikey: str = ""):
        if apikey != settings.API_TOKEN:
            return schemas.Response(success=False, message="API密钥错误")

        historys = self.get_data("history") or []
        if not historys:
            return schemas.Response(success=False, message="未找到历史记录")

        historys = [item for item in historys if item.get("unique") != key]
        self.save_data("history", historys)
        return schemas.Response(success=True, message="删除成功")

    def __schedule_once(self):
        try:
            self._scheduler = BackgroundScheduler(timezone=self.__get_timezone())
            self._scheduler.add_job(
                func=self.__refresh_rss,
                trigger="date",
                run_date=self.__now() + datetime.timedelta(seconds=3),
                name="豆瓣即将开播订阅立即执行",
            )
            self._scheduler.start()
            logger.info("豆瓣即将开播订阅：已注册一次性执行任务")
        except Exception as err:
            logger.error(f"豆瓣即将开播订阅：注册一次性执行任务失败：{err}")

    def __update_config(self):
        self.update_config(
            {
                "enabled": self._enabled,
                "notify_before_air": self._notify_before_air,
                "notify_hours": self._notify_hours,
                "proxy": self._proxy,
                "onlyonce": False,
                "cron": self._cron,
                "rsshub": self._rsshub,
                "sort_by": self._sort_by,
                "count": self._count,
                "wish_count_threshold": self._wish_count_threshold,
                "advance_days": self._advance_days,
                "clear": False,
            }
        )

    @staticmethod
    def __normalize_sort_by(value: Any) -> str:
        text = str(value or "").strip().lower()
        return text if text in {"hot", "time"} else "hot"

    @staticmethod
    def __safe_positive_int(value: Any, default: int) -> int:
        try:
            ivalue = int(value)
            if ivalue > 0:
                return ivalue
        except Exception:
            pass

        text = str(value or "").strip().lower()
        match = re.search(r"(\d+)", text)
        if match:
            try:
                ivalue = int(match.group(1))
                if ivalue > 0:
                    return ivalue
            except Exception:
                pass
        return default

    @staticmethod
    def __normalize_url(url: Any) -> str:
        raw_url = str(url or "").strip()
        if not raw_url:
            return ""
        if raw_url.startswith("//"):
            return f"https:{raw_url}"
        if raw_url.startswith("http://") or raw_url.startswith("https://"):
            return raw_url
        return ""

    def __build_rss_url(self) -> str:
        base = (self._rsshub or "https://rsshub.ddsrem.com").strip().rstrip("/")
        return f"{base}/douban/tv/coming/{self._sort_by}/{self._count}"

    @staticmethod
    def __get_timezone():
        tz_name = getattr(settings, "TZ", "Asia/Shanghai") or "Asia/Shanghai"
        try:
            return pytz.timezone(tz_name)
        except Exception:
            return pytz.timezone("Asia/Shanghai")

    def __now(self) -> datetime.datetime:
        return datetime.datetime.now(self.__get_timezone())

    @staticmethod
    def __tag_value(node, tag_name: str, default: str = "") -> str:
        try:
            elements = node.getElementsByTagName(tag_name)
            if not elements:
                return default
            target = elements[0]
            values = []
            for child in target.childNodes:
                if getattr(child, "nodeValue", None):
                    values.append(child.nodeValue)
            return "".join(values).strip() if values else default
        except Exception:
            return default

    @staticmethod
    def __strip_html(text: Any) -> str:
        raw = str(text or "")
        if not raw:
            return ""
        raw = re.sub(r"<br\s*/?>", "\n", raw, flags=re.IGNORECASE)
        raw = re.sub(r"</p\s*>", "\n", raw, flags=re.IGNORECASE)
        raw = re.sub(r"<[^>]+>", " ", raw)
        raw = raw.replace("&nbsp;", " ").replace("&#160;", " ").replace("&", "&")
        raw = re.sub(r"\s+", " ", raw).strip()
        return raw

    def __extract_poster(self, text: Any) -> str:
        raw = str(text or "")
        if not raw:
            return ""

        patterns = [
            r"""<img[^>]+src=["']([^"']+)["']""",
            r"""url=["']([^"']+)["']""",
            r"""(https?://[^\s"'<>]+?\.(?:jpg|jpeg|png|webp|gif))""",
            r"""(//[^\s"'<>]+?\.(?:jpg|jpeg|png|webp|gif))""",
        ]
        for pattern in patterns:
            match = re.search(pattern, raw, re.IGNORECASE)
            if not match:
                continue
            poster = self.__normalize_url(match.group(1))
            if poster:
                return poster
        return ""

    @staticmethod
    def __extract_wish_count(text: Any) -> int:
        raw = str(text or "")
        if not raw:
            return 0

        clean_text = raw.replace(",", "").replace("，", "")
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
    def __extract_year(text: Any) -> Optional[str]:
        raw = str(text or "")
        if not raw:
            return None
        match = re.search(r"\b(19\d{2}|20\d{2})\b", raw)
        if match:
            return match.group(1)
        return None

    @staticmethod
    def __parse_date_value(text: Any) -> Optional[str]:
        raw = str(text or "").strip()
        if not raw:
            return None

        match = re.search(r"(19\d{2}|20\d{2})[-/年.](\d{1,2})[-/月.](\d{1,2})", raw)
        if not match:
            return None

        try:
            year = int(match.group(1))
            month = int(match.group(2))
            day = int(match.group(3))
            return datetime.date(year, month, day).strftime("%Y-%m-%d")
        except Exception:
            return None

    def __extract_air_date(self, text: Any) -> Optional[str]:
        raw = str(text or "")
        if not raw:
            return None

        clean_text = self.__strip_html(raw)
        keyword_patterns = [
            r"(?:首播|播出|开播|上映|上線|首映|播映|放送|开播日期|首播日期|首播时间|上映日期)\s*[:：]?\s*((?:19|20)\d{2}[-/年.]\d{1,2}[-/月.]\d{1,2})",
        ]
        for pattern in keyword_patterns:
            match = re.search(pattern, clean_text, re.IGNORECASE)
            if not match:
                continue
            date_value = self.__parse_date_value(match.group(1))
            if date_value:
                return date_value

        return self.__parse_date_value(clean_text)

    @staticmethod
    def __chinese_to_int(text: str) -> Optional[int]:
        if not text:
            return None

        mapping = {
            "零": 0,
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
            value = mapping.get(text)
            return value if value and value > 0 else None

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
            r"第([零一二三四五六七八九十]{1,3})季",
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

    def __build_title_candidates(self, title: str) -> List[str]:
        candidates = []
        raw_title = str(title or "").strip()
        if raw_title:
            candidates.append(raw_title)

        stripped_title = re.sub(
            r"\s*(第[零一二三四五六七八九十\d]{1,3}季|Season\s*\d{1,2}|S\s*\d{1,2})\s*$",
            "",
            raw_title,
            flags=re.IGNORECASE,
        ).strip()
        if stripped_title and stripped_title not in candidates:
            candidates.append(stripped_title)

        return candidates

    def __build_meta(self, title: str, year: Optional[str] = None, season: Optional[int] = None) -> MetaInfo:
        meta = MetaInfo(title)
        if year:
            try:
                meta.year = str(year)
            except Exception:
                pass
        try:
            meta.type = MediaType.TV
        except Exception:
            pass
        if season:
            try:
                meta.begin_season = season
            except Exception:
                pass
        return meta

    def __recognize_media_compat(self, title: str, year: Optional[str] = None, douban_id: Optional[str] = None):
        recognize = getattr(self.chain, "recognize_media", None)
        if not callable(recognize):
            logger.warning("豆瓣即将开播订阅：当前版本未提供 recognize_media 接口")
            return None, None

        season = self.__extract_season_from_title(title)

        for candidate_title in self.__build_title_candidates(title):
            meta = self.__build_meta(candidate_title, year=year, season=season)
            attempts = []

            if douban_id:
                attempts.extend(
                    [
                        {"meta": meta, "doubanid": douban_id, "mtype": MediaType.TV},
                        {"meta": meta, "doubanid": douban_id},
                    ]
                )

            attempts.extend(
                [
                    {"meta": meta, "mtype": MediaType.TV},
                    {"meta": meta},
                ]
            )

            for kwargs in attempts:
                try:
                    mediainfo = recognize(**kwargs)
                    if mediainfo:
                        return mediainfo, meta
                except TypeError:
                    continue
                except Exception as err:
                    logger.warning(f"豆瓣即将开播订阅：媒体识别失败，title={candidate_title}，错误：{err}")
                    continue

        return None, None

    @staticmethod
    def __get_media_type_value(mediainfo: Any) -> str:
        media_type = getattr(mediainfo, "type", None)
        if media_type is None:
            return ""
        if hasattr(media_type, "value"):
            return str(media_type.value)
        return str(media_type)

    def __is_tv_media(self, mediainfo: Any) -> bool:
        media_type = self.__get_media_type_value(mediainfo).strip().lower()
        if not media_type:
            return True
        return media_type in {"tv", "电视剧", "剧集", "series"}

    @staticmethod
    def __get_media_attr(mediainfo: Any, names: List[str], default: Any = None) -> Any:
        for name in names:
            value = getattr(mediainfo, name, None)
            if value not in [None, ""]:
                return value
        return default

    def __get_media_title(self, mediainfo: Any) -> str:
        return str(self.__get_media_attr(mediainfo, ["title", "name"], "") or "")

    def __get_media_title_year(self, mediainfo: Any) -> str:
        title_year = self.__get_media_attr(mediainfo, ["title_year"], None)
        if title_year:
            return str(title_year)
        title = self.__get_media_title(mediainfo)
        year = self.__get_media_year(mediainfo)
        return f"{title} {year}".strip()

    def __get_media_year(self, mediainfo: Any) -> str:
        return str(self.__get_media_attr(mediainfo, ["year"], "") or "")

    def __get_media_tmdbid(self, mediainfo: Any) -> Any:
        return self.__get_media_attr(mediainfo, ["tmdb_id", "tmdbid"], None)

    def __get_media_doubanid(self, mediainfo: Any) -> Any:
        return self.__get_media_attr(mediainfo, ["douban_id", "doubanid"], None)

    def __get_media_season(self, mediainfo: Any) -> int:
        value = self.__get_media_attr(mediainfo, ["season", "begin_season"], 1)
        try:
            value = int(value)
            return value if value > 0 else 1
        except Exception:
            return 1

    def __get_media_overview(self, mediainfo: Any) -> str:
        return str(self.__get_media_attr(mediainfo, ["overview", "summary", "description"], "") or "")

    def __get_media_poster(self, mediainfo: Any) -> str:
        get_poster_image = getattr(mediainfo, "get_poster_image", None)
        if callable(get_poster_image):
            try:
                poster = get_poster_image()
                if poster:
                    return str(poster)
            except Exception:
                pass

        poster = self.__get_media_attr(
            mediainfo,
            ["poster", "poster_url", "image", "image_url", "thumb", "cover"],
            "",
        )
        return self.__normalize_url(poster)

    def __get_media_genres(self, mediainfo: Any) -> List[str]:
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

        deduped: List[str] = []
        for item in genre_values:
            if item and item not in deduped:
                deduped.append(item)

        return deduped

    def __extract_air_date_from_mediainfo(self, mediainfo: Any) -> Optional[str]:
        for attr in ["air_date", "first_air_date", "release_date", "premiere_date", "date"]:
            value = getattr(mediainfo, attr, None)
            date_value = self.__parse_date_value(value)
            if date_value:
                return date_value

        details = getattr(mediainfo, "detail", None) or getattr(mediainfo, "details", None)
        if isinstance(details, dict):
            for key in ["air_date", "first_air_date", "release_date", "premiere_date", "date"]:
                date_value = self.__parse_date_value(details.get(key))
                if date_value:
                    return date_value

        return None

    @staticmethod
    def __result_to_bool(result: Any) -> bool:
        if result is None:
            return False
        if isinstance(result, (list, tuple)):
            if not result:
                return False
            return bool(result[0])
        return bool(result)

    def __get_subscribe_chain(self) -> Optional[SubscribeChain]:
        if self.subscribechain:
            return self.subscribechain

        try:
            self.subscribechain = SubscribeChain()
        except Exception as err:
            logger.warning(f"豆瓣即将开播订阅：获取订阅链失败：{err}")
            self.subscribechain = None

        return self.subscribechain

    def __subscription_exists(self, subscribe_chain: Optional[SubscribeChain], meta: MetaInfo, mediainfo: Any) -> bool:
        if not subscribe_chain:
            return False

        exists_method = getattr(subscribe_chain, "exists", None)
        if not callable(exists_method):
            return False

        tmdbid = self.__get_media_tmdbid(mediainfo)
        title = self.__get_media_title(mediainfo) or getattr(meta, "title", None) or ""
        year = self.__get_media_year(mediainfo) or getattr(meta, "year", None) or ""

        attempts = [
            {"mediainfo": mediainfo, "meta": meta},
            {"mediainfo": mediainfo},
            {"meta": meta},
        ]
        if tmdbid:
            attempts.append({"tmdbid": tmdbid, "mtype": MediaType.TV})
        if title:
            attempts.append({"title": title, "year": year, "mtype": MediaType.TV})

        for kwargs in attempts:
            try:
                result = exists_method(**kwargs)
                if result is not None:
                    return self.__result_to_bool(result)
            except TypeError:
                continue
            except Exception as err:
                logger.warning(f"豆瓣即将开播订阅：检查订阅存在状态失败：{err}")
                continue

        return False

    def __is_in_library(self, meta: MetaInfo, mediainfo: Any) -> bool:
        media_exists = getattr(self.chain, "media_exists", None)
        if callable(media_exists):
            attempts = [
                {"mediainfo": mediainfo},
                {"mediainfo": mediainfo, "meta": meta},
                {"meta": meta, "mediainfo": mediainfo},
            ]
            for kwargs in attempts:
                try:
                    result = media_exists(**kwargs)
                    if result is None:
                        continue
                    if hasattr(result, "itemid"):
                        return bool(getattr(result, "itemid", None))
                    return self.__result_to_bool(result)
                except TypeError:
                    continue
                except Exception as err:
                    logger.warning(f"豆瓣即将开播订阅：media_exists 调用失败：{err}")
                    break

        try:
            module = importlib.import_module("app.chain.download")
            chain_cls = getattr(module, "DownloadChain", None)
            if not chain_cls:
                return False

            download_chain = chain_cls()
            method = getattr(download_chain, "get_no_exists_info", None)
            if not callable(method):
                return False

            attempts = [
                {"meta": meta, "mediainfo": mediainfo},
                {"meta": meta},
            ]
            for kwargs in attempts:
                try:
                    result = method(**kwargs)
                    if result is None:
                        continue
                    if isinstance(result, tuple):
                        return bool(result[0])
                    return bool(result)
                except TypeError:
                    continue
                except Exception as err:
                    logger.warning(f"豆瓣即将开播订阅：get_no_exists_info 调用失败：{err}")
                    break
        except Exception:
            return False

        return False

    @staticmethod
    def __compact_kwargs(kwargs: Dict[str, Any], keep_false_keys: Optional[List[str]] = None) -> Dict[str, Any]:
        keep_false_keys = keep_false_keys or []
        compacted = {}
        for key, value in kwargs.items():
            if value is None:
                continue
            if value is False and key not in keep_false_keys:
                continue
            if isinstance(value, str) and value == "":
                continue
            compacted[key] = value
        return compacted

    @staticmethod
    def __parse_add_result(result: Any) -> Tuple[bool, str]:
        if isinstance(result, (list, tuple)):
            sid = result[0] if len(result) > 0 else None
            msg = result[1] if len(result) > 1 else ""
            return bool(sid), str(msg or "")
        return bool(result), ""

    def __add_subscription(
        self,
        subscribe_chain: Optional[SubscribeChain],
        meta: MetaInfo,
        mediainfo: Any,
        douban_id: Optional[str],
    ) -> Tuple[bool, str]:
        if not subscribe_chain:
            return False, "订阅链不可用"

        add_method = getattr(subscribe_chain, "add", None)
        if not callable(add_method):
            return False, "当前版本未提供订阅接口"

        season = getattr(meta, "begin_season", None) or self.__get_media_season(mediainfo) or 1
        base_kwargs = {
            "title": self.__get_media_title(mediainfo) or getattr(meta, "title", None) or "",
            "year": self.__get_media_year(mediainfo) or getattr(meta, "year", None) or "",
            "mtype": MediaType.TV,
            "tmdbid": self.__get_media_tmdbid(mediainfo),
            "doubanid": self.__get_media_doubanid(mediainfo) or douban_id,
            "season": season,
            "exist_ok": True,
            "username": self.plugin_name,
            "message": False,
        }

        variant_definitions = [
            self.__compact_kwargs(base_kwargs, keep_false_keys=["message"]),
            self.__compact_kwargs({k: v for k, v in base_kwargs.items() if k not in {"message"}}),
            self.__compact_kwargs({k: v for k, v in base_kwargs.items() if k not in {"message", "doubanid"}}),
            self.__compact_kwargs({k: v for k, v in base_kwargs.items() if k not in {"message", "doubanid", "season"}}),
            self.__compact_kwargs({k: v for k, v in base_kwargs.items() if k not in {"message", "doubanid", "season", "exist_ok"}}),
            self.__compact_kwargs(
                {
                    "title": base_kwargs.get("title"),
                    "year": base_kwargs.get("year"),
                    "mtype": MediaType.TV,
                    "tmdbid": base_kwargs.get("tmdbid"),
                }
            ),
            self.__compact_kwargs(
                {
                    "title": base_kwargs.get("title"),
                    "year": base_kwargs.get("year"),
                    "mtype": MediaType.TV,
                }
            ),
        ]

        variants: List[Dict[str, Any]] = []
        seen = set()
        for item in variant_definitions:
            key = tuple(sorted((k, str(v)) for k, v in item.items()))
            if key in seen:
                continue
            seen.add(key)
            variants.append(item)

        last_error = "添加订阅失败"
        for kwargs in variants:
            try:
                result = add_method(**kwargs)
                success, message = self.__parse_add_result(result)
                if success:
                    return True, message or "添加订阅成功"
                if message:
                    last_error = message
            except TypeError:
                continue
            except Exception as err:
                last_error = str(err)
                logger.warning(f"豆瓣即将开播订阅：添加订阅失败，参数={list(kwargs.keys())}，错误：{err}")
                continue

        return False, last_error

    def __build_notify_unique_key(self, douban_id: Optional[str], title: str, air_date: Optional[str]) -> str:
        return f"air_notify:{douban_id or title}:{air_date or 'unknown'}"

    def __has_sent_air_notify(self, unique_key: str) -> bool:
        notify_history = self.get_data("notify_history") or []
        return any(item.get("unique") == unique_key for item in notify_history)

    def __save_air_notify_history(self, item: dict):
        notify_history = self.get_data("notify_history") or []
        notify_history.append(item)
        self.save_data("notify_history", notify_history)

    def __post_message_compat(self, title: str, text: str, image: Optional[str] = None):
        kwargs = {
            "title": title,
            "text": text,
        }
        if image:
            kwargs["image"] = image

        try:
            self.post_message(**kwargs)
            return
        except Exception:
            pass

        try:
            self.post_message(title=title, text=text)
        except Exception as err:
            logger.error(f"豆瓣即将开播订阅：发送消息失败：{err}")

    def __send_air_notify_message(
        self,
        title: str,
        mediainfo: Any,
        douban_id: Optional[str],
        wish_count: int,
        air_date: Optional[str],
        subscribed: bool,
        poster: Optional[str],
    ):
        if not self._notify_before_air:
            return

        genres = self.__get_media_genres(mediainfo)
        genre_text = " / ".join(genres) if genres else "-"
        douban_link = f"https://movie.douban.com/subject/{douban_id}" if douban_id else "-"
        tmdbid = self.__get_media_tmdbid(mediainfo)
        year = self.__get_media_year(mediainfo) or "-"
        subscribed_text = "已订阅" if subscribed else "未订阅"

        lines = [
            f"🎬 名称：{title}",
            "📂 类型：电视剧",
            f"🏷️ 分类：{genre_text}",
            f"⏰ 开播时间：{air_date or '-'}",
            f"👀 想看人数：{wish_count}",
            f"🔔 订阅状态：{subscribed_text}",
            f"📅 年份：{year}",
            f"🆔 TMDB ID：{tmdbid or '-'}",
            f"🔗 豆瓣链接：{douban_link}",
        ]
        self.__post_message_compat(
            title="**📺 豆瓣开播提醒 ✨**",
            text="\n".join(lines),
            image=poster,
        )

    def __get_rss_info(self, addr: str) -> List[dict]:
        try:
            proxy = getattr(settings, "PROXY", None)
            response = RequestUtils(proxies=proxy).get_res(addr) if self._proxy else RequestUtils().get_res(addr)
            if not response:
                return []

            dom_tree = xml.dom.minidom.parseString(response.text)
            items = dom_tree.getElementsByTagName("item")
            result: List[dict] = []

            for item in items:
                try:
                    title = self.__tag_value(item, "title", default="")
                    link = self.__tag_value(item, "link", default="")
                    description = self.__tag_value(item, "description", default="")
                    item_xml = item.toxml() if hasattr(item, "toxml") else description

                    if not title and not link:
                        continue

                    douban_match = re.search(r"/subject/(\d+)", link) or re.search(r"/(\d+)(?=/|$)", link)
                    douban_id = douban_match.group(1) if douban_match else None

                    clean_description = self.__strip_html(description)
                    result.append(
                        {
                            "title": title.strip(),
                            "link": str(link or "").strip(),
                            "description": clean_description,
                            "raw_description": description or "",
                            "poster": self.__extract_poster(item_xml) or self.__extract_poster(description),
                            "doubanid": douban_id,
                            "wish_count": self.__extract_wish_count(description),
                            "year": self.__extract_year(clean_description),
                            "air_date": self.__extract_air_date(description),
                        }
                    )
                except Exception as item_err:
                    logger.error(f"豆瓣即将开播订阅：解析 RSS 条目失败：{item_err}")
                    continue

            return result
        except Exception as err:
            logger.error(f"豆瓣即将开播订阅：获取 RSS 失败：{err}")
            return []

    @staticmethod
    def __days_until_air(air_date_str: Optional[str]) -> Optional[int]:
        if not air_date_str:
            return None
        try:
            air_date = datetime.datetime.strptime(air_date_str, "%Y-%m-%d").date()
            return (air_date - datetime.datetime.now().date()).days
        except Exception:
            return None

    def __hours_until_air(self, air_date_str: Optional[str]) -> Optional[float]:
        if not air_date_str:
            return None
        try:
            air_date = datetime.datetime.strptime(air_date_str, "%Y-%m-%d").date()
            air_datetime = self.__get_timezone().localize(datetime.datetime.combine(air_date, datetime.time.min))
            return (air_datetime - self.__now()).total_seconds() / 3600
        except Exception:
            return None

    @staticmethod
    def __history_unique_key(title: str, douban_id: Optional[str]) -> str:
        return f"doubancomingnotice:{douban_id or title}"

    def __build_history_card(self, history: Dict[str, Any]) -> Dict[str, Any]:
        title = history.get("title") or "-"
        poster = history.get("poster") or ""
        mtype = history.get("type") or "电视剧"
        genres = " / ".join(history.get("genres") or []) or "-"
        wish_count = history.get("wish_count") or "-"
        air_date = history.get("air_date") or "-"
        subscribed = "已订阅" if history.get("subscribed") else "未订阅"
        notified = "已提醒" if history.get("air_notify_sent") else "未提醒"
        time_str = history.get("time") or "-"
        overview = history.get("overview") or "暂无简介"
        douban_id = history.get("doubanid")
        douban_url = f"https://movie.douban.com/subject/{douban_id}" if douban_id else ""

        poster_block = {
            "component": "div",
            "props": {
                "style": "width: 96px; min-width: 96px; height: 144px; margin-right: 12px;",
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
        } if poster else {
            "component": "div",
            "props": {
                "class": "d-flex align-center justify-center text-caption text-medium-emphasis",
                "style": "width: 96px; min-width: 96px; height: 144px; margin-right: 12px; background: rgba(0,0,0,0.04); border-radius: 12px;",
            },
            "text": "暂无海报",
        }

        title_block = {
            "component": "span",
            "props": {
                "class": "text-body-1 font-weight-bold",
                "style": "line-height: 1.2rem;",
            },
            "text": title,
        }
        if douban_url:
            title_block = {
                "component": "a",
                "props": {
                    "href": douban_url,
                    "target": "_blank",
                    "style": "color: inherit; text-decoration: none; line-height: 1.2rem;",
                    "class": "text-body-1 font-weight-bold",
                },
                "text": title,
            }

        return {
            "component": "VCard",
            "props": {
                "class": "h-100",
                "style": "min-height: 250px; border-radius: 14px; overflow: hidden;",
            },
            "content": [
                {
                    "component": "div",
                    "props": {
                        "class": "d-flex flex-nowrap flex-row h-100",
                        "style": "padding: 12px;",
                    },
                    "content": [
                        poster_block,
                        {
                            "component": "div",
                            "props": {
                                "class": "d-flex flex-column justify-space-between flex-grow-1",
                                "style": "min-width: 0;",
                            },
                            "content": [
                                {
                                    "component": "div",
                                    "content": [
                                        {
                                            "component": "div",
                                            "props": {
                                                "style": "display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; min-height: 2.4rem;",
                                            },
                                            "content": [title_block],
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {
                                                "class": "pa-0 pt-1 text-body-2",
                                                "style": "line-height: 1rem;",
                                            },
                                            "text": f"类型：{mtype}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {
                                                "class": "pa-0 text-body-2",
                                                "style": "line-height: 1rem;",
                                            },
                                            "text": f"分类：{genres}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {
                                                "class": "pa-0 text-body-2",
                                                "style": "line-height: 1rem;",
                                            },
                                            "text": f"想看：{wish_count}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {
                                                "class": "pa-0 text-body-2",
                                                "style": "line-height: 1rem;",
                                            },
                                            "text": f"开播：{air_date}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {
                                                "class": "pa-0 text-body-2",
                                                "style": "line-height: 1rem;",
                                            },
                                            "text": f"状态：{subscribed} / {notified}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {
                                                "class": "pa-0 text-caption text-medium-emphasis",
                                                "style": "line-height: 1rem;",
                                            },
                                            "text": f"处理时间：{time_str}",
                                        },
                                        {
                                            "component": "VCardText",
                                            "props": {
                                                "class": "pa-0 pt-2 text-caption",
                                                "style": "display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; line-height: 1rem; min-height: 3rem;",
                                            },
                                            "text": overview,
                                        },
                                    ],
                                },
                                {
                                    "component": "VCardActions",
                                    "props": {
                                        "class": "pa-0 pt-2",
                                    },
                                    "content": [
                                        {
                                            "component": "VBtn",
                                            "props": {
                                                "color": "error",
                                                "variant": "tonal",
                                                "size": "small",
                                            },
                                            "text": "删除记录",
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
                                        }
                                    ],
                                },
                            ],
                        },
                    ],
                }
            ],
        }

    def __refresh_rss(self):
        logger.info("豆瓣即将开播订阅：开始刷新 RSS 数据")
        rss_url = self.__build_rss_url()
        logger.info(f"豆瓣即将开播订阅：请求地址 {rss_url}")

        history: List[dict] = self.get_data("history") or []
        rss_infos = self.__get_rss_info(rss_url)

        if not rss_infos:
            logger.warning(f"豆瓣即将开播订阅：未从 RSS 获取到数据 {rss_url}")
            return

        subscribe_chain = self.__get_subscribe_chain()
        added_count = 0

        for rss_info in rss_infos:
            try:
                if self._event.is_set():
                    logger.info("豆瓣即将开播订阅：任务已停止")
                    return

                title = str(rss_info.get("title") or "").strip()
                if not title:
                    continue

                douban_id = rss_info.get("doubanid")
                description = rss_info.get("description") or ""
                wish_count = self.__safe_positive_int(rss_info.get("wish_count"), 0)
                year = rss_info.get("year")
                air_date = rss_info.get("air_date")

                if wish_count < self._wish_count_threshold:
                    logger.info(
                        f"豆瓣即将开播订阅：{title} 想看人数 {wish_count} 低于阈值 {self._wish_count_threshold}，已跳过"
                    )
                    continue

                mediainfo, meta = self.__recognize_media_compat(title=title, year=year, douban_id=douban_id)
                if not mediainfo or not meta:
                    logger.warning(f"豆瓣即将开播订阅：未识别到媒体信息，标题：{title}")
                    continue

                if not self.__is_tv_media(mediainfo):
                    logger.info(f"豆瓣即将开播订阅：{self.__get_media_title_year(mediainfo)} 不是电视剧类型，已跳过")
                    continue

                if not air_date:
                    air_date = self.__extract_air_date_from_mediainfo(mediainfo)

                in_library = self.__is_in_library(meta=meta, mediainfo=mediainfo)
                subscribed = self.__subscription_exists(subscribe_chain=subscribe_chain, meta=meta, mediainfo=mediainfo)

                unique_flag = self.__history_unique_key(title=title, douban_id=douban_id)
                history_item = next((item for item in history if item.get("unique") == unique_flag), None)

                newly_subscribed = False
                days_until_air = self.__days_until_air(air_date)
                if days_until_air is None:
                    logger.info(f"豆瓣即将开播订阅：{title} 未解析到开播日期，跳过提前订阅判断")
                elif days_until_air < 0:
                    logger.info(f"豆瓣即将开播订阅：{title} 已开播 {abs(days_until_air)} 天，跳过订阅")
                elif days_until_air > self._advance_days:
                    logger.info(
                        f"豆瓣即将开播订阅：{title} 距开播还有 {days_until_air} 天，超过提前订阅阈值 {self._advance_days} 天"
                    )
                elif in_library:
                    logger.info(f"豆瓣即将开播订阅：{self.__get_media_title_year(mediainfo)} 媒体库中已存在")
                elif subscribed:
                    logger.info(f"豆瓣即将开播订阅：{self.__get_media_title_year(mediainfo)} 订阅已存在")
                else:
                    success, message = self.__add_subscription(
                        subscribe_chain=subscribe_chain,
                        meta=meta,
                        mediainfo=mediainfo,
                        douban_id=douban_id,
                    )
                    if success:
                        subscribed = True
                        newly_subscribed = True
                        added_count += 1
                        logger.info(f"豆瓣即将开播订阅：已添加订阅 {self.__get_media_title_year(mediainfo)}")
                    else:
                        logger.warning(f"豆瓣即将开播订阅：添加订阅失败 {title}，原因：{message}")

                now_str = self.__now().strftime("%Y-%m-%d %H:%M:%S")
                poster = rss_info.get("poster") or self.__get_media_poster(mediainfo)
                genres = self.__get_media_genres(mediainfo)
                overview = self.__get_media_overview(mediainfo) or description

                if not history_item:
                    history_item = {
                        "title": title,
                        "type": self.__get_media_type_value(mediainfo) or "电视剧",
                        "year": self.__get_media_year(mediainfo) or year,
                        "poster": poster,
                        "overview": overview,
                        "tmdbid": self.__get_media_tmdbid(mediainfo),
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
                            "type": self.__get_media_type_value(mediainfo) or history_item.get("type") or "电视剧",
                            "year": self.__get_media_year(mediainfo) or year or history_item.get("year"),
                            "poster": poster or history_item.get("poster"),
                            "overview": overview or history_item.get("overview"),
                            "tmdbid": self.__get_media_tmdbid(mediainfo) or history_item.get("tmdbid"),
                            "doubanid": douban_id or history_item.get("doubanid"),
                            "wish_count": wish_count,
                            "air_date": air_date or history_item.get("air_date"),
                            "genres": genres or history_item.get("genres") or [],
                            "subscribed": subscribed,
                            "time": now_str,
                        }
                    )

                if not self._notify_before_air:
                    continue

                hours_until_air = self.__hours_until_air(air_date)
                if hours_until_air is None:
                    logger.info(f"豆瓣即将开播订阅：{title} 未解析到开播日期，跳过开播提醒")
                    continue

                if hours_until_air < 0:
                    logger.info(f"豆瓣即将开播订阅：{title} 已开播，跳过开播提醒")
                    continue

                if hours_until_air > self._notify_hours:
                    logger.info(f"豆瓣即将开播订阅：{title} 距开播还有 {hours_until_air:.2f} 小时，未进入提醒窗口")
                    continue

                notify_unique = self.__build_notify_unique_key(douban_id, title, air_date)
                if self.__has_sent_air_notify(notify_unique):
                    logger.info(f"豆瓣即将开播订阅：{title} 开播提醒已发送过，跳过")
                    history_item["air_notify_sent"] = True
                    continue

                self.__send_air_notify_message(
                    title=title,
                    mediainfo=mediainfo,
                    douban_id=douban_id,
                    wish_count=wish_count,
                    air_date=air_date,
                    subscribed=subscribed,
                    poster=poster,
                )

                self.__save_air_notify_history(
                    {
                        "unique": notify_unique,
                        "title": title,
                        "doubanid": douban_id,
                        "tmdbid": self.__get_media_tmdbid(mediainfo),
                        "type": self.__get_media_type_value(mediainfo) or "电视剧",
                        "genres": genres,
                        "wish_count": wish_count,
                        "air_date": air_date,
                        "subscribed": subscribed,
                        "in_library": in_library,
                        "notified_at": now_str,
                    }
                )

                history_item["air_notify_sent"] = True
                history_item["subscribed"] = subscribed

                if newly_subscribed:
                    logger.info(f"豆瓣即将开播订阅：{title} 已完成订阅并进入提醒流程")
                else:
                    logger.info(f"豆瓣即将开播订阅：{title} 已发送开播提醒")
            except Exception as err:
                logger.error(f"豆瓣即将开播订阅：处理条目失败，错误：{err}")

        self.save_data("history", history)
        logger.info(f"豆瓣即将开播订阅：刷新完成，本次新增订阅 {added_count} 条")
