import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

FIRST_RUN_LOGIN = False
AUTH_URL = "https://gmgn.ai/tglogin?user_id=53b06598-3e2b-4d2f-aec6-f2e5881def90&code=2de464bb-1737-4c9f-823d-b7544dadc92e&id=0eae54fb142533ac"

LOG_FILE = str(BASE_DIR / "twitter_monitor.log")
USER_DATA_DIR = str(BASE_DIR / "browser_data")
SCREENSHOT_PATH = str(BASE_DIR / "monitor_running.png")
SUMMARY_DB_PATH = os.getenv("SUMMARY_DB_PATH", str(BASE_DIR / "twitter_monitor.db"))
MONITOR_URL = "https://gmgn.ai/follow?target=xTracker&chain=bsc"
PROXY_SERVER = "socks5://127.0.0.1:40000"
WATCHDOG_TIMEOUT = 120
WATCHDOG_POLL_INTERVAL = 5
XVFB_WIDTH = 1920
XVFB_HEIGHT = 1080

# ---------- WebSocket 分发配置 ----------
WS_HOST = "0.0.0.0"
WS_PORT = 8765
WS_TOKEN = os.getenv("WS_TOKEN", "change-me-to-a-strong-token")
WS_HEARTBEAT_INTERVAL = 30

# ---------- Telegram 推送配置 ----------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_ENABLE_DEFAULT = os.getenv("TG_ENABLE_DEFAULT", "False").lower() in ("true", "1", "yes")
TG_CHANNEL_ID = os.getenv("TG_CHANNEL_ID", "")

# 动态解析路由分组
TG_CHANNEL_MAP: dict[str, list[str]] = {}
# TG 赛道过滤: {handle: {channel_id: [关键词...]}}，空列表表示不过滤
TG_CHANNEL_TRACK_FILTER: dict[str, dict[str, list[str]]] = {}
FEISHU_CHANNEL_MAP: dict[str, list[dict]] = {}
_routing_handles = set()

for k, v in os.environ.items():
    if k.startswith("TG_ROUTING_") and v:
        group_name = k[len("TG_ROUTING_"):]
        handles = [h.strip().lower() for h in v.split(",") if h.strip()]

        # 解析 TG 路由
        tg_enable_str = os.getenv(f"TG_ENABLE_{group_name}", "True").lower()
        if tg_enable_str in ("true", "1", "yes"):
            channel_id = os.getenv(f"TG_CHANNEL_ID_{group_name}")
            # 赛道过滤关键词（逗号分隔，空则不过滤）
            tg_track_raw = os.getenv(f"TG_TRACK_FILTER_{group_name}", "")
            tg_track_filter = [kw.strip() for kw in tg_track_raw.split(",") if kw.strip()]
            if channel_id:
                for h in handles:
                    if h not in TG_CHANNEL_MAP:
                        TG_CHANNEL_MAP[h] = []
                    if channel_id not in TG_CHANNEL_MAP[h]:
                        TG_CHANNEL_MAP[h].append(channel_id)
                    # 记录该 handle+channel 的赛道过滤规则
                    if tg_track_filter:
                        if h not in TG_CHANNEL_TRACK_FILTER:
                            TG_CHANNEL_TRACK_FILTER[h] = {}
                        TG_CHANNEL_TRACK_FILTER[h][channel_id] = tg_track_filter
                    _routing_handles.add(h)
        
        # 解析飞书路由 (共用 TG_ROUTING 的 handle 列表)
        fs_enable_str = os.getenv(f"FEISHU_ENABLE_{group_name}", "True").lower()
        if fs_enable_str in ("true", "1", "yes"):
            fs_webhook = os.getenv(f"FEISHU_WEBHOOK_{group_name}")
            fs_secret = os.getenv(f"FEISHU_SECRET_{group_name}", "")
            # 赛道过滤关键词（逗号分隔，空则不过滤）
            fs_track_raw = os.getenv(f"FEISHU_TRACK_FILTER_{group_name}", "")
            fs_track_filter = [kw.strip() for kw in fs_track_raw.split(",") if kw.strip()]
            if fs_webhook:
                for h in handles:
                    if h not in FEISHU_CHANNEL_MAP:
                        FEISHU_CHANNEL_MAP[h] = []
                    if not any(item['webhook'] == fs_webhook for item in FEISHU_CHANNEL_MAP[h]):
                        entry: dict = {"webhook": fs_webhook, "secret": fs_secret}
                        if fs_track_filter:
                            entry["track_filter"] = fs_track_filter
                        FEISHU_CHANNEL_MAP[h].append(entry)
                _routing_handles.add(h)

TG_FILTER_HANDLES = [
    h.strip().lower()
    for h in os.getenv("TG_FILTER_HANDLES", "").split(",")
    if h.strip()
]
# 自动将启用路由组中的博主并入全局监控白名单
if _routing_handles:
    TG_FILTER_HANDLES = list(set(TG_FILTER_HANDLES) | _routing_handles)

# ---------- Binance Square 配置 ----------
BINANCE_SQUARE_HANDLES = [
    h.strip().lower()
    for h in os.getenv("BINANCE_SQUARE_HANDLES", "").split(",")
    if h.strip()
]

# ---------- 飞书推送配置 ----------
FEISHU_APP_ID = os.getenv("FEISHU_APP_ID", "")
FEISHU_APP_SECRET = os.getenv("FEISHU_APP_SECRET", "")
FEISHU_WEBHOOK_DEFAULT = os.getenv("FEISHU_WEBHOOK_DEFAULT", "")
FEISHU_SECRET_DEFAULT = os.getenv("FEISHU_SECRET_DEFAULT", "")
FEISHU_ENABLE_DEFAULT = os.getenv("FEISHU_ENABLE_DEFAULT", "False").lower() in ("true", "1", "yes")

# ---------- Webhook 推送配置 ----------
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# ---------- DeepSeek 翻译配置 ----------
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
DEEPSEEK_BASE_URL = "https://api.deepseek.com"
DEEPSEEK_MODEL = "deepseek-chat"

# ---------- AI 分析（赛道分类 + 摘要 + 翻译）----------
AI_ANALYZE_HANDLES: set[str] = {
    h.strip().lower()
    for h in os.getenv("AI_ANALYZE_HANDLES", "").split(",")
    if h.strip()
}

# ---------- 定时频道总结配置 ----------
SUMMARY_ENABLE = os.getenv("SUMMARY_ENABLE", "False").lower() in ("true", "1", "yes")
SUMMARY_TIMEZONE = os.getenv("SUMMARY_TIMEZONE", "Asia/Shanghai")
SUMMARY_TIMES = [
    t.strip()
    for t in os.getenv("SUMMARY_TIMES", "07:30,20:00").split(",")
    if t.strip()
]
SUMMARY_GROUPS = [
    g.strip().upper()
    for g in os.getenv("SUMMARY_GROUPS", "BINANCE").split(",")
    if g.strip()
]
SUMMARY_MAX_TWEETS = int(os.getenv("SUMMARY_MAX_TWEETS", "120"))
SUMMARY_AI_TIMEOUT_SECONDS = int(os.getenv("SUMMARY_AI_TIMEOUT_SECONDS", "180"))
SUMMARY_TWEET_TEXT_LIMIT = int(os.getenv("SUMMARY_TWEET_TEXT_LIMIT", "500"))

SUMMARY_CHANNELS: list[dict] = []
for group_name in SUMMARY_GROUPS:
    source_channel_id = (
        os.getenv(f"SUMMARY_SOURCE_CHANNEL_ID_{group_name}")
        or os.getenv(f"TG_CHANNEL_ID_{group_name}", "")
    )
    target_tg_channel_id = (
        os.getenv(f"SUMMARY_TG_CHANNEL_ID_{group_name}")
        or source_channel_id
    )
    target_feishu_webhook = (
        os.getenv(f"SUMMARY_FEISHU_WEBHOOK_{group_name}")
        or os.getenv(f"FEISHU_WEBHOOK_{group_name}", "")
    )
    target_feishu_secret = (
        os.getenv(f"SUMMARY_FEISHU_SECRET_{group_name}")
        or os.getenv(f"FEISHU_SECRET_{group_name}", "")
    )

    if source_channel_id:
        SUMMARY_CHANNELS.append({
            "key": group_name,
            "label": os.getenv(f"SUMMARY_LABEL_{group_name}", group_name),
            "source_platform": os.getenv(f"SUMMARY_SOURCE_PLATFORM_{group_name}", "telegram"),
            "source_target_id": source_channel_id,
            "target_tg_channel_id": target_tg_channel_id,
            "target_feishu_webhook": target_feishu_webhook,
            "target_feishu_secret": target_feishu_secret,
        })
