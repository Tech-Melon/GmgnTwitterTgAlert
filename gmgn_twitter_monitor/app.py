import asyncio
import json
import os
import signal
import subprocess
import time
from typing import Any

from loguru import logger
from playwright.async_api import async_playwright

try:
    from xvfbwrapper import Xvfb
except ImportError:
    print("Xvfb is missing. Please run `uv pip install xvfbwrapper` first.")
    raise

from . import config
from .browser import BrowserManager
from .distributor import (
    DistributorHub,
    LoggingDistributor,
    TelegramDistributor,
    FeishuDistributor,
    WebhookDistributor,
    WebSocketDistributor,
)
from .logging_setup import setup_logging
from .parser import build_standardized_message, extract_triggers_map, parse_socketio_payload
from .storage import SQLiteStorage
from .summary_scheduler import DailySummaryScheduler
from .watchdog import Watchdog


# ---------------------------------------------------------------------------
#  cp 去重缓冲器：防止快照版(cp=0)和完整版(cp=1)重复推送
# ---------------------------------------------------------------------------
class MessageDeduplicator:
    """基于 internal_id 的消息去重器。

    策略：
    - TG 渠道：快照版立即推送，启动 5s 定时器。如果在 5s 内收到完整版，则触发 TG_UPDATE 以更新消息。
    - DEFAULT（如飞书）：维持 0.8s 缓冲防抖逻辑。
    """

    TIMEOUT_FEISHU = 0.8  # 800ms 等待完整版
    TIMEOUT_UPDATE = 5.0  # 5s 等待 TG 的完整版更新

    def __init__(self, publish_callback):
        self._publish = publish_callback
        self._pending_feishu: dict[str, tuple[dict, asyncio.TimerHandle]] = {}
        self._pending_update: dict[str, tuple[dict, asyncio.TimerHandle]] = {}
        self._processed_feishu_ids: set[str] = set()
        self._processed_tg_ids: set[str] = set()
        self._history_queue: list[str] = []
        # 关键：持有 asyncio.Task 引用，防止 GC 回收导致协程中途消失
        self._background_tasks: set[asyncio.Task] = set()

    def _mark_history(self, internal_id: str) -> None:
        if internal_id and internal_id not in self._history_queue:
            self._history_queue.append(internal_id)
            if len(self._history_queue) > 1000:
                old_id = self._history_queue.pop(0)
                self._processed_feishu_ids.discard(old_id)
                self._processed_tg_ids.discard(old_id)

    def process(self, raw_item: dict) -> None:
        """处理一条原始 gmgn 数据项。"""
        internal_id = raw_item.get("i", "")
        if not internal_id:
            return

        cp = raw_item.get("cp")

        # --- 1. TG 实时推送 & 5s 更新逻辑 ---
        if internal_id not in self._processed_tg_ids:
            self._processed_tg_ids.add(internal_id)
            self._mark_history(internal_id)
            self._dispatch(raw_item, target="TG_FAST")

            if cp != 1:
                loop = asyncio.get_event_loop()
                timer = loop.call_later(
                    self.TIMEOUT_UPDATE,
                    self._timeout_update,
                    internal_id,
                )
                self._pending_update[internal_id] = (raw_item, timer)
            else:
                # cp=1 直接到达：完整版已在手，立即触发 TG_UPDATE 进行翻译编辑
                self._dispatch(raw_item, target="TG_UPDATE")

        elif cp == 1 and internal_id in self._pending_update:
            _, timer = self._pending_update.pop(internal_id)
            timer.cancel()
            self._dispatch(raw_item, target="TG_UPDATE")

        # --- 2. 其他默认渠道的 0.5s 延迟去重逻辑 ---
        if internal_id not in self._processed_feishu_ids:
            if cp == 1:
                if internal_id in self._pending_feishu:
                    _, timer = self._pending_feishu.pop(internal_id)
                    timer.cancel()
                self._processed_feishu_ids.add(internal_id)
                self._mark_history(internal_id)
                self._dispatch(raw_item, target="DEFAULT")
            else:
                if internal_id not in self._pending_feishu:
                    loop = asyncio.get_event_loop()
                    timer = loop.call_later(
                        self.TIMEOUT_FEISHU,
                        self._timeout_feishu,
                        internal_id,
                    )
                    self._pending_feishu[internal_id] = (raw_item, timer)

    def _timeout_feishu(self, internal_id: str) -> None:
        """超时兜底：完整版没来，用快照版推送，保证不丢消息。"""
        if internal_id in self._pending_feishu:
            raw_item, _ = self._pending_feishu.pop(internal_id)
            logger.warning(f"⏱️ 默认渠道等待完整版超时: {internal_id[:20]}... 使用快照兜底推送")
            self._processed_feishu_ids.add(internal_id)
            self._mark_history(internal_id)
            self._dispatch(raw_item, target="DEFAULT")

    def _timeout_update(self, internal_id: str) -> None:
        if internal_id in self._pending_update:
            raw_item, _ = self._pending_update.pop(internal_id)
            logger.info(f"⏱️ TG等待完整版更新超时(5s): {internal_id[:20]}... 使用快照更新TG")
            self._dispatch(raw_item, target="TG_UPDATE")

    def _dispatch(self, raw_item: dict, target: str) -> None:
        """标准化并推送消息。"""
        try:
            message = build_standardized_message(raw_item)
            standardized_msg = message.to_dict()
            standardized_msg["_internal_id"] = raw_item.get("i", "")
            standardized_msg["_dispatch_target"] = target

            log_tag = f"[{message.action.upper()}]"
            summary_text = (
                f"{message.author.handle}: {message.content.text[:50]}..."
                if message.content.text
                else f"{message.author.handle} (无正文)"
            )
            if message.reference:
                summary_text += f" (REF: @{message.reference.author_handle})"

            summary_text += _build_delay_string(raw_item.get("ts", 0))

            logger.info(f"✨ 标准化推送 ({target}) {log_tag} | {summary_text}")
            task = asyncio.create_task(self._publish(standardized_msg))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
        except Exception as e:
            logger.error(f"❌ 数据标准化失败: {e}")


def _build_delay_string(raw_ts: Any) -> str:
    if not raw_ts:
        return ""
    try:
        ts_ms = int(raw_ts)
        is_ms_timestamp = ts_ms > 9_999_999_999
        ts_sec = ts_ms / 1000.0 if is_ms_timestamp else float(ts_ms)
        ms_part = ts_ms % 1000 if is_ms_timestamp else 0
        
        # 1. 源端时间
        ts_str = time.strftime('%H:%M:%S', time.localtime(ts_sec))
        if is_ms_timestamp:
            ts_str += f".{ms_part:03d}"
            
        # 2. 本机收到时间
        recv_time = time.time()
        recv_str = time.strftime('%H:%M:%S', time.localtime(recv_time))
        recv_ms_part = int((recv_time - int(recv_time)) * 1000)
        recv_str += f".{recv_ms_part:03d}"
            
        # 3. 延迟计算
        delay_ms = (recv_time - ts_sec) * 1000
        return f" [GMGN抓取发推时间: {ts_str} | 服务器收到时间: {recv_str} | 端到端耗时: {delay_ms:.0f}ms]"
    except (ValueError, TypeError):
        pass
    return ""

def _format_delay_info(parsed: dict) -> str:
    try:
        if "data" in parsed and isinstance(parsed["data"], list) and len(parsed["data"]) > 0:
            return _build_delay_string(parsed["data"][0].get("ts", 0))
    except Exception:
        pass
    return ""


# ---------------------------------------------------------------------------
#  主入口
# ---------------------------------------------------------------------------
def _cleanup_orphan_processes() -> None:
    """清理上次异常退出遗留的孤儿进程（Xvfb / Chromium）。"""
    for target in ("chromium", "Xvfb"):
        result = subprocess.run(
            ["pkill", "-u", os.environ.get("USER", "ubuntu"), "-f", target],
            capture_output=True,
        )
        killed = result.returncode == 0
        logger.info(f"清理孤儿 {target} 进程: {'✅ 已清理' if killed else '⬜ 无残留'}")


def _build_distributor_hub(storage: SQLiteStorage | None = None) -> DistributorHub:
    """根据 config 组装分发器集线器。"""
    distributors = [
        # 1. 日志分发器（始终启用）
        LoggingDistributor(),
        # 2. Telegram 频道推送
        TelegramDistributor(
            bot_token=config.TG_BOT_TOKEN,
            default_channel_id=config.TG_CHANNEL_ID,
            enable_default=config.TG_ENABLE_DEFAULT,
            channel_map=config.TG_CHANNEL_MAP,
            filter_handles=config.TG_FILTER_HANDLES,
            storage=storage,
        ),
        # 3. 飞书分组推送 (与 TG 分组同源并发)
        FeishuDistributor(
            app_id=config.FEISHU_APP_ID,
            app_secret=config.FEISHU_APP_SECRET,
            default_webhook=config.FEISHU_WEBHOOK_DEFAULT,
            default_secret=config.FEISHU_SECRET_DEFAULT,
            enable_default=config.FEISHU_ENABLE_DEFAULT,
            channel_map=config.FEISHU_CHANNEL_MAP,
            filter_handles=config.TG_FILTER_HANDLES,
            storage=storage,
        ),
        # 4. Webhook HTTP POST
        WebhookDistributor(
            url=config.WEBHOOK_URL,
            secret=config.WEBHOOK_SECRET,
        ),
    ]
    if config.WS_ENABLE:
        distributors.insert(
            1,
            WebSocketDistributor(
                host=config.WS_HOST,
                port=config.WS_PORT,
                token=config.WS_TOKEN,
                heartbeat_interval=config.WS_HEARTBEAT_INTERVAL,
            ),
        )
    return DistributorHub(distributors, storage=storage)


async def main():
    setup_logging()
    _cleanup_orphan_processes()

    # 打印本次启动时间与 systemd 12h 后预计重启时间
    start_ts = time.time()
    next_restart_ts = start_ts + 43200  # 与 RuntimeMaxSec=43200 对应
    logger.info(
        f"🚀 服务启动 | 本次启动: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_ts))}"
        f" | 预计重启: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(next_restart_ts))}"
    )

    vdisplay = Xvfb(width=config.XVFB_WIDTH, height=config.XVFB_HEIGHT)
    vdisplay.start()

    # 注册 SIGTERM 处理器（systemd stop / kill 均会触发）
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(
        signal.SIGTERM,
        lambda: loop.call_soon_threadsafe(loop.stop),
    )

    browser = BrowserManager()
    watchdog = Watchdog(config.WATCHDOG_TIMEOUT)
    storage = SQLiteStorage(config.SUMMARY_DB_PATH)
    hub = _build_distributor_hub(storage)
    summary_scheduler = DailySummaryScheduler(storage, hub)
    deduplicator = MessageDeduplicator(hub.publish)
    connected_ws = set()

    try:
        await storage.start()
        await hub.start_all()
        await summary_scheduler.start()

        async with async_playwright() as playwright:
            page = await browser.launch(playwright)

            def handle_ws_frame(frame_data):
                watchdog.feed()
                try:
                    parsed = parse_socketio_payload(frame_data)
                    if not parsed:
                        return

                    delay_info = _format_delay_info(parsed)
                    logger.info(f"📦 原始解析消息: {json.dumps(parsed, ensure_ascii=False)}{delay_info}")

                    triggers_map = extract_triggers_map(parsed["data"])
                    for item in parsed["data"]:
                        deduplicator.process(item)

                    if triggers_map:
                        logger.info(f"🎯 动作提取简报: {triggers_map}")
                except Exception as e:
                    logger.error(f"❌ 处理 WS 数据时发生错误: {e}")

            def on_web_socket(ws):
                if "gmgn.ai/ws" in ws.url:
                    if ws.url not in connected_ws:
                        connected_ws.add(ws.url)
                        logger.success("[WS 建立连接] 监听中...")

                    watchdog.feed()
                    ws.on("framereceived", lambda frame: handle_ws_frame(frame))
                    ws.on("close", lambda _: connected_ws.discard(ws.url))

            async def handle_http_response(response):
                """拦截 Socket.io HTTP 降级轮询响应，防止 WS 重连间隙漏消息。"""
                try:
                    if "gmgn.ai/ws" not in response.url or "transport=polling" not in response.url:
                        return
                    if response.status != 200:
                        return

                    text = await response.text()
                    if '42["message"' not in text:
                        return

                    # Engine.IO v4 Polling 格式: "长度:消息内容长度:消息内容..."
                    idx = 0
                    while idx < len(text):
                        colon_idx = text.find(':', idx)
                        if colon_idx == -1:
                            break
                        length_str = text[idx:colon_idx]
                        if not length_str.isdigit():
                            break
                        msg_len = int(length_str)
                        msg_start = colon_idx + 1
                        msg_end = msg_start + msg_len
                        if msg_end > len(text):
                            break
                        msg_content = text[msg_start:msg_end]

                        if msg_content.startswith('42'):
                            # 复用 parse_socketio_payload，确保与 WS 通道完全一致的
                            # 频道过滤 (twitter_user_monitor_basic) + 字符串反序列化
                            parsed = parse_socketio_payload(msg_content)
                            if parsed:
                                watchdog.feed()
                                delay_info = _format_delay_info(parsed)
                                logger.info(f"📦 原始解析消息(Polling): {json.dumps(parsed, ensure_ascii=False)}{delay_info}")
                                triggers_map = extract_triggers_map(parsed["data"])
                                for item in parsed["data"]:
                                    deduplicator.process(item)
                                if triggers_map:
                                    logger.info(f"🎯 动作提取简报(Polling): {triggers_map}")

                        idx = msg_end
                except Exception as e:
                    logger.debug(f"Polling 响应解析跳过: {e}")

            page.on("websocket", on_web_socket)
            page.on("response", handle_http_response)

            await browser.run_first_login_if_needed()
            await browser.goto_monitor_page()
            await browser.handle_popups()
            await browser.switch_to_mine_tab()
            await browser.save_screenshot()

            logger.success(
                f"进入挂机监听模式... (已配置 {config.WATCHDOG_TIMEOUT}s 看门狗，按 Ctrl+C 终止)"
            )

            while True:
                await asyncio.sleep(config.WATCHDOG_POLL_INTERVAL)
                if watchdog.is_timed_out():
                    time_since_last_msg = watchdog.time_since_last_msg()
                    logger.warning(f"⚠️ 看门狗警报: {time_since_last_msg:.0f}秒内未收到任何WS消息，频道可能卡死断开！")
                    logger.info("尝试刷新整个网页结构...")
                    try:
                        await browser.recover_after_timeout()
                        watchdog.feed()
                    except Exception as e:
                        logger.error(f"刷新重连时发生异常: {e}")
    finally:
        await summary_scheduler.stop()
        await hub.stop_all()
        await storage.close()
        await browser.close()
        vdisplay.stop()
