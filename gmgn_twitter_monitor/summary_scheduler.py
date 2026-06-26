"""Twice-daily channel summary scheduler."""

import asyncio
from datetime import datetime, timedelta, timezone

from loguru import logger

from . import config
from .distributor import FeishuDistributor, TelegramDistributor
from .summarizer import summarize_channel_tweets

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


class DailySummaryScheduler:
    def __init__(self, storage, hub):
        self.storage = storage
        self.hub = hub
        self._task: asyncio.Task | None = None
        self._tz = self._load_timezone()

    async def start(self) -> None:
        if not config.SUMMARY_ENABLE:
            logger.info("🧾 定时频道总结未启用")
            return
        if not config.SUMMARY_CHANNELS:
            logger.warning("🧾 定时频道总结已启用，但未解析到 SUMMARY_CHANNELS")
            return
        if not config.SUMMARY_TIMES:
            logger.warning("🧾 定时频道总结已启用，但 SUMMARY_TIMES 为空")
            return

        await self._catch_up_missed_window()
        self._task = asyncio.create_task(self._run_loop())
        logger.success(
            f"🧾 定时频道总结已启动 (时间: {', '.join(config.SUMMARY_TIMES)}, "
            f"频道: {', '.join(c['key'] for c in config.SUMMARY_CHANNELS)})"
        )

    async def stop(self) -> None:
        if not self._task:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        logger.info("🧾 定时频道总结已关闭")

    async def _run_loop(self) -> None:
        while True:
            now = datetime.now(self._tz)
            run_at = self._next_run_at(now)
            wait_seconds = max(1, (run_at - now).total_seconds())
            logger.info(f"🧾 下一次频道总结: {run_at.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            await asyncio.sleep(wait_seconds)

            window_start, window_end = self._window_for_run(run_at)
            for summary_conf in config.SUMMARY_CHANNELS:
                try:
                    await self._run_summary(summary_conf, window_start, window_end)
                except Exception as e:
                    logger.error(f"🧾 频道总结任务异常 ({summary_conf.get('key')}): {e}")

    async def _run_summary(self, summary_conf: dict, start_dt: datetime, end_dt: datetime) -> None:
        summary_key = summary_conf["key"]
        source_platform = summary_conf.get("source_platform", "telegram")
        source_target_id = summary_conf["source_target_id"]
        window_start = int(start_dt.timestamp())
        window_end = int(end_dt.timestamp())

        await self.storage.flush_background_writes()

        if await self.storage.summary_run_exists(
            summary_key, source_platform, source_target_id, window_start, window_end
        ):
            logger.info(f"🧾 频道总结已生成过，跳过: {summary_key} {start_dt} -> {end_dt}")
            return

        existing_run = await self.storage.get_summary_run(
            summary_key, source_platform, source_target_id, window_start, window_end
        )
        existing_tg_sent = bool(existing_run and existing_run.get("tg_sent"))
        existing_feishu_sent = bool(existing_run and existing_run.get("feishu_sent"))
        summary_text = (existing_run or {}).get("content") or ""
        total_count = await self.storage.count_delivered_messages(
            source_platform,
            source_target_id,
            window_start,
            window_end,
        )

        items = await self.storage.fetch_delivered_messages(
            source_platform,
            source_target_id,
            window_start,
            window_end,
            config.SUMMARY_MAX_TWEETS,
        )
        if not items:
            await self.storage.record_summary_run(
                summary_key,
                source_platform,
                source_target_id,
                window_start,
                window_end,
                status="empty",
                item_count=0,
            )
            logger.info(f"🧾 频道总结无新内容，跳过推送: {summary_key}")
            return

        truncated = total_count > len(items)
        if not summary_text:
            summary_text = await summarize_channel_tweets(
                summary_conf.get("label") or summary_key,
                items,
                window_start,
                window_end,
                total_count=total_count,
                truncated=truncated,
            )
        if not summary_text:
            await self.storage.record_summary_run(
                summary_key,
                source_platform,
                source_target_id,
                window_start,
                window_end,
                status="failed",
                item_count=len(items),
                tg_sent=existing_tg_sent,
                feishu_sent=existing_feishu_sent,
                error="AI summary returned empty",
            )
            return

        title = f"{summary_conf.get('label') or summary_key} 频道摘要"
        tg_ok, fs_ok = await self._send_summary(
            summary_conf,
            title,
            summary_text,
            skip_tg=existing_tg_sent,
            skip_feishu=existing_feishu_sent,
        )
        final_tg_sent = existing_tg_sent or tg_ok or not summary_conf.get("target_tg_channel_id")
        final_feishu_sent = existing_feishu_sent or fs_ok or not summary_conf.get("target_feishu_webhook")
        status = "sent_all" if final_tg_sent and final_feishu_sent else "partial"
        await self.storage.record_summary_run(
            summary_key,
            source_platform,
            source_target_id,
            window_start,
            window_end,
            status=status,
            item_count=len(items),
            tg_sent=final_tg_sent,
            feishu_sent=final_feishu_sent,
            content=summary_text,
        )
        logger.info(f"🧾 频道总结完成: {summary_key} items={len(items)} status={status}")

    async def _send_summary(
        self,
        summary_conf: dict,
        title: str,
        text: str,
        skip_tg: bool = False,
        skip_feishu: bool = False,
    ) -> tuple[bool, bool]:
        tg_ok = False
        fs_ok = False
        tg_channel_id = summary_conf.get("target_tg_channel_id")
        fs_webhook = summary_conf.get("target_feishu_webhook")
        fs_secret = summary_conf.get("target_feishu_secret", "")

        send_tasks = []
        send_targets = []
        for distributor in self.hub.distributors:
            if tg_channel_id and not skip_tg and isinstance(distributor, TelegramDistributor):
                send_tasks.append(distributor.send_summary(tg_channel_id, text))
                send_targets.append("telegram")
            if fs_webhook and not skip_feishu and isinstance(distributor, FeishuDistributor):
                send_tasks.append(distributor.send_summary(fs_webhook, fs_secret, title, text))
                send_targets.append("feishu")

        if not send_tasks:
            logger.warning("🧾 未找到可用的 TG/飞书分发器，摘要无法推送")
            return False, False

        results = await asyncio.gather(*send_tasks, return_exceptions=True)
        for target, result in zip(send_targets, results):
            ok = result is True
            if target == "telegram":
                tg_ok = ok
            elif target == "feishu":
                fs_ok = ok
            if isinstance(result, Exception):
                logger.error(f"🧾 {target} 摘要推送异常: {result}")
        return tg_ok, fs_ok

    async def _catch_up_missed_window(self) -> None:
        now = datetime.now(self._tz)
        last_run_at = self._previous_run_at(now)
        window_start, window_end = self._window_for_run(last_run_at)
        for summary_conf in config.SUMMARY_CHANNELS:
            try:
                await self._run_summary(summary_conf, window_start, window_end)
            except Exception as e:
                logger.error(f"🧾 启动补偿总结异常 ({summary_conf.get('key')}): {e}")

    def _next_run_at(self, now: datetime) -> datetime:
        candidates = []
        for time_str in config.SUMMARY_TIMES:
            hour, minute = self._parse_time(time_str)
            candidate = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if candidate <= now:
                candidate += timedelta(days=1)
            candidates.append(candidate)
        return min(candidates)

    def _previous_run_at(self, now: datetime) -> datetime:
        candidates = []
        for day_delta in (-1, 0):
            base = now.date() + timedelta(days=day_delta)
            for time_str in config.SUMMARY_TIMES:
                hour, minute = self._parse_time(time_str)
                candidate = datetime(base.year, base.month, base.day, hour, minute, tzinfo=self._tz)
                if candidate < now:
                    candidates.append(candidate)
        return max(candidates)

    def _window_for_run(self, run_at: datetime) -> tuple[datetime, datetime]:
        all_slots = []
        for day_delta in (-1, 0):
            base = run_at.date() + timedelta(days=day_delta)
            for time_str in config.SUMMARY_TIMES:
                hour, minute = self._parse_time(time_str)
                all_slots.append(
                    datetime(base.year, base.month, base.day, hour, minute, tzinfo=self._tz)
                )
        previous_slots = [slot for slot in all_slots if slot < run_at]
        return max(previous_slots), run_at

    @staticmethod
    def _parse_time(time_str: str) -> tuple[int, int]:
        hour_str, minute_str = time_str.split(":", 1)
        return int(hour_str), int(minute_str)

    @staticmethod
    def _load_timezone():
        if ZoneInfo:
            try:
                return ZoneInfo(config.SUMMARY_TIMEZONE)
            except Exception:
                logger.warning(f"🧾 无法加载时区 {config.SUMMARY_TIMEZONE}，使用 UTC+8")
        return timezone(timedelta(hours=8))
