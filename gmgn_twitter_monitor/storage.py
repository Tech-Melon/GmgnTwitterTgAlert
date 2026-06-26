"""SQLite persistence for normalized tweets and downstream deliveries."""

import asyncio
import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from loguru import logger


class SQLiteStorage:
    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()
        self._background_tasks: set[asyncio.Task] = set()
        self._closing = False

    async def start(self) -> None:
        self._closing = False
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        await self._execute_script(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS messages (
                internal_id TEXT PRIMARY KEY,
                tweet_id TEXT,
                action TEXT,
                author_handle TEXT,
                author_name TEXT,
                timestamp INTEGER,
                content_text TEXT,
                reference_text TEXT,
                tweet_url TEXT,
                raw_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS deliveries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                internal_id TEXT NOT NULL,
                platform TEXT NOT NULL,
                target_id TEXT NOT NULL,
                target_label TEXT,
                external_message_id TEXT,
                delivered_at INTEGER NOT NULL,
                UNIQUE(internal_id, platform, target_id)
            );
            CREATE INDEX IF NOT EXISTS idx_deliveries_target_time
                ON deliveries(platform, target_id, delivered_at);
            CREATE TABLE IF NOT EXISTS summary_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                summary_key TEXT NOT NULL,
                source_platform TEXT NOT NULL,
                source_target_id TEXT NOT NULL,
                window_start INTEGER NOT NULL,
                window_end INTEGER NOT NULL,
                generated_at INTEGER NOT NULL,
                status TEXT NOT NULL,
                item_count INTEGER NOT NULL DEFAULT 0,
                tg_sent INTEGER NOT NULL DEFAULT 0,
                feishu_sent INTEGER NOT NULL DEFAULT 0,
                content TEXT,
                error TEXT,
                UNIQUE(summary_key, source_platform, source_target_id, window_start, window_end)
            );
            """
        )
        await self._ensure_column("summary_runs", "tg_sent", "INTEGER NOT NULL DEFAULT 0")
        await self._ensure_column("summary_runs", "feishu_sent", "INTEGER NOT NULL DEFAULT 0")
        await self._migrate_summary_runs_unique_key()
        logger.success(f"🗄️ SQLite 存储已启动: {self.db_path}")

    async def close(self) -> None:
        if not self._conn:
            return
        self._closing = True
        await self.flush_background_writes()
        async with self._lock:
            self._conn.close()
            self._conn = None
        logger.info("🗄️ SQLite 存储已关闭")

    async def flush_background_writes(self) -> None:
        while self._background_tasks:
            tasks = list(self._background_tasks)
            logger.info(f"🗄️ 等待 {len(tasks)} 个后台写库任务完成")
            await asyncio.gather(*tasks, return_exceptions=True)

    def record_message_background(self, message: dict[str, Any]) -> None:
        if not self._conn or self._closing:
            return
        self._schedule_background_write(self.record_message(self._snapshot_message(message)))

    def record_delivery_background(
        self,
        message: dict[str, Any],
        platform: str,
        target_id: str,
        target_label: str = "",
        external_message_id: str | int | None = None,
    ) -> None:
        if not self._conn or self._closing:
            return
        message_snapshot = self._snapshot_message(message)
        self._schedule_background_write(
            self.record_delivery(
                message_snapshot,
                platform=platform,
                target_id=target_id,
                target_label=target_label,
                external_message_id=external_message_id,
            )
        )

    async def record_message(self, message: dict[str, Any]) -> None:
        if not self._conn:
            return
        internal_id = message.get("_internal_id") or message.get("internal_id")
        if not internal_id:
            return

        author = message.get("author") or {}
        content = message.get("content") or {}
        reference = message.get("reference") or {}
        payload = (
            internal_id,
            message.get("tweet_id") or "",
            message.get("action") or "",
            author.get("handle") or "",
            author.get("name") or "",
            int(message.get("timestamp") or 0),
            content.get("text") or "",
            reference.get("text") or "",
            self._build_tweet_url(message),
            json.dumps(message, ensure_ascii=False, default=str),
            int(time.time()),
        )
        await self._execute(
            """
            INSERT INTO messages (
                internal_id, tweet_id, action, author_handle, author_name,
                timestamp, content_text, reference_text, tweet_url, raw_json, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(internal_id) DO UPDATE SET
                tweet_id = excluded.tweet_id,
                action = excluded.action,
                author_handle = excluded.author_handle,
                author_name = excluded.author_name,
                timestamp = excluded.timestamp,
                content_text = excluded.content_text,
                reference_text = excluded.reference_text,
                tweet_url = excluded.tweet_url,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            """,
            payload,
        )

    async def record_delivery(
        self,
        message: dict[str, Any],
        platform: str,
        target_id: str,
        target_label: str = "",
        external_message_id: str | int | None = None,
    ) -> None:
        if not self._conn:
            return
        internal_id = message.get("_internal_id") or message.get("internal_id")
        if not internal_id or not target_id:
            return

        await self._execute(
            """
            INSERT OR IGNORE INTO deliveries (
                internal_id, platform, target_id, target_label,
                external_message_id, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                internal_id,
                platform,
                target_id,
                target_label,
                str(external_message_id or ""),
                int(time.time()),
            ),
        )

    async def fetch_delivered_messages(
        self,
        platform: str,
        target_id: str,
        window_start: int,
        window_end: int,
        limit: int,
    ) -> list[dict[str, Any]]:
        if not self._conn:
            return []
        rows = await self._fetchall(
            """
            SELECT
                d.delivered_at,
                m.internal_id,
                m.tweet_id,
                m.action,
                m.author_handle,
                m.author_name,
                m.timestamp,
                m.content_text,
                m.reference_text,
                m.tweet_url,
                m.raw_json
            FROM deliveries d
            JOIN messages m ON m.internal_id = d.internal_id
            WHERE d.platform = ?
              AND d.target_id = ?
              AND d.delivered_at >= ?
              AND d.delivered_at < ?
            ORDER BY d.delivered_at ASC
            LIMIT ?
            """,
            (platform, target_id, window_start, window_end, limit),
        )
        return [dict(row) for row in rows]

    async def count_delivered_messages(
        self,
        platform: str,
        target_id: str,
        window_start: int,
        window_end: int,
    ) -> int:
        if not self._conn:
            return 0
        rows = await self._fetchall(
            """
            SELECT COUNT(*) AS count
            FROM deliveries
            WHERE platform = ?
              AND target_id = ?
              AND delivered_at >= ?
              AND delivered_at < ?
            """,
            (platform, target_id, window_start, window_end),
        )
        return int(rows[0]["count"]) if rows else 0

    async def summary_run_exists(
        self,
        summary_key: str,
        source_platform: str,
        source_target_id: str,
        window_start: int,
        window_end: int,
    ) -> bool:
        rows = await self._fetchall(
            """
            SELECT 1 FROM summary_runs
            WHERE summary_key = ?
              AND source_platform = ?
              AND source_target_id = ?
              AND window_start = ?
              AND window_end = ?
              AND status IN ('sent_all', 'empty')
            LIMIT 1
            """,
            (summary_key, source_platform, source_target_id, window_start, window_end),
        )
        return bool(rows)

    async def get_summary_run(
        self,
        summary_key: str,
        source_platform: str,
        source_target_id: str,
        window_start: int,
        window_end: int,
    ) -> dict[str, Any] | None:
        rows = await self._fetchall(
            """
            SELECT * FROM summary_runs
            WHERE summary_key = ?
              AND source_platform = ?
              AND source_target_id = ?
              AND window_start = ?
              AND window_end = ?
            LIMIT 1
            """,
            (summary_key, source_platform, source_target_id, window_start, window_end),
        )
        return dict(rows[0]) if rows else None

    async def record_summary_run(
        self,
        summary_key: str,
        source_platform: str,
        source_target_id: str,
        window_start: int,
        window_end: int,
        status: str,
        item_count: int = 0,
        tg_sent: bool = False,
        feishu_sent: bool = False,
        content: str = "",
        error: str = "",
    ) -> None:
        await self._execute(
            """
            INSERT OR REPLACE INTO summary_runs (
                summary_key, source_platform, source_target_id,
                window_start, window_end, generated_at,
                status, item_count, tg_sent, feishu_sent, content, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                summary_key,
                source_platform,
                source_target_id,
                window_start,
                window_end,
                int(time.time()),
                status,
                item_count,
                1 if tg_sent else 0,
                1 if feishu_sent else 0,
                content,
                error,
            ),
        )

    @staticmethod
    def anonymize_target(target: str) -> str:
        return hashlib.sha256(target.encode("utf-8")).hexdigest()[:16]

    def _schedule_background_write(self, coro) -> None:
        if not self._conn or self._closing:
            coro.close()
            return
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._on_background_write_done)

    def _on_background_write_done(self, task: asyncio.Task) -> None:
        self._background_tasks.discard(task)
        try:
            task.result()
        except Exception as e:
            logger.warning(f"🗄️ 后台写库失败: {e}")

    @staticmethod
    def _snapshot_message(message: dict[str, Any]) -> dict[str, Any]:
        """Copy only fields needed by persistence, avoiding heavy/private runtime objects."""
        keep_keys = (
            "_internal_id",
            "internal_id",
            "tweet_id",
            "action",
            "timestamp",
            "author",
            "content",
            "reference",
            "bio_change",
            "avatar_change",
            "unfollow_target",
            "original_action",
        )
        snapshot = {key: message.get(key) for key in keep_keys if key in message}
        return json.loads(json.dumps(snapshot, ensure_ascii=False, default=str))

    async def _execute(self, sql: str, params: tuple = ()) -> None:
        if not self._conn:
            return
        async with self._lock:
            await asyncio.to_thread(self._execute_sync, sql, params)

    async def _fetchall(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        if not self._conn:
            return []
        async with self._lock:
            return await asyncio.to_thread(self._fetchall_sync, sql, params)

    async def _execute_script(self, sql: str) -> None:
        if not self._conn:
            return
        async with self._lock:
            await asyncio.to_thread(self._execute_script_sync, sql)

    async def _ensure_column(self, table: str, column: str, definition: str) -> None:
        if not self._conn:
            return
        async with self._lock:
            await asyncio.to_thread(self._ensure_column_sync, table, column, definition)

    async def _migrate_summary_runs_unique_key(self) -> None:
        if not self._conn:
            return
        async with self._lock:
            await asyncio.to_thread(self._migrate_summary_runs_unique_key_sync)

    def _execute_sync(self, sql: str, params: tuple) -> None:
        self._conn.execute(sql, params)
        self._conn.commit()

    def _fetchall_sync(self, sql: str, params: tuple) -> list[sqlite3.Row]:
        cur = self._conn.execute(sql, params)
        return cur.fetchall()

    def _execute_script_sync(self, sql: str) -> None:
        self._conn.executescript(sql)
        self._conn.commit()

    def _ensure_column_sync(self, table: str, column: str, definition: str) -> None:
        rows = self._conn.execute(f"PRAGMA table_info({table})").fetchall()
        if any(row["name"] == column for row in rows):
            return
        self._conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        self._conn.commit()

    def _migrate_summary_runs_unique_key_sync(self) -> None:
        index_rows = self._conn.execute("PRAGMA index_list(summary_runs)").fetchall()
        has_old_unique = False
        for index_row in index_rows:
            index_name = index_row["name"]
            if not index_row["unique"]:
                continue
            cols = [
                row["name"]
                for row in self._conn.execute(f"PRAGMA index_info({index_name})").fetchall()
            ]
            if cols == ["summary_key", "window_start", "window_end"]:
                has_old_unique = True
                break

        if not has_old_unique:
            return

        logger.info("🗄️ 迁移 summary_runs 唯一键，加入 source_platform/source_target_id")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS summary_runs_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                summary_key TEXT NOT NULL,
                source_platform TEXT NOT NULL,
                source_target_id TEXT NOT NULL,
                window_start INTEGER NOT NULL,
                window_end INTEGER NOT NULL,
                generated_at INTEGER NOT NULL,
                status TEXT NOT NULL,
                item_count INTEGER NOT NULL DEFAULT 0,
                tg_sent INTEGER NOT NULL DEFAULT 0,
                feishu_sent INTEGER NOT NULL DEFAULT 0,
                content TEXT,
                error TEXT,
                UNIQUE(summary_key, source_platform, source_target_id, window_start, window_end)
            );
            INSERT OR REPLACE INTO summary_runs_new (
                id, summary_key, source_platform, source_target_id,
                window_start, window_end, generated_at, status, item_count,
                tg_sent, feishu_sent, content, error
            )
            SELECT
                id, summary_key, source_platform, source_target_id,
                window_start, window_end, generated_at, status, item_count,
                COALESCE(tg_sent, 0), COALESCE(feishu_sent, 0), content, error
            FROM summary_runs;
            DROP TABLE summary_runs;
            ALTER TABLE summary_runs_new RENAME TO summary_runs;
            """
        )
        self._conn.commit()

    @staticmethod
    def _build_tweet_url(message: dict[str, Any]) -> str:
        action = message.get("action", "")
        handle = (message.get("author") or {}).get("handle") or ""
        tweet_id = message.get("tweet_id") or ""
        reference = message.get("reference") or {}
        ref_handle = reference.get("author_handle") or ""
        ref_tweet_id = reference.get("tweet_id") or ""

        if action in ("tweet", "reply", "quote", "pin", "unpin") and tweet_id and handle:
            return f"https://x.com/{handle}/status/{tweet_id}"
        if action in ("repost", "delete_post"):
            if ref_handle and ref_tweet_id:
                return f"https://x.com/{ref_handle}/status/{ref_tweet_id}"
            if tweet_id and handle:
                return f"https://x.com/{handle}/status/{tweet_id}"
        if action in ("photo", "description", "name") and handle:
            return f"https://x.com/{handle}"
        return ""
