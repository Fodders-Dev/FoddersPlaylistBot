from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List, Optional

import aiosqlite


class Database:
    def __init__(self, path: Path | str):
        self.path = Path(path)
        self._conn: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        self._conn.row_factory = aiosqlite.Row
        await self._conn.execute("PRAGMA foreign_keys = ON;")
        await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database is not connected")
        return self._conn

    async def init_schema(self) -> None:
        await self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_channel_id TEXT NOT NULL,
                telegram_channel_name TEXT,
                content_source TEXT NOT NULL,
                content_config TEXT NOT NULL,
                autopost_interval INTEGER NOT NULL DEFAULT 900,
                like_threshold INTEGER NOT NULL DEFAULT 20,
                dislike_threshold INTEGER NOT NULL DEFAULT -10,
                pinterest_board_id TEXT,
                pinterest_section_id TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(telegram_channel_id, content_source)
            );

            CREATE TABLE IF NOT EXISTS content_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_type, source_id)
            );

            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id INTEGER NOT NULL,
                content_item_id INTEGER NOT NULL,
                telegram_message_id INTEGER,
                telegram_chat_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                posted_at TIMESTAMP,
                pinned_at TIMESTAMP,
                quarantine_at TIMESTAMP,
                FOREIGN KEY(channel_id) REFERENCES channels(id) ON DELETE CASCADE,
                FOREIGN KEY(content_item_id) REFERENCES content_items(id) ON DELETE CASCADE,
                UNIQUE(channel_id, content_item_id)
            );

            CREATE TABLE IF NOT EXISTS votes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id INTEGER NOT NULL,
                telegram_user_id TEXT NOT NULL,
                vote INTEGER NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(post_id, telegram_user_id),
                FOREIGN KEY(post_id) REFERENCES posts(id) ON DELETE CASCADE
            );
            """
        )
        await self.conn.commit()

    async def add_channel(
        self,
        telegram_channel_id: str,
        content_source: str,
        content_config: dict[str, Any],
        telegram_channel_name: str | None = None,
        autopost_interval: int = 900,
        like_threshold: int = 20,
        dislike_threshold: int = -10,
        pinterest_board_id: str | None = None,
        pinterest_section_id: str | None = None,
    ) -> int:
        cursor = await self.conn.execute(
            """
            INSERT INTO channels (
                telegram_channel_id, telegram_channel_name, content_source, content_config,
                autopost_interval, like_threshold, dislike_threshold, pinterest_board_id,
                pinterest_section_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(telegram_channel_id, content_source) DO UPDATE SET
                content_config=excluded.content_config,
                autopost_interval=excluded.autopost_interval,
                like_threshold=excluded.like_threshold,
                dislike_threshold=excluded.dislike_threshold,
                pinterest_board_id=excluded.pinterest_board_id,
                pinterest_section_id=excluded.pinterest_section_id,
                updated_at=CURRENT_TIMESTAMP
            RETURNING id;
            """,
            (
                telegram_channel_id,
                telegram_channel_name,
                content_source,
                json.dumps(content_config),
                autopost_interval,
                like_threshold,
                dislike_threshold,
                pinterest_board_id,
                pinterest_section_id,
            ),
        )
        row = await cursor.fetchone()
        await self.conn.commit()
        return int(row[0])

    async def iter_channels(self) -> List[aiosqlite.Row]:
        cursor = await self.conn.execute(
            "SELECT * FROM channels WHERE enabled = 1 ORDER BY id;"
        )
        rows = await cursor.fetchall()
        return rows

    async def upsert_content_item(self, source_type: str, source_id: str, payload: dict[str, Any]) -> int:
        cursor = await self.conn.execute(
            """
            INSERT INTO content_items (source_type, source_id, payload)
            VALUES (?, ?, ?)
            ON CONFLICT(source_type, source_id) DO UPDATE SET
                payload=excluded.payload
            RETURNING id;
            """,
            (source_type, source_id, json.dumps(payload)),
        )
        row = await cursor.fetchone()
        await self.conn.commit()
        return int(row[0])

    async def get_unposted_item(
        self,
        channel_id: int,
        content_item_id: int,
    ) -> Optional[int]:
        cursor = await self.conn.execute(
            "SELECT id FROM posts WHERE channel_id = ? AND content_item_id = ?;",
            (channel_id, content_item_id),
        )
        row = await cursor.fetchone()
        return int(row[0]) if row else None

    async def create_post(self, channel_id: int, content_item_id: int) -> int:
        cursor = await self.conn.execute(
            """
            INSERT INTO posts (channel_id, content_item_id)
            VALUES (?, ?)
            ON CONFLICT(channel_id, content_item_id) DO UPDATE SET
                status='pending'
            RETURNING id;
            """,
            (channel_id, content_item_id),
        )
        row = await cursor.fetchone()
        await self.conn.commit()
        return int(row[0])

    async def mark_posted(
        self,
        post_id: int,
        telegram_chat_id: str,
        telegram_message_id: int,
    ) -> None:
        await self.conn.execute(
            """
            UPDATE posts
            SET status='posted', telegram_chat_id=?, telegram_message_id=?, posted_at=CURRENT_TIMESTAMP
            WHERE id=?;
            """,
            (telegram_chat_id, telegram_message_id, post_id),
        )
        await self.conn.commit()

    async def set_pinned(self, post_id: int) -> None:
        await self.conn.execute(
            "UPDATE posts SET status='pinned', pinned_at=CURRENT_TIMESTAMP WHERE id=?;",
            (post_id,),
        )
        await self.conn.commit()

    async def set_quarantined(self, post_id: int) -> None:
        await self.conn.execute(
            "UPDATE posts SET status='quarantined', quarantine_at=CURRENT_TIMESTAMP WHERE id=?;",
            (post_id,),
        )
        await self.conn.commit()

    async def record_vote(self, post_id: int, telegram_user_id: str, vote: int) -> None:
        await self.conn.execute(
            """
            INSERT INTO votes (post_id, telegram_user_id, vote)
            VALUES (?, ?, ?)
            ON CONFLICT(post_id, telegram_user_id) DO UPDATE SET
                vote=excluded.vote,
                updated_at=CURRENT_TIMESTAMP;
            """,
            (post_id, telegram_user_id, vote),
        )
        await self.conn.commit()

    async def aggregate_votes(self, post_id: int) -> tuple[int, int]:
        cursor = await self.conn.execute(
            "SELECT vote, COUNT(1) as c FROM votes WHERE post_id=? GROUP BY vote;",
            (post_id,),
        )
        rows = await cursor.fetchall()
        likes = sum(row[1] for row in rows if row[0] > 0)
        dislikes = sum(row[1] for row in rows if row[0] < 0)
        return likes, dislikes

    async def fetch_post_by_message(self, chat_id: str, message_id: int) -> Optional[aiosqlite.Row]:
        cursor = await self.conn.execute(
            "SELECT * FROM posts WHERE telegram_chat_id=? AND telegram_message_id=?;",
            (chat_id, message_id),
        )
        return await cursor.fetchone()

    async def fetch_post(self, post_id: int) -> Optional[aiosqlite.Row]:
        cursor = await self.conn.execute(
            "SELECT p.*, c.like_threshold, c.dislike_threshold, c.pinterest_board_id, c.pinterest_section_id "
            "FROM posts p JOIN channels c ON p.channel_id = c.id WHERE p.id=?;",
            (post_id,),
        )
        return await cursor.fetchone()

    async def fetch_content_item(self, content_item_id: int) -> Optional[aiosqlite.Row]:
        cursor = await self.conn.execute(
            "SELECT * FROM content_items WHERE id=?;",
            (content_item_id,),
        )
        return await cursor.fetchone()
