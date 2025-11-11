from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from typing import Any, Dict, List, Optional
from html import escape

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest

from memebot.content_sources.base import ContentItem, ContentSourceFactory
from memebot.content_sources.pinterest import PinterestClient
from memebot.content_sources.spotify import SpotifyClient
from memebot.db import Database
from memebot.services.voting import build_vote_keyboard
from memebot.config import Settings

logger = logging.getLogger(__name__)


class AutoPoster:
    def __init__(
        self,
        db: Database,
        bot: Bot,
        settings: Settings,
        pinterest_client: Optional[PinterestClient] = None,
        spotify_client: Optional[SpotifyClient] = None,
    ) -> None:
        self.db = db
        self.bot = bot
        self.settings = settings
        self.pinterest_client = pinterest_client
        self.spotify_client = spotify_client
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()
        self._last_run: Dict[int, float] = {}

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._runner())
        logger.info("AutoPoster started")

    async def stop(self) -> None:
        if self._task:
            self._stop_event.set()
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            logger.info("AutoPoster stopped")

    async def _runner(self) -> None:
        while not self._stop_event.is_set():
            try:
                await self.tick()
            except asyncio.CancelledError:  # pragma: no cover - task shutdown
                raise
            except Exception:
                logger.exception("AutoPoster tick failed")
            await asyncio.sleep(self.settings.posting_interval_seconds)

    async def tick(self) -> None:
        now = time.time()
        channels = await self.db.iter_channels()
        for channel in channels:
            if not channel["enabled"]:
                continue
            last = self._last_run.get(channel["id"], 0)
            if now - last < channel["autopost_interval"]:
                continue
            content_source_key = channel["content_source"]
            source_config = json.loads(channel["content_config"] or "{}")
            source_kwargs = await self._build_source_kwargs(content_source_key, channel, source_config)
            if source_kwargs is None:
                logger.warning("Skipping channel %s because source isn't configured", channel["telegram_channel_id"])
                continue
            try:
                source = ContentSourceFactory.create(content_source_key, **source_kwargs)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to init source %s", content_source_key)
                continue
            items = await self._fetch_items(source, self.settings.max_posts_per_run)
            for item in items:
                await self._publish_if_needed(channel, item)
            self._last_run[channel["id"]] = now

    async def _build_source_kwargs(
        self,
        key: str,
        channel: Dict[str, Any],
        config: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        if key == "pinterest":
            if not self.pinterest_client:
                logger.error("Pinterest client is not configured")
                return None
            query = config.get("query") or self.settings.pinterest_recommendation_query
            board_id = channel["pinterest_board_id"] or self.settings.pinterest_board_id
            section_id = channel["pinterest_section_id"] or self.settings.pinterest_section_id
            return {
                "client": self.pinterest_client,
                "query": query,
                "board_id": board_id,
                "section_id": section_id,
            }
        if key == "spotify_playlist":
            if not self.spotify_client:
                logger.error("Spotify client is not configured")
                return None
            playlist_id = config.get("playlist_id")
            if not playlist_id:
                logger.error("Spotify source requires playlist_id")
                return None
            return {
                "client": self.spotify_client,
                "playlist_id": playlist_id,
                "caption_template": config.get("caption_template"),
            }
        # Other sources can return config directly
        return config

    async def _fetch_items(self, source, limit: int) -> List[ContentItem]:
        try:
            items = await source.fetch(limit=limit)
            return items
        except Exception:
            logger.exception("Failed to fetch content from %s", getattr(source, "name", "unknown"))
            return []

    async def _publish_if_needed(self, channel_row, item: ContentItem) -> None:
        content_item_id = await self.db.upsert_content_item(
            source_type=item.source_type,
            source_id=item.source_id,
            payload={
                "title": item.title,
                "caption": item.caption,
                "media_url": item.media_url,
                "permalink": item.permalink,
                "extra": item.extra,
            },
        )
        existing_post_id = await self.db.get_unposted_item(channel_row["id"], content_item_id)
        if existing_post_id:
            logger.debug("Skipping already posted item %s", item.source_id)
            return
        post_id = await self.db.create_post(channel_row["id"], content_item_id)
        caption = self._build_caption(item)
        markup = build_vote_keyboard(post_id, 0, 0)
        try:
            message = await self.bot.send_photo(
                chat_id=channel_row["telegram_channel_id"],
                photo=item.media_url,
                caption=caption,
                reply_markup=markup,
            )
        except TelegramBadRequest as err:
            logger.error("Unable to send post to %s: %s", channel_row["telegram_channel_id"], err)
            return
        await self.db.mark_posted(post_id, str(message.chat.id), message.message_id)
        logger.info("Posted %s to %s", item.source_id, channel_row["telegram_channel_id"])

    def _build_caption(self, item: ContentItem) -> str:
        caption = escape(item.caption or item.title or "")
        if item.permalink:
            caption = caption + f'\n<a href="{item.permalink}">Источник</a>'
        return caption or "Новый мем"
