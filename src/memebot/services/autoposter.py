from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import shutil
import tempfile
import time
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Any, Dict, List, Optional

from http import cookies

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import FSInputFile
import httpx

from memebot.content_sources.base import ContentItem, ContentSourceFactory
from memebot.content_sources.pinterest import PinterestClient
from memebot.content_sources.pinterest_rss import PinterestRssSource  # noqa: F401  # register
from memebot.content_sources.pinterest_search import PinterestSearchSource  # noqa: F401
from memebot.content_sources.pinterest_board_ideas import PinterestBoardIdeasSource  # noqa: F401
from memebot.content_sources.spotify import SpotifyClient
from memebot.db import Database
from memebot.services.voting import build_vote_keyboard
from memebot.config import Settings, get_timezone

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
        self._tz = get_timezone()
        self._source_cache: Dict[int, Any] = {}
        self._source_signature: Dict[int, tuple[str, str]] = {}

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
        now_ts = time.time()
        now_dt = datetime.now(self._tz)
        channels = await self.db.iter_channels()
        for channel in channels:
            if not channel["enabled"]:
                continue
            if not self._is_within_window(now_dt):
                continue
            last = self._last_run.get(channel["id"], 0)
            if now_ts - last < channel["autopost_interval"]:
                continue
            content_source_key = channel["content_source"]
            source_config = json.loads(channel["content_config"] or "{}")
            source_kwargs = await self._build_source_kwargs(content_source_key, channel, source_config)
            if source_kwargs is None:
                logger.warning("Skipping channel %s because source isn't configured", channel["telegram_channel_id"])
                continue
            try:
                source = self._get_or_create_source(channel["id"], content_source_key, source_kwargs)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception("Failed to init source %s", content_source_key)
                continue
            queue_target = max(self.settings.max_posts_per_run * 2, self.settings.max_posts_per_run)
            pending = await self.db.count_pending_posts(channel["id"])
            pending = await self._ensure_queue(channel, source, queue_target, pending)
            posted = await self._publish_from_queue(channel, self.settings.max_posts_per_run)
            if posted:
                self._last_run[channel["id"]] = now_ts
            else:
                logger.info(
                    "No posts published for %s (pending queue=%s)",
                    channel["telegram_channel_id"],
                    pending,
                )

    def _is_within_window(self, now: datetime) -> bool:
        start = self.settings.posting_start_hour
        end = self.settings.posting_end_hour
        hour = now.hour
        if start <= end:
            return start <= hour < end
        # window spans midnight
        return hour >= start or hour < end

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
        if key == "pinterest_rss":
            feed_url = config.get("feed_url")
            if not feed_url:
                logger.error("Pinterest RSS source requires feed_url")
                return None
            return {"feed_url": feed_url, "limit": config.get("limit")}
        if key == "pinterest_search":
            query = config.get("query") or self.settings.pinterest_recommendation_query
            if not query:
                logger.error("Pinterest search source requires query")
                return None
            cookie = self.settings.pinterest_cookie
            if not cookie:
                logger.error("Pinterest search requires PINTEREST_COOKIE in environment")
                return None
            return {
                "query": query,
                "locale": config.get("locale", "ru-RU"),
                "cookie_header": cookie,
                "user_agent": self.settings.pinterest_user_agent,
            }
        if key == "pinterest_board_ideas":
            board_id = (
                config.get("board_id")
                or channel["pinterest_board_id"]
                or self.settings.pinterest_board_id
            )
            if not board_id:
                logger.error("Pinterest board ideas source requires board_id")
                return None
            cookie = self.settings.pinterest_cookie
            if not cookie:
                logger.error("Pinterest board ideas requires PINTEREST_COOKIE")
                return None
            return {
                "board_id": board_id,
                "cookie_header": cookie,
                "locale": config.get("locale", "ru-RU"),
                "user_agent": self.settings.pinterest_user_agent,
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

    def _get_or_create_source(self, channel_id: int, key: str, kwargs: Dict[str, Any]):
        signature = json.dumps(kwargs, sort_keys=True, default=str)
        cached_sig = self._source_signature.get(channel_id)
        cached = self._source_cache.get(channel_id)
        if cached and cached_sig == (key, signature):
            return cached
        source = ContentSourceFactory.create(key, **kwargs)
        self._source_cache[channel_id] = source
        self._source_signature[channel_id] = (key, signature)
        return source

    async def _fetch_items(self, source, limit: int) -> List[ContentItem]:
        try:
            items = await source.fetch(limit=limit)
            return items
        except Exception:
            logger.exception("Failed to fetch content from %s", getattr(source, "name", "unknown"))
            return []

    async def _ensure_queue(self, channel_row, source, queue_target: int, pending: int) -> int:
        if pending >= queue_target:
            return pending
        while pending < queue_target:
            needed = queue_target - pending
            fetch_amount = max(
                needed * 3,
                self.settings.max_posts_per_run * 10,
                30,
            )
            batch = await self._fetch_items(source, fetch_amount)
            if not batch:
                break
            new_added = 0
            for item in batch:
                if await self._enqueue_item(channel_row, item):
                    pending += 1
                    new_added += 1
                    if pending >= queue_target:
                        break
            if new_added == 0:
                logger.debug("Queue refill produced no new items for %s", channel_row["telegram_channel_id"])
                break
        return pending

    async def _enqueue_item(self, channel_row, item: ContentItem) -> bool:
        if not item.media_url and not item.video_url:
            return False
        payload = self._serialize_item(item)
        content_item_id = await self.db.upsert_content_item(
            source_type=item.source_type,
            source_id=item.source_id,
            payload=payload,
        )
        existing_post_id = await self.db.get_unposted_item(channel_row["id"], content_item_id)
        if existing_post_id:
            logger.debug("Item %s already queued for %s", item.source_id, channel_row["telegram_channel_id"])
            return False
        await self.db.create_post(channel_row["id"], content_item_id)
        return True

    def _serialize_item(self, item: ContentItem) -> Dict[str, Any]:
        return {
            "title": item.title,
            "caption": item.caption,
            "media_url": item.media_url,
            "media_type": item.media_type,
            "video_url": item.video_url,
            "permalink": item.permalink,
            "extra": item.extra,
        }

    async def _publish_from_queue(self, channel_row, max_posts: int) -> int:
        pending_posts = await self.db.fetch_pending_posts(channel_row["id"], max_posts)
        posted = 0
        for row in pending_posts:
            item = self._deserialize_item(row)
            if await self._send_post(channel_row, row["id"], item):
                posted += 1
        return posted

    def _deserialize_item(self, row) -> ContentItem:
        payload = json.loads(row["content_payload"])
        return ContentItem(
            source_type=row["source_type"],
            source_id=row["source_id"],
            title=payload.get("title"),
            caption=payload.get("caption"),
            media_url=payload.get("media_url"),
            media_type=payload.get("media_type", "photo"),
            video_url=payload.get("video_url"),
            permalink=payload.get("permalink"),
            extra=payload.get("extra") or {},
        )

    @staticmethod
    def _is_supported_video(url: Optional[str]) -> bool:
        if not url:
            return False
        clean = url.split("?", 1)[0].lower()
        return clean.endswith(".mp4")

    async def _download_video_direct(self, url: str) -> Optional[Path]:
        temp_dir = Path(tempfile.mkdtemp(prefix="memebot_video_"))
        target = temp_dir / "video.mp4"
        headers = {
            "User-Agent": self.settings.pinterest_user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "*/*",
        }
        cookie_header = self.settings.pinterest_cookie
        cookie_jar = None
        if cookie_header:
            cookie_jar = httpx.Cookies()
            simple = cookies.SimpleCookie()
            simple.load(cookie_header)
            for key, morsel in simple.items():
                cookie_jar.set(key, morsel.value)
        try:
            async with httpx.AsyncClient(
                timeout=60,
                headers=headers,
                cookies=cookie_jar,
                follow_redirects=True,
            ) as client:
                async with client.stream("GET", url) as resp:
                    resp.raise_for_status()
                    with target.open("wb") as fh:
                        async for chunk in resp.aiter_bytes():
                            if chunk:
                                fh.write(chunk)
        except Exception:
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
        return target

    async def _download_and_convert_video(self, url: str) -> Optional[Path]:
        if not shutil.which("ffmpeg"):
            logger.error("ffmpeg is not installed; cannot convert %s", url)
            return None
        temp_dir = Path(tempfile.mkdtemp(prefix="memebot_video_"))
        output = temp_dir / "video.mp4"
        cmd = [
            "ffmpeg",
            "-loglevel",
            "error",
            "-y",
            "-i",
            url,
            "-c",
            "copy",
            str(output),
        ]
        try:
            proc = await asyncio.create_subprocess_exec(*cmd)
            await proc.communicate()
        except FileNotFoundError:
            logger.error("ffmpeg binary not found in PATH")
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
        if proc.returncode != 0 or not output.exists():
            logger.error("ffmpeg failed (%s) while converting %s", proc.returncode, url)
            shutil.rmtree(temp_dir, ignore_errors=True)
            return None
        return output

    async def _prepare_video_file(self, url: str) -> Optional[Path]:
        local_file: Optional[Path] = None
        if self._is_supported_video(url):
            local_file = await self._download_video_direct(url)
        if local_file:
            return local_file
        return await self._download_and_convert_video(url)

    async def _send_post(self, channel_row, post_id: int, item: ContentItem) -> bool:
        caption = self._build_caption(item)
        markup = build_vote_keyboard(post_id, 0, 0)
        temp_file: Optional[Path] = None
        temp_dir: Optional[Path] = None
        if item.video_url:
            temp_file = await self._prepare_video_file(item.video_url)
            if not temp_file:
                logger.warning("Skipping video %s: cannot download stream", item.source_id)
                await self.db.mark_failed(post_id)
                return False
            temp_dir = temp_file.parent
            send_method = self.bot.send_video
            media_kwargs = {"video": FSInputFile(temp_file, filename=temp_file.name)}
        else:
            media_url = item.media_url
            if not media_url:
                logger.error("Post %s has no media URL, skipping", item.source_id)
                await self.db.mark_failed(post_id)
                return False
            send_method = self.bot.send_photo
            media_kwargs = {"photo": media_url}
        try:
            message = await send_method(
                chat_id=channel_row["telegram_channel_id"],
                caption=caption,
                reply_markup=markup,
                **media_kwargs,
            )
        except TelegramBadRequest as err:
            logger.error("Unable to send post to %s: %s", channel_row["telegram_channel_id"], err)
            await self.db.mark_failed(post_id)
            return False
        finally:
            if temp_file:
                try:
                    if temp_file.exists():
                        temp_file.unlink()
                finally:
                    if temp_dir:
                        shutil.rmtree(temp_dir, ignore_errors=True)
        audience_size = None
        try:
            audience_size = await self.bot.get_chat_member_count(message.chat.id)
        except TelegramBadRequest:
            logger.debug("Cannot fetch member count for %s", message.chat.id)
        await self.db.mark_posted(
            post_id,
            telegram_chat_id=str(message.chat.id),
            telegram_message_id=message.message_id,
            audience_size=audience_size,
        )
        logger.info("Posted %s to %s", item.source_id, channel_row["telegram_channel_id"])
        return True

    def _build_caption(self, item: ContentItem) -> str:
        caption = escape(item.caption or item.title or "")
        if item.permalink:
            caption = caption + f'\n<a href="{item.permalink}">Источник</a>'
        return caption or "Новый мем"
