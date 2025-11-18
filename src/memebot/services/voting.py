from __future__ import annotations

import base64
import json
import logging
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from memebot.content_sources.pinterest import PinterestClient
from memebot.db import Database
from memebot.services.pinterest_web import PinterestWebClient
from memebot.utils.http import download_binary

logger = logging.getLogger(__name__)


def build_vote_keyboard(post_id: int, likes: int, dislikes: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text=f"🔥 {likes}", callback_data=f"vote:{post_id}:1"),
                InlineKeyboardButton(text=f"💩 {dislikes}", callback_data=f"vote:{post_id}:-1"),
            ]
        ]
    )


class VotingService:
    def __init__(
        self,
        db: Database,
        bot: Bot,
        pinterest_client: Optional[PinterestClient],
        pinterest_web_client: Optional[PinterestWebClient],
        quarantine_chat_id: Optional[str] = None,
    ) -> None:
        self.db = db
        self.bot = bot
        self.pinterest_client = pinterest_client
        self.pinterest_web_client = pinterest_web_client
        self.quarantine_chat_id = quarantine_chat_id

    async def register_vote(self, post_id: int, user_id: int, vote_value: int) -> tuple[int, int, Optional[str], bool]:
        inserted = await self.db.record_vote_once(post_id, str(user_id), vote_value)
        likes, dislikes = await self.db.aggregate_votes(post_id)
        post_row = await self.db.fetch_post(post_id)
        action: Optional[str] = None
        if not post_row:
            return likes, dislikes, action, inserted
        status = post_row["status"]
        net_score = likes - dislikes
        audience = post_row["audience_size"] or 0
        dynamic_threshold = (audience + 1) // 2 if audience else None
        manual_like = post_row["like_threshold"] or 0
        manual_dislike = post_row["dislike_threshold"] or 0
        like_threshold = manual_like if manual_like > 0 else (dynamic_threshold or 1)
        dislike_threshold = abs(manual_dislike) if manual_dislike < 0 else (dynamic_threshold or 1)
        net_threshold = manual_dislike or -9999
        good_board = post_row["pinterest_board_id"]
        bad_board = post_row["pinterest_bad_board_id"]
        if status == "pinned":
            good_board = None
        pinned = False
        quarantined = False
        if likes >= like_threshold and good_board:
            pinned = await self._pin_to_pinterest(
                post_row,
                board_id=good_board,
                section_id=post_row["pinterest_section_id"],
            )
            if pinned:
                action = "pinned"
        if dislikes >= dislike_threshold and bad_board:
            await self._pin_bad(post_row, bad_board)
        if net_score <= net_threshold and status != "quarantined":
            quarantined = await self._send_to_quarantine(post_row)
            if quarantined:
                action = "quarantined"
        return likes, dislikes, action, inserted

    async def _pin_to_pinterest(self, post_row, board_id: str, section_id: Optional[str]) -> bool:
        success = await self._save_pin(post_row, board_id=board_id, section_id=section_id)
        if not success:
            return False
        await self.db.set_pinned(post_row["id"])
        try:
            await self.bot.send_message(
                chat_id=post_row["telegram_chat_id"],
                text="🔥 Мем попал в доску!",
                reply_to_message_id=post_row["telegram_message_id"],
            )
        except TelegramBadRequest:
            logger.debug("Failed to notify pin for post %s", post_row["id"])
        return True

    async def _save_pin(self, post_row, board_id: str, section_id: Optional[str]) -> bool:
        content_row = await self.db.fetch_content_item(post_row["content_item_id"])
        if not content_row:
            return False
        payload = json.loads(content_row["payload"])
        media_url = payload["media_url"]
        title = payload.get("title") or "Memebot pick"
        description = payload.get("caption")
        link = payload.get("permalink")
        extra = payload.get("extra") or {}
        source_pin_id = None
        image_signature = None
        story_pin_data_id = None
        if isinstance(extra, dict):
            source_pin_id = extra.get("source_pin_id") or extra.get("pin_id")
            image_signature = extra.get("image_signature")
            story_pin_data_id = extra.get("story_pin_data_id")
        video_url = payload.get("video_url")

        media_source_payload = None
        headers = None
        if self.pinterest_web_client:
            headers = {
                "Referer": "https://www.pinterest.com/",
                "User-Agent": self.pinterest_web_client.user_agent,
            }
        try:
            if video_url:
                video_bytes, video_type = await download_binary(video_url, headers=headers)
                cover_bytes, cover_type = await download_binary(media_url, headers=headers)
                media_source_payload = {
                    "source_type": "video_base64",
                    "content_type": video_type or "video/mp4",
                    "data": base64.b64encode(video_bytes).decode(),
                    "cover_image_data": base64.b64encode(cover_bytes).decode(),
                    "cover_image_content_type": cover_type or "image/jpeg",
                }
            else:
                image_bytes, image_type = await download_binary(media_url, headers=headers)
                media_source_payload = {
                    "source_type": "image_base64",
                    "content_type": image_type or "image/jpeg",
                    "data": base64.b64encode(image_bytes).decode(),
                }
        except Exception:
            media_source_payload = None
            logger.exception("Failed to download media for pin %s", post_row["id"])

        if self.pinterest_client:
            try:
                await self.pinterest_client.pin_to_board(
                    board_id=board_id,
                    media_url=media_url,
                    title=title,
                    description=description,
                    section_id=section_id,
                    link=link,
                )
            except Exception:
                logger.exception("Failed to pin post %s via API", post_row["id"])
                return False
        elif self.pinterest_web_client:
            try:
                if source_pin_id:
                    await self.pinterest_web_client.save_existing_pin(
                        pin_id=source_pin_id,
                        board_id=board_id,
                        section_id=section_id,
                        description=description,
                    )
                else:
                    await self.pinterest_web_client.create_pin(
                        board_id=board_id,
                        title=title,
                        description=description,
                        section_id=section_id,
                        link=link,
                        media_source=media_source_payload,
                        fallback_media_url=media_url,
                        fallback_video_url=video_url,
                        image_signature=image_signature,
                        story_pin_data_id=story_pin_data_id,
                    )
            except Exception:
                logger.exception("Failed to pin post %s via web client", post_row["id"])
                return False
        else:
            logger.warning("No Pinterest client configured")
            return False
        return True

    async def _pin_bad(self, post_row, bad_board_id: Optional[str]) -> None:
        if not bad_board_id:
            return
        await self._save_pin(
            post_row,
            board_id=bad_board_id,
            section_id=post_row["pinterest_bad_section_id"],
        )

    async def _send_to_quarantine(self, post_row) -> bool:
        if not self.quarantine_chat_id:
            await self.db.set_quarantined(post_row["id"])
            return True
        try:
            await self.bot.forward_message(
                chat_id=self.quarantine_chat_id,
                from_chat_id=post_row["telegram_chat_id"],
                message_id=post_row["telegram_message_id"],
            )
        except TelegramBadRequest:
            logger.exception("Failed to forward to quarantine")
            return False
        await self.db.set_quarantined(post_row["id"])
        return True
