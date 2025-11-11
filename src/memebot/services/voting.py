from __future__ import annotations

import json
import logging
from typing import Optional

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from memebot.content_sources.pinterest import PinterestClient
from memebot.db import Database

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
        quarantine_chat_id: Optional[str] = None,
    ) -> None:
        self.db = db
        self.bot = bot
        self.pinterest_client = pinterest_client
        self.quarantine_chat_id = quarantine_chat_id

    async def register_vote(self, post_id: int, user_id: int, vote_value: int) -> tuple[int, int, Optional[str]]:
        await self.db.record_vote(post_id, str(user_id), vote_value)
        likes, dislikes = await self.db.aggregate_votes(post_id)
        post_row = await self.db.fetch_post(post_id)
        action: Optional[str] = None
        if post_row:
            net_score = likes - dislikes
            like_threshold = post_row["like_threshold"] or 0
            dislike_threshold = post_row["dislike_threshold"] or -9999
            if likes >= like_threshold and post_row["status"] != "pinned":
                pinned = await self._pin_to_pinterest(post_row)
                if pinned:
                    action = "pinned"
            elif net_score <= dislike_threshold and post_row["status"] != "quarantined":
                quarantined = await self._send_to_quarantine(post_row)
                if quarantined:
                    action = "quarantined"
        return likes, dislikes, action

    async def _pin_to_pinterest(self, post_row) -> bool:
        if not self.pinterest_client:
            logger.warning("Pinterest client missing, cannot pin")
            return False
        board_id = post_row["pinterest_board_id"]
        if not board_id:
            logger.warning("Board ID missing for post %s", post_row["id"])
            return False
        content_row = await self.db.fetch_content_item(post_row["content_item_id"])
        if not content_row:
            return False
        payload = json.loads(content_row["payload"])
        try:
            await self.pinterest_client.pin_to_board(
                board_id=board_id,
                media_url=payload["media_url"],
                title=payload.get("title") or "Memebot pick",
                description=payload.get("caption"),
                section_id=post_row["pinterest_section_id"],
                link=payload.get("permalink"),
            )
        except Exception:
            logger.exception("Failed to pin post %s", post_row["id"])
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
