from __future__ import annotations

import json
import logging
import shlex
from typing import Any, Dict, Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, Message

from memebot.config import Settings
from memebot.db import Database
from memebot.services.autoposter import AutoPoster
from memebot.services.voting import VotingService, build_vote_keyboard

logger = logging.getLogger(__name__)


class TelegramApp:
    def __init__(
        self,
        bot: Bot,
        db: Database,
        settings: Settings,
        autoposter: AutoPoster,
        voting: VotingService,
    ) -> None:
        self.bot = bot
        self.db = db
        self.settings = settings
        self.autoposter = autoposter
        self.voting = voting
        self.dispatcher = Dispatcher()
        self.router = Router()
        self._register_handlers()
        self.dispatcher.include_router(self.router)

    def _register_handlers(self) -> None:
        self.router.message(Command("start"))(self.handle_start)
        self.router.message(Command("health"))(self.handle_health)
        self.router.message(Command("channels"))(self.handle_channels)
        self.router.message(Command("register_channel"))(self.handle_register_channel)
        self.router.callback_query(F.data.startswith("vote:"))(self.handle_vote)

    async def run(self) -> None:
        await self.autoposter.start()
        await self.dispatcher.start_polling(self.bot)

    async def shutdown(self) -> None:
        await self.autoposter.stop()

    async def handle_start(self, message: Message, command: CommandObject) -> None:  # type: ignore[override]
        await message.answer(
            "Привет! Я подбираю мемы из Pinterest и публикую их в канале."
            "\nИспользуй /register_channel чтобы добавить канал."
        )

    async def handle_health(self, message: Message, command: CommandObject) -> None:  # type: ignore[override]
        await message.answer("Memebot работает ✅")

    async def handle_channels(self, message: Message, command: CommandObject) -> None:  # type: ignore[override]
        if not self._is_admin(message.from_user.id if message.from_user else None):
            await message.answer("Нет прав")
            return
        rows = await self.db.iter_channels()
        if not rows:
            await message.answer("Нет каналов")
            return
        lines = []
        for row in rows:
            cfg = json.loads(row["content_config"])
            lines.append(
                f"• {row['telegram_channel_id']} ({row['content_source']}) — interval {row['autopost_interval']}s, "
                f"like ≥ {row['like_threshold']}" + (f", query={cfg.get('query')}" if cfg.get('query') else "")
            )
        await message.answer("\n".join(lines))

    async def handle_register_channel(self, message: Message, command: CommandObject) -> None:  # type: ignore[override]
        if not self._is_admin(message.from_user.id if message.from_user else None):
            await message.answer("Нет прав")
            return
        if not command.args:
            await message.answer(
                "Использование: /register_channel channel=@name source=pinterest query='funny memes' board=123"
            )
            return
        params = self._parse_args(command.args)
        channel = params.get("channel")
        source = params.get("source", "pinterest")
        if not channel:
            await message.answer("Укажи channel=@example")
            return
        query = params.get("query")
        board_id = params.get("board") or params.get("board_id") or self.settings.pinterest_board_id
        section = params.get("section") or params.get("section_id")
        like_threshold = int(params.get("like") or self.settings.like_threshold)
        dislike_threshold = int(params.get("dislike") or self.settings.dislike_threshold)
        interval = int(params.get("interval") or self.settings.posting_interval_seconds)
        source_config: Dict[str, Any] = {}
        if source == "pinterest":
            source_config["query"] = query or self.settings.pinterest_recommendation_query
        else:
            for key, value in params.items():
                if key in {"channel", "source", "interval", "like", "dislike", "board", "board_id", "section", "section_id"}:
                    continue
                source_config[key] = value
        source_config = {k: v for k, v in source_config.items() if v is not None}
        channel_id = await self.db.add_channel(
            telegram_channel_id=channel,
            telegram_channel_name=None,
            content_source=source,
            content_config=source_config,
            autopost_interval=interval,
            like_threshold=like_threshold,
            dislike_threshold=dislike_threshold,
            pinterest_board_id=board_id,
            pinterest_section_id=section,
        )
        await message.answer(f"Канал записан (id={channel_id})")

    async def handle_vote(self, callback: CallbackQuery) -> None:
        if not callback.data or not callback.message:
            return
        parts = callback.data.split(":")
        if len(parts) != 3:
            return
        _, post_id_str, vote_str = parts
        post_id = int(post_id_str)
        vote_value = int(vote_str)
        likes, dislikes, action = await self.voting.register_vote(
            post_id=post_id,
            user_id=callback.from_user.id,
            vote_value=vote_value,
        )
        try:
            await callback.message.edit_reply_markup(
                reply_markup=build_vote_keyboard(post_id, likes, dislikes)
            )
        except TelegramBadRequest:
            logger.debug("Cannot update keyboard for post %s", post_id)
        answer_text = "Учтено"
        if action == "pinned":
            answer_text = "Добавлено в Pinterest"
        elif action == "quarantined":
            answer_text = "Отправлено в карантин"
        await callback.answer(answer_text, show_alert=False)

    def _parse_args(self, args: str) -> Dict[str, str]:
        result: Dict[str, str] = {}
        for token in shlex.split(args):
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            result[key.lstrip("-")] = value
        return result

    def _is_admin(self, user_id: Optional[int]) -> bool:
        if user_id is None:
            return False
        return user_id in self.settings.telegram_admin_ids
