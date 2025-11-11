from __future__ import annotations

import asyncio
import logging

from aiogram import Bot
from aiogram.enums import ParseMode

from memebot.config import settings
from memebot.content_sources.pinterest import PinterestClient
from memebot.content_sources.spotify import SpotifyClient
from memebot.db import Database
from memebot.services.autoposter import AutoPoster
from memebot.services.voting import VotingService
from memebot.telegram_bot import TelegramApp
from memebot.utils.logging import configure_logging


async def app() -> None:
    configure_logging()
    logger = logging.getLogger(__name__)
    bot = Bot(token=settings.telegram_bot_token, parse_mode=ParseMode.HTML)
    db = Database(settings.database_path)
    await db.connect()
    await db.init_schema()

    pinterest_client = None
    if settings.pinterest_access_token:
        pinterest_client = PinterestClient(settings.pinterest_access_token)

    spotify_client = None
    if settings.spotify_client_id and settings.spotify_client_secret:
        spotify_client = SpotifyClient(
            client_id=settings.spotify_client_id,
            client_secret=settings.spotify_client_secret,
            refresh_token=settings.spotify_refresh_token,
        )

    autoposter = AutoPoster(
        db=db,
        bot=bot,
        settings=settings,
        pinterest_client=pinterest_client,
        spotify_client=spotify_client,
    )
    voting = VotingService(
        db=db,
        bot=bot,
        pinterest_client=pinterest_client,
        quarantine_chat_id=settings.quarantine_chat_id,
    )
    telegram_app = TelegramApp(bot=bot, db=db, settings=settings, autoposter=autoposter, voting=voting)

    try:
        await telegram_app.run()
    except (KeyboardInterrupt, asyncio.CancelledError):  # pragma: no cover
        logger.info("Stopping bot")
    finally:
        await telegram_app.shutdown()
        await db.close()
        if pinterest_client:
            await pinterest_client.close()
        if spotify_client:
            await spotify_client.close()
        await bot.session.close()


def main() -> None:
    asyncio.run(app())


if __name__ == "__main__":  # pragma: no cover
    main()
