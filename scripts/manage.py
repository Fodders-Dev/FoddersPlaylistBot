from __future__ import annotations

import asyncio
from pathlib import Path

import typer

from memebot.config import Settings
from memebot.db import Database

app = typer.Typer(add_completion=False)


@app.command()
def init_db(db_path: str | None = typer.Option(None, help="Path to SQLite DB")) -> None:
    """Create tables if they do not exist."""
    settings = Settings.load()
    target_path = Path(db_path) if db_path else settings.database_path

    async def _run() -> None:
        db = Database(target_path)
        await db.connect()
        await db.init_schema()
        await db.close()

    asyncio.run(_run())
    typer.echo(f"DB ready at {target_path}")


@app.command()
def register_channel(
    channel: str = typer.Option(..., help="Telegram channel username or chat id"),
    source: str = typer.Option("pinterest", help="Content source key"),
    query: str | None = typer.Option(None, help="Pinterest search query"),
    board: str | None = typer.Option(None, help="Pinterest board id"),
    section: str | None = typer.Option(None, help="Pinterest section id"),
    like: int = typer.Option(20, help="Like threshold"),
    dislike: int = typer.Option(-10, help="Net score needed for quarantine"),
    interval: int = typer.Option(900, help="Interval between posting attempts (seconds)"),
) -> None:
    settings = Settings.load()

    async def _run() -> None:
        db = Database(settings.database_path)
        await db.connect()
        await db.init_schema()
        source_config: dict[str, str | None] = {}
        if source == "pinterest":
            source_config["query"] = query or settings.pinterest_recommendation_query
        else:
            source_config["query"] = query
        source_config = {k: v for k, v in source_config.items() if v is not None}
        channel_id = await db.add_channel(
            telegram_channel_id=channel,
            telegram_channel_name=None,
            content_source=source,
            content_config=source_config,
            autopost_interval=interval,
            like_threshold=like,
            dislike_threshold=dislike,
            pinterest_board_id=board or settings.pinterest_board_id,
            pinterest_section_id=section or settings.pinterest_section_id,
        )
        await db.close()
        typer.echo(f"Channel registered (id={channel_id})")

    asyncio.run(_run())


if __name__ == "__main__":
    app()
