from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
from pydantic import Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore")
    telegram_bot_token: str = Field(..., alias="TELEGRAM_BOT_TOKEN")
    telegram_admin_ids: List[int] = Field(default_factory=list, alias="TELEGRAM_ADMIN_IDS")
    database_path: Path = Field(default=Path("./memebot.db"), alias="DATABASE_PATH")

    posting_interval_seconds: int = Field(default=1800, alias="POSTING_INTERVAL_SECONDS")
    like_threshold: int = Field(default=20, alias="LIKE_THRESHOLD")
    dislike_threshold: int = Field(default=-10, alias="DISLIKE_THRESHOLD")
    max_posts_per_run: int = Field(default=5, alias="MAX_POSTS_PER_RUN")
    posting_start_hour: int = Field(default=0, alias="POSTING_START_HOUR")
    posting_end_hour: int = Field(default=23, alias="POSTING_END_HOUR")
    posting_timezone: str = Field(default="UTC", alias="POSTING_TIMEZONE")

    pinterest_access_token: Optional[str] = Field(default=None, alias="PINTEREST_ACCESS_TOKEN")
    pinterest_board_id: Optional[str] = Field(default=None, alias="PINTEREST_BOARD_ID")
    pinterest_section_id: Optional[str] = Field(default=None, alias="PINTEREST_SECTION_ID")
    pinterest_bad_board_id: Optional[str] = Field(default=None, alias="PINTEREST_BAD_BOARD_ID")
    pinterest_bad_section_id: Optional[str] = Field(default=None, alias="PINTEREST_BAD_SECTION_ID")
    pinterest_recommendation_query: Optional[str] = Field(
        default=None, alias="PINTEREST_RECOMMENDATION_QUERY"
    )
    pinterest_cookie: Optional[str] = Field(default=None, alias="PINTEREST_COOKIE")
    pinterest_user_agent: Optional[str] = Field(default=None, alias="PINTEREST_USER_AGENT")

    spotify_client_id: Optional[str] = Field(default=None, alias="SPOTIFY_CLIENT_ID")
    spotify_client_secret: Optional[str] = Field(default=None, alias="SPOTIFY_CLIENT_SECRET")
    spotify_refresh_token: Optional[str] = Field(default=None, alias="SPOTIFY_REFRESH_TOKEN")

    quarantine_chat_id: Optional[str] = Field(default=None, alias="QUARANTINE_CHAT_ID")
    prompt_for_captions: bool = Field(default=True, alias="PROMPT_FOR_CAPTIONS")

    @model_validator(mode="before")
    def _split_admins(cls, values: dict) -> dict:
        admins = values.get("TELEGRAM_ADMIN_IDS") or values.get("telegram_admin_ids")
        if isinstance(admins, str):
            parsed = [int(x) for x in admins.replace(";", ",").split(",") if x.strip()]
            values["TELEGRAM_ADMIN_IDS"] = parsed
        elif isinstance(admins, int):
            values["TELEGRAM_ADMIN_IDS"] = [admins]
        return values

    @classmethod
    def load(cls, env_file: str | None = ".env") -> "Settings":
        if env_file and os.path.exists(env_file):
            load_dotenv(env_file)
        try:
            return cls()
        except ValidationError as exc:  # pragma: no cover - configuration stage
            missing = ", ".join(err["loc"][0] for err in exc.errors())
            raise RuntimeError(f"Configuration error: {missing}") from exc


settings = Settings.load()


def get_timezone() -> ZoneInfo:
    return ZoneInfo(settings.posting_timezone)
