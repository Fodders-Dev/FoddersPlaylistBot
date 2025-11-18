from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Protocol


@dataclass(slots=True)
class ContentItem:
    source_type: str
    source_id: str
    title: str
    media_url: str
    media_type: str = "photo"  # photo | video | animation
    video_url: str | None = None
    permalink: str | None = None
    caption: str | None = None
    extra: Dict[str, Any] = field(default_factory=dict)


class ContentSource(Protocol):
    name: str

    async def fetch(self, limit: int = 10) -> List[ContentItem]:
        """Return up to `limit` fresh content items sorted from newest to oldest."""


class ContentSourceFactory:
    _registry: Dict[str, type[ContentSource]] = {}

    @classmethod
    def register(cls, key: str):
        def decorator(source_cls: type[ContentSource]) -> type[ContentSource]:
            cls._registry[key] = source_cls
            return source_cls

        return decorator

    @classmethod
    def create(cls, key: str, **kwargs: Any) -> ContentSource:
        if key not in cls._registry:
            raise KeyError(f"Unknown content source: {key}")
        return cls._registry[key](**kwargs)

    @classmethod
    def choices(cls) -> List[str]:
        return sorted(cls._registry.keys())
