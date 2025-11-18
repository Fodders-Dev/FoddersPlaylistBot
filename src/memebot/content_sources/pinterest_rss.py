from __future__ import annotations

import logging
from typing import List
from urllib.parse import urlparse

import feedparser

from .base import ContentItem, ContentSource, ContentSourceFactory

logger = logging.getLogger(__name__)


@ContentSourceFactory.register("pinterest_rss")
class PinterestRssSource:
    """Fetch pins from any public Pinterest board RSS feed."""

    name = "pinterest_rss"

    def __init__(self, feed_url: str, limit: int | None = None) -> None:
        if not feed_url:
            raise ValueError("feed_url is required for pinterest_rss source")
        parsed = urlparse(feed_url)
        if "pinterest" not in parsed.netloc:
            raise ValueError("feed_url must belong to pinterest.com")
        self.feed_url = feed_url
        self.limit = limit or 25

    async def fetch(self, limit: int = 10) -> List[ContentItem]:
        max_items = min(limit, self.limit)
        feed = feedparser.parse(self.feed_url)
        items: List[ContentItem] = []
        for entry in feed.entries[:max_items]:
            media_url = None
            if media_content := entry.get("media_content"):
                first = media_content[0]
                media_url = first.get("url")
            if not media_url:
                media_url = entry.get("link")
            if not media_url:
                continue
            items.append(
                ContentItem(
                    source_type=self.name,
                    source_id=entry.get("id") or entry.get("guid") or entry.get("link"),
                    title=entry.get("title") or "RSS Pin",
                    caption=entry.get("summary") or entry.get("title"),
                    media_url=media_url,
                    permalink=entry.get("link"),
                    extra={"published": entry.get("published")},
                )
            )
        logger.info("Fetched %s items from Pinterest RSS", len(items))
        return items
