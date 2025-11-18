from __future__ import annotations

import logging
from typing import Any, List, Optional

import httpx

from .base import ContentItem, ContentSource, ContentSourceFactory

logger = logging.getLogger(__name__)


class PinterestClient:
    BASE_URL = "https://api.pinterest.com/v5"

    def __init__(self, access_token: str):
        self._client = httpx.AsyncClient(base_url=self.BASE_URL, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        })

    async def close(self) -> None:
        await self._client.aclose()

    async def fetch_recommendations(
        self,
        query: str,
        limit: int = 10,
        board_id: Optional[str] = None,
    ) -> List[dict[str, Any]]:
        params = {"query": query, "page_size": limit}
        if board_id:
            params["board_id"] = board_id
        response = await self._client.get("/search/pins", params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("items", [])

    async def pin_to_board(
        self,
        board_id: str,
        media_url: str,
        title: str,
        description: Optional[str] = None,
        section_id: Optional[str] = None,
        link: Optional[str] = None,
    ) -> dict[str, Any]:
        safe_title = title or "Memebot pick"
        payload = {
            "board_id": board_id,
            "title": safe_title[:60],
            "media_source": {
                "source_type": "image_url",
                "url": media_url,
            },
        }
        if description:
            payload["description"] = description[:500]
        if section_id:
            payload["board_section_id"] = section_id
        if link:
            payload["link"] = link
        response = await self._client.post("/pins", json=payload, timeout=30)
        response.raise_for_status()
        return response.json()


@ContentSourceFactory.register("pinterest")
class PinterestRecommendationsSource:
    name = "pinterest"

    def __init__(
        self,
        client: PinterestClient,
        query: str,
        board_id: Optional[str] = None,
        section_id: Optional[str] = None,
    ):
        self.client = client
        self.query = query
        self.board_id = board_id
        self.section_id = section_id

    async def fetch(self, limit: int = 10) -> List[ContentItem]:
        if not self.query:
            raise RuntimeError("Pinterest source requires recommendation query")
        pins = await self.client.fetch_recommendations(self.query, limit=limit, board_id=self.board_id)
        items: List[ContentItem] = []
        for pin in pins:
            pin_id = pin.get("id") or pin.get("pin_id")
            if not pin_id:
                continue
            media_type = "photo"
            video_url = None
            media = pin.get("media") or {}
            images = media.get("images") or {}
            first_image = next(iter(images.values()), None)
            if "videos" in media and media["videos"]:
                video_variant = next(iter(media["videos"].values()))
                video_url = video_variant.get("url")
                media_type = "video"
            if not first_image and not video_url:
                continue
            media_url = first_image.get("url") if first_image else video_url
            items.append(
                ContentItem(
                    source_type=self.name,
                    source_id=str(pin_id),
                    title=pin.get("title") or "Untitled meme",
                    caption=pin.get("description") or pin.get("title"),
                    media_url=media_url,
                    media_type=media_type,
                    video_url=video_url,
                    permalink=pin.get("link"),
                    extra={"pin": pin, "board_id": self.board_id, "section_id": self.section_id},
                )
            )
        return items
