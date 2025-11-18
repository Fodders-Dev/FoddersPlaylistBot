from __future__ import annotations

import json
import logging
from http import cookies
from typing import Dict, Optional

import httpx

logger = logging.getLogger(__name__)


class PinterestWebClient:
    """Interact with pinterest.com internal endpoints via browser cookies."""

    BASE_URL = "https://www.pinterest.com"

    def __init__(self, cookie_header: str, user_agent: str | None = None) -> None:
        if not cookie_header:
            raise ValueError("PinterestWebClient requires cookie header")
        self.cookies = self._parse_cookie_header(cookie_header)
        self.csrf = self.cookies.get("csrftoken")
        if not self.csrf:
            raise ValueError("csrftoken cookie is required")
        self.user_agent = user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36"
        headers = {
            "User-Agent": self.user_agent,
            "X-CSRFToken": self.csrf,
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Referer": "https://www.pinterest.com/pin-builder/",
        }
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=headers,
            cookies=self.cookies,
            timeout=30,
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def create_pin(
        self,
        board_id: str,
        title: str,
        description: Optional[str] = None,
        link: Optional[str] = None,
        section_id: Optional[str] = None,
        media_source: Optional[Dict[str, str]] = None,
        fallback_media_url: Optional[str] = None,
        fallback_video_url: Optional[str] = None,
        image_signature: Optional[str] = None,
        story_pin_data_id: Optional[str] = None,
    ) -> dict:
        sanitized_link = link
        if sanitized_link and "pinterest.com" in sanitized_link:
            sanitized_link = None
        source = media_source
        if not source:
            if fallback_video_url:
                source = {
                    "source_type": "video_url",
                    "url": fallback_video_url,
                    "cover_image_url": fallback_media_url or fallback_video_url,
                }
            elif fallback_media_url:
                source = {"source_type": "image_url", "url": fallback_media_url}
            else:
                raise ValueError("media_source or fallback media URL must be provided")
        payload = {
            "options": {
                "board_id": board_id,
                "description": description or title,
                "link": sanitized_link or fallback_media_url or fallback_video_url,
                "media_source": source,
                "method": "scraped",
                "title": title,
            },
            "context": {},
        }
        if section_id:
            payload["options"]["board_section_id"] = section_id
        if image_signature:
            payload["options"]["image_signature"] = image_signature
        if story_pin_data_id:
            payload["options"]["story_pin_data_id"] = story_pin_data_id
        data = {
            "source_url": "/pin-builder/",
            "data": json.dumps(payload),
        }
        response = await self._client.post("/resource/PinResource/create/", data=data)
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError:
            logger.exception("Pin creation failed: %s", response.text)
            raise
        body = response.json()
        if body.get("status") != "success":
            logger.error("Pinterest response is not success: %s", body)
            raise RuntimeError("Pinterest did not accept pin")
        return body

    async def save_existing_pin(
        self,
        pin_id: str,
        board_id: str,
        section_id: Optional[str] = None,
        description: Optional[str] = None,
    ) -> dict:
        payload = {
            "options": {
                "pin_id": pin_id,
                "board_id": board_id,
                "board_section_id": section_id,
                "description": description or "",
                "link": None,
                "retain_comments": True,
            },
            "context": {},
        }
        data = {
            "source_url": f"/pin/{pin_id}/",
            "data": json.dumps(payload),
        }
        response = await self._client.post("/resource/RepinResource/create/", data=data)
        response.raise_for_status()
        body = response.json()
        if body.get("resource_response", {}).get("status") != "success":
            logger.error("Pinterest save response error: %s", body)
            raise RuntimeError("Pinterest did not accept save request")
        return body

    @staticmethod
    def _parse_cookie_header(header: str) -> Dict[str, str]:
        jar = cookies.SimpleCookie()
        jar.load(header)
        parsed = {key: morsel.value for key, morsel in jar.items()}
        if not parsed:
            raise ValueError("Failed to parse Pinterest cookies")
        return parsed
