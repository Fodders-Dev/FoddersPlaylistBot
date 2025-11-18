from __future__ import annotations

import html
import json
import logging
import re
from http import cookies
from typing import List, Optional

import httpx

from .base import ContentItem, ContentSourceFactory

logger = logging.getLogger(__name__)


@ContentSourceFactory.register("pinterest_board_ideas")
class PinterestBoardIdeasSource:
    """Scrape Pinterest board recommendations (Другие идеи для этой доски)."""

    BASE_URL = "https://www.pinterest.com/resource/BoardContentRecommendationResource/get/"

    def __init__(
        self,
        board_id: str,
        cookie_header: str,
        locale: str = "ru-RU",
        user_agent: Optional[str] = None,
    ) -> None:
        if not board_id:
            raise ValueError("board_id is required for pinterest_board_ideas source")
        if not cookie_header:
            raise ValueError("pinterest_board_ideas requires PINTEREST_COOKIE")
        self.board_id = board_id
        self.locale = locale
        self.cookie_header = cookie_header
        self.cookies = self._parse_cookies(cookie_header)
        self.csrf_token = self._extract_csrf(cookie_header)
        self.user_agent = (
            user_agent
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
        )
        self.app_version: Optional[str] = None
        self.request_identifier: Optional[str] = None
        self.source_url = f"/?boardId={self.board_id}"
        self._bookmark: str = "-end-"

    async def fetch(self, limit: int = 10) -> List[ContentItem]:
        await self._ensure_bootstrap()
        if limit <= 0:
            return []
        headers = self._build_headers()
        items: List[ContentItem] = []
        pages = 0
        max_pages = 5
        bookmark = self._bookmark or "-end-"
        while pages < max_pages and len(items) < limit:
            options = {
                "type": "board",
                "id": self.board_id,
                "__track__referrer": 19,
            }
            if bookmark and bookmark != "-end-":
                options["bookmarks"] = [bookmark]
            params = {
                "source_url": self.source_url,
                "data": json.dumps({"options": options, "context": {}}),
            }
            async with httpx.AsyncClient(timeout=25, cookies=self.cookies) as client:
                response = await client.get(self.BASE_URL, params=params, headers=headers)
            if response.status_code == 403:
                logger.error("Pinterest denied board recommendation request. Check cookies")
                break
            response.raise_for_status()
            payload = response.json()
            data_sections = payload.get("resource_response", {}).get("data") or []
            if not data_sections:
                logger.warning("Board recommendations returned empty response for board %s", self.board_id)
                break
            section_items = await self._extract_pins(data_sections)
            items.extend(section_items)
            bookmark = self._extract_bookmark(payload) or "-end-"
            pages += 1
            if bookmark == "-end-":
                break
        self._bookmark = bookmark
        logger.info(
            "Scraped %s board-idea pins for board '%s' over %s page(s)",
            len(items),
            self.board_id,
            pages,
        )
        return items[:limit]

    async def _extract_pins(self, sections: list[dict]) -> List[ContentItem]:
        pins: List[ContentItem] = []
        seen: set[str] = set()
        for section in sections:
            if self._is_header_section(section):
                continue
            candidates: List[dict] = []
            for key in ("objects", "expanded_viewport_objects"):
                data = section.get(key) or []
                candidates.extend(data)
            if not candidates and self._looks_like_pin(section):
                candidates.append(section)
            for obj in candidates:
                if self._is_own_board_pin(obj):
                    continue
                pin_id = obj.get("id")
                if pin_id and pin_id in seen:
                    continue
                pin = await self._build_content_item(obj)
                if pin:
                    pins.append(pin)
                    if pin_id:
                        seen.add(pin_id)
        return pins

    async def _build_content_item(self, pin: dict) -> Optional[ContentItem]:
        pin_id = pin.get("id")
        if not pin_id:
            return None
        media = pin.get("images") or {}
        media_entry = media.get("orig") or media.get("736x") or media.get("474x")
        fallback_media_url = media_entry.get("url") if media_entry else None
        video_url = await self._extract_video_url(pin)
        if video_url and not self._is_supported_video(video_url):
            video_url = None
        media_url = video_url or fallback_media_url
        if not media_url:
            return None
        media_type = "video" if video_url else "photo"
        extra = {
            "dominant_color": pin.get("dominant_color"),
            "source_pin_id": str(pin_id),
            "image_signature": pin.get("image_signature"),
            "external_link": pin.get("link") if pin.get("link") else None,
        }
        return ContentItem(
            source_type="pinterest_board_ideas",
            source_id=str(pin_id),
            title=pin.get("title") or pin.get("grid_title") or "Pinterest pin",
            caption=pin.get("description") or pin.get("grid_description"),
            media_url=media_url,
            media_type=media_type,
            video_url=video_url,
            permalink=f"https://www.pinterest.com/pin/{pin_id}/",
            extra={k: v for k, v in extra.items() if v},
        )

    async def _extract_video_url(self, pin: dict) -> Optional[str]:
        videos = pin.get("videos") or {}
        video_list = videos.get("video_list") or {}
        if video_list:
            first = next(iter(video_list.values()), None)
            if first and first.get("url"):
                return first["url"]
        video_url: Optional[str] = None
        needs_pin_resource = bool(
            pin.get("is_video") or pin.get("is_playable") or pin.get("story_pin_data_id") or videos
        )
        if needs_pin_resource:
            video_url = await self._fetch_pin_video(pin.get("id"))
        if not video_url:
            video_url = await self._scrape_video_from_html(pin.get("id"))
        return video_url

    @staticmethod
    def _is_supported_video(url: str) -> bool:
        if not url:
            return False
        clean = url.split("?", 1)[0].lower()
        return clean.endswith(".mp4")

    def _extract_bookmark(self, payload: dict) -> str:
        bookmark = payload.get("resource_response", {}).get("bookmark")
        if bookmark:
            return bookmark
        options = payload.get("resource", {}).get("options") or {}
        bookmarks = options.get("bookmarks") or []
        if bookmarks:
            return bookmarks[-1]
        return "-end-"

    def _build_headers(self) -> dict:
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Language": self.locale,
            "Referer": "https://www.pinterest.com",
            "X-CSRFToken": self.csrf_token,
            "X-Requested-With": "XMLHttpRequest",
            "X-App-Version": self.app_version or "3d6bfb0",
            "X-Pinterest-PWS-Handler": "www/index.js",
            "X-Pinterest-Source-Url": self.source_url,
        }
        return headers

    async def _ensure_bootstrap(self) -> None:
        if self.app_version:
            return
        params = {"boardId": self.board_id}
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Language": self.locale,
        }
        async with httpx.AsyncClient(timeout=20, cookies=self.cookies, follow_redirects=True) as client:
            response = await client.get("https://www.pinterest.com/", params=params, headers=headers)
        if response.status_code != 200:
            logger.warning("Unable to bootstrap Pinterest board page for %s", self.board_id)
            return
        match = re.search(
            r'<script id="__PWS_DATA__" type="application/json">(.*?)</script>',
            response.text,
            re.DOTALL,
        )
        if not match:
            logger.warning("Cannot locate __PWS_DATA__ for board page")
            return
        data = json.loads(html.unescape(match.group(1)))
        context = data.get("context", {})
        app_version = data.get("appVersion") or context.get("app_version")
        if app_version:
            self.app_version = app_version
        request_identifier = context.get("request_identifier")
        if request_identifier:
            self.request_identifier = request_identifier

    async def _fetch_pin_video(self, pin_id: Optional[str]) -> Optional[str]:
        if not pin_id:
            return None
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Language": self.locale,
            "Referer": f"https://www.pinterest.com/pin/{pin_id}/",
            "Origin": "https://www.pinterest.com",
            "X-CSRFToken": self.csrf_token,
            "X-Requested-With": "XMLHttpRequest",
            "X-App-Version": self.app_version or "3d6bfb0",
        }
        form = {
            "source_url": f"/pin/{pin_id}/",
            "data": json.dumps({"options": {"id": pin_id}, "context": {}}),
        }
        async with httpx.AsyncClient(timeout=15, cookies=self.cookies) as client:
            resp = await client.post(
                "https://www.pinterest.com/resource/PinResource/get/",
                headers=headers,
                data=form,
            )
        if resp.status_code != 200:
            return None
        payload = resp.json().get("resource_response", {}).get("data") or {}
        videos = payload.get("videos") or {}
        video_list = videos.get("video_list") or {}
        first = next(iter(video_list.values()), None)
        if first and first.get("url"):
            return first["url"]
        return None

    async def _scrape_video_from_html(self, pin_id: Optional[str]) -> Optional[str]:
        if not pin_id:
            return None
        html_resp = await self._fetch_pin_html(pin_id)
        if not html_resp:
            return None
        match = re.search(r"https:\/\/v1\.pinimg\.com[^\"']+\.mp4", html_resp)
        if match:
            return match.group(0)
        return None

    async def _fetch_pin_html(self, pin_id: str) -> Optional[str]:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.8",
        }
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"https://www.pinterest.com/pin/{pin_id}/",
                headers=headers,
                follow_redirects=True,
            )
        if resp.status_code == 200:
            return resp.text
        return None

    @staticmethod
    def _parse_cookies(header: str) -> httpx.Cookies:
        jar = httpx.Cookies()
        simple = cookies.SimpleCookie()
        simple.load(header)
        for key, morsel in simple.items():
            jar.set(key, morsel.value)
        if not jar:
            raise ValueError("Failed to parse PINTEREST_COOKIE")
        return jar

    @staticmethod
    def _extract_csrf(header: str) -> str:
        simple = cookies.SimpleCookie()
        simple.load(header)
        token = simple.get("csrftoken")
        if not token:
            raise ValueError("PINTEREST_COOKIE must contain csrftoken")
        return token.value

    def _is_header_section(self, section: dict) -> bool:
        if not section:
            return True
        story_type = section.get("story_type")
        if story_type == "simple_feed_header":
            return True
        section_type = section.get("type")
        if section_type == "story" and not (
            section.get("objects") or section.get("expanded_viewport_objects")
        ):
            return True
        return False

    def _looks_like_pin(self, section: dict) -> bool:
        if not section:
            return False
        if section.get("type") in {"pin", "pin_rep"}:
            return True
        return bool(section.get("images") or section.get("videos"))

    def _is_own_board_pin(self, pin: dict) -> bool:
        board = pin.get("board") or {}
        board_id = board.get("id")
        if not board_id:
            return False
        return str(board_id) == str(self.board_id)
