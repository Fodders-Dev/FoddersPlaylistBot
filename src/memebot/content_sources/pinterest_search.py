from __future__ import annotations

import html
import json
import logging
import re
from http import cookies
from typing import List, Optional
from urllib.parse import quote_plus

import httpx

from .base import ContentItem, ContentSource, ContentSourceFactory

logger = logging.getLogger(__name__)
BOOKMARK_RE = re.compile(r'"nextBookmark":"(Y2[0-9A-Za-z+/=]+)"')
PIN_HTML_VIDEO_RE = re.compile(r"https:\/\/v1\.pinimg\.com[^\"']+\.mp4")


@ContentSourceFactory.register("pinterest_search")
class PinterestSearchSource:
    """Scrape Pinterest search feed via internal API using browser cookies."""

    name = "pinterest_search"
    BASE_URL = "https://www.pinterest.com/resource/BaseSearchResource/get/"

    def __init__(
        self,
        query: str,
        locale: str = "ru-RU",
        cookie_header: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> None:
        if not query:
            raise ValueError("query is required for pinterest_search source")
        if not cookie_header:
            raise ValueError("pinterest_search requires PINTEREST_COOKIE")
        self.query = query
        self.locale = locale
        self.cookie_header = cookie_header
        self.cookies = self._parse_cookies(cookie_header)
        self.csrf_token = self._extract_csrf(cookie_header)
        self.encoded_query = quote_plus(self.query)
        self.source_url = f"/search/pins/?q={self.encoded_query}&rs=typed"
        self.user_agent = (
            user_agent
            or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
        )
        self.app_version: Optional[str] = None
        self.request_identifier: Optional[str] = None
        self._cursor_bookmark: str = "-end-"
        self._cursor_source_url: str = self.source_url
        self._cursor_source_id: Optional[str] = None
        self._cursor_rs: str = "typed"

    async def fetch(self, limit: int = 10) -> List[ContentItem]:
        await self._ensure_bootstrap()
        if limit <= 0:
            return []
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Language": self.locale,
            "Referer": "https://www.pinterest.com/",
            "Origin": "https://www.pinterest.com/",
            "X-CSRFToken": self.csrf_token,
            "X-Requested-With": "XMLHttpRequest",
        }
        if self.app_version:
            headers["X-App-Version"] = self.app_version
            headers["X-Pinterest-PWS-Handler"] = "www/search/[scope].js"
            headers["X-Pinterest-Source-Url"] = self.source_url

        bookmark = self._cursor_bookmark or "-end-"
        source_url = self._cursor_source_url or self.source_url
        source_id = self._cursor_source_id
        rs_value = self._cursor_rs or "typed"
        items: List[ContentItem] = []
        max_pages = 6
        pages = 0

        while pages < max_pages and len(items) < limit:
            page_size = max(24, limit - len(items))
            options = {
                "query": self.query,
                "scope": "pins",
                "appliedProductFilters": "---",
                "domains": None,
                "user": None,
                "seoDrawerEnabled": False,
                "applied_unified_filters": None,
                "auto_correction_disabled": False,
                "journey_depth": None,
                "source_id": source_id,
                "source_module_id": None,
                "source_url": source_url,
                "static_feed": False,
                "selected_one_bar_modules": None,
                "query_pin_sigs": None,
                "page_size": page_size,
                "price_max": None,
                "price_min": None,
                "request_params": None,
                "top_pin_ids": None,
                "article": None,
                "corpus": None,
                "customized_rerank_type": None,
                "filters": None,
                "rs": rs_value,
                "redux_normalize_feed": True,
                "bookmarks": [bookmark],
                "no_fetch_context_on_resource": False,
                "top_level_filters": [],
            }
            form_data = {
                "data": json.dumps({"options": options, "context": self._build_context()}),
                "source_url": source_url,
            }
            async with httpx.AsyncClient(timeout=20, cookies=self.cookies) as client:
                response = await client.post(self.BASE_URL, data=form_data, headers=headers)
            if response.status_code == 403:
                logger.error("Pinterest denied search request. Check cookies")
                break
            response.raise_for_status()
            payload = response.json()
            results = payload.get("resource_response", {}).get("data", {}).get("results") or []
            if not results:
                logger.warning("Pinterest search returned empty response for %s (page %s)", self.query, pages + 1)
                break

            for pin in results:
                pin_id = pin.get("id")
                if not pin_id:
                    continue
                media = pin.get("images") or {}
                media_entry = media.get("orig") or media.get("564x")
                fallback_media_url = media_entry.get("url") if media_entry else None
                video_url = None
                is_video_pin = False
                videos = pin.get("videos") or {}
                video_list = videos.get("video_list") or {}
                if video_list:
                    first_video = next(iter(video_list.values()), None)
                    if first_video and first_video.get("url"):
                        video_url = first_video["url"]
                        is_video_pin = True
                else:
                    is_story = pin.get("story_pin_data_id") or pin.get("story_pin_data")
                    is_playable = pin.get("is_playable") or pin.get("is_video")
                    if is_story or is_playable:
                        detail = await self._fetch_pin_detail(pin_id)
                        if detail and detail.get("video_url"):
                            video_url = detail["video_url"]
                            is_video_pin = True
                if is_video_pin:
                    if not video_url or not self._is_supported_video(video_url):
                        continue
                    media_type = "video"
                    media_url = video_url
                else:
                    media_type = "photo"
                    media_url = fallback_media_url
                if not media_url:
                    continue
                extra = {
                    "dominant_color": pin.get("dominant_color"),
                    "source_pin_id": str(pin_id),
                }
                if pin.get("image_signature"):
                    extra["image_signature"] = pin.get("image_signature")
                if pin.get("story_pin_data_id"):
                    extra["story_pin_data_id"] = pin.get("story_pin_data_id")
                items.append(
                    ContentItem(
                        source_type=self.name,
                        source_id=str(pin_id),
                        title=pin.get("title") or pin.get("grid_title") or "Pinterest pin",
                        caption=pin.get("description") or pin.get("grid_description"),
                        media_url=media_url,
                        media_type=media_type,
                        video_url=video_url,
                        permalink=pin.get("link") or f"https://www.pinterest.com/pin/{pin_id}/",
                        extra=extra,
                    )
                )
            pages += 1

            bookmark, source_url, source_id, rs_value = self._extract_pagination_state(
                payload, source_url, source_id, rs_value
            )
            if not bookmark or bookmark == "-end-":
                break

        self._update_cursor(bookmark, source_url, source_id, rs_value)
        logger.info("Scraped %s pins for query '%s' over %s page(s)", len(items), self.query, pages)
        return items[:limit]

    @staticmethod
    def _parse_cookies(header: str) -> httpx.Cookies:
        simple = cookies.SimpleCookie()
        simple.load(header)
        jar = httpx.Cookies()
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

    async def _ensure_bootstrap(self) -> None:
        if self.app_version:
            return
        params = {"q": self.query, "rs": "typed"}
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Language": self.locale,
        }
        async with httpx.AsyncClient(timeout=20, cookies=self.cookies, follow_redirects=True) as client:
            response = await client.get(
                "https://www.pinterest.com/search/pins/",
                params=params,
                headers=headers,
            )
        if response.status_code != 200:
            logger.warning("Unable to bootstrap Pinterest search page")
            return
        match = re.search(
            r'<script id="__PWS_DATA__" type="application/json">(.*?)</script>',
            response.text,
            re.DOTALL,
        )
        if not match:
            logger.warning("Cannot locate __PWS_DATA__ payload")
            return
        data = json.loads(html.unescape(match.group(1)))
        context = data.get("context", {})
        app_version = data.get("appVersion") or context.get("app_version")
        if app_version:
            self.app_version = app_version
        request_identifier = context.get("request_identifier")
        if request_identifier:
            self.request_identifier = request_identifier
        if not BOOKMARK_RE.search(response.text):
            logger.warning("Could not extract bookmarks for Pinterest search")

    def _build_context(self) -> dict:
        context: dict = {}
        if self.request_identifier:
            context["request_identifier"] = self.request_identifier
        return context

    def _extract_pagination_state(
        self,
        payload: dict,
        fallback_source_url: str,
        fallback_source_id: Optional[str],
        fallback_rs: str,
    ) -> tuple[str, str, Optional[str], str]:
        resource_opts = payload.get("resource", {}).get("options") or {}
        bookmark = payload.get("resource_response", {}).get("bookmark")
        if not bookmark:
            bookmarks = resource_opts.get("bookmarks") or []
            if bookmarks:
                bookmark = bookmarks[-1]
        next_source_url = resource_opts.get("source_url") or fallback_source_url
        next_source_id = resource_opts.get("source_id") or fallback_source_id
        next_rs = resource_opts.get("rs") or fallback_rs
        return bookmark or "", next_source_url, next_source_id, next_rs

    def _update_cursor(
        self,
        bookmark: str,
        source_url: str,
        source_id: Optional[str],
        rs_value: str,
    ) -> None:
        if not bookmark or bookmark == "-end-":
            self._cursor_bookmark = "-end-"
            self._cursor_source_url = self.source_url
            self._cursor_source_id = None
            self._cursor_rs = "typed"
            return
        self._cursor_bookmark = bookmark
        self._cursor_source_url = source_url or self.source_url
        self._cursor_source_id = source_id
        self._cursor_rs = rs_value or "typed"

    @staticmethod
    def _is_supported_video(url: str) -> bool:
        clean = url.split("?", 1)[0].lower()
        return clean.endswith(".mp4")

    async def _fetch_pin_detail(self, pin_id: str) -> Optional[dict]:
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Language": self.locale,
            "Referer": f"https://www.pinterest.com/pin/{pin_id}/",
            "Origin": "https://www.pinterest.com",
            "X-CSRFToken": self.csrf_token,
            "X-Requested-With": "XMLHttpRequest",
            "X-App-Version": self.app_version or "ecf1375",
        }
        form_data = {
            "source_url": f"/pin/{pin_id}/",
            "data": json.dumps({"options": {"id": pin_id}, "context": {}}),
        }
        async with httpx.AsyncClient(timeout=15, cookies=self.cookies) as client:
            resp = await client.post(
                "https://www.pinterest.com/resource/PinResource/get/",
                headers=headers,
                data=form_data,
            )
        if resp.status_code != 200:
            logger.debug("Pin detail request failed for %s", pin_id)
            return None
        payload = resp.json().get("resource_response", {}).get("data") or {}
        videos = payload.get("videos") or {}
        video_list = videos.get("video_list") or {}
        first = next(iter(video_list.values()), None)
        if first and first.get("url"):
            return {"video_url": first["url"]}
        # Fallback to scraping pin HTML for mp4 link
        async with httpx.AsyncClient(timeout=15) as client:
            html_resp = await client.get(
                f"https://www.pinterest.com/pin/{pin_id}/",
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                    "Accept-Language": "en-US,en;q=0.8",
                },
                follow_redirects=True,
            )
        if html_resp.status_code == 200:
            match = PIN_HTML_VIDEO_RE.search(html_resp.text)
            if match:
                return {"video_url": match.group(0)}
        return None
