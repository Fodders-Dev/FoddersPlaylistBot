from __future__ import annotations

import base64
import time
from typing import Any, List, Optional

import httpx

from .base import ContentItem, ContentSource, ContentSourceFactory


class SpotifyClient:
    TOKEN_URL = "https://accounts.spotify.com/api/token"
    API_URL = "https://api.spotify.com/v1"

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: Optional[str] = None,
    ) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.refresh_token = refresh_token
        self._token: Optional[str] = None
        self._expires_at: float = 0
        self._client = httpx.AsyncClient(base_url=self.API_URL)

    async def close(self) -> None:
        await self._client.aclose()

    async def _ensure_token(self) -> None:
        if self._token and time.time() < self._expires_at - 30:
            return
        auth_header = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode()).decode()
        data: dict[str, str] = {"grant_type": "client_credentials"}
        if self.refresh_token:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
            }
        async with httpx.AsyncClient() as client:
            response = await client.post(
                self.TOKEN_URL,
                data=data,
                headers={"Authorization": f"Basic {auth_header}"},
                timeout=15,
            )
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        self._expires_at = time.time() + payload.get("expires_in", 3600)

    async def fetch_playlist(self, playlist_id: str, limit: int = 20) -> List[dict[str, Any]]:
        await self._ensure_token()
        response = await self._client.get(
            f"/playlists/{playlist_id}/tracks",
            params={"limit": limit},
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("items", [])


@ContentSourceFactory.register("spotify_playlist")
class SpotifyPlaylistSource:
    name = "spotify_playlist"

    def __init__(self, client: SpotifyClient, playlist_id: str, caption_template: str | None = None) -> None:
        self.client = client
        self.playlist_id = playlist_id
        self.caption_template = caption_template or "{artist} — {title}"

    async def fetch(self, limit: int = 10) -> List[ContentItem]:
        items = await self.client.fetch_playlist(self.playlist_id, limit=limit)
        content: List[ContentItem] = []
        for entry in items:
            track = entry.get("track")
            if not track:
                continue
            track_id = track.get("id")
            if not track_id:
                continue
            images = track.get("album", {}).get("images", [])
            cover = images[0]["url"] if images else None
            if not cover:
                continue
            artist = ", ".join(a["name"] for a in track.get("artists", []))
            title = track.get("name")
            caption = self.caption_template.format(artist=artist, title=title)
            content.append(
                ContentItem(
                    source_type=self.name,
                    source_id=track_id,
                    title=title,
                    caption=caption,
                    media_url=cover,
                    permalink=track.get("external_urls", {}).get("spotify"),
                    extra={"audio_preview": track.get("preview_url")},
                )
            )
        return content
