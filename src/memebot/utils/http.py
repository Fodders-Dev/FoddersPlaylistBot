from __future__ import annotations

import mimetypes
from typing import Dict, Optional, Tuple

import httpx


async def download_binary(url: str, timeout: int = 30, headers: Optional[Dict[str, str]] = None) -> Tuple[bytes, str]:
    req_headers = headers or {}
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True, headers=req_headers) as client:
        response = await client.get(url)
        response.raise_for_status()
        content_type = response.headers.get("content-type") or mimetypes.guess_type(url)[0] or "application/octet-stream"
        return response.content, content_type
