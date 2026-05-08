from __future__ import annotations

import asyncio
from typing import Any

import httpx


class GammaClient:
    def __init__(self, base_url: str, timeout: float = 20, max_retries: int = 3, backoff: float = 0.75):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=timeout,
            headers={"User-Agent": "PolymarketWhaleTracker/0.1 observation-only"},
        )

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = await self._client.get(path, params=params)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                await asyncio.sleep(self.backoff * (2**attempt))
        raise RuntimeError(f"Gamma API request failed: {path}") from last_error

    async def get_market_by_slug(self, slug: str) -> dict[str, Any] | None:
        if not slug:
            return None
        data = await self._get("/markets", {"slug": slug})
        if isinstance(data, list):
            return data[0] if data else None
        markets = data.get("markets") or data.get("data") or []
        return markets[0] if markets else None

    async def get_event_by_slug(self, slug: str) -> dict[str, Any] | None:
        if not slug:
            return None
        data = await self._get(f"/events/slug/{slug}")
        if isinstance(data, dict) and data:
            return data
        data = await self._get("/events", {"slug": slug})
        if isinstance(data, list):
            return data[0] if data else None
        events = data.get("events") or data.get("data") or []
        return events[0] if events else None

    async def get_markets(self, **params: Any) -> list[dict[str, Any]]:
        data = await self._get("/markets", params)
        if isinstance(data, list):
            return data
        return data.get("markets") or data.get("data") or []

    async def get_market_by_token(self, token_id: str) -> dict[str, Any] | None:
        if not token_id:
            return None
        try:
            data = await self._get(f"/markets/token/{token_id}")
            if isinstance(data, dict):
                return data
        except RuntimeError:
            return None
        return None
