from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

import httpx


class PolymarketDataClient:
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
        raise RuntimeError(f"Data API request failed: {path}") from last_error

    async def get_trades(
        self,
        user: str,
        *,
        limit: int = 100,
        offset: int = 0,
        taker_only: bool = False,
        side: str | None = None,
        market: str | None = None,
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"user": user, "limit": limit, "offset": offset, "takerOnly": str(taker_only).lower()}
        if side:
            params["side"] = side.upper()
        if market:
            params["market"] = market
        data = await self._get("/trades", params)
        return data if isinstance(data, list) else data.get("data", [])

    async def iter_trades(self, user: str, *, page_limit: int = 500, max_pages: int = 20) -> AsyncIterator[dict[str, Any]]:
        for page in range(max_pages):
            rows = await self.get_trades(user, limit=page_limit, offset=page * page_limit, taker_only=False)
            if not rows:
                break
            for row in rows:
                yield row
            if len(rows) < page_limit:
                break

    async def get_activity(self, user: str, *, limit: int = 100, offset: int = 0, activity_type: str = "TRADE") -> list[dict[str, Any]]:
        params = {"user": user, "limit": limit, "offset": offset, "type": activity_type}
        data = await self._get("/activity", params)
        return data if isinstance(data, list) else data.get("data", [])

    async def get_positions(self, user: str, *, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        data = await self._get("/positions", {"user": user, "limit": limit, "offset": offset})
        return data if isinstance(data, list) else data.get("data", [])

    async def get_closed_positions(self, user: str, *, limit: int = 500, offset: int = 0) -> list[dict[str, Any]]:
        data = await self._get("/closed-positions", {"user": user, "limit": limit, "offset": offset})
        return data if isinstance(data, list) else data.get("data", [])

    async def get_value(self, user: str) -> Any:
        return await self._get("/value", {"user": user})
