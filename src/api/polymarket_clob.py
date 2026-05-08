from __future__ import annotations

import asyncio
from typing import Any

import httpx


class PolymarketClobClient:
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

    async def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: Any = None) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = await self._client.request(method, path, params=params, json=json_body)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                await asyncio.sleep(self.backoff * (2**attempt))
        raise RuntimeError(f"CLOB API request failed: {method} {path}") from last_error

    async def get_orderbook(self, token_id: str) -> dict[str, Any]:
        return await self._request("GET", "/book", params={"token_id": token_id})

    async def get_orderbooks(self, token_ids: list[str]) -> list[dict[str, Any]]:
        body = [{"token_id": token_id} for token_id in token_ids]
        data = await self._request("POST", "/books", json_body=body)
        return data if isinstance(data, list) else data.get("data", [])

    async def get_midpoint(self, token_id: str) -> Any:
        return await self._request("GET", "/midpoint", params={"token_id": token_id})

    async def get_spread(self, token_id: str) -> Any:
        return await self._request("GET", "/spread", params={"token_id": token_id})

    async def get_prices_history(self, token_id: str, *, interval: str | None = None, fidelity: int | None = None, start_ts: int | None = None, end_ts: int | None = None) -> Any:
        params: dict[str, Any] = {"market": token_id}
        if interval:
            params["interval"] = interval
        if fidelity:
            params["fidelity"] = fidelity
        if start_ts:
            params["startTs"] = start_ts
        if end_ts:
            params["endTs"] = end_ts
        return await self._request("GET", "/prices-history", params=params)
