from __future__ import annotations

import asyncio
from typing import Any

import httpx


class AviationWeatherClient:
    def __init__(
        self,
        *,
        base_url: str = "https://aviationweather.gov/api/data",
        timeout: float = 20,
        max_retries: int = 3,
        backoff: float = 0.75,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        headers = {"User-Agent": "PolymarketWhaleTracker/0.1 metar-observer"}
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout, headers=headers)

    async def close(self) -> None:
        await self._client.aclose()

    async def _get(self, path: str, params: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = await self._client.get(path, params=params)
                if response.status_code == 204:
                    return []
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                await asyncio.sleep(self.backoff * (2**attempt))
        raise RuntimeError(f"AviationWeather request failed: {path}") from last_error

    async def get_metars(self, station_ids: list[str], *, hours: float = 2.0) -> list[dict[str, Any]]:
        ids = sorted({station.strip().upper() for station in station_ids if station and station.strip()})
        if not ids:
            return []
        data = await self._get(
            "/metar",
            {
                "ids": ",".join(ids),
                "format": "json",
                "hours": hours,
            },
        )
        return data if isinstance(data, list) else []
