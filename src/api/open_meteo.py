from __future__ import annotations

import asyncio
from typing import Any

import httpx


class OpenMeteoClient:
    def __init__(
        self,
        *,
        forecast_base_url: str = "https://api.open-meteo.com",
        geocoding_base_url: str = "https://geocoding-api.open-meteo.com",
        timeout: float = 20,
        max_retries: int = 3,
        backoff: float = 0.75,
    ):
        self.forecast_base_url = forecast_base_url.rstrip("/")
        self.geocoding_base_url = geocoding_base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        headers = {"User-Agent": "PolymarketWhaleTracker/0.1 weather-forecast-observer"}
        self._forecast = httpx.AsyncClient(base_url=self.forecast_base_url, timeout=timeout, headers=headers)
        self._geocoding = httpx.AsyncClient(base_url=self.geocoding_base_url, timeout=timeout, headers=headers)

    async def close(self) -> None:
        await self._forecast.aclose()
        await self._geocoding.aclose()

    async def _get(self, client: httpx.AsyncClient, path: str, params: dict[str, Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = await client.get(path, params=params)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                await asyncio.sleep(self.backoff * (2**attempt))
        raise RuntimeError(f"Open-Meteo request failed: {path}") from last_error

    async def geocode(self, city: str, *, count: int = 5, language: str = "en") -> list[dict[str, Any]]:
        data = await self._get(self._geocoding, "/v1/search", {"name": city, "count": count, "language": language, "format": "json"})
        return data.get("results") or []

    async def forecast_daily_high(
        self,
        *,
        latitude: float,
        longitude: float,
        forecast_date: str,
        unit: str = "C",
        timezone: str = "UTC",
    ) -> dict[str, Any]:
        temperature_unit = "fahrenheit" if unit.upper() == "F" else "celsius"
        return await self._get(
            self._forecast,
            "/v1/forecast",
            {
                "latitude": latitude,
                "longitude": longitude,
                "start_date": forecast_date,
                "end_date": forecast_date,
                "current": "temperature_2m",
                "daily": "temperature_2m_max",
                "hourly": "temperature_2m",
                "temperature_unit": temperature_unit,
                "timezone": timezone,
            },
        )
