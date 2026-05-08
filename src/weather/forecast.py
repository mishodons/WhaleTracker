from __future__ import annotations

from typing import Any

from src.utils.dedupe import raw_json
from src.utils.time import utc_now_iso


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def choose_geocode_result(results: list[dict[str, Any]], city: str) -> dict[str, Any] | None:
    if not results:
        return None
    exact = [row for row in results if str(row.get("name", "")).lower() == city.lower()]
    candidates = exact or results
    return sorted(candidates, key=lambda row: int(row.get("population") or 0), reverse=True)[0]


def extract_forecast_high(payload: dict[str, Any], forecast_date: str) -> dict[str, float | None]:
    daily_high = None
    daily = payload.get("daily") or {}
    daily_times = daily.get("time") or []
    daily_values = daily.get("temperature_2m_max") or []
    for index, day in enumerate(daily_times):
        if day == forecast_date and index < len(daily_values):
            daily_high = _num(daily_values[index])
            break

    hourly_high = None
    hourly = payload.get("hourly") or {}
    hourly_times = hourly.get("time") or []
    hourly_values = hourly.get("temperature_2m") or []
    values = [
        _num(value)
        for index, value in enumerate(hourly_values)
        if index < len(hourly_times) and str(hourly_times[index]).startswith(forecast_date + "T")
    ]
    values = [value for value in values if value is not None]
    if values:
        hourly_high = max(values)
    return {
        "daily_high": daily_high,
        "hourly_high": hourly_high,
        "predicted_high": daily_high if daily_high is not None else hourly_high,
    }


async def geocode_city(repository: Any, client: Any, city: str) -> dict[str, Any] | None:
    cached = repository.get_weather_city_geocode(city)
    if cached:
        return dict(cached)
    results = await client.geocode(city)
    chosen = choose_geocode_result(results, city)
    if not chosen:
        repository.log("WARN", "weather_forecast", "geocode failed", {"city": city})
        return None
    row = {
        "city": city,
        "provider": "open-meteo",
        "provider_location_id": str(chosen.get("id")) if chosen.get("id") is not None else None,
        "matched_name": chosen.get("name"),
        "country_code": chosen.get("country_code"),
        "country": chosen.get("country"),
        "admin1": chosen.get("admin1"),
        "latitude": chosen.get("latitude"),
        "longitude": chosen.get("longitude"),
        "timezone": chosen.get("timezone") or "UTC",
        "population": chosen.get("population"),
        "confidence": "exact_city_name" if str(chosen.get("name", "")).lower() == city.lower() else "fuzzy_city_match",
        "raw_json": raw_json(chosen),
    }
    repository.upsert_weather_city_geocode(row)
    return row


async def capture_forecast_for_basket(repository: Any, client: Any, basket: dict[str, Any]) -> dict[str, Any] | None:
    city = basket.get("city")
    forecast_date = basket.get("forecast_date")
    if not city or not forecast_date:
        return None
    geocode = await geocode_city(repository, client, city)
    if not geocode:
        return None
    unit = (basket.get("unit") or "C").upper()
    payload = await client.forecast_daily_high(
        latitude=float(geocode["latitude"]),
        longitude=float(geocode["longitude"]),
        forecast_date=forecast_date,
        unit=unit,
        timezone=geocode.get("timezone") or "UTC",
    )
    highs = extract_forecast_high(payload, forecast_date)
    record = {
        "basket_id": basket.get("id"),
        "source": "open-meteo",
        "city": city,
        "forecast_date": forecast_date,
        "unit": unit,
        "latitude": geocode.get("latitude"),
        "longitude": geocode.get("longitude"),
        "provider_timezone": payload.get("timezone") or geocode.get("timezone"),
        "captured_at": utc_now_iso(),
        "forecast_generated_at": payload.get("generationtime_ms"),
        "predicted_high": highs.get("predicted_high"),
        "daily_high": highs.get("daily_high"),
        "hourly_high": highs.get("hourly_high"),
        "model": "open-meteo-auto",
        "quality_flags": "daily_temperature_2m_max,public_forecast_api",
        "raw_json": raw_json(payload),
    }
    repository.insert_weather_forecast_snapshot(record)
    return record


async def capture_forecasts_for_active_baskets(repository: Any, client: Any) -> list[dict[str, Any]]:
    snapshots: list[dict[str, Any]] = []
    for basket in repository.list_weather_baskets_for_forecasts():
        try:
            snapshot = await capture_forecast_for_basket(repository, client, dict(basket))
        except Exception as exc:
            repository.log("WARN", "weather_forecast", "forecast capture failed", {"basket": dict(basket), "error": str(exc)})
            continue
        if snapshot:
            snapshots.append(snapshot)
    repository.log("INFO", "weather_forecast", "forecast capture completed", {"snapshots": len(snapshots)})
    return snapshots

