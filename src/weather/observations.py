from __future__ import annotations

from typing import Any

from src.utils.dedupe import raw_json
from src.utils.time import utc_now_iso
from src.weather.forecast import extract_forecast_high, geocode_city


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _current_temperature(payload: dict[str, Any]) -> tuple[str | None, float | None]:
    current = payload.get("current") or {}
    return current.get("time"), _num(current.get("temperature_2m"))


async def capture_observation_for_basket(repository: Any, client: Any, basket: dict[str, Any]) -> dict[str, Any] | None:
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
    observation_time, current_temp = _current_temperature(payload)
    quality_flags = ["open_meteo_forecast_endpoint", "not_official_station_settlement"]
    if observation_time and not str(observation_time).startswith(forecast_date + "T"):
        current_temp = None
        quality_flags.append("current_temperature_not_same_forecast_date")
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
        "observation_time": observation_time,
        "current_temperature": current_temp,
        "intraday_high": highs.get("hourly_high"),
        "daily_high": highs.get("daily_high"),
        "observed_high": highs.get("hourly_high"),
        "observation_status": "provisional_model_observation",
        "quality_flags": ",".join(quality_flags),
        "raw_json": raw_json(payload),
    }
    repository.insert_weather_observation(record)
    return record


async def capture_observations_for_active_baskets(repository: Any, client: Any) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for basket in repository.list_weather_baskets_for_forecasts():
        try:
            observation = await capture_observation_for_basket(repository, client, dict(basket))
        except Exception as exc:
            repository.log("WARN", "weather_observations", "observation capture failed", {"basket": dict(basket), "error": str(exc)})
            continue
        if observation:
            observations.append(observation)
    repository.log("INFO", "weather_observations", "observation capture completed", {"observations": len(observations)})
    return observations
