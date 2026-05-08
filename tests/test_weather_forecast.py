from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.weather.forecast import choose_geocode_result, extract_forecast_high


def test_choose_geocode_prefers_exact_high_population() -> None:
    results = [
        {"name": "London", "population": 1000, "latitude": 1, "longitude": 2},
        {"name": "London", "population": 9_000_000, "latitude": 51.5, "longitude": -0.1},
        {"name": "Londonderry", "population": 80_000, "latitude": 55, "longitude": -7},
    ]
    chosen = choose_geocode_result(results, "London")
    assert chosen["population"] == 9_000_000


def test_extract_forecast_high_prefers_daily_high() -> None:
    payload = {
        "daily": {"time": ["2026-05-04"], "temperature_2m_max": [24.7]},
        "hourly": {
            "time": ["2026-05-04T00:00", "2026-05-04T01:00", "2026-05-05T00:00"],
            "temperature_2m": [20.0, 22.0, 99.0],
        },
    }
    high = extract_forecast_high(payload, "2026-05-04")
    assert high["daily_high"] == 24.7
    assert high["hourly_high"] == 22.0
    assert high["predicted_high"] == 24.7


def test_extract_forecast_high_falls_back_to_hourly() -> None:
    payload = {
        "daily": {"time": [], "temperature_2m_max": []},
        "hourly": {
            "time": ["2026-05-04T00:00", "2026-05-04T14:00"],
            "temperature_2m": [20.0, 26.5],
        },
    }
    high = extract_forecast_high(payload, "2026-05-04")
    assert high["daily_high"] is None
    assert high["hourly_high"] == 26.5
    assert high["predicted_high"] == 26.5

