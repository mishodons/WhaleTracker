from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.weather.metar import normalize_metar_report, seed_known_station_mappings
from src.storage.repositories import Repository


def test_normalize_metar_report() -> None:
    row = normalize_metar_report(
        {
            "icaoId": "KMIA",
            "obsTime": "2026-05-05T12:00:00Z",
            "rawOb": "METAR KMIA 051200Z 09008KT 10SM 26/20 A3001",
            "temp": 26,
            "dewp": 20,
            "wdir": 90,
            "wspd": 8,
        },
        city="Miami",
        first_seen_at="2026-05-05T12:00:42+00:00",
    )
    assert row["station_id"] == "KMIA"
    assert row["city"] == "Miami"
    assert row["report_type"] == "METAR"
    assert row["temperature_c"] == 26
    assert row["first_seen_at"] == "2026-05-05T12:00:42+00:00"


def test_seed_known_station_mappings(tmp_path) -> None:
    repo = Repository(tmp_path / "test.sqlite")
    repo.upsert_weather_basket(
        {
            "event_slug": "highest-temperature-in-miami-on-may-5-2026",
            "city": "Miami",
            "forecast_date": "2026-05-05",
            "unit": "F",
        }
    )
    assert seed_known_station_mappings(repo) == 1
    rows = repo.list_weather_station_mappings_for_active_baskets()
    assert rows[0]["station_id"] == "KMIA"
