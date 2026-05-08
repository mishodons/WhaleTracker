from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.storage.repositories import Repository
from src.weather.discovery import bucket_infos_from_market, is_weather_trade, parse_bucket_from_title, upsert_weather_event_markets, weather_event_slug


def test_weather_title_range_bucket() -> None:
    parsed = parse_bucket_from_title("Will the highest temperature in Miami be between 90-91°F on May 3?")
    assert parsed["city"] == "Miami"
    assert parsed["unit"] == "F"
    assert parsed["bucket_label"] == "90-91F"
    assert parsed["lower_temp"] == 90
    assert parsed["upper_temp"] == 91
    assert parsed["bound_type"] == "range"


def test_weather_title_lower_bound_bucket() -> None:
    parsed = parse_bucket_from_title("Will the highest temperature in Toronto be 15°C or higher on May 3?")
    assert parsed["city"] == "Toronto"
    assert parsed["bucket_label"] == "15C+"
    assert parsed["lower_temp"] == 15
    assert parsed["upper_temp"] is None
    assert parsed["bound_type"] == "lower_bound"


def test_weather_market_to_bucket_infos() -> None:
    raw = {
        "conditionId": "0xcond",
        "asset": "123",
        "slug": "highest-temperature-in-buenos-aires-on-may-3-2026-17c",
        "eventSlug": "highest-temperature-in-buenos-aires-on-may-3-2026",
        "title": "Will the highest temperature in Buenos Aires be 17°C on May 3?",
        "outcome": "Yes",
    }
    assert is_weather_trade(raw) is True
    assert weather_event_slug(raw) == "highest-temperature-in-buenos-aires-on-may-3-2026"
    info = bucket_infos_from_market(raw)[0]
    assert info.city == "Buenos Aires"
    assert info.forecast_date == "2026-05-03"
    assert info.bucket_label == "17C"
    assert info.token_id == "123"


def test_upsert_weather_event_markets_adds_sibling_buckets(tmp_path) -> None:
    repo = Repository(tmp_path / "test.sqlite")
    buckets = upsert_weather_event_markets(
        repo,
        {
            "slug": "highest-temperature-in-shenzhen-on-may-5-2026",
            "markets": [
                {
                    "conditionId": "0x1",
                    "asset": "tok19",
                    "slug": "highest-temperature-in-shenzhen-on-may-5-2026-19corbelow",
                    "question": "Will the highest temperature in Shenzhen be 19C or below on May 5?",
                    "outcome": "Yes",
                },
                {
                    "conditionId": "0x2",
                    "asset": "tok20",
                    "slug": "highest-temperature-in-shenzhen-on-may-5-2026-20c",
                    "question": "Will the highest temperature in Shenzhen be 20C on May 5?",
                    "outcome": "Yes",
                },
            ],
        },
    )
    assert len(buckets) == 2
    basket = repo.get_weather_basket_by_event_slug("highest-temperature-in-shenzhen-on-may-5-2026")
    assert basket is not None
    rows = repo.list_weather_bucket_markets_for_basket(int(basket["id"]))
    assert len(rows) == 2
