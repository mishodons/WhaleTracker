from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.storage.repositories import Repository
from src.weather.settlements import (
    compute_final_bucket_pnl,
    generate_weather_day_report,
    market_yes_won,
    set_manual_settlement,
    winning_bucket_from_temp,
)


def _seed_basket(repo: Repository) -> tuple[int, int]:
    basket_id = repo.upsert_weather_basket(
        {
            "event_slug": "highest-temperature-in-test-city-on-may-4-2026",
            "city": "Test City",
            "forecast_date": "2026-05-04",
            "unit": "C",
            "event_title": "Test City temp",
            "status": "active",
        }
    )
    repo.upsert_weather_bucket_market(
        {
            "basket_id": basket_id,
            "condition_id": "0x20",
            "token_id": "tok20",
            "market_slug": "highest-temperature-in-test-city-on-may-4-2026-20c",
            "market_title": "Will the highest temperature in Test City be 20C on May 4?",
            "outcome": "Yes",
            "bucket_label": "20C",
            "lower_temp": 20,
            "upper_temp": 20,
            "bound_type": "exact",
            "active": True,
            "closed": False,
        }
    )
    repo.upsert_weather_bucket_market(
        {
            "basket_id": basket_id,
            "condition_id": "0x21",
            "token_id": "tok21",
            "market_slug": "highest-temperature-in-test-city-on-may-4-2026-21c",
            "market_title": "Will the highest temperature in Test City be 21C on May 4?",
            "outcome": "Yes",
            "bucket_label": "21C",
            "lower_temp": 21,
            "upper_temp": 21,
            "bound_type": "exact",
            "active": True,
            "closed": False,
        }
    )
    trader_id = repo.upsert_trader("0xabc", "test")
    return basket_id, trader_id


def test_winning_bucket_from_final_temp(tmp_path) -> None:
    repo = Repository(tmp_path / "test.sqlite")
    basket_id, _ = _seed_basket(repo)
    buckets = [dict(row) for row in repo.list_weather_bucket_markets_for_basket(basket_id)]
    winner = winning_bucket_from_temp(buckets, 20)
    assert winner is not None
    assert winner["bucket_label"] == "20C"


def test_compute_final_bucket_pnl(tmp_path) -> None:
    repo = Repository(tmp_path / "test.sqlite")
    _, trader_id = _seed_basket(repo)
    repo.insert_trade(
        trader_id,
        {
            "proxyWallet": "0xabc",
            "side": "BUY",
            "asset": "tok20",
            "conditionId": "0x20",
            "size": 10,
            "price": 0.4,
            "timestamp": 1777900000,
            "title": "Will the highest temperature in Test City be 20C on May 4?",
            "slug": "highest-temperature-in-test-city-on-may-4-2026-20c",
            "eventSlug": "highest-temperature-in-test-city-on-may-4-2026",
            "outcome": "Yes",
            "transactionHash": "0x1",
        },
    )
    repo.insert_trade(
        trader_id,
        {
            "proxyWallet": "0xabc",
            "side": "BUY",
            "asset": "tok21",
            "conditionId": "0x21",
            "size": 5,
            "price": 0.2,
            "timestamp": 1777900001,
            "title": "Will the highest temperature in Test City be 21C on May 4?",
            "slug": "highest-temperature-in-test-city-on-may-4-2026-21c",
            "eventSlug": "highest-temperature-in-test-city-on-may-4-2026",
            "outcome": "Yes",
            "transactionHash": "0x2",
        },
    )
    set_manual_settlement(repo, "highest-temperature-in-test-city-on-may-4-2026", final_temp=20)
    _, rows = compute_final_bucket_pnl(repo, "highest-temperature-in-test-city-on-may-4-2026")
    by_bucket = {row["bucket_label"]: row for row in rows}
    assert by_bucket["20C"]["final_pnl"] == 6.0
    assert by_bucket["21C"]["final_pnl"] == -1.0
    report = generate_weather_day_report(repo, "highest-temperature-in-test-city-on-may-4-2026")
    assert "Final PnL: $5.00" in report
    assert "20C [WIN]" in report


def test_market_yes_won_from_gamma_outcome_prices() -> None:
    assert market_yes_won({"outcomes": '["Yes","No"]', "outcomePrices": '["1","0"]'}) is True
    assert market_yes_won({"outcomes": '["Yes","No"]', "outcomePrices": '["0","1"]'}) is False
