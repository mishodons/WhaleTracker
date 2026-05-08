from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.weather.positions import compute_weather_basket_pnl


def test_complete_basket_pnl() -> None:
    positions = [
        {"token_id": "a", "net_size": 10, "avg_entry_price": 0.2},
        {"token_id": "b", "net_size": 10, "avg_entry_price": 0.3},
        {"token_id": "c", "net_size": 10, "avg_entry_price": 0.1},
    ]
    buckets = [
        {"basket_id": 1, "token_id": "a", "outcome": "Yes", "bucket_label": "16C"},
        {"basket_id": 1, "token_id": "b", "outcome": "Yes", "bucket_label": "17C"},
        {"basket_id": 1, "token_id": "c", "outcome": "Yes", "bucket_label": "18C"},
    ]
    pnl = compute_weather_basket_pnl(1, positions, buckets)[0]
    assert pnl["coverage_type"] == "complete_yes_basket"
    assert round(pnl["total_cost"], 6) == 6
    assert round(pnl["worst_case_pnl"], 6) == 4
    assert round(pnl["guaranteed_edge"], 6) == 4


def test_partial_basket_has_no_guaranteed_edge() -> None:
    positions = [{"token_id": "a", "net_size": 10, "avg_entry_price": 0.2}]
    buckets = [
        {"basket_id": 1, "token_id": "a", "outcome": "Yes", "bucket_label": "16C"},
        {"basket_id": 1, "token_id": "b", "outcome": "Yes", "bucket_label": "17C"},
    ]
    pnl = compute_weather_basket_pnl(1, positions, buckets)[0]
    assert pnl["coverage_type"] == "partial_yes_basket"
    assert pnl["guaranteed_edge"] is None
    assert pnl["worst_case_pnl"] == -2

