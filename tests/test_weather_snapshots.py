from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.weather.snapshots import basket_metrics, followup_record_from_state, followup_schedules


def test_basket_metrics_computes_one_share_edges() -> None:
    metrics = basket_metrics(
        [
            {"outcome": "Yes", "best_ask": 0.4, "best_bid": 0.35, "matched": True},
            {"outcome": "Yes", "best_ask": 0.5, "best_bid": 0.45, "matched": True},
        ]
    )
    assert metrics["token_count"] == 2
    assert metrics["matched_token_count"] == 2
    assert metrics["complete_yes_ask_cost"] == 0.9
    assert round(metrics["one_share_yes_ask_edge"], 6) == 0.1
    assert round(metrics["one_share_yes_bid_edge"], 6) == -0.2


def test_followup_record_marks_favorable_buy_move() -> None:
    record = followup_record_from_state(
        {
            "id": 1,
            "token_id": "tok",
            "trade_price": 0.4,
            "trade_side": "BUY",
        },
        {
            "summaries": [{"token_id": "tok", "outcome": "Yes", "midpoint": 0.55, "best_ask": 0.56, "best_bid": 0.54, "matched": True}],
            "metrics": basket_metrics([{"outcome": "Yes", "best_ask": 0.56, "best_bid": 0.54, "matched": True}]),
        },
    )
    assert round(record["price_change_from_trade"], 6) == 0.15
    assert record["favorable_move_boolean"] is True
    assert "clob_rest" in record["quality_flags"]


def test_followup_schedules_are_relative_to_trade_time() -> None:
    schedules = followup_schedules("2026-05-04T10:00:00+00:00", ["30s", "5m"])
    assert schedules == [
        ("30s", "2026-05-04T10:00:30+00:00"),
        ("5m", "2026-05-04T10:05:00+00:00"),
    ]
