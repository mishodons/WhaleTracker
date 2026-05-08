from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.analysis.positions import reconstruct_positions_from_trades


def test_position_reconstruction_add_reduce_exit() -> None:
    trades = [
        {"id": 1, "token_id": "yes", "market_id": "m", "condition_id": "c", "outcome": "Yes", "side": "BUY", "size": 100, "price": 0.4, "trade_timestamp": "2024-01-01T00:00:00+00:00"},
        {"id": 2, "token_id": "yes", "market_id": "m", "condition_id": "c", "outcome": "Yes", "side": "BUY", "size": 50, "price": 0.5, "trade_timestamp": "2024-01-01T01:00:00+00:00"},
        {"id": 3, "token_id": "yes", "market_id": "m", "condition_id": "c", "outcome": "Yes", "side": "SELL", "size": 75, "price": 0.6, "trade_timestamp": "2024-01-01T02:00:00+00:00"},
        {"id": 4, "token_id": "yes", "market_id": "m", "condition_id": "c", "outcome": "Yes", "side": "SELL", "size": 75, "price": 0.3, "trade_timestamp": "2024-01-01T03:00:00+00:00"},
    ]
    pos = reconstruct_positions_from_trades(trades, trader_id=1)[0]
    assert pos["net_size"] == 0
    assert round(pos["estimated_realized_pnl"], 6) == 2.5
    assert pos["direction"] == "FLAT"


def test_position_reconstruction_marks_incomplete_history_on_oversell() -> None:
    trades = [
        {"id": 1, "token_id": "yes", "market_id": "m", "condition_id": "c", "outcome": "Yes", "side": "SELL", "size": 25, "price": 0.7, "trade_timestamp": "2024-01-01T00:00:00+00:00"}
    ]
    pos = reconstruct_positions_from_trades(trades, trader_id=1)[0]
    assert pos["net_size"] == -25
    assert pos["direction"] == "OBSERVED_NEGATIVE"
    assert pos["confidence"] == "approximate_position_reconstruction"

def test_position_reconstruction_separates_opposite_outcomes() -> None:
    trades = [
        {"id": 1, "token_id": "yes", "condition_id": "c", "outcome": "Yes", "side": "BUY", "size": 10, "price": 0.4, "trade_timestamp": "2024-01-01T00:00:00+00:00"},
        {"id": 2, "token_id": "no", "condition_id": "c", "outcome": "No", "side": "BUY", "size": 7, "price": 0.6, "trade_timestamp": "2024-01-01T01:00:00+00:00"},
    ]
    positions = reconstruct_positions_from_trades(trades, trader_id=1)
    assert {row["token_id"] for row in positions} == {"yes", "no"}

