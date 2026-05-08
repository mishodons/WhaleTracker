from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.analysis.metrics import classify_position_action, classify_price_bucket, compute_orderbook_metrics, favorable_move


def sample_book() -> dict:
    return {
        "market": "0xcond",
        "asset_id": "123",
        "bids": [{"price": "0.48", "size": "100"}, {"price": "0.47", "size": "200"}],
        "asks": [{"price": "0.52", "size": "150"}, {"price": "0.53", "size": "250"}],
    }


def test_orderbook_metrics() -> None:
    metrics = compute_orderbook_metrics(sample_book(), trade_side="BUY", slippage_notional_sizes=[50], liquidity_bands_pct=[1, 10])
    assert metrics["best_bid"] == 0.48
    assert metrics["best_ask"] == 0.52
    assert round(metrics["spread"], 4) == 0.04
    assert round(metrics["midpoint"], 4) == 0.50
    assert metrics["liquidity_10pct"] > metrics["liquidity_1pct"]
    assert metrics["slippage"]["50"]["complete"] is True


def test_price_buckets_and_favorable_moves() -> None:
    assert classify_price_bucket(0.09) == "under_10c"
    assert classify_price_bucket(0.24) == "10_25c"
    assert classify_price_bucket(0.90) == "over_90c"
    assert favorable_move("BUY", 0.4, 0.5) is True
    assert favorable_move("SELL", 0.4, 0.3) is True


def test_position_action_labels() -> None:
    assert classify_position_action(0, 10) == "entering"
    assert classify_position_action(10, 15) == "adding"
    assert classify_position_action(10, 3) == "reducing"
    assert classify_position_action(10, 0) == "exiting"
    assert classify_position_action(10, -2) == "flipping_or_incomplete_history"

