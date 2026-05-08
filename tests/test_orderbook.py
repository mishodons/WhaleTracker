from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.tracking.orderbook_snapshotter import snapshot_record_from_orderbook, unavailable_snapshot_record


def test_snapshot_record_from_orderbook() -> None:
    book = {
        "market": "0xcond",
        "asset_id": "123",
        "timestamp": "1710000000",
        "hash": "0xhash",
        "bids": [{"price": "0.48", "size": "100"}],
        "asks": [{"price": "0.52", "size": "100"}],
    }
    trade = {"id": 1, "token_id": "123", "side": "BUY", "trade_timestamp": "1710000000"}
    record = snapshot_record_from_orderbook(book=book, trade=trade, snapshot_type="entry")
    assert record["trade_id"] == 1
    assert record["best_bid"] == 0.48
    assert record["best_ask"] == 0.52
    assert record["book_hash"] == "0xhash"
    assert "nearest_post_detection_orderbook" in record["quality_flags"]


def test_unavailable_snapshot_is_explicit() -> None:
    record = unavailable_snapshot_record(trade={"id": 1, "token_id": "123"})
    assert record["snapshot_source"] == "unavailable"
    assert record["quality_flags"] == "historical_orderbook_unavailable"

