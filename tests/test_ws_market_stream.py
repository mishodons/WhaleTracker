from __future__ import annotations

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.storage.repositories import Repository
from src.ws.market_stream import BookState, MarketStreamRecorder, exchange_ts_ms, normalize_ws_payload


def test_exchange_timestamp_ms() -> None:
    assert exchange_ts_ms("1757908892351") == 1757908892351
    assert exchange_ts_ms("1757908892") == 1757908892000


def test_book_and_price_change_normalization_updates_state() -> None:
    states: dict[str, BookState] = {}
    rows = normalize_ws_payload(
        {
            "event_type": "book",
            "asset_id": "tok",
            "market": "0xcond",
            "bids": [{"price": "0.48", "size": "10"}],
            "asks": [{"price": "0.52", "size": "15"}],
            "timestamp": "1757908892351",
            "hash": "h1",
        },
        states=states,
        local_received_at="2026-01-01T00:00:00+00:00",
    )
    assert rows[0]["best_bid"] == 0.48
    assert rows[0]["best_ask"] == 0.52
    assert json.loads(rows[0]["full_book_json"])["asset_id"] == "tok"

    rows = normalize_ws_payload(
        {
            "event_type": "price_change",
            "market": "0xcond",
            "timestamp": "1757908892451",
            "price_changes": [{"asset_id": "tok", "price": "0.49", "size": "20", "side": "BUY"}],
        },
        states=states,
        local_received_at="2026-01-01T00:00:01+00:00",
    )
    assert rows[0]["event_type"] == "price_change"
    assert rows[0]["best_bid"] == 0.49
    assert states["tok"].best_bid() == 0.49


def test_last_trade_price_normalization() -> None:
    states = {"tok": BookState(token_id="tok", bids={0.4: 10}, asks={0.5: 10})}
    rows = normalize_ws_payload(
        {
            "event_type": "last_trade_price",
            "asset_id": "tok",
            "market": "0xcond",
            "price": "0.5",
            "size": "7",
            "side": "BUY",
            "timestamp": "1757908892551",
            "transaction_hash": "0xtx",
        },
        states=states,
        local_received_at="2026-01-01T00:00:02+00:00",
    )
    assert rows[0]["transaction_hash"] == "0xtx"
    assert rows[0]["price"] == 0.5
    assert rows[0]["midpoint"] == 0.45


def test_recorder_persists_only_selected_execution_rows(tmp_path) -> None:
    repo = Repository(tmp_path / "test.sqlite")
    recorder = MarketStreamRecorder(repo, websocket_url="ws://example.invalid", persist_raw_events=False)
    pre = {
        "event_type": "book",
        "token_id": "tok",
        "event_exchange_timestamp_ms": 1757908891000,
        "event_exchange_timestamp": "2025-09-15T01:21:31+00:00",
        "local_received_at": "2025-09-15T01:21:31+00:00",
        "full_book_json": "{}",
        "raw_json": "{}",
    }
    fill = {
        "event_type": "last_trade_price",
        "token_id": "tok",
        "event_exchange_timestamp_ms": 1757908892001,
        "event_exchange_timestamp": "2025-09-15T01:21:32.001000+00:00",
        "local_received_at": "2025-09-15T01:21:32.010000+00:00",
        "transaction_hash": "0xtx",
        "price": 0.5,
        "size": 7,
        "raw_json": "{}",
    }
    post = dict(pre, event_exchange_timestamp_ms=1757908893000, event_exchange_timestamp="2025-09-15T01:21:33+00:00")
    recorder._remember(pre)
    recorder._remember(fill)
    recorder._remember(post)

    with repo.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM ws_orderbook_events").fetchone()[0] == 0

    persisted = recorder.persist_execution_rows(
        {
            "token_id": "tok",
            "trade_timestamp": "2025-09-15T01:21:32+00:00",
            "tx_hash": "0xtx",
            "price": 0.5,
            "size": 7,
        }
    )
    assert persisted["ws_trade"]["id"]
    assert persisted["pre_book"]["id"]
    assert persisted["post_book"]["id"]
    with repo.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM ws_orderbook_events").fetchone()[0] == 3

    recorder.persist_execution_rows(
        {
            "token_id": "tok",
            "trade_timestamp": "2025-09-15T01:21:32+00:00",
            "tx_hash": "0xtx",
            "price": 0.5,
            "size": 7,
        }
    )
    with repo.connect() as conn:
        assert conn.execute("SELECT COUNT(*) FROM ws_orderbook_events").fetchone()[0] == 3
