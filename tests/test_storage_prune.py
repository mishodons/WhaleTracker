from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.storage.repositories import Repository
from src.weather.execution import trade_timestamp_ms


def _ws_event(token_id: str, timestamp_ms: int, *, event_type: str = "book") -> dict:
    return {
        "event_type": event_type,
        "token_id": token_id,
        "market_id": "0xcond",
        "event_exchange_timestamp_ms": timestamp_ms,
        "event_exchange_timestamp": "2025-09-15T01:21:31+00:00",
        "local_received_at": "2025-09-15T01:21:31+00:00",
        "message_hash": None,
        "transaction_hash": None,
        "side": None,
        "price": None,
        "size": None,
        "best_bid": 0.4,
        "best_ask": 0.5,
        "spread": 0.1,
        "midpoint": 0.45,
        "full_book_json": "{}",
        "raw_json": "{}",
    }


def test_prune_unlinked_ws_events_keeps_execution_context_rows(tmp_path) -> None:
    repo = Repository(tmp_path / "test.sqlite")
    trader_id = repo.upsert_trader("0xabc", "test")
    raw_trade = {
        "proxyWallet": "0xabc",
        "side": "BUY",
        "asset": "tok",
        "conditionId": "0xcond",
        "size": 7,
        "price": 0.5,
        "timestamp": 1757908892,
        "title": "Will the highest temperature in Toronto be 15C or higher on May 3?",
        "slug": "highest-temperature-in-toronto-on-may-3-2026-15corhigher",
        "eventSlug": "highest-temperature-in-toronto-on-may-3-2026",
        "outcome": "Yes",
        "transactionHash": "0xtx",
    }
    trade_id, _ = repo.insert_trade(trader_id, raw_trade)
    keep_id = repo.insert_ws_orderbook_event(_ws_event("tok", 1757908891000))
    delete_id = repo.insert_ws_orderbook_event(_ws_event("tok", 1757908890000))
    repo.upsert_trade_execution_context(
        {
            "trade_id": trade_id,
            "token_id": "tok",
            "execution_timestamp_ms": trade_timestamp_ms(dict(repo.get_trade(trade_id))),
            "execution_timestamp": "2025-09-15T01:21:32+00:00",
            "execution_timestamp_source": "ws_millisecond_precision",
            "ws_trade_event_id": None,
            "pre_book_event_id": keep_id,
            "post_book_event_id": None,
            "pre_book_delta_ms": 1000,
            "post_book_delta_ms": None,
            "match_confidence": "probable_ws_match",
            "quality_flags": "pre_trade_book_cached",
        }
    )

    dry_run = repo.prune_unlinked_ws_events(execute=False)
    assert dry_run["eligible_unlinked_ws_events"] == 1
    assert dry_run["deleted_ws_events"] == 0

    result = repo.prune_unlinked_ws_events(execute=True)
    assert result["deleted_ws_events"] == 1
    with repo.connect() as conn:
        assert conn.execute("SELECT id FROM ws_orderbook_events WHERE id=?", (keep_id,)).fetchone()
        assert conn.execute("SELECT id FROM ws_orderbook_events WHERE id=?", (delete_id,)).fetchone() is None
