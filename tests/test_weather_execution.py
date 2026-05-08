from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.storage.repositories import Repository
from src.weather.execution import attach_execution_context


def test_attach_execution_context_exact_ws_match(tmp_path) -> None:
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
        "title": "Will the highest temperature in Toronto be 15°C or higher on May 3?",
        "slug": "highest-temperature-in-toronto-on-may-3-2026-15corhigher",
        "eventSlug": "highest-temperature-in-toronto-on-may-3-2026",
        "outcome": "Yes",
        "transactionHash": "0xtx",
    }
    trade_id, _ = repo.insert_trade(trader_id, raw_trade)
    repo.insert_ws_orderbook_event(
        {
            "event_type": "book",
            "token_id": "tok",
            "market_id": "0xcond",
            "event_exchange_timestamp_ms": 1757908891000,
            "event_exchange_timestamp": "2025-09-15T01:21:31+00:00",
            "local_received_at": "2025-09-15T01:21:31+00:00",
            "message_hash": "h1",
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
    )
    repo.insert_ws_orderbook_event(
        {
            "event_type": "last_trade_price",
            "token_id": "tok",
            "market_id": "0xcond",
            "event_exchange_timestamp_ms": 1757908892001,
            "event_exchange_timestamp": "2025-09-15T01:21:32.001000+00:00",
            "local_received_at": "2025-09-15T01:21:32.010000+00:00",
            "message_hash": None,
            "transaction_hash": "0xtx",
            "side": "BUY",
            "price": 0.5,
            "size": 7,
            "best_bid": 0.4,
            "best_ask": 0.5,
            "spread": 0.1,
            "midpoint": 0.45,
            "full_book_json": None,
            "raw_json": "{}",
        }
    )
    context = attach_execution_context(repo, repo.get_trade(trade_id))
    assert context["match_confidence"] == "exact_ws_tx_match"
    assert context["execution_timestamp_source"] == "ws_millisecond_precision"
    assert context["pre_book_delta_ms"] == 1001


def test_attach_execution_context_ignores_stale_book(tmp_path) -> None:
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
    repo.insert_ws_orderbook_event(
        {
            "event_type": "book",
            "token_id": "tok",
            "market_id": "0xcond",
            "event_exchange_timestamp_ms": 1757908292000,
            "event_exchange_timestamp": "2025-09-15T01:11:32+00:00",
            "local_received_at": "2025-09-15T01:11:32+00:00",
            "message_hash": "old",
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
    )
    context = attach_execution_context(repo, repo.get_trade(trade_id), book_window_ms=300000)
    assert context["pre_book_event_id"] is None
    assert context["pre_book_delta_ms"] is None
    assert "stale_pre_trade_book_ignored" in context["quality_flags"]
