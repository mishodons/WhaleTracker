from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.analysis.metrics import compute_orderbook_metrics
from src.utils.dedupe import raw_json
from src.utils.quality import HISTORICAL_BOOK_UNAVAILABLE, POST_DETECTION_BOOK, join_flags
from src.utils.time import to_utc_datetime, utc_now_iso


def _latency_ms(trade_timestamp: Any, snapshot_timestamp: Any) -> float | None:
    trade_dt = to_utc_datetime(trade_timestamp)
    snapshot_dt = to_utc_datetime(snapshot_timestamp)
    if not trade_dt or not snapshot_dt:
        return None
    return (snapshot_dt - trade_dt).total_seconds() * 1000


def snapshot_record_from_orderbook(
    *,
    book: dict[str, Any],
    trade: dict[str, Any],
    snapshot_type: str,
    snapshot_source: str = "rest_polling",
    slippage_notional_sizes: list[float] | None = None,
    liquidity_bands_pct: list[float] | None = None,
) -> dict[str, Any]:
    metrics = compute_orderbook_metrics(
        book,
        trade_side=trade.get("side"),
        slippage_notional_sizes=slippage_notional_sizes,
        liquidity_bands_pct=liquidity_bands_pct,
    )
    snapshot_ts = to_utc_datetime(book.get("timestamp")) or to_utc_datetime(utc_now_iso())
    snapshot_iso = snapshot_ts.isoformat() if snapshot_ts else utc_now_iso()
    flags = POST_DETECTION_BOOK if snapshot_type in {"entry", "backfill_current"} else None
    return {
        "trade_id": trade.get("id"),
        "market_id": book.get("market") or trade.get("market_id") or trade.get("condition_id"),
        "token_id": str(book.get("asset_id") or trade.get("token_id")),
        "snapshot_type": snapshot_type,
        "snapshot_timestamp": snapshot_iso,
        "best_bid": metrics.get("best_bid"),
        "best_ask": metrics.get("best_ask"),
        "spread": metrics.get("spread"),
        "midpoint": metrics.get("midpoint"),
        "bid_depth_total": metrics.get("bid_depth_total"),
        "ask_depth_total": metrics.get("ask_depth_total"),
        "bid_depth_near_touch": metrics.get("bid_depth_near_touch"),
        "ask_depth_near_touch": metrics.get("ask_depth_near_touch"),
        "imbalance": metrics.get("imbalance"),
        "liquidity_1pct": metrics.get("liquidity_1pct"),
        "liquidity_2pct": metrics.get("liquidity_2pct"),
        "liquidity_5pct": metrics.get("liquidity_5pct"),
        "liquidity_10pct": metrics.get("liquidity_10pct"),
        "book_hash": book.get("hash"),
        "latency_ms": _latency_ms(trade.get("trade_timestamp") or trade.get("timestamp"), snapshot_iso),
        "snapshot_source": snapshot_source,
        "depth_json": json.dumps(metrics.get("depth", {}), sort_keys=True),
        "slippage_json": json.dumps(metrics.get("slippage", {}), sort_keys=True),
        "quality_flags": join_flags(flags),
        "raw_orderbook_json": raw_json(book),
    }


def unavailable_snapshot_record(*, trade: dict[str, Any], snapshot_type: str = "historical_unavailable") -> dict[str, Any]:
    return {
        "trade_id": trade.get("id"),
        "market_id": trade.get("market_id") or trade.get("condition_id"),
        "token_id": str(trade.get("token_id")),
        "snapshot_type": snapshot_type,
        "snapshot_timestamp": utc_now_iso(),
        "best_bid": None,
        "best_ask": None,
        "spread": None,
        "midpoint": None,
        "bid_depth_total": None,
        "ask_depth_total": None,
        "bid_depth_near_touch": None,
        "ask_depth_near_touch": None,
        "imbalance": None,
        "liquidity_1pct": None,
        "liquidity_2pct": None,
        "liquidity_5pct": None,
        "liquidity_10pct": None,
        "book_hash": None,
        "latency_ms": None,
        "snapshot_source": "unavailable",
        "depth_json": "{}",
        "slippage_json": "{}",
        "quality_flags": HISTORICAL_BOOK_UNAVAILABLE,
        "raw_orderbook_json": "{}",
    }


class OrderbookSnapshotter:
    def __init__(self, clob_client: Any, repository: Any, *, slippage_notional_sizes: list[float], liquidity_bands_pct: list[float]):
        self.clob_client = clob_client
        self.repository = repository
        self.slippage_notional_sizes = slippage_notional_sizes
        self.liquidity_bands_pct = liquidity_bands_pct

    async def capture_for_trade(self, trade: dict[str, Any], *, snapshot_type: str = "entry") -> tuple[int, dict[str, Any] | None]:
        token_id = trade.get("token_id")
        if not token_id:
            record = unavailable_snapshot_record(trade=trade, snapshot_type="missing_token")
            return self.repository.insert_orderbook_snapshot(record), None
        book = await self.clob_client.get_orderbook(str(token_id))
        record = snapshot_record_from_orderbook(
            book=book,
            trade=trade,
            snapshot_type=snapshot_type,
            slippage_notional_sizes=self.slippage_notional_sizes,
            liquidity_bands_pct=self.liquidity_bands_pct,
        )
        return self.repository.insert_orderbook_snapshot(record), record

    def mark_unavailable(self, trade: dict[str, Any]) -> int:
        return self.repository.insert_orderbook_snapshot(unavailable_snapshot_record(trade=trade))

