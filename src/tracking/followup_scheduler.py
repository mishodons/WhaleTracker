from __future__ import annotations

from datetime import datetime
from typing import Any

from src.analysis.metrics import compute_orderbook_metrics, favorable_move
from src.utils.time import parse_duration, to_utc_datetime, utc_now, utc_now_iso


def build_followup_schedule(base_timestamp: Any, intervals: list[str]) -> list[tuple[str, str]]:
    base = to_utc_datetime(base_timestamp) or utc_now()
    return [(label, (base + parse_duration(label)).isoformat()) for label in intervals]


async def process_due_followups(
    repository: Any,
    clob_client: Any,
    *,
    limit: int = 100,
    slippage_notional_sizes: list[float] | None = None,
    liquidity_bands_pct: list[float] | None = None,
) -> int:
    due = repository.pending_followups(utc_now_iso(), limit=limit)
    captured = 0
    for row in due:
        book = await clob_client.get_orderbook(row["token_id"])
        metrics = compute_orderbook_metrics(
            book,
            trade_side=row["trade_side"],
            slippage_notional_sizes=slippage_notional_sizes,
            liquidity_bands_pct=liquidity_bands_pct,
        )
        midpoint = metrics.get("midpoint")
        trade_price = row["trade_price"]
        price_change = midpoint - trade_price if midpoint is not None and trade_price is not None else None
        repository.complete_followup(
            row["id"],
            {
                "captured_at": utc_now_iso(),
                "best_bid": metrics.get("best_bid"),
                "best_ask": metrics.get("best_ask"),
                "midpoint": midpoint,
                "spread": metrics.get("spread"),
                "price_change_from_trade": price_change,
                "favorable_move_boolean": favorable_move(row["trade_side"], trade_price, midpoint),
                "raw_json": {"book": book, "metrics": metrics},
            },
        )
        captured += 1
    return captured

