from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.utils.time import utc_now_iso
from src.utils.dedupe import raw_json


def _num(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass
class WeatherPositionState:
    trader_id: int
    basket_id: int
    bucket_market_id: int
    token_id: str
    outcome: str | None
    net_size: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl: float = 0.0
    last_trade_at: str | None = None

    def apply(self, side: str, size: float, price: float, timestamp: str | None) -> None:
        side = side.upper()
        self.last_trade_at = timestamp or self.last_trade_at
        if side == "BUY":
            new_size = self.net_size + size
            if self.net_size > 0:
                self.avg_entry_price = ((self.avg_entry_price * self.net_size) + (price * size)) / new_size
            else:
                self.avg_entry_price = price
            self.net_size = new_size
        elif side == "SELL":
            matched = min(size, max(self.net_size, 0.0))
            if matched:
                self.realized_pnl += (price - self.avg_entry_price) * matched
            self.net_size -= size
            if self.net_size <= 1e-12:
                self.net_size = 0.0
                self.avg_entry_price = 0.0

    def as_row(self, midpoint: float | None = None) -> dict[str, Any]:
        cost_basis = self.net_size * self.avg_entry_price
        mark_value = self.net_size * midpoint if midpoint is not None else None
        unrealized = mark_value - cost_basis if mark_value is not None else None
        return {
            "trader_id": self.trader_id,
            "basket_id": self.basket_id,
            "bucket_market_id": self.bucket_market_id,
            "token_id": self.token_id,
            "outcome": self.outcome,
            "net_size": self.net_size,
            "avg_entry_price": self.avg_entry_price if self.avg_entry_price else None,
            "cost_basis": cost_basis,
            "realized_pnl": self.realized_pnl,
            "current_midpoint": midpoint,
            "mark_value": mark_value,
            "unrealized_pnl": unrealized,
            "last_trade_at": self.last_trade_at,
            "confidence": "weather_local_trade_history",
            "last_updated": utc_now_iso(),
        }


def _latest_midpoints(repository: Any) -> dict[str, float]:
    with repository.connect() as conn:
        rows = conn.execute(
            """
            SELECT token_id, midpoint
            FROM ws_orderbook_events
            WHERE midpoint IS NOT NULL
            ORDER BY event_exchange_timestamp_ms DESC, id DESC
            """
        ).fetchall()
    midpoints: dict[str, float] = {}
    for row in rows:
        token_id = str(row["token_id"])
        if token_id not in midpoints:
            midpoints[token_id] = float(row["midpoint"])
    return midpoints


def recompute_weather_positions(repository: Any, trader_id: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    with repository.connect() as conn:
        trades = conn.execute(
            """
            SELECT t.*, bm.id AS bucket_market_id, bm.basket_id, bm.bucket_label, bm.outcome AS bucket_outcome
            FROM trades t
            JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
            WHERE t.trader_id=?
            ORDER BY t.trade_timestamp ASC, t.id ASC
            """,
            (trader_id,),
        ).fetchall()
        basket_buckets = conn.execute(
            """
            SELECT b.id AS basket_id, b.event_slug, bm.id AS bucket_market_id, bm.token_id,
                   bm.bucket_label, bm.outcome
            FROM weather_baskets b
            JOIN weather_bucket_markets bm ON bm.basket_id=b.id
            """
        ).fetchall()

    states: dict[str, WeatherPositionState] = {}
    for trade in trades:
        token_id = str(trade["token_id"])
        state = states.get(token_id)
        if state is None:
            state = WeatherPositionState(
                trader_id=trader_id,
                basket_id=int(trade["basket_id"]),
                bucket_market_id=int(trade["bucket_market_id"]),
                token_id=token_id,
                outcome=trade["outcome"] or trade["bucket_outcome"],
            )
            states[token_id] = state
        state.apply(str(trade["side"]), _num(trade["size"]), _num(trade["price"]), trade["trade_timestamp"])

    midpoints = _latest_midpoints(repository)
    position_rows = [state.as_row(midpoints.get(token_id)) for token_id, state in states.items()]
    repository.replace_weather_positions(trader_id, position_rows)

    pnl_rows = compute_weather_basket_pnl(trader_id, position_rows, [dict(row) for row in basket_buckets])
    repository.replace_weather_basket_pnl(trader_id, pnl_rows)
    return position_rows, pnl_rows


def compute_weather_basket_pnl(
    trader_id: int,
    position_rows: list[dict[str, Any]],
    basket_bucket_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    now = utc_now_iso()
    positions_by_token = {str(row["token_id"]): row for row in position_rows}
    buckets_by_basket: dict[int, list[dict[str, Any]]] = {}
    for row in basket_bucket_rows:
        if str(row.get("outcome") or "").lower() not in {"yes", ""}:
            continue
        buckets_by_basket.setdefault(int(row["basket_id"]), []).append(row)

    results: list[dict[str, Any]] = []
    for basket_id, bucket_rows in buckets_by_basket.items():
        payouts: list[float] = []
        total_cost = 0.0
        total_net_size = 0.0
        positioned = 0
        raw_positions = []
        for bucket in bucket_rows:
            pos = positions_by_token.get(str(bucket["token_id"]))
            net_size = _num(pos.get("net_size")) if pos else 0.0
            avg_entry = _num(pos.get("avg_entry_price")) if pos else 0.0
            if net_size > 0:
                positioned += 1
                total_cost += net_size * avg_entry
                total_net_size += net_size
            payouts.append(max(net_size, 0.0))
            raw_positions.append({"bucket": bucket, "position": pos})
        if total_net_size <= 0:
            continue
        min_payout = min(payouts) if payouts else 0.0
        max_payout = max(payouts) if payouts else 0.0
        worst_case_pnl = min_payout - total_cost
        best_case_pnl = max_payout - total_cost
        coverage_type = "complete_yes_basket" if positioned == len(bucket_rows) and len(bucket_rows) > 1 else "partial_yes_basket"
        results.append(
            {
                "trader_id": trader_id,
                "basket_id": basket_id,
                "total_cost": total_cost,
                "total_net_size": total_net_size,
                "min_payout": min_payout,
                "max_payout": max_payout,
                "worst_case_pnl": worst_case_pnl,
                "best_case_pnl": best_case_pnl,
                "guaranteed_edge": worst_case_pnl if coverage_type == "complete_yes_basket" else None,
                "roi_worst_case": worst_case_pnl / total_cost if total_cost else None,
                "coverage_type": coverage_type,
                "computed_at": now,
                "raw_json": raw_json({"positions": raw_positions}),
            }
        )
    return results

