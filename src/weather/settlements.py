from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from src.utils.dedupe import raw_json
from src.utils.time import utc_now_iso


def _num(value: Any, default: float | None = None) -> float | None:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _decode_jsonish(value: Any) -> Any:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _market_yes_price(raw: dict[str, Any]) -> float | None:
    outcomes = _decode_jsonish(raw.get("outcomes") or [])
    prices = _decode_jsonish(raw.get("outcomePrices") or raw.get("outcome_prices") or [])
    if isinstance(outcomes, dict):
        outcomes = list(outcomes.values())
    if isinstance(prices, dict):
        prices = list(prices.values())
    if isinstance(outcomes, list) and isinstance(prices, list):
        for index, outcome in enumerate(outcomes):
            if str(outcome).lower() == "yes" and index < len(prices):
                return _num(prices[index])
    return _num(raw.get("yesPrice") or raw.get("yes_price") or raw.get("lastTradePrice"))


def market_yes_won(raw: dict[str, Any]) -> bool | None:
    for key in ("winningOutcome", "winning_outcome", "winner", "resolvedOutcome", "resolved_outcome"):
        value = raw.get(key)
        if value:
            text = str(value).lower()
            if text in {"yes", "no"}:
                return text == "yes"
    price = _market_yes_price(raw)
    if price is not None:
        if price >= 0.99:
            return True
        if price <= 0.01:
            return False
    return None


def bucket_contains_temp(bucket: dict[str, Any], final_temp: float) -> bool:
    bound_type = str(bucket.get("bound_type") or "").lower()
    lower = _num(bucket.get("lower_temp"))
    upper = _num(bucket.get("upper_temp"))
    if bound_type == "lower_bound":
        return lower is not None and final_temp >= lower
    if bound_type == "upper_bound":
        return upper is not None and final_temp <= upper
    if bound_type == "range":
        return lower is not None and upper is not None and lower <= final_temp <= upper
    if bound_type == "exact":
        return lower is not None and abs(final_temp - lower) < 1e-9
    return False


def winning_bucket_from_temp(bucket_rows: list[dict[str, Any]], final_temp: float) -> dict[str, Any] | None:
    yes_buckets = [row for row in bucket_rows if str(row.get("outcome") or "Yes").lower() == "yes"]
    matches = [row for row in yes_buckets if bucket_contains_temp(row, final_temp)]
    if len(matches) == 1:
        return matches[0]
    return None


def settlement_row_from_bucket(
    basket: dict[str, Any],
    bucket: dict[str, Any] | None,
    *,
    final_temp: float | None,
    source: str,
    confidence: str,
    status: str = "settled",
    raw: dict[str, Any] | None = None,
    quality_flags: list[str] | None = None,
) -> dict[str, Any]:
    flags = list(quality_flags or [])
    if bucket is None:
        flags.append("winning_bucket_unknown")
    if final_temp is None:
        flags.append("final_temp_unknown")
    return {
        "basket_id": basket.get("id"),
        "event_slug": basket.get("event_slug"),
        "city": basket.get("city"),
        "forecast_date": basket.get("forecast_date"),
        "unit": basket.get("unit"),
        "final_temp": final_temp,
        "winning_bucket_market_id": bucket.get("id") if bucket else None,
        "winning_token_id": bucket.get("token_id") if bucket else None,
        "winning_bucket_label": bucket.get("bucket_label") if bucket else None,
        "winning_market_slug": bucket.get("market_slug") if bucket else None,
        "settlement_status": status,
        "source": source,
        "confidence": confidence,
        "captured_at": utc_now_iso(),
        "settled_at": utc_now_iso() if status == "settled" else None,
        "quality_flags": ",".join(flags),
        "raw_json": raw_json(raw or {}),
    }


async def capture_settlement_from_gamma(repository: Any, gamma: Any, event_slug: str) -> dict[str, Any]:
    basket_row = repository.get_weather_basket_by_event_slug(event_slug)
    if not basket_row:
        raise ValueError(f"unknown weather basket: {event_slug}")
    basket = dict(basket_row)
    buckets = [dict(row) for row in repository.list_weather_bucket_markets_for_basket(int(basket["id"]))]
    winner: dict[str, Any] | None = None
    raw_markets = []
    for bucket in buckets:
        slug = bucket.get("market_slug")
        if not slug:
            continue
        market = await gamma.get_market_by_slug(slug)
        if not market:
            continue
        raw_markets.append(market)
        yes_won = market_yes_won(market)
        if yes_won is True and str(bucket.get("outcome") or "Yes").lower() == "yes":
            winner = bucket
    final_temp = None
    quality_flags = ["gamma_market_resolution"]
    confidence = "gamma_yes_resolution" if winner else "unresolved_or_unknown_gamma_resolution"
    status = "settled" if winner else "unresolved"
    if winner and winner.get("bound_type") == "exact":
        final_temp = _num(winner.get("lower_temp"))
        quality_flags.append("final_temp_inferred_from_exact_winning_bucket")
    settlement = settlement_row_from_bucket(
        basket,
        winner,
        final_temp=final_temp,
        source="gamma",
        confidence=confidence,
        status=status,
        raw={"markets": raw_markets},
        quality_flags=quality_flags,
    )
    settlement_id = repository.upsert_weather_settlement(settlement)
    settlement["id"] = settlement_id
    return settlement


def set_manual_settlement(
    repository: Any,
    event_slug: str,
    *,
    final_temp: float | None = None,
    winning_bucket_label: str | None = None,
    source: str = "manual",
    confidence: str = "manual_user_supplied",
) -> dict[str, Any]:
    basket_row = repository.get_weather_basket_by_event_slug(event_slug)
    if not basket_row:
        raise ValueError(f"unknown weather basket: {event_slug}")
    basket = dict(basket_row)
    buckets = [dict(row) for row in repository.list_weather_bucket_markets_for_basket(int(basket["id"]))]
    winner = None
    flags: list[str] = ["manual_settlement"]
    if winning_bucket_label:
        winner = next((row for row in buckets if str(row.get("bucket_label") or "").lower() == winning_bucket_label.lower()), None)
        if not winner:
            flags.append("manual_bucket_label_not_found")
    elif final_temp is not None:
        winner = winning_bucket_from_temp(buckets, float(final_temp))
        if winner:
            flags.append("winning_bucket_derived_from_final_temp")
        else:
            flags.append("final_temp_did_not_match_single_bucket")
    settlement = settlement_row_from_bucket(
        basket,
        winner,
        final_temp=final_temp,
        source=source,
        confidence=confidence,
        status="settled" if winner or final_temp is not None else "unresolved",
        raw={"final_temp": final_temp, "winning_bucket_label": winning_bucket_label},
        quality_flags=flags,
    )
    settlement_id = repository.upsert_weather_settlement(settlement)
    settlement["id"] = settlement_id
    return settlement


@dataclass
class BucketPnlState:
    net_size: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl: float = 0.0
    trade_count: int = 0
    first_trade_at: str | None = None
    last_trade_at: str | None = None

    def apply(self, side: str, size: float, price: float, timestamp: str | None) -> None:
        side = side.upper()
        self.trade_count += 1
        self.first_trade_at = self.first_trade_at or timestamp
        self.last_trade_at = timestamp or self.last_trade_at
        if side == "BUY":
            new_size = self.net_size + size
            if new_size > 0:
                self.avg_entry_price = ((self.avg_entry_price * self.net_size) + (price * size)) / new_size
            self.net_size = new_size
        elif side == "SELL":
            matched = min(size, max(self.net_size, 0.0))
            if matched:
                self.realized_pnl += (price - self.avg_entry_price) * matched
            self.net_size -= size
            if self.net_size <= 1e-12:
                self.net_size = 0.0
                self.avg_entry_price = 0.0


def compute_final_bucket_pnl(repository: Any, event_slug: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    settlement_row = repository.get_weather_settlement_by_event_slug(event_slug)
    if not settlement_row:
        raise ValueError(f"no settlement stored for {event_slug}")
    settlement = dict(settlement_row)
    basket_id = int(settlement["basket_id"])
    buckets = [dict(row) for row in repository.list_weather_bucket_markets_for_basket(basket_id)]
    states = {str(bucket["token_id"]): BucketPnlState() for bucket in buckets}
    with repository.connect() as conn:
        trades = conn.execute(
            """
            SELECT t.*
            FROM trades t
            JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
            WHERE bm.basket_id=?
            ORDER BY t.trade_timestamp ASC, t.id ASC
            """,
            (basket_id,),
        ).fetchall()
    for trade in trades:
        token_id = str(trade["token_id"])
        if token_id not in states:
            continue
        states[token_id].apply(
            str(trade["side"]),
            float(trade["size"] or 0),
            float(trade["price"] or 0),
            trade["trade_timestamp"],
        )
    winning_token_id = str(settlement.get("winning_token_id") or "")
    now = utc_now_iso()
    rows: list[dict[str, Any]] = []
    for bucket in buckets:
        token_id = str(bucket["token_id"])
        state = states[token_id]
        cost_basis = state.net_size * state.avg_entry_price
        is_winner = bool(winning_token_id and token_id == winning_token_id)
        payout = state.net_size if is_winner else 0.0
        final_pnl = state.realized_pnl + payout - cost_basis
        rows.append(
            {
                "settlement_id": settlement["id"],
                "basket_id": basket_id,
                "bucket_market_id": bucket["id"],
                "token_id": token_id,
                "bucket_label": bucket.get("bucket_label"),
                "outcome": bucket.get("outcome"),
                "net_size": state.net_size,
                "avg_entry_price": state.avg_entry_price if state.avg_entry_price else None,
                "cost_basis": cost_basis,
                "realized_pnl": state.realized_pnl,
                "final_payout": payout,
                "final_pnl": final_pnl,
                "trade_count": state.trade_count,
                "first_trade_at": state.first_trade_at,
                "last_trade_at": state.last_trade_at,
                "winning_bucket": 1 if is_winner else 0,
                "confidence": settlement.get("confidence"),
                "computed_at": now,
                "raw_json": raw_json({"bucket": bucket, "settlement": settlement}),
            }
        )
    repository.replace_weather_bucket_final_pnl(int(settlement["id"]), rows)
    return settlement, rows


def _money(value: Any) -> str:
    number = _num(value)
    return "n/a" if number is None else f"${number:,.2f}"


def _fmt(value: Any, digits: int = 4) -> str:
    number = _num(value)
    return "" if number is None else f"{number:.{digits}f}".rstrip("0").rstrip(".")


def _nearest_forecast(conn: Any, basket_id: int, timestamp: str | None) -> dict[str, Any] | None:
    if not timestamp:
        return None
    row = conn.execute(
        """
        SELECT predicted_high, captured_at, source, quality_flags
        FROM weather_forecast_snapshots
        WHERE basket_id=? AND captured_at <= ?
        ORDER BY captured_at DESC
        LIMIT 1
        """,
        (basket_id, timestamp),
    ).fetchone()
    if row:
        return dict(row)
    row = conn.execute(
        """
        SELECT predicted_high, captured_at, source, quality_flags
        FROM weather_forecast_snapshots
        WHERE basket_id=?
        ORDER BY captured_at ASC
        LIMIT 1
        """,
        (basket_id,),
    ).fetchone()
    return dict(row) if row else None


def generate_weather_day_report(repository: Any, event_slug: str, *, trade_limit: int = 80, store_pnl: bool = True) -> str:
    settlement = repository.get_weather_settlement_by_event_slug(event_slug)
    if settlement and store_pnl:
        settlement_dict, pnl_rows = compute_final_bucket_pnl(repository, event_slug)
    else:
        settlement_dict = dict(settlement) if settlement else {}
        pnl_rows = []
    basket = repository.get_weather_basket_by_event_slug(event_slug)
    if not basket:
        raise ValueError(f"unknown weather basket: {event_slug}")
    basket_dict = dict(basket)
    basket_id = int(basket_dict["id"])
    with repository.connect() as conn:
        trades = [
            dict(row)
            for row in conn.execute(
                """
                SELECT t.id, t.trade_timestamp, t.detected_at, t.side, t.price, t.size, t.notional,
                       t.tx_hash, bm.bucket_label, bm.market_slug, bm.token_id,
                       c.match_confidence, c.execution_timestamp, c.quality_flags,
                       s.complete_yes_ask_cost, s.one_share_yes_ask_edge, s.matched_token_count, s.token_count
                FROM trades t
                JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
                LEFT JOIN trade_execution_context c ON c.trade_id=t.id
                LEFT JOIN weather_basket_snapshots s ON s.trade_id=t.id
                WHERE bm.basket_id=?
                ORDER BY t.trade_timestamp ASC, t.id ASC
                LIMIT ?
                """,
                (basket_id, trade_limit),
            ).fetchall()
        ]
        observations = [
            dict(row)
            for row in conn.execute(
                """
                SELECT captured_at, current_temperature, intraday_high, daily_high, observed_high,
                       observation_status, quality_flags
                FROM weather_observations
                WHERE basket_id=?
                ORDER BY captured_at DESC
                LIMIT 3
                """,
                (basket_id,),
            ).fetchall()
        ]
        followups = [
            dict(row)
            for row in conn.execute(
                """
                SELECT interval_label, COUNT(*) scheduled,
                       SUM(CASE WHEN captured_at IS NOT NULL THEN 1 ELSE 0 END) captured,
                       SUM(CASE WHEN favorable_move_boolean=1 THEN 1 ELSE 0 END) favorable
                FROM weather_followup_snapshots
                WHERE basket_id=?
                GROUP BY interval_label
                ORDER BY scheduled_for
                """,
                (basket_id,),
            ).fetchall()
        ]
        for trade in trades:
            trade["forecast"] = _nearest_forecast(conn, basket_id, trade.get("trade_timestamp"))

    lines = [f"# Weather Day Report: {event_slug}", ""]
    lines.append(f"- City/date: {basket_dict.get('city')} {basket_dict.get('forecast_date')} ({basket_dict.get('unit') or ''})")
    if settlement_dict:
        final = _fmt(settlement_dict.get("final_temp"), 2)
        lines.append(
            f"- Settlement: {settlement_dict.get('settlement_status')} | final temp {final or 'unknown'} | "
            f"winner {settlement_dict.get('winning_bucket_label') or 'unknown'} | source {settlement_dict.get('source')}"
        )
    else:
        lines.append("- Settlement: not stored yet")

    if observations:
        latest = observations[0]
        lines.append(
            f"- Latest provisional observed high: {_fmt(latest.get('observed_high'), 2)} captured {latest.get('captured_at')} "
            f"({latest.get('quality_flags')})"
        )
    lines.extend(["", "## Final Bucket PnL"])
    if pnl_rows:
        total_cost = sum(float(row["cost_basis"] or 0) for row in pnl_rows)
        total_payout = sum(float(row["final_payout"] or 0) for row in pnl_rows)
        total_pnl = sum(float(row["final_pnl"] or 0) for row in pnl_rows)
        lines.append(f"- Total cost: {_money(total_cost)}")
        lines.append(f"- Final payout: {_money(total_payout)}")
        lines.append(f"- Final PnL: {_money(total_pnl)}")
        for row in sorted(pnl_rows, key=lambda item: (item.get("bucket_label") or "")):
            marker = "WIN" if row["winning_bucket"] else "lose"
            lines.append(
                f"- {row.get('bucket_label') or 'unknown'} [{marker}]: "
                f"{_fmt(row['net_size'], 4)} shares, avg {_fmt(row['avg_entry_price'], 4)}, "
                f"cost {_money(row['cost_basis'])}, payout {_money(row['final_payout'])}, pnl {_money(row['final_pnl'])}, "
                f"trades {row['trade_count']}"
            )
    else:
        lines.append("- No final PnL available yet. Store a settlement first.")

    lines.extend(["", "## Trade Timeline"])
    if not trades:
        lines.append("- No trades for this basket.")
    for trade in trades:
        forecast = trade.get("forecast") or {}
        forecast_value = _fmt(forecast.get("predicted_high"), 2)
        basket_edge = _fmt(trade.get("one_share_yes_ask_edge"), 4)
        ask_cost = _fmt(trade.get("complete_yes_ask_cost"), 4)
        matched = ""
        if trade.get("matched_token_count") is not None:
            matched = f", basket books {trade.get('matched_token_count')}/{trade.get('token_count')}"
        lines.append(
            f"- {trade.get('trade_timestamp')} | {trade.get('side')} {trade.get('bucket_label')} "
            f"{_fmt(trade.get('size'), 4)} @ {_fmt(trade.get('price'), 4)} "
            f"({_money(trade.get('notional'))}) | forecast {forecast_value or 'n/a'} | "
            f"ask-sum {ask_cost or 'n/a'} edge {basket_edge or 'n/a'} | "
            f"{trade.get('match_confidence') or 'no_context'}{matched}"
        )

    lines.extend(["", "## Followup Summary"])
    if followups:
        for row in followups:
            lines.append(
                f"- {row['interval_label']}: scheduled {int(row['scheduled'])}, "
                f"captured {int(row['captured'] or 0)}, favorable {int(row['favorable'] or 0)}"
            )
    else:
        lines.append("- No followups scheduled/captured for this basket yet.")

    lines.extend(
        [
            "",
            "## Data Confidence",
            "- Forecast values are nearest stored forecast snapshots before the trade when available.",
            "- Basket book values are exact only when a live basket snapshot exists for that trade.",
            "- Final PnL is exact only when settlement/winning bucket is confirmed; manual settlements are labeled manual.",
        ]
    )
    return "\n".join(lines)
