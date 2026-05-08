from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from src.utils.dedupe import raw_json
from src.utils.time import parse_duration, to_utc_datetime, utc_now_iso


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _book_levels_total(levels: list[dict[str, Any]]) -> float | None:
    values = [_num(level.get("size")) for level in levels]
    values = [value for value in values if value is not None]
    return sum(values) if values else None


def _best_bid_from_book(book: dict[str, Any]) -> float | None:
    prices = [_num(level.get("price")) for level in book.get("bids") or []]
    prices = [price for price in prices if price is not None]
    return max(prices) if prices else None


def _best_ask_from_book(book: dict[str, Any]) -> float | None:
    prices = [_num(level.get("price")) for level in book.get("asks") or []]
    prices = [price for price in prices if price is not None]
    return min(prices) if prices else None


def _market_row(row: Any) -> dict[str, Any]:
    return dict(row) if not isinstance(row, dict) else row


def _ws_summary(bucket: dict[str, Any], row: dict[str, Any] | None, execution_ms: int | None) -> dict[str, Any]:
    token_id = str(bucket.get("token_id"))
    if not row:
        return {
            "token_id": token_id,
            "bucket_label": bucket.get("bucket_label"),
            "market_slug": bucket.get("market_slug"),
            "outcome": bucket.get("outcome"),
            "source": "missing_ws_cache",
            "matched": False,
        }
    event_ms = row.get("event_exchange_timestamp_ms")
    delta_ms = int(event_ms) - int(execution_ms) if execution_ms is not None and event_ms is not None else None
    book = {}
    if row.get("full_book_json"):
        try:
            book = json.loads(row["full_book_json"])
        except ValueError:
            book = {}
    return {
        "token_id": token_id,
        "bucket_label": bucket.get("bucket_label"),
        "market_slug": bucket.get("market_slug"),
        "outcome": bucket.get("outcome"),
        "source": "ws_cache",
        "matched": True,
        "event_id": row.get("id") or row.get("_db_id"),
        "event_exchange_timestamp": row.get("event_exchange_timestamp"),
        "event_exchange_timestamp_ms": event_ms,
        "delta_ms": delta_ms,
        "best_bid": row.get("best_bid"),
        "best_ask": row.get("best_ask"),
        "spread": row.get("spread"),
        "midpoint": row.get("midpoint"),
        "bid_depth_total": _book_levels_total(book.get("bids") or []),
        "ask_depth_total": _book_levels_total(book.get("asks") or []),
        "full_book_json": row.get("full_book_json"),
    }


def _clob_summary(bucket: dict[str, Any], book: dict[str, Any] | None) -> dict[str, Any]:
    token_id = str(bucket.get("token_id"))
    if not book:
        return {
            "token_id": token_id,
            "bucket_label": bucket.get("bucket_label"),
            "market_slug": bucket.get("market_slug"),
            "outcome": bucket.get("outcome"),
            "source": "missing_clob_rest",
            "matched": False,
        }
    best_bid = _best_bid_from_book(book)
    best_ask = _best_ask_from_book(book)
    midpoint = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    return {
        "token_id": token_id,
        "bucket_label": bucket.get("bucket_label"),
        "market_slug": bucket.get("market_slug"),
        "outcome": bucket.get("outcome"),
        "source": "clob_rest",
        "matched": True,
        "event_exchange_timestamp": book.get("timestamp"),
        "event_exchange_timestamp_ms": None,
        "delta_ms": None,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "midpoint": midpoint,
        "bid_depth_total": _book_levels_total(book.get("bids") or []),
        "ask_depth_total": _book_levels_total(book.get("asks") or []),
        "full_book_json": raw_json(book),
    }


def basket_metrics(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    yes_summaries = [row for row in summaries if str(row.get("outcome") or "Yes").lower() == "yes"]
    ask_values = [_num(row.get("best_ask")) for row in yes_summaries]
    bid_values = [_num(row.get("best_bid")) for row in yes_summaries]
    ask_complete = bool(yes_summaries) and all(value is not None for value in ask_values)
    bid_complete = bool(yes_summaries) and all(value is not None for value in bid_values)
    ask_cost = sum(value for value in ask_values if value is not None) if ask_complete else None
    bid_value = sum(value for value in bid_values if value is not None) if bid_complete else None
    matched = sum(1 for row in summaries if row.get("matched"))
    missing = len(summaries) - matched
    return {
        "token_count": len(summaries),
        "matched_token_count": matched,
        "missing_token_count": missing,
        "complete_yes_ask_cost": ask_cost,
        "complete_yes_bid_value": bid_value,
        "one_share_yes_ask_edge": 1 - ask_cost if ask_cost is not None else None,
        "one_share_yes_bid_edge": bid_value - 1 if bid_value is not None else None,
    }


def _quality_flags(metrics: dict[str, Any], *, source: str) -> str:
    flags = [source]
    if metrics["missing_token_count"]:
        flags.append("partial_basket_book")
    else:
        flags.append("complete_basket_book")
    if metrics.get("one_share_yes_ask_edge") is not None and metrics["one_share_yes_ask_edge"] > 0:
        flags.append("positive_one_share_yes_ask_edge")
    return ",".join(flags)


def entry_basket_snapshot_from_cache(
    repository: Any,
    recorder: Any,
    trade: dict[str, Any],
    context: dict[str, Any],
    *,
    book_window_ms: int = 300000,
) -> dict[str, Any] | None:
    bucket = repository.get_weather_bucket_by_token(str(trade.get("token_id") or ""))
    if not bucket:
        return None
    bucket_dict = _market_row(bucket)
    basket_id = int(bucket_dict["basket_id"])
    markets = [_market_row(row) for row in repository.list_weather_bucket_markets_for_basket(basket_id)]
    execution_ms = context.get("execution_timestamp_ms")
    token_ids = [str(row.get("token_id")) for row in markets]
    row_map = recorder.basket_book_rows(token_ids, execution_ms=execution_ms, max_delta_ms=book_window_ms)
    summaries = [_ws_summary(row, row_map.get(str(row.get("token_id"))), execution_ms) for row in markets]
    metrics = basket_metrics(summaries)
    source = "ws_cache"
    return {
        "trade_id": trade.get("id"),
        "basket_id": basket_id,
        "snapshot_type": "entry_basket",
        "execution_timestamp": context.get("execution_timestamp"),
        "execution_timestamp_ms": execution_ms,
        "captured_at": utc_now_iso(),
        "snapshot_source": source,
        **metrics,
        "traded_token_id": trade.get("token_id"),
        "traded_bucket_label": bucket_dict.get("bucket_label"),
        "traded_price": trade.get("price"),
        "traded_side": trade.get("side"),
        "quality_flags": _quality_flags(metrics, source=source),
        "bucket_prices_json": raw_json(summaries),
        "metrics_json": raw_json(metrics),
        "raw_json": raw_json({"trade": trade, "context": context, "summaries": summaries, "metrics": metrics}),
    }


def entry_basket_snapshot_from_clob_state(
    repository: Any,
    trade: dict[str, Any],
    context: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any] | None:
    bucket = repository.get_weather_bucket_by_token(str(trade.get("token_id") or ""))
    if not bucket:
        return None
    bucket_dict = _market_row(bucket)
    metrics = state["metrics"]
    source = "clob_rest_post_detection"
    return {
        "trade_id": trade.get("id"),
        "basket_id": int(bucket_dict["basket_id"]),
        "snapshot_type": "entry_basket",
        "execution_timestamp": context.get("execution_timestamp"),
        "execution_timestamp_ms": context.get("execution_timestamp_ms"),
        "captured_at": utc_now_iso(),
        "snapshot_source": source,
        **metrics,
        "traded_token_id": trade.get("token_id"),
        "traded_bucket_label": bucket_dict.get("bucket_label"),
        "traded_price": trade.get("price"),
        "traded_side": trade.get("side"),
        "quality_flags": _quality_flags(metrics, source=source),
        "bucket_prices_json": raw_json(state["summaries"]),
        "metrics_json": raw_json(metrics),
        "raw_json": raw_json({"trade": trade, "context": context, "summaries": state["summaries"], "metrics": metrics}),
    }


def followup_schedules(base_timestamp: str | None, intervals: list[str]) -> list[tuple[str, str]]:
    base = to_utc_datetime(base_timestamp) or to_utc_datetime(utc_now_iso())
    if base is None:
        return []
    return [(label, (base + parse_duration(label)).isoformat()) for label in intervals]


def schedule_weather_followups_for_trade(
    repository: Any,
    trade: dict[str, Any],
    basket_id: int,
    *,
    base_timestamp: str | None,
    intervals: list[str],
) -> None:
    repository.schedule_weather_followups(
        int(trade["id"]),
        int(basket_id),
        followup_schedules(base_timestamp, intervals),
    )


async def current_basket_state_from_clob(repository: Any, clob: Any, basket_id: int) -> dict[str, Any]:
    markets = [_market_row(row) for row in repository.list_weather_bucket_markets_for_basket(basket_id)]
    token_ids = [str(row.get("token_id")) for row in markets if row.get("token_id")]
    books: dict[str, dict[str, Any]] = {}
    if token_ids:
        for book in await clob.get_orderbooks(token_ids):
            token_id = str(book.get("asset_id") or book.get("token_id") or "")
            if token_id:
                books[token_id] = book
        missing = [token_id for token_id in token_ids if token_id not in books]
        for token_id in missing:
            try:
                book = await clob.get_orderbook(token_id)
            except Exception:
                continue
            books[token_id] = book
    summaries = [_clob_summary(row, books.get(str(row.get("token_id")))) for row in markets]
    metrics = basket_metrics(summaries)
    return {"summaries": summaries, "metrics": metrics}


def followup_record_from_state(followup: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    summaries = state["summaries"]
    metrics = state["metrics"]
    traded_token_id = str(followup.get("token_id") or "")
    traded = next((row for row in summaries if str(row.get("token_id")) == traded_token_id), None)
    midpoint = _num(traded.get("midpoint")) if traded else None
    trade_price = _num(followup.get("trade_price"))
    trade_side = str(followup.get("trade_side") or "").upper()
    price_change = midpoint - trade_price if midpoint is not None and trade_price is not None else None
    favorable = None
    if price_change is not None:
        favorable = price_change > 0 if trade_side == "BUY" else price_change < 0 if trade_side == "SELL" else None
    source = "clob_rest"
    return {
        "captured_at": utc_now_iso(),
        "snapshot_source": source,
        "traded_token_id": traded_token_id,
        "traded_price": trade_price,
        "traded_midpoint": midpoint,
        "price_change_from_trade": price_change,
        "favorable_move_boolean": favorable,
        **metrics,
        "quality_flags": _quality_flags(metrics, source=source),
        "bucket_prices_json": raw_json(summaries),
        "raw_json": {"followup": followup, "summaries": summaries, "metrics": metrics},
    }


async def process_due_weather_followups(repository: Any, clob: Any, *, limit: int = 200) -> int:
    pending = [dict(row) for row in repository.pending_weather_followups(utc_now_iso(), limit=limit)]
    if not pending:
        return 0
    grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in pending:
        grouped[int(row["basket_id"])].append(row)
    captured = 0
    for basket_id, rows in grouped.items():
        try:
            state = await current_basket_state_from_clob(repository, clob, basket_id)
        except Exception as exc:
            repository.log("WARN", "weather_followups", "basket followup capture failed", {"basket_id": basket_id, "error": str(exc)})
            continue
        for row in rows:
            repository.complete_weather_followup(int(row["id"]), followup_record_from_state(row, state))
            captured += 1
    if captured:
        repository.log("INFO", "weather_followups", "captured weather followups", {"captured": captured})
    return captured
