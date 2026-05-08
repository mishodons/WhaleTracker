from __future__ import annotations

from typing import Any

from src.utils.time import to_utc_datetime
from src.ws.market_stream import exchange_ts_iso


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row) if not isinstance(row, dict) else row


def trade_timestamp_ms(trade: dict[str, Any]) -> int | None:
    raw = trade.get("trade_timestamp") or trade.get("timestamp")
    parsed = to_utc_datetime(raw)
    if parsed:
        return int(parsed.timestamp() * 1000)
    if raw in (None, ""):
        return None
    try:
        value = int(float(raw))
    except (TypeError, ValueError):
        return None
    return value if value > 10_000_000_000 else value * 1000


def attach_execution_context(
    repository: Any,
    trade_row: Any,
    *,
    window_ms: int = 5000,
    recorder: Any | None = None,
    book_window_ms: int | None = 300000,
) -> dict[str, Any]:
    trade = _row_dict(trade_row)
    token_id = str(trade.get("token_id") or "")
    rest_ms = trade_timestamp_ms(trade)
    match_query = dict(trade)
    match_query["trade_timestamp_ms"] = rest_ms
    cached_rows: dict[str, Any] = {}
    if recorder is not None and hasattr(recorder, "persist_execution_rows"):
        cached_rows = recorder.persist_execution_rows(match_query, window_ms=window_ms, book_window_ms=book_window_ms)
    ws_match = cached_rows.get("ws_trade") or repository.find_ws_trade_match(match_query, window_ms=window_ms)

    flags: list[str] = []
    if ws_match:
        match = _row_dict(ws_match)
        execution_ms = match.get("event_exchange_timestamp_ms") or rest_ms
        source = "ws_millisecond_precision"
        if (trade.get("tx_hash") or "").lower() and (match.get("transaction_hash") or "").lower() == (trade.get("tx_hash") or "").lower():
            confidence = "exact_ws_tx_match"
        else:
            confidence = "probable_ws_match"
        ws_trade_event_id = match.get("id")
    else:
        execution_ms = rest_ms
        source = "data_api_second_precision"
        confidence = "data_api_only"
        ws_trade_event_id = None

    pre = post = None
    pre_delta = post_delta = None
    if token_id and execution_ms is not None:
        pre = cached_rows.get("pre_book")
        post = cached_rows.get("post_book")
        if not pre or not post:
            db_pre, db_post = repository.nearest_ws_book_events(token_id, int(execution_ms))
            pre = pre or db_pre
            post = post or db_post
        if pre:
            candidate_pre_delta = int(execution_ms) - int(pre["event_exchange_timestamp_ms"])
            if book_window_ms is not None and candidate_pre_delta > book_window_ms:
                pre = None
                flags.append("stale_pre_trade_book_ignored")
            else:
                pre_delta = candidate_pre_delta
                flags.append("pre_trade_book_cached")
        else:
            flags.append("missed_pre_trade_book")
        if post:
            candidate_post_delta = int(post["event_exchange_timestamp_ms"]) - int(execution_ms)
            if book_window_ms is not None and candidate_post_delta > book_window_ms:
                post = None
                flags.append("stale_post_trade_book_ignored")
            else:
                post_delta = candidate_post_delta
                flags.append("post_trade_book_cached")

    flags.append(source)
    context = {
        "trade_id": trade.get("id"),
        "token_id": token_id,
        "execution_timestamp_ms": execution_ms,
        "execution_timestamp": exchange_ts_iso(execution_ms) if execution_ms else trade.get("trade_timestamp"),
        "execution_timestamp_source": source,
        "ws_trade_event_id": ws_trade_event_id,
        "pre_book_event_id": pre["id"] if pre else None,
        "post_book_event_id": post["id"] if post else None,
        "pre_book_delta_ms": pre_delta,
        "post_book_delta_ms": post_delta,
        "match_confidence": confidence,
        "quality_flags": ",".join(flags),
        "trade": trade,
    }
    repository.upsert_trade_execution_context(context)
    return context
