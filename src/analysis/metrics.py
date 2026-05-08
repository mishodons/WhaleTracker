from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from src.utils.time import to_utc_datetime


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _levels(raw_levels: list[dict[str, Any]], *, reverse: bool) -> list[dict[str, float]]:
    levels: list[dict[str, float]] = []
    for level in raw_levels or []:
        price = _num(level.get("price"))
        size = _num(level.get("size"))
        if price is None or size is None or size <= 0:
            continue
        levels.append({"price": price, "size": size, "notional": price * size})
    return sorted(levels, key=lambda row: row["price"], reverse=reverse)


def liquidity_within_band(levels: list[dict[str, float]], midpoint: float | None, pct: float, side: str) -> float | None:
    if midpoint is None:
        return None
    if side == "bid":
        threshold = midpoint * (1 - pct / 100)
        selected = [row for row in levels if row["price"] >= threshold]
    else:
        threshold = midpoint * (1 + pct / 100)
        selected = [row for row in levels if row["price"] <= threshold]
    return sum(row["notional"] for row in selected)


def estimate_slippage(
    levels: list[dict[str, float]],
    *,
    target_notional: float,
    midpoint: float | None,
) -> dict[str, Any]:
    remaining = float(target_notional)
    filled_notional = 0.0
    filled_size = 0.0
    for level in levels:
        if remaining <= 0:
            break
        max_notional = level["price"] * level["size"]
        take_notional = min(max_notional, remaining)
        take_size = take_notional / level["price"] if level["price"] else 0.0
        filled_notional += take_notional
        filled_size += take_size
        remaining -= take_notional
    avg_price = filled_notional / filled_size if filled_size else None
    slippage = None
    if avg_price is not None and midpoint:
        slippage = abs(avg_price - midpoint)
    return {
        "target_notional": target_notional,
        "filled_notional": filled_notional,
        "filled_size": filled_size,
        "estimated_avg_price": avg_price,
        "complete": remaining <= 1e-9,
        "slippage_vs_midpoint": slippage,
    }


def compute_orderbook_metrics(
    book: dict[str, Any],
    *,
    trade_side: str | None = None,
    slippage_notional_sizes: list[float] | None = None,
    liquidity_bands_pct: list[float] | None = None,
) -> dict[str, Any]:
    bids = _levels(book.get("bids", []), reverse=True)
    asks = _levels(book.get("asks", []), reverse=False)
    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    midpoint = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else _num(book.get("last_trade_price"))
    bid_depth_total = sum(row["notional"] for row in bids)
    ask_depth_total = sum(row["notional"] for row in asks)
    denominator = bid_depth_total + ask_depth_total
    imbalance = (bid_depth_total - ask_depth_total) / denominator if denominator else None
    bid_depth_near_touch = sum(row["notional"] for row in bids[:3])
    ask_depth_near_touch = sum(row["notional"] for row in asks[:3])

    bands = liquidity_bands_pct or [1, 2, 5, 10]
    liquidity = {}
    for pct in bands:
        bid_liq = liquidity_within_band(bids, midpoint, pct, "bid") or 0.0
        ask_liq = liquidity_within_band(asks, midpoint, pct, "ask") or 0.0
        liquidity[f"liquidity_{int(pct)}pct"] = bid_liq + ask_liq

    slippage_sizes = slippage_notional_sizes or [100, 500, 1000]
    side = (trade_side or "BUY").upper()
    consumable = asks if side == "BUY" else bids
    slippage = {
        str(size): estimate_slippage(consumable, target_notional=float(size), midpoint=midpoint)
        for size in slippage_sizes
    }

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "midpoint": midpoint,
        "bid_depth_total": bid_depth_total,
        "ask_depth_total": ask_depth_total,
        "bid_depth_near_touch": bid_depth_near_touch,
        "ask_depth_near_touch": ask_depth_near_touch,
        "imbalance": imbalance,
        **liquidity,
        "depth": {"bids": bids, "asks": asks},
        "slippage": slippage,
    }


def classify_price_bucket(price: float | None) -> str:
    if price is None:
        return "unknown"
    if price < 0.10:
        return "under_10c"
    if price < 0.25:
        return "10_25c"
    if price < 0.50:
        return "25_50c"
    if price < 0.75:
        return "50_75c"
    if price < 0.90:
        return "75_90c"
    return "over_90c"


def favorable_move(side: str | None, trade_price: float | None, current_price: float | None) -> bool | None:
    if trade_price is None or current_price is None:
        return None
    if (side or "").upper() == "BUY":
        return current_price > trade_price
    if (side or "").upper() == "SELL":
        return current_price < trade_price
    return None


def classify_position_action(before: float, after: float) -> str:
    if before == 0 and after > 0:
        return "entering"
    if before > 0 and after > before:
        return "adding"
    if before > 0 and after == 0:
        return "exiting"
    if before > 0 and after < 0:
        return "flipping_or_incomplete_history"
    if before > 0 and 0 < after < before:
        return "reducing"
    if before < 0 and after > 0:
        return "flipping_or_incomplete_history"
    return "unchanged_or_uncertain"


def _duration_between(start: Any, end: Any) -> str | None:
    start_dt = to_utc_datetime(start)
    end_dt = to_utc_datetime(end)
    if not start_dt or not end_dt:
        return None
    seconds = int((end_dt - start_dt).total_seconds())
    return str(seconds)


def infer_entry_hypothesis(trade: dict[str, Any], metrics: dict[str, Any]) -> tuple[str, float, str]:
    price = _num(trade.get("price"))
    side = str(trade.get("side") or "").upper()
    spread = metrics.get("spread")
    midpoint = metrics.get("midpoint")
    best_bid = metrics.get("best_bid")
    best_ask = metrics.get("best_ask")
    notional = _num(trade.get("notional")) or 0.0
    liquidity_2pct = metrics.get("liquidity_2pct") or 0.0
    size_vs_depth = notional / liquidity_2pct if liquidity_2pct else None

    notes: list[str] = []
    confidence = 0.45
    hypothesis = "uncertain"
    if side == "BUY" and price is not None and best_ask is not None and abs(price - best_ask) <= 0.005:
        hypothesis = "likely_taking_ask"
        confidence = 0.7
    elif side == "SELL" and price is not None and best_bid is not None and abs(price - best_bid) <= 0.005:
        hypothesis = "likely_hitting_bid"
        confidence = 0.7
    elif midpoint is not None and price is not None and abs(price - midpoint) <= 0.005:
        hypothesis = "near_midpoint_uncertain_maker_taker"
    if spread is not None and spread >= 0.05:
        notes.append("wide_spread")
    if size_vs_depth is not None and size_vs_depth >= 0.5:
        notes.append("large_vs_near_touch_liquidity")
    if price is not None and price < 0.10:
        notes.append("longshot_bucket")
    if price is not None and price >= 0.75:
        notes.append("favorite_bucket")
    return hypothesis, confidence, ",".join(notes)


def build_strategy_metric(trade: dict[str, Any], book_metrics: dict[str, Any], market: dict[str, Any] | None = None) -> dict[str, Any]:
    market = market or {}
    price = _num(trade.get("price"))
    notional = _num(trade.get("notional")) or ((_num(trade.get("price")) or 0.0) * (_num(trade.get("size")) or 0.0))
    liquidity_2pct = book_metrics.get("liquidity_2pct") or 0.0
    size_vs_depth = notional / liquidity_2pct if liquidity_2pct else None
    hypothesis, confidence, notes = infer_entry_hypothesis(trade, book_metrics)
    trade_ts = trade.get("trade_timestamp") or trade.get("timestamp")
    return {
        "trade_id": trade.get("id"),
        "spread_at_entry": book_metrics.get("spread"),
        "liquidity_score": liquidity_2pct,
        "trade_size_vs_depth": size_vs_depth,
        "price_bucket": classify_price_bucket(price),
        "market_age_at_trade": _duration_between(market.get("created_at") or market.get("createdAt"), trade_ts),
        "time_to_resolution": _duration_between(trade_ts, market.get("end_date") or market.get("endDate")),
        "category": market.get("category"),
        "entry_type_hypothesis": hypothesis,
        "confidence_score": confidence,
        "notes": notes,
        "raw_json": json.dumps({"trade": trade, "book_metrics": book_metrics, "market": market}, default=str),
    }

