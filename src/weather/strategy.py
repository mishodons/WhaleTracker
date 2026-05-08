from __future__ import annotations

import math
import statistics
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from src.weather.settlements import bucket_contains_temp


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt(value: Any, digits: int = 2) -> str:
    number = _num(value)
    if number is None or math.isnan(number):
        return "n/a"
    return f"{number:,.{digits}f}"


def _money(value: Any) -> str:
    number = _num(value)
    if number is None:
        return "n/a"
    return f"${number:,.2f}"


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("UTC"))
    return parsed


def _local_dt(value: str | None, timezone: str | None) -> datetime | None:
    parsed = _dt(value)
    if not parsed:
        return None
    try:
        return parsed.astimezone(ZoneInfo(timezone or "UTC"))
    except Exception:
        return parsed.astimezone(ZoneInfo("UTC"))


def _minutes_after_local_midnight(local_dt: datetime | None, forecast_date: str | None) -> float | None:
    if not local_dt or not forecast_date:
        return None
    try:
        start = datetime.combine(datetime.fromisoformat(forecast_date).date(), time.min, tzinfo=local_dt.tzinfo)
    except ValueError:
        return None
    return (local_dt - start).total_seconds() / 60


def _seconds_between(later: str | None, earlier: str | None) -> float | None:
    a = _dt(later)
    b = _dt(earlier)
    if not a or not b:
        return None
    return (a - b).total_seconds()


def classify_lifecycle_timing(minutes_after_midnight: float | None, minutes_after_first_seen: float | None = None) -> str:
    if minutes_after_first_seen is not None and 0 <= minutes_after_first_seen <= 15:
        return "posted_immediate"
    if minutes_after_midnight is None:
        return "unknown"
    local_minute = minutes_after_midnight % 1440
    hour = local_minute / 60
    if hour < 3:
        return "overnight"
    if hour < 9:
        return "early_morning"
    if hour < 12:
        return "late_morning"
    if hour < 14:
        return "midday"
    if hour < 17:
        return "afternoon"
    return "late_day"


def _bucket_sort_key(row: dict[str, Any]) -> tuple[float, float, str]:
    lower = _num(row.get("lower_temp"))
    upper = _num(row.get("upper_temp"))
    if lower is None:
        lower = -9999.0
    if upper is None:
        upper = 9999.0
    return lower, upper, str(row.get("bucket_label") or "")


def forecast_bucket_index(bucket_rows: list[dict[str, Any]], forecast_temp: float | None) -> int | None:
    if forecast_temp is None:
        return None
    yes_rows = [row for row in sorted(bucket_rows, key=_bucket_sort_key) if str(row.get("outcome") or "Yes").lower() == "yes"]
    for index, row in enumerate(yes_rows):
        if bucket_contains_temp(row, float(forecast_temp)):
            return index
    return None


def classify_ladder_shape(bucket_rows: list[dict[str, Any]], forecast_temp: float | None = None) -> str:
    yes_rows = [row for row in sorted(bucket_rows, key=_bucket_sort_key) if str(row.get("outcome") or "Yes").lower() == "yes"]
    held = [row for row in yes_rows if (_num(row.get("buy_notional")) or _num(row.get("cost_basis")) or 0.0) > 0]
    total_cost = sum((_num(row.get("buy_notional")) or _num(row.get("cost_basis")) or 0.0) for row in held)
    if not held or total_cost <= 0:
        return "unclear"
    sell_notional = sum(_num(row.get("sell_notional")) or 0.0 for row in yes_rows)
    if sell_notional > total_cost * 0.25:
        return "exit_or_flip"

    sizes = [_num(row.get("net_size")) or 0.0 for row in held if (_num(row.get("net_size")) or 0.0) > 0]
    if len(sizes) >= 3 and statistics.mean(sizes) > 0:
        cv = statistics.pstdev(sizes) / statistics.mean(sizes)
        if cv <= 0.12 and len(held) >= max(3, int(len(yes_rows) * 0.6)):
            return "flat_arb"

    indexed = list(enumerate(yes_rows))
    cost_by_index = {
        index: (_num(row.get("buy_notional")) or _num(row.get("cost_basis")) or 0.0)
        for index, row in indexed
    }
    top = sorted([(cost, index) for index, cost in cost_by_index.items() if cost > 0], reverse=True)
    forecast_index = forecast_bucket_index(yes_rows, forecast_temp)
    if forecast_index is not None:
        core_indices = {forecast_index - 1, forecast_index, forecast_index + 1}
        core_cost = sum(cost for index, cost in cost_by_index.items() if index in core_indices)
        tail_cost = total_cost - core_cost
        if core_cost / total_cost >= 0.6:
            return "forecast_core_ladder" if tail_cost > 0 else "forecast_core_ladder"
        if tail_cost / total_cost >= 0.5:
            return "tail_speculation"

    if len(top) >= 2:
        top_cost = top[0][0] + top[1][0]
        top_adjacent = abs(top[0][1] - top[1][1]) == 1
        if top_adjacent and top_cost / total_cost >= 0.65:
            return "two_core_ladder"
    if top and top[0][0] / total_cost >= 0.45:
        tail_cost = total_cost - top[0][0]
        if 0.05 <= tail_cost / total_cost <= 0.35:
            return "tail_hedged_core"
    return "unclear"


def nearest_prior_metar_delta_seconds(trade_timestamp: str, metar_rows: list[dict[str, Any]]) -> float | None:
    trade_dt = _dt(trade_timestamp)
    if not trade_dt:
        return None
    prior: list[datetime] = []
    for row in metar_rows:
        first_seen = _dt(row.get("first_seen_at"))
        if first_seen and first_seen <= trade_dt:
            prior.append(first_seen)
    if not prior:
        return None
    return (trade_dt - max(prior)).total_seconds()


def _pct(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[int((len(values) - 1) * pct)]


def _human_delta_seconds(value: float | None) -> str:
    if value is None:
        return "n/a"
    sign = "+" if value >= 0 else "-"
    seconds = abs(int(value))
    minutes, sec = divmod(seconds, 60)
    hours, minute = divmod(minutes, 60)
    if hours:
        return f"{sign}{hours}h{minute:02d}m"
    if minute:
        return f"{sign}{minute}m{sec:02d}s"
    return f"{sign}{sec}s"


def _basket_lifecycle_rows(repository: Any) -> list[dict[str, Any]]:
    with repository.connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT b.id basket_id, b.event_slug, b.city, b.forecast_date, b.unit,
                       COALESCE(sm.timezone, g.timezone, 'UTC') timezone,
                       MIN(t.trade_timestamp) first_buy_at,
                       MAX(t.trade_timestamp) last_buy_at,
                       COUNT(t.id) fills,
                       SUM(COALESCE(t.notional, 0)) buy_notional,
                       MIN(COALESCE(m.created_at, bm.discovered_at, b.discovered_at)) market_posted_at,
                       MIN(COALESCE(bm.discovered_at, b.discovered_at)) first_seen_at,
                       MAX(m.end_date) market_end_at
                FROM weather_baskets b
                JOIN weather_bucket_markets bm ON bm.basket_id=b.id
                JOIN trades t ON t.token_id=bm.token_id AND UPPER(COALESCE(t.side,''))='BUY'
                LEFT JOIN markets m ON LOWER(m.condition_id)=LOWER(bm.condition_id)
                LEFT JOIN weather_city_geocodes g ON LOWER(g.city)=LOWER(b.city)
                LEFT JOIN weather_station_mappings sm ON LOWER(sm.city)=LOWER(b.city)
                GROUP BY b.id
                ORDER BY first_buy_at DESC
                """
            ).fetchall()
        ]
    for row in rows:
        local_first = _local_dt(row.get("first_buy_at"), row.get("timezone"))
        minutes_midnight = _minutes_after_local_midnight(local_first, row.get("forecast_date"))
        minutes_seen = _seconds_between(row.get("first_buy_at"), row.get("first_seen_at"))
        minutes_seen = minutes_seen / 60 if minutes_seen is not None else None
        minutes_end = _seconds_between(row.get("market_end_at"), row.get("first_buy_at"))
        row["first_buy_local"] = local_first.strftime("%Y-%m-%d %H:%M:%S %Z") if local_first else None
        row["minutes_after_local_midnight"] = minutes_midnight
        row["minutes_after_first_seen"] = minutes_seen
        row["minutes_before_market_end"] = minutes_end / 60 if minutes_end is not None else None
        row["timing_bucket"] = classify_lifecycle_timing(minutes_midnight, minutes_seen)
    return rows


def generate_strategy_timing_report(repository: Any, *, limit: int = 20) -> str:
    rows = _basket_lifecycle_rows(repository)
    minutes = [row["minutes_after_local_midnight"] for row in rows if row.get("minutes_after_local_midnight") is not None]
    buckets: dict[str, int] = {}
    for row in rows:
        buckets[row["timing_bucket"]] = buckets.get(row["timing_bucket"], 0) + 1
    lines = ["First-buy timing is mostly late morning to early afternoon local time.", "", "## Numbers"]
    lines.append(f"- City/date baskets with buys: {len(rows)}")
    if minutes:
        lines.append(
            f"- First-buy local time: p25 {_human_delta_seconds((_pct(minutes, 0.25) or 0) * 60)}, "
            f"median {_human_delta_seconds(statistics.median(minutes) * 60)}, "
            f"p75 {_human_delta_seconds((_pct(minutes, 0.75) or 0) * 60)} after local midnight"
        )
    lines.append("- Timing buckets: " + ", ".join(f"{key}={value}" for key, value in sorted(buckets.items())))
    lines.extend(["", "## Examples"])
    for row in rows[:limit]:
        lines.append(
            f"- {row['city']} {row['forecast_date']}: {row.get('first_buy_local') or row.get('first_buy_at')} "
            f"({row.get('first_buy_at')}) | {row['timing_bucket']} | cost {_money(row.get('buy_notional'))} | fills {row['fills']}"
        )
    lines.extend(
        [
            "",
            "## Data Quality",
            "- First-buy timestamps use Data API trade timestamps unless an exact WebSocket match exists for a specific fill.",
            "- Market-posted timing uses Gamma/market metadata when present, otherwise local discovery time.",
        ]
    )
    return "\n".join(lines)


def _basket_bucket_rows(conn: Any, basket_id: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT bm.id bucket_market_id, bm.bucket_label, bm.outcome, bm.lower_temp, bm.upper_temp,
                   bm.bound_type, bm.token_id,
                   SUM(CASE WHEN UPPER(COALESCE(t.side,''))='BUY' THEN COALESCE(t.notional,0) ELSE 0 END) buy_notional,
                   SUM(CASE WHEN UPPER(COALESCE(t.side,''))='SELL' THEN COALESCE(t.notional,0) ELSE 0 END) sell_notional,
                   SUM(CASE WHEN UPPER(COALESCE(t.side,''))='BUY' THEN COALESCE(t.size,0)
                            WHEN UPPER(COALESCE(t.side,''))='SELL' THEN -COALESCE(t.size,0)
                            ELSE 0 END) net_size,
                   CASE WHEN SUM(CASE WHEN UPPER(COALESCE(t.side,''))='BUY' THEN COALESCE(t.size,0) ELSE 0 END) > 0
                        THEN SUM(CASE WHEN UPPER(COALESCE(t.side,''))='BUY' THEN COALESCE(t.notional,0) ELSE 0 END) /
                             SUM(CASE WHEN UPPER(COALESCE(t.side,''))='BUY' THEN COALESCE(t.size,0) ELSE 0 END)
                        ELSE NULL END avg_entry_price
            FROM weather_bucket_markets bm
            LEFT JOIN trades t ON t.token_id=bm.token_id
            WHERE bm.basket_id=?
            GROUP BY bm.id
            ORDER BY bm.lower_temp, bm.upper_temp, bm.bucket_label
            """,
            (basket_id,),
        ).fetchall()
    ]


def _nearest_prior_forecast(conn: Any, basket_id: int, timestamp: str | None) -> dict[str, Any] | None:
    if timestamp:
        row = conn.execute(
            """
            SELECT * FROM weather_forecast_snapshots
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
        SELECT * FROM weather_forecast_snapshots
        WHERE basket_id=?
        ORDER BY captured_at ASC
        LIMIT 1
        """,
        (basket_id,),
    ).fetchone()
    return dict(row) if row else None


def generate_strategy_buckets_report(repository: Any, *, limit: int = 20) -> str:
    with repository.connect() as conn:
        baskets = [
            dict(row)
            for row in conn.execute(
                """
                SELECT b.id basket_id, b.city, b.forecast_date, b.unit,
                       MIN(t.trade_timestamp) first_buy_at,
                       SUM(CASE WHEN UPPER(COALESCE(t.side,''))='BUY' THEN COALESCE(t.notional,0) ELSE 0 END) buy_notional
                FROM weather_baskets b
                JOIN weather_bucket_markets bm ON bm.basket_id=b.id
                JOIN trades t ON t.token_id=bm.token_id
                GROUP BY b.id
                HAVING buy_notional > 0
                ORDER BY first_buy_at DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        ]
        for basket in baskets:
            forecast = _nearest_prior_forecast(conn, int(basket["basket_id"]), basket.get("first_buy_at"))
            bucket_rows = _basket_bucket_rows(conn, int(basket["basket_id"]))
            forecast_temp = _num(forecast.get("predicted_high")) if forecast else None
            forecast_index = forecast_bucket_index(bucket_rows, forecast_temp)
            basket["forecast"] = forecast
            basket["shape"] = classify_ladder_shape(bucket_rows, forecast_temp)
            basket["forecast_index"] = forecast_index
            basket["top_buckets"] = sorted(
                [row for row in bucket_rows if (_num(row.get("buy_notional")) or 0) > 0],
                key=lambda row: _num(row.get("buy_notional")) or 0,
                reverse=True,
            )[:4]
    shape_counts: dict[str, int] = {}
    for basket in baskets:
        shape_counts[basket["shape"]] = shape_counts.get(basket["shape"], 0) + 1
    lines = ["Bucket construction is classified by capital concentration around the nearest forecast bucket.", "", "## Numbers"]
    lines.append("- Recent sample shape mix: " + (", ".join(f"{key}={value}" for key, value in sorted(shape_counts.items())) or "n/a"))
    lines.extend(["", "## Examples"])
    for basket in baskets:
        forecast = basket.get("forecast") or {}
        top = ", ".join(f"{row.get('bucket_label')} {_money(row.get('buy_notional'))}" for row in basket["top_buckets"])
        lines.append(
            f"- {basket['city']} {basket['forecast_date']}: {basket['shape']} | "
            f"forecast {_fmt(forecast.get('predicted_high'))}{basket.get('unit') or ''} captured {forecast.get('captured_at') or 'n/a'} | "
            f"cost {_money(basket.get('buy_notional'))} | top {top or 'n/a'}"
        )
    lines.extend(
        [
            "",
            "## Data Quality",
            "- Forecast alignment uses the nearest prior Open-Meteo snapshot when available; older trades before forecast capture use earliest stored forecast as a fallback.",
            "- Shape labels are heuristics, not proof of intent.",
        ]
    )
    return "\n".join(lines)


def generate_strategy_orders_report(repository: Any) -> str:
    with repository.connect() as conn:
        fills = [
            float(row["notional"])
            for row in conn.execute(
                """
                SELECT t.notional
                FROM trades t
                JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
                WHERE UPPER(COALESCE(t.side,''))='BUY' AND t.notional > 0
                """
            ).fetchall()
        ]
        clusters = [
            dict(row)
            for row in conn.execute(
                """
                SELECT b.id basket_id, b.city, b.forecast_date, substr(t.trade_timestamp,1,19) cluster_second,
                       COUNT(*) legs, SUM(t.notional) notional, SUM(t.size) shares
                FROM trades t
                JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
                JOIN weather_baskets b ON b.id=bm.basket_id
                WHERE UPPER(COALESCE(t.side,''))='BUY' AND t.notional > 0
                GROUP BY b.id, substr(t.trade_timestamp,1,19)
                ORDER BY notional DESC
                """
            ).fetchall()
        ]
        basket_costs = [
            float(row["cost"])
            for row in conn.execute(
                """
                SELECT SUM(t.notional) cost
                FROM trades t
                JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
                WHERE UPPER(COALESCE(t.side,''))='BUY' AND t.notional > 0
                GROUP BY bm.basket_id
                """
            ).fetchall()
        ]
    cluster_values = [float(row["notional"]) for row in clusters]
    lines = ["He expresses size through many small fills, not single huge orders.", "", "## Numbers"]
    if fills:
        lines.append(f"- Fill notional: avg {_money(statistics.mean(fills))}, median {_money(statistics.median(fills))}, p90 {_money(_pct(fills, 0.90))}, max {_money(max(fills))}")
    if cluster_values:
        lines.append(f"- Same-second cluster: avg {_money(statistics.mean(cluster_values))}, median {_money(statistics.median(cluster_values))}, p90 {_money(_pct(cluster_values, 0.90))}, max {_money(max(cluster_values))}")
        lines.append(f"- Average legs per cluster: {_fmt(statistics.mean([float(row['legs']) for row in clusters]), 2)}")
    if basket_costs:
        lines.append(f"- City/date basket cost: avg {_money(statistics.mean(basket_costs))}, median {_money(statistics.median(basket_costs))}, p90 {_money(_pct(basket_costs, 0.90))}, max {_money(max(basket_costs))}")
    lines.extend(["", "## Examples"])
    for row in clusters[:12]:
        lines.append(f"- {row['city']} {row['forecast_date']} {row['cluster_second']}: {_money(row['notional'])}, {row['legs']} legs")
    lines.extend(["", "## Data Quality", "- Same-second clusters approximate a multi-leg decision, but multiple separate fills can share the same second."])
    return "\n".join(lines)


def generate_strategy_pnl_report(repository: Any) -> str:
    with repository.connect() as conn:
        rows = [
            dict(row)
            for row in conn.execute(
                """
                SELECT ws.city, ws.forecast_date, ws.winning_bucket_label, ws.settlement_status,
                       SUM(w.final_payout) payout, SUM(w.cost_basis) cost, SUM(w.final_pnl) pnl
                FROM weather_settlements ws
                JOIN weather_bucket_final_pnl w ON w.settlement_id=ws.id
                GROUP BY ws.id
                ORDER BY ws.forecast_date DESC, ws.city
                """
            ).fetchall()
        ]
        settlement_count = conn.execute("SELECT COUNT(*) c FROM weather_settlements").fetchone()["c"]
    resolved = [row for row in rows if str(row.get("settlement_status")) == "settled"]
    wins = [row for row in resolved if (row.get("pnl") or 0) > 0]
    lines = ["Win rate and average profit require confirmed winning buckets; current resolved coverage is thin.", "", "## Numbers"]
    lines.append(f"- Stored settlement rows: {settlement_count}")
    lines.append(f"- Final PnL rows by city/date: {len(rows)}")
    if resolved:
        pnls = [float(row["pnl"] or 0) for row in resolved]
        lines.append(f"- Win rate per city/date: {len(wins)}/{len(resolved)} = {_fmt(len(wins) / len(resolved) * 100, 1)}%")
        lines.append(f"- Average PnL per resolved city/date: {_money(statistics.mean(pnls))}")
    else:
        lines.append("- Win rate: unavailable until settled baskets have winning buckets and final PnL rows.")
    lines.extend(["", "## Examples"])
    if rows:
        for row in rows[:20]:
            roi = None if not row.get("cost") else (float(row["pnl"] or 0) / float(row["cost"])) * 100
            lines.append(
                f"- {row['city']} {row['forecast_date']}: winner {row.get('winning_bucket_label') or 'unknown'} | "
                f"cost {_money(row.get('cost'))} payout {_money(row.get('payout'))} pnl {_money(row.get('pnl'))} ROI {_fmt(roi, 1)}%"
            )
    else:
        lines.append("- No final PnL rows yet. Capture or manually set confirmed settlements, then run day reports to populate final PnL.")
    lines.extend(["", "## Data Quality", "- PnL is exact only when the winning bucket is confirmed. Unresolved Gamma rows are not counted as losses."])
    return "\n".join(lines)


def generate_strategy_metar_report(repository: Any, *, limit: int = 30) -> str:
    with repository.connect() as conn:
        reports_by_city: dict[str, list[dict[str, Any]]] = {}
        for row in conn.execute("SELECT * FROM weather_metar_reports ORDER BY first_seen_at ASC").fetchall():
            reports_by_city.setdefault(str(row["city"] or ""), []).append(dict(row))
        trades = [
            dict(row)
            for row in conn.execute(
                """
                SELECT t.id, t.trade_timestamp, t.side, t.price, t.size, t.notional,
                       b.city, b.forecast_date, bm.bucket_label, sm.station_id, sm.mapping_confidence
                FROM trades t
                JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
                JOIN weather_baskets b ON b.id=bm.basket_id
                LEFT JOIN weather_station_mappings sm ON LOWER(sm.city)=LOWER(b.city)
                WHERE UPPER(COALESCE(t.side,''))='BUY'
                ORDER BY t.trade_timestamp DESC, t.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        ]
    deltas = []
    for trade in trades:
        delta = nearest_prior_metar_delta_seconds(str(trade["trade_timestamp"]), reports_by_city.get(str(trade.get("city") or ""), []))
        trade["metar_delta_seconds"] = delta
        if delta is not None:
            deltas.append(delta)
    lines = ["METAR timing cannot be answered historically until first-seen METAR reports are captured live.", "", "## Numbers"]
    lines.append(f"- Stored METAR reports: {sum(len(v) for v in reports_by_city.values())}")
    lines.append(f"- Trades checked in sample: {len(trades)}")
    if deltas:
        close = sum(1 for value in deltas if 0 <= value <= 180)
        lines.append(f"- Buys within 3 minutes after prior METAR first-seen: {close}/{len(deltas)}")
        lines.append(f"- Median buy-after-METAR delta: {_human_delta_seconds(statistics.median(deltas))}")
    else:
        lines.append("- Buy-after-METAR delta: unavailable; start METAR capture and collect live releases.")
    lines.extend(["", "## Examples"])
    for trade in trades[:limit]:
        lines.append(
            f"- {trade['city']} {trade['forecast_date']} {trade['trade_timestamp']}: {trade['bucket_label']} "
            f"{_money(trade.get('notional'))} | station {trade.get('station_id') or 'unmapped'} "
            f"({trade.get('mapping_confidence') or 'no_mapping'}) | METAR delta {_human_delta_seconds(trade.get('metar_delta_seconds'))}"
        )
    lines.extend(
        [
            "",
            "## Data Quality",
            "- `first_seen_at` is the local poller time when this system first observed the METAR, not the meteorological observation time.",
            "- Station mappings are `likely` until verified against Polymarket settlement rules.",
        ]
    )
    return "\n".join(lines)


def generate_strategy_full_report(repository: Any) -> str:
    sections = [
        "# Whale Weather Strategy Full Report",
        "",
        "## Timing",
        generate_strategy_timing_report(repository, limit=8),
        "",
        "## Bucket Construction",
        generate_strategy_buckets_report(repository, limit=8),
        "",
        "## Order Size",
        generate_strategy_orders_report(repository),
        "",
        "## PnL / Win Rate",
        generate_strategy_pnl_report(repository),
        "",
        "## METAR Timing",
        generate_strategy_metar_report(repository, limit=8),
        "",
        "## Next Thing To Watch",
        "- Collect live METAR first-seen reports and confirmed settlements; those are the two missing pieces for proving release-timing edge and realized city-level expectancy.",
    ]
    return "\n".join(sections)
