from __future__ import annotations

from typing import Any


def _money(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "n/a"


def generate_weather_report(repository: Any) -> str:
    with repository.connect() as conn:
        baskets = conn.execute(
            """
            SELECT b.event_slug, b.city, b.forecast_date, p.total_cost, p.min_payout,
                   p.max_payout, p.worst_case_pnl, p.best_case_pnl, p.guaranteed_edge,
                   p.coverage_type
            FROM weather_basket_pnl p
            JOIN weather_baskets b ON b.id=p.basket_id
            ORDER BY p.computed_at DESC, ABS(p.total_cost) DESC
            LIMIT 20
            """
        ).fetchall()
        hours = conn.execute(
            """
            SELECT strftime('%H', trade_timestamp) hour_utc, COUNT(*) c, SUM(notional) n
            FROM trades t
            JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
            GROUP BY strftime('%H', trade_timestamp)
            ORDER BY c DESC
            """
        ).fetchall()
        executions = conn.execute(
            """
            SELECT c.match_confidence, COUNT(*) c
            FROM trade_execution_context c
            JOIN trades t ON t.id=c.trade_id
            JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
            GROUP BY c.match_confidence
            """
        ).fetchall()
        forecasts = conn.execute(
            """
            WITH latest AS (
              SELECT basket_id, MAX(captured_at) captured_at
              FROM weather_forecast_snapshots
              GROUP BY basket_id
            )
            SELECT b.event_slug, f.city, f.forecast_date, f.unit, f.predicted_high, f.captured_at
            FROM latest l
            JOIN weather_forecast_snapshots f
              ON f.basket_id=l.basket_id AND f.captured_at=l.captured_at
            JOIN weather_baskets b ON b.id=f.basket_id
            ORDER BY f.forecast_date DESC, f.city
            LIMIT 12
            """
        ).fetchall()
        basket_snapshots = conn.execute(
            """
            SELECT COUNT(*) snapshots,
                   SUM(CASE WHEN matched_token_count=token_count THEN 1 ELSE 0 END) complete_snapshots,
                   AVG(one_share_yes_ask_edge) avg_one_share_ask_edge
            FROM weather_basket_snapshots
            """
        ).fetchone()
        followups = conn.execute(
            """
            SELECT COUNT(*) followups,
                   SUM(CASE WHEN captured_at IS NOT NULL THEN 1 ELSE 0 END) captured,
                   SUM(CASE WHEN favorable_move_boolean=1 THEN 1 ELSE 0 END) favorable
            FROM weather_followup_snapshots
            """
        ).fetchone()
        observations = conn.execute(
            """
            WITH latest AS (
              SELECT basket_id, MAX(captured_at) captured_at
              FROM weather_observations
              GROUP BY basket_id
            )
            SELECT COUNT(*) observations
            FROM latest
            """
        ).fetchone()

    lines = ["# Weather Arb Report", ""]
    lines.append("## Time-of-Day Activity")
    if hours:
        for row in hours:
            lines.append(f"- {row['hour_utc']}:00 UTC: {int(row['c'])} trades, {_money(row['n'])} notional")
    else:
        lines.append("- No weather trades stored yet.")

    lines.extend(["", "## Execution Match Quality"])
    if executions:
        for row in executions:
            lines.append(f"- {row['match_confidence']}: {int(row['c'])}")
    else:
        lines.append("- No execution contexts stored yet. Start `weather-watch` before trades occur.")

    lines.extend(["", "## Basket PnL"])
    if baskets:
        for row in baskets:
            edge = _money(row["guaranteed_edge"]) if row["guaranteed_edge"] is not None else "not guaranteed"
            lines.append(
                f"- {row['event_slug']}: cost {_money(row['total_cost'])}, "
                f"worst {_money(row['worst_case_pnl'])}, best {_money(row['best_case_pnl'])}, "
                f"edge {edge}, {row['coverage_type']}"
            )
    else:
        lines.append("- No weather basket PnL rows yet.")

    lines.extend(["", "## Latest Forecast Snapshots"])
    if forecasts:
        for row in forecasts:
            value = "n/a" if row["predicted_high"] is None else f"{row['predicted_high']:.2f}{row['unit'] or ''}"
            lines.append(f"- {row['event_slug']}: predicted high {value}, captured {row['captured_at']}")
    else:
        lines.append("- No forecast snapshots stored yet.")

    lines.extend(["", "## Dissection Capture"])
    lines.append(
        f"- Basket entry snapshots: {int(basket_snapshots['snapshots'] or 0)}, "
        f"complete books: {int(basket_snapshots['complete_snapshots'] or 0)}, "
        f"avg one-share ask edge: {basket_snapshots['avg_one_share_ask_edge'] if basket_snapshots['avg_one_share_ask_edge'] is not None else 'n/a'}"
    )
    lines.append(
        f"- Weather followups scheduled: {int(followups['followups'] or 0)}, "
        f"captured: {int(followups['captured'] or 0)}, "
        f"favorable: {int(followups['favorable'] or 0)}"
    )
    lines.append(f"- Latest observation rows by basket: {int(observations['observations'] or 0)}")

    lines.extend(
        [
            "",
            "## Interpretation Notes",
            "- `exact_ws_tx_match` is the best available live execution timestamp source.",
            "- `data_api_only` means the trade was detected by REST but not matched to a live WebSocket trade event.",
            "- Basket PnL assumes mutually exclusive YES buckets and is marked partial unless every discovered YES bucket has a position.",
        ]
    )
    return "\n".join(lines)
