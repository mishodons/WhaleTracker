from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def _money(value: Any) -> str:
    try:
        return f"${float(value):,.2f}"
    except (TypeError, ValueError):
        return "$0.00"


def generate_strategy_report(repository: Any, *, period_days: int | None = None) -> str:
    since_clause = ""
    params: tuple[Any, ...] = ()
    if period_days:
        since = (datetime.now(timezone.utc) - timedelta(days=period_days)).isoformat()
        since_clause = "WHERE trade_timestamp >= ?"
        params = (since,)

    with repository.connect() as conn:
        total = conn.execute(f"SELECT COUNT(*) c, SUM(notional) n FROM trades {since_clause}", params).fetchone()
        side_rows = conn.execute(f"SELECT side, COUNT(*) c, SUM(notional) n FROM trades {since_clause} GROUP BY side", params).fetchall()
        category_rows = conn.execute(
            """
            SELECT COALESCE(sm.category, 'unknown') category, COUNT(*) c, SUM(t.notional) n
            FROM trades t
            LEFT JOIN strategy_metrics sm ON sm.trade_id=t.id
            """ + (f" {since_clause.replace('trade_timestamp', 't.trade_timestamp')}" if since_clause else "") + """
            GROUP BY COALESCE(sm.category, 'unknown')
            ORDER BY n DESC
            LIMIT 8
            """,
            params,
        ).fetchall()
        bucket_rows = conn.execute(
            """
            SELECT COALESCE(sm.price_bucket, 'unknown') bucket, COUNT(*) c, SUM(t.notional) n
            FROM trades t
            LEFT JOIN strategy_metrics sm ON sm.trade_id=t.id
            """ + (f" {since_clause.replace('trade_timestamp', 't.trade_timestamp')}" if since_clause else "") + """
            GROUP BY COALESCE(sm.price_bucket, 'unknown')
            ORDER BY c DESC
            """,
            params,
        ).fetchall()
        wide_spread = conn.execute(
            """
            SELECT COUNT(*) c
            FROM strategy_metrics sm
            JOIN trades t ON t.id=sm.trade_id
            WHERE sm.spread_at_entry >= 0.05
            """ + (" AND t.trade_timestamp >= ?" if period_days else ""),
            params,
        ).fetchone()
        scale_rows = conn.execute(
            """
            SELECT market_slug, outcome, token_id, COUNT(*) c, SUM(notional) n
            FROM trades
            WHERE side='BUY'
            GROUP BY token_id
            HAVING COUNT(*) >= 2
            ORDER BY n DESC
            LIMIT 10
            """
        ).fetchall()
        exits = conn.execute("SELECT COUNT(*) c, SUM(notional) n FROM trades WHERE side='SELL'").fetchone()
        open_positions = conn.execute("SELECT COUNT(*) c, SUM(ABS(net_size * COALESCE(estimated_current_price, avg_entry_price, 0))) n FROM positions WHERE ABS(net_size) > 1e-9").fetchone()

    lines = [
        "# Strategy Report",
        "",
        f"Trades observed: {int(total['c'] or 0)}",
        f"Observed notional: {_money(total['n'])}",
        f"Open position count: {int(open_positions['c'] or 0)}",
        f"Approx open exposure: {_money(open_positions['n'])}",
        "",
        "## Flow",
    ]
    for row in side_rows:
        lines.append(f"- {row['side'] or 'UNKNOWN'}: {int(row['c'])} trades, {_money(row['n'])}")
    lines.extend(["", "## Categories"])
    for row in category_rows:
        lines.append(f"- {row['category']}: {int(row['c'])} trades, {_money(row['n'])}")
    lines.extend(["", "## Price Buckets"])
    for row in bucket_rows:
        lines.append(f"- {row['bucket']}: {int(row['c'])} trades, {_money(row['n'])}")
    lines.extend(
        [
            "",
            "## Behavior Signals",
            f"- Wide-spread entries: {int(wide_spread['c'] or 0)}",
            f"- Suspected exits/reductions (SELL flow): {int(exits['c'] or 0)} trades, {_money(exits['n'])}",
            f"- Markets with repeated BUYs: {len(scale_rows)}",
        ]
    )
    if scale_rows:
        lines.append("")
        lines.append("## Scale-In Candidates")
        for row in scale_rows:
            lines.append(f"- {row['market_slug'] or row['token_id']}: {row['outcome'] or '?'} x{int(row['c'])}, {_money(row['n'])}")
    lines.extend(["", "## Archetype Hypothesis"])
    if wide_spread["c"] and int(wide_spread["c"]) > max(2, int((total["c"] or 0) * 0.25)):
        lines.append("The trader appears willing to operate in wider-spread or less efficient markets. Treat stale-price and liquidity-taking hypotheses as high-priority research targets.")
    elif scale_rows:
        lines.append("The trader shows scaling behavior. Focus on timing of adds versus adverse price movement and liquidity changes.")
    else:
        lines.append("Insufficient or mixed signal. Keep collecting observations before assigning a strong archetype.")
    lines.append("")
    lines.append("All PnL, maker/taker, and timing interpretations are approximate unless explicitly marked exact in stored data quality flags.")
    return "\n".join(lines)

