from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from src.analysis.positions import refresh_positions
from src.analysis.strategy_report import generate_strategy_report
from src.storage.repositories import Repository
from src.tracking.followup_scheduler import process_due_followups
from src.tracking.trader_tracker import TraderTracker
from src.utils.config import AppConfig, load_config
from src.utils.logging import configure_logging
from src.weather.positions import recompute_weather_positions
from src.weather.report import generate_weather_report
from src.weather.settlements import compute_final_bucket_pnl, capture_settlement_from_gamma, generate_weather_day_report, set_manual_settlement
from src.weather.strategy import (
    generate_strategy_buckets_report,
    generate_strategy_full_report,
    generate_strategy_metar_report,
    generate_strategy_orders_report,
    generate_strategy_pnl_report,
    generate_strategy_timing_report,
)
from src.weather.tracker import WeatherTracker

console = Console()


def _default_config_path() -> Path:
    local = Path("config.yaml")
    if local.exists():
        return local
    return Path(__file__).resolve().parents[2] / "config.yaml"


def _load(args: argparse.Namespace) -> tuple[AppConfig, Repository]:
    config = load_config(args.config or _default_config_path())
    if getattr(args, "db", None):
        config = AppConfig(**{**config.__dict__, "database_path": Path(args.db)})
    configure_logging(config.log_level, config.log_file)
    return config, Repository(config.database_path)


def _table(title: str, columns: list[str], rows: list[Any]) -> None:
    table = Table(title=title)
    for column in columns:
        table.add_column(column)
    for row in rows:
        table.add_row(*["" if row.get(column) is None else str(row.get(column)) for column in columns])
    console.print(table)


def _rows(repo: Repository, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with repo.connect() as conn:
        return [dict(row) for row in conn.execute(query, params).fetchall()]


async def _run_tracker_command(args: argparse.Namespace, action: str) -> None:
    config, repo = _load(args)
    tracker = TraderTracker(config, repo)
    try:
        if action == "track":
            if args.once:
                count = await tracker.run_once(args.wallet or config.target_wallet)
                console.print(f"Captured {count} new trade(s).")
            else:
                await tracker.run_forever(args.wallet or config.target_wallet, interval_seconds=args.interval)
        elif action == "backfill":
            count = await tracker.backfill(args.wallet or config.target_wallet, max_pages=args.pages)
            console.print(f"Backfilled {count} new trade(s).")
    finally:
        await tracker.close()


def cmd_track(args: argparse.Namespace) -> None:
    asyncio.run(_run_tracker_command(args, "track"))


def cmd_backfill(args: argparse.Namespace) -> None:
    asyncio.run(_run_tracker_command(args, "backfill"))


def cmd_followups(args: argparse.Namespace) -> None:
    config, repo = _load(args)
    from src.api.polymarket_clob import PolymarketClobClient

    async def runner() -> None:
        clob = PolymarketClobClient(config.clob_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        try:
            count = await process_due_followups(
                repo,
                clob,
                limit=args.limit,
                slippage_notional_sizes=config.slippage_notional_sizes,
                liquidity_bands_pct=config.liquidity_bands_pct,
            )
            console.print(f"Captured {count} due follow-up snapshot(s).")
        finally:
            await clob.close()

    asyncio.run(runner())


def cmd_recent(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = [dict(row) for row in repo.list_recent_trades(args.limit)]
    _table("Recent Trades", ["id", "trade_timestamp", "side", "outcome", "price", "size", "notional", "market_slug"], rows)


def cmd_positions(args: argparse.Namespace) -> None:
    config, repo = _load(args)
    if args.refresh:
        with repo.connect() as conn:
            trader = conn.execute("SELECT id FROM traders ORDER BY id DESC LIMIT 1").fetchone()
        if trader:
            refresh_positions(repo, int(trader["id"]))
    rows = _rows(repo, "SELECT token_id, outcome, net_size, avg_entry_price, estimated_realized_pnl, estimated_unrealized_pnl, direction, confidence FROM positions ORDER BY ABS(net_size) DESC")
    _table("Positions", ["token_id", "outcome", "net_size", "avg_entry_price", "estimated_realized_pnl", "estimated_unrealized_pnl", "direction", "confidence"], rows)


def cmd_market(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        "SELECT id, trade_timestamp, side, outcome, price, size, notional, market_title FROM trades WHERE market_slug=? OR condition_id=? ORDER BY trade_timestamp DESC",
        (args.slug, args.slug),
    )
    _table("Market Trades", ["id", "trade_timestamp", "side", "outcome", "price", "size", "notional", "market_title"], rows)


def cmd_category(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT t.id, t.trade_timestamp, t.side, t.outcome, t.price, t.notional, sm.category, t.market_slug
        FROM trades t
        LEFT JOIN strategy_metrics sm ON sm.trade_id=t.id
        WHERE sm.category=?
        ORDER BY t.trade_timestamp DESC
        """,
        (args.category,),
    )
    _table("Category Trades", ["id", "trade_timestamp", "side", "outcome", "price", "notional", "category", "market_slug"], rows)


def cmd_whale_size(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(repo, "SELECT id, trade_timestamp, side, outcome, price, size, notional, market_slug FROM trades WHERE notional >= ? ORDER BY notional DESC", (args.amount,))
    _table("Large Trades", ["id", "trade_timestamp", "side", "outcome", "price", "size", "notional", "market_slug"], rows)


def cmd_wide_spread(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT t.id, t.trade_timestamp, t.side, t.outcome, t.price, t.notional, sm.spread_at_entry, t.market_slug
        FROM trades t JOIN strategy_metrics sm ON sm.trade_id=t.id
        WHERE sm.spread_at_entry >= ?
        ORDER BY sm.spread_at_entry DESC
        """,
        (args.spread,),
    )
    _table("Wide Spread Trades", ["id", "trade_timestamp", "side", "outcome", "price", "notional", "spread_at_entry", "market_slug"], rows)


def cmd_favorable(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT t.id, t.market_slug, t.side, t.price, f.interval_label, f.midpoint, f.price_change_from_trade
        FROM followup_snapshots f JOIN trades t ON t.id=f.trade_id
        WHERE f.interval_label=? AND f.favorable_move_boolean=1
        ORDER BY f.captured_at DESC
        """,
        (args.interval,),
    )
    _table("Favorable Follow-Ups", ["id", "market_slug", "side", "price", "interval_label", "midpoint", "price_change_from_trade"], rows)


def cmd_scaled_in(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT market_slug, outcome, token_id, COUNT(*) buys, SUM(notional) notional
        FROM trades
        WHERE side='BUY'
        GROUP BY token_id
        HAVING COUNT(*) >= 2
        ORDER BY notional DESC
        """,
    )
    _table("Scale-In Candidates", ["market_slug", "outcome", "token_id", "buys", "notional"], rows)


def cmd_exits(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(repo, "SELECT id, trade_timestamp, market_slug, outcome, price, size, notional FROM trades WHERE side='SELL' ORDER BY trade_timestamp DESC")
    _table("Suspected Exits/Reductions", ["id", "trade_timestamp", "market_slug", "outcome", "price", "size", "notional"], rows)


def cmd_profitable(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT token_id, outcome, net_size, avg_entry_price, estimated_realized_pnl, estimated_unrealized_pnl,
               COALESCE(estimated_realized_pnl,0) + COALESCE(estimated_unrealized_pnl,0) total_estimated_pnl
        FROM positions
        ORDER BY total_estimated_pnl DESC
        LIMIT ?
        """,
        (args.limit,),
    )
    _table("Top Estimated PnL", ["token_id", "outcome", "net_size", "avg_entry_price", "estimated_realized_pnl", "estimated_unrealized_pnl", "total_estimated_pnl"], rows)


def cmd_unresolved(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(repo, "SELECT token_id, outcome, net_size, avg_entry_price, direction, confidence FROM positions WHERE ABS(net_size) > 1e-9 ORDER BY ABS(net_size) DESC")
    _table("Unresolved/Open Positions", ["token_id", "outcome", "net_size", "avg_entry_price", "direction", "confidence"], rows)


def cmd_summary(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT COUNT(*) trades, SUM(notional) notional, AVG(price) avg_price,
               SUM(CASE WHEN side='BUY' THEN 1 ELSE 0 END) buys,
               SUM(CASE WHEN side='SELL' THEN 1 ELSE 0 END) sells
        FROM trades
        """,
    )
    _table("Trader Summary", ["trades", "notional", "avg_price", "buys", "sells"], rows)


def cmd_report(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_strategy_report(repo, period_days=args.days))


def cmd_doctor(args: argparse.Namespace) -> None:
    config, repo = _load(args)
    console.print("Observation-only Polymarket Whale Tracker")
    console.print(f"Database: {config.database_path}")
    console.print(f"Target wallet configured: {bool(config.target_wallet)}")
    console.print(f"Data API: {config.data_base_url}")
    console.print(f"Gamma API: {config.gamma_base_url}")
    console.print(f"CLOB API: {config.clob_base_url}")
    console.print("Trading/auth code paths: disabled by design")
    repo.log("INFO", "doctor", "doctor command executed")


def cmd_prune_db(args: argparse.Namespace) -> None:
    config, repo = _load(args)
    before_bytes = config.database_path.stat().st_size if config.database_path.exists() else 0
    summary = repo.ws_event_storage_summary()
    result = repo.prune_unlinked_ws_events(
        keep_recent_minutes=args.keep_recent_minutes,
        execute=args.execute,
        vacuum=args.vacuum,
        batch_size=args.batch_size,
    )
    after_bytes = config.database_path.stat().st_size if config.database_path.exists() else 0
    console.print("WebSocket storage summary")
    console.print(f"Total WS events: {summary['total_ws_events']}")
    console.print(f"Linked WS events: {summary['linked_ws_events']}")
    console.print(f"Unlinked WS events: {summary['unlinked_ws_events']}")
    console.print(f"Eligible for prune: {result['eligible_unlinked_ws_events']}")
    console.print(f"Deleted: {result['deleted_ws_events']}")
    console.print(f"DB bytes before: {before_bytes}")
    console.print(f"DB bytes after: {after_bytes}")
    if not args.execute:
        console.print("Dry run only. Re-run with --execute to delete eligible unlinked WS rows.")
    elif args.vacuum:
        console.print("VACUUM completed.")
    repo.log("INFO", "prune_db", "prune-db command executed", {**summary, **result, "before_bytes": before_bytes, "after_bytes": after_bytes})


async def _run_weather_command(args: argparse.Namespace, action: str) -> None:
    config, repo = _load(args)
    tracker = WeatherTracker(config, repo)
    try:
        if action == "discover":
            count = await tracker.discover_once()
            console.print(f"Discovered/updated {count} weather bucket token(s).")
        elif action == "watch":
            if args.once:
                discovered = await tracker.discover_once()
                inserted = await tracker.poll_trades_once(args.wallet or config.weather_target_wallet)
                console.print(f"Discovery rows: {discovered}; new weather trades: {inserted}.")
            else:
                await tracker.run_watch(args.wallet or config.weather_target_wallet)
    finally:
        await tracker.close()


def cmd_weather_discover(args: argparse.Namespace) -> None:
    asyncio.run(_run_weather_command(args, "discover"))


def cmd_weather_watch(args: argparse.Namespace) -> None:
    asyncio.run(_run_weather_command(args, "watch"))


def cmd_weather_forecast_capture(args: argparse.Namespace) -> None:
    async def runner() -> None:
        config, repo = _load(args)
        tracker = WeatherTracker(config, repo)
        try:
            if args.discover_first:
                discovered = await tracker.discover_once()
                console.print(f"Discovery rows: {discovered}")
            count = await tracker.capture_forecasts_once()
            console.print(f"Captured {count} forecast snapshot(s).")
        finally:
            await tracker.close()

    asyncio.run(runner())


def cmd_weather_observation_capture(args: argparse.Namespace) -> None:
    async def runner() -> None:
        config, repo = _load(args)
        tracker = WeatherTracker(config, repo)
        try:
            if args.discover_first:
                discovered = await tracker.discover_once()
                console.print(f"Discovery rows: {discovered}")
            count = await tracker.capture_observations_once()
            console.print(f"Captured {count} observation snapshot(s).")
        finally:
            await tracker.close()

    asyncio.run(runner())


def cmd_weather_metar_capture(args: argparse.Namespace) -> None:
    async def runner() -> None:
        config, repo = _load(args)
        tracker = WeatherTracker(config, repo)
        try:
            if args.discover_first:
                discovered = await tracker.discover_once()
                console.print(f"Discovery rows: {discovered}")
            count = await tracker.capture_metars_once()
            console.print(f"Captured {count} new METAR report(s).")
        finally:
            await tracker.close()

    asyncio.run(runner())


def cmd_weather_forecasts(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = [dict(row) for row in repo.latest_weather_forecasts(args.limit)]
    _table(
        "Latest Weather Forecasts",
        ["event_slug", "city", "forecast_date", "unit", "predicted_high", "daily_high", "hourly_high", "captured_at", "matched_name", "country_code", "confidence"],
        rows,
    )


def cmd_weather_recent(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT t.id, t.trade_timestamp, t.side, t.outcome, t.price, t.size, t.notional,
               b.city, b.forecast_date, bm.bucket_label, t.market_slug
        FROM trades t
        JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
        JOIN weather_baskets b ON b.id=bm.basket_id
        ORDER BY t.trade_timestamp DESC, t.id DESC
        LIMIT ?
        """,
        (args.limit,),
    )
    _table("Recent Weather Trades", ["id", "trade_timestamp", "side", "outcome", "price", "size", "notional", "city", "forecast_date", "bucket_label", "market_slug"], rows)


def cmd_weather_baskets(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT b.event_slug, b.city, b.forecast_date, COUNT(bm.id) buckets,
               SUM(CASE WHEN bm.active=1 AND bm.closed=0 THEN 1 ELSE 0 END) active_buckets
        FROM weather_baskets b
        LEFT JOIN weather_bucket_markets bm ON bm.basket_id=b.id
        GROUP BY b.id
        ORDER BY b.forecast_date DESC, b.city
        LIMIT ?
        """,
        (args.limit,),
    )
    _table("Weather Baskets", ["event_slug", "city", "forecast_date", "buckets", "active_buckets"], rows)


def cmd_weather_basket(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT bm.bucket_label, bm.outcome, bm.token_id, bm.market_slug, bm.active, bm.closed,
               wp.net_size, wp.avg_entry_price, wp.cost_basis, wp.unrealized_pnl
        FROM weather_baskets b
        JOIN weather_bucket_markets bm ON bm.basket_id=b.id
        LEFT JOIN weather_positions wp ON wp.token_id=bm.token_id
        WHERE b.event_slug=?
        ORDER BY bm.lower_temp, bm.upper_temp, bm.outcome
        """,
        (args.event_slug,),
    )
    _table("Weather Basket", ["bucket_label", "outcome", "token_id", "market_slug", "active", "closed", "net_size", "avg_entry_price", "cost_basis", "unrealized_pnl"], rows)


def cmd_weather_positions(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    if args.refresh:
        with repo.connect() as conn:
            trader = conn.execute("SELECT id FROM traders ORDER BY id DESC LIMIT 1").fetchone()
        if trader:
            recompute_weather_positions(repo, int(trader["id"]))
    rows = _rows(
        repo,
        """
        SELECT b.city, b.forecast_date, bm.bucket_label, wp.outcome, wp.net_size,
               wp.avg_entry_price, wp.cost_basis, wp.current_midpoint, wp.unrealized_pnl
        FROM weather_positions wp
        JOIN weather_bucket_markets bm ON bm.id=wp.bucket_market_id
        JOIN weather_baskets b ON b.id=wp.basket_id
        ORDER BY b.forecast_date DESC, b.city, bm.lower_temp
        """,
    )
    _table("Weather Positions", ["city", "forecast_date", "bucket_label", "outcome", "net_size", "avg_entry_price", "cost_basis", "current_midpoint", "unrealized_pnl"], rows)


def cmd_weather_executions(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT t.id, t.trade_timestamp, c.execution_timestamp, c.match_confidence,
               c.pre_book_delta_ms, c.post_book_delta_ms, t.side, t.price, t.size,
               b.city, b.forecast_date, bm.bucket_label
        FROM trade_execution_context c
        JOIN trades t ON t.id=c.trade_id
        JOIN weather_bucket_markets bm ON bm.token_id=t.token_id
        JOIN weather_baskets b ON b.id=bm.basket_id
        ORDER BY c.execution_timestamp_ms DESC
        LIMIT ?
        """,
        (args.limit,),
    )
    _table("Weather Executions", ["id", "trade_timestamp", "execution_timestamp", "match_confidence", "pre_book_delta_ms", "post_book_delta_ms", "side", "price", "size", "city", "forecast_date", "bucket_label"], rows)


def cmd_weather_basket_snapshots(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT s.id, s.captured_at, b.city, b.forecast_date, s.traded_bucket_label,
               s.traded_side, s.traded_price, s.token_count, s.matched_token_count,
               s.complete_yes_ask_cost, s.one_share_yes_ask_edge, s.quality_flags
        FROM weather_basket_snapshots s
        JOIN weather_baskets b ON b.id=s.basket_id
        ORDER BY s.id DESC
        LIMIT ?
        """,
        (args.limit,),
    )
    _table(
        "Weather Basket Snapshots",
        ["id", "captured_at", "city", "forecast_date", "traded_bucket_label", "traded_side", "traded_price", "token_count", "matched_token_count", "complete_yes_ask_cost", "one_share_yes_ask_edge", "quality_flags"],
        rows,
    )


def cmd_weather_followups(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = _rows(
        repo,
        """
        SELECT f.id, f.interval_label, f.scheduled_for, f.captured_at, b.city,
               b.forecast_date, f.traded_price, f.traded_midpoint,
               f.price_change_from_trade, f.favorable_move_boolean,
               f.one_share_yes_ask_edge, f.quality_flags
        FROM weather_followup_snapshots f
        JOIN weather_baskets b ON b.id=f.basket_id
        ORDER BY f.id DESC
        LIMIT ?
        """,
        (args.limit,),
    )
    _table(
        "Weather Followups",
        ["id", "interval_label", "scheduled_for", "captured_at", "city", "forecast_date", "traded_price", "traded_midpoint", "price_change_from_trade", "favorable_move_boolean", "one_share_yes_ask_edge", "quality_flags"],
        rows,
    )


def cmd_weather_observations(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = [dict(row) for row in repo.latest_weather_observations(args.limit)]
    _table(
        "Weather Observations",
        ["event_slug", "city", "forecast_date", "unit", "current_temperature", "intraday_high", "daily_high", "observed_high", "observation_status", "captured_at", "quality_flags"],
        rows,
    )


def cmd_weather_metars(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    rows = [dict(row) for row in repo.latest_weather_metars(args.limit)]
    _table(
        "Latest Weather METARs",
        ["city", "station_id", "report_type", "report_time", "first_seen_at", "temperature_c", "raw_text", "quality_flags"],
        rows,
    )


def cmd_strategy_timing(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_strategy_timing_report(repo, limit=args.limit))


def cmd_strategy_buckets(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_strategy_buckets_report(repo, limit=args.limit))


def cmd_strategy_orders(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_strategy_orders_report(repo))


def cmd_strategy_pnl(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_strategy_pnl_report(repo))


def cmd_strategy_metar(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_strategy_metar_report(repo, limit=args.limit))


def cmd_strategy_full_report(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_strategy_full_report(repo))


def cmd_weather_settlement_capture(args: argparse.Namespace) -> None:
    async def runner() -> None:
        config, repo = _load(args)
        from src.api.gamma import GammaClient

        gamma = GammaClient(config.gamma_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        try:
            settlement = await capture_settlement_from_gamma(repo, gamma, args.event_slug)
            if settlement.get("settlement_status") == "settled" and settlement.get("winning_token_id"):
                compute_final_bucket_pnl(repo, args.event_slug)
            console.print(
                f"Settlement {settlement['settlement_status']}: "
                f"winner={settlement.get('winning_bucket_label') or 'unknown'} "
                f"final_temp={settlement.get('final_temp') or 'unknown'} "
                f"confidence={settlement.get('confidence')}"
            )
        finally:
            await gamma.close()

    asyncio.run(runner())


def cmd_weather_settlement_set(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    settlement = set_manual_settlement(
        repo,
        args.event_slug,
        final_temp=args.final_temp,
        winning_bucket_label=args.winning_bucket_label,
        source=args.source,
        confidence="manual_user_supplied",
    )
    if settlement.get("settlement_status") == "settled" and settlement.get("winning_token_id"):
        compute_final_bucket_pnl(repo, args.event_slug)
    console.print(
        f"Stored manual settlement: {settlement['event_slug']} "
        f"winner={settlement.get('winning_bucket_label') or 'unknown'} "
        f"final_temp={settlement.get('final_temp') or 'unknown'}"
    )


def cmd_weather_day_report(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_weather_day_report(repo, args.event_slug, trade_limit=args.limit, store_pnl=not args.no_store_pnl))


def cmd_weather_report(args: argparse.Namespace) -> None:
    _, repo = _load(args)
    console.print(generate_weather_report(repo))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Observation-only Polymarket whale tracker")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--db", default=None, help="Override SQLite database path")
    sub = parser.add_subparsers(dest="command", required=True)

    track = sub.add_parser("track", help="Poll target wallet for new trades")
    track.add_argument("--wallet", default=None)
    track.add_argument("--interval", type=float, default=None)
    track.add_argument("--once", action="store_true")
    track.set_defaults(func=cmd_track)

    backfill = sub.add_parser("backfill", help="Backfill historical target wallet trades")
    backfill.add_argument("--wallet", default=None)
    backfill.add_argument("--pages", type=int, default=None)
    backfill.set_defaults(func=cmd_backfill)

    followups = sub.add_parser("followups", help="Capture due follow-up snapshots")
    followups.add_argument("--limit", type=int, default=100)
    followups.set_defaults(func=cmd_followups)

    recent = sub.add_parser("recent", help="Show recent trades")
    recent.add_argument("--limit", type=int, default=20)
    recent.set_defaults(func=cmd_recent)

    positions = sub.add_parser("positions", help="Show reconstructed positions")
    positions.add_argument("--refresh", action="store_true")
    positions.set_defaults(func=cmd_positions)

    market = sub.add_parser("market", help="Show trades by market slug or condition id")
    market.add_argument("--slug", required=True)
    market.set_defaults(func=cmd_market)

    category = sub.add_parser("category", help="Show trades by strategy metric category")
    category.add_argument("--category", required=True)
    category.set_defaults(func=cmd_category)

    whale_size = sub.add_parser("whale-size", help="Show trades over a notional threshold")
    whale_size.add_argument("--amount", type=float, required=True)
    whale_size.set_defaults(func=cmd_whale_size)

    wide_spread = sub.add_parser("wide-spread", help="Show trades where entry spread exceeded threshold")
    wide_spread.add_argument("--spread", type=float, required=True)
    wide_spread.set_defaults(func=cmd_wide_spread)

    favorable = sub.add_parser("favorable", help="Show trades that moved in whale's favor after an interval")
    favorable.add_argument("--interval", default="1h")
    favorable.set_defaults(func=cmd_favorable)

    sub.add_parser("scaled-in", help="Show markets where wallet bought repeatedly").set_defaults(func=cmd_scaled_in)
    sub.add_parser("exits", help="Show suspected exits/reductions").set_defaults(func=cmd_exits)

    profitable = sub.add_parser("profitable", help="Show top estimated profitable positions")
    profitable.add_argument("--limit", type=int, default=20)
    profitable.set_defaults(func=cmd_profitable)

    sub.add_parser("unresolved", help="Show open/unresolved positions").set_defaults(func=cmd_unresolved)
    sub.add_parser("summary", help="Summarize trader behavior").set_defaults(func=cmd_summary)

    report = sub.add_parser("report", help="Generate strategy report")
    report.add_argument("--days", type=int, default=None)
    report.set_defaults(func=cmd_report)
    analyze = sub.add_parser("analyze-strategy", help="Alias for report")
    analyze.add_argument("--days", type=int, default=None)
    analyze.set_defaults(func=cmd_report)
    sub.add_parser("doctor", help="Check local configuration").set_defaults(func=cmd_doctor)

    prune = sub.add_parser("prune-db", help="Prune unlinked WebSocket rows that are not used by execution context")
    prune.add_argument("--execute", action="store_true", help="Actually delete rows. Omitted means dry-run.")
    prune.add_argument("--vacuum", action="store_true", help="Run VACUUM after deleting rows to shrink the SQLite file.")
    prune.add_argument("--keep-recent-minutes", type=float, default=0, help="Keep recent unlinked WS rows for a live matcher grace window.")
    prune.add_argument("--batch-size", type=int, default=10000, help="Delete batch size for large SQLite databases.")
    prune.set_defaults(func=cmd_prune_db)

    weather_watch = sub.add_parser("weather-watch", help="Discover weather markets, record WebSocket books, and poll target weather trades")
    weather_watch.add_argument("--wallet", default=None)
    weather_watch.add_argument("--once", action="store_true")
    weather_watch.set_defaults(func=cmd_weather_watch)

    sub.add_parser("weather-discover", help="Discover active weather bucket markets").set_defaults(func=cmd_weather_discover)

    weather_recent = sub.add_parser("weather-recent", help="Show recent weather trades")
    weather_recent.add_argument("--limit", type=int, default=20)
    weather_recent.set_defaults(func=cmd_weather_recent)

    weather_baskets = sub.add_parser("weather-baskets", help="Show discovered weather baskets")
    weather_baskets.add_argument("--limit", type=int, default=50)
    weather_baskets.set_defaults(func=cmd_weather_baskets)

    weather_basket = sub.add_parser("weather-basket", help="Show one weather basket and positions")
    weather_basket.add_argument("--event-slug", required=True)
    weather_basket.set_defaults(func=cmd_weather_basket)

    weather_positions = sub.add_parser("weather-positions", help="Show weather bucket positions")
    weather_positions.add_argument("--refresh", action="store_true")
    weather_positions.set_defaults(func=cmd_weather_positions)

    weather_executions = sub.add_parser("weather-executions", help="Show weather execution context matches")
    weather_executions.add_argument("--limit", type=int, default=50)
    weather_executions.set_defaults(func=cmd_weather_executions)

    weather_basket_snapshots = sub.add_parser("weather-basket-snapshots", help="Show full basket snapshots captured at whale trades")
    weather_basket_snapshots.add_argument("--limit", type=int, default=50)
    weather_basket_snapshots.set_defaults(func=cmd_weather_basket_snapshots)

    weather_followups = sub.add_parser("weather-followups", help="Show scheduled post-trade weather basket followups")
    weather_followups.add_argument("--limit", type=int, default=50)
    weather_followups.set_defaults(func=cmd_weather_followups)

    weather_observations = sub.add_parser("weather-observations", help="Show latest stored weather observations/provisional highs")
    weather_observations.add_argument("--limit", type=int, default=50)
    weather_observations.set_defaults(func=cmd_weather_observations)

    weather_metar_capture = sub.add_parser("weather-metar-capture", help="Capture latest public METAR reports for mapped active weather cities")
    weather_metar_capture.add_argument("--discover-first", action="store_true")
    weather_metar_capture.set_defaults(func=cmd_weather_metar_capture)

    weather_metars = sub.add_parser("weather-metars", help="Show latest stored METAR reports")
    weather_metars.add_argument("--limit", type=int, default=50)
    weather_metars.set_defaults(func=cmd_weather_metars)

    settlement_capture = sub.add_parser("weather-settlement-capture", help="Capture final/winning bucket from Gamma market resolution")
    settlement_capture.add_argument("--event-slug", required=True)
    settlement_capture.set_defaults(func=cmd_weather_settlement_capture)

    settlement_set = sub.add_parser("weather-settlement-set", help="Manually store final temperature or winning bucket")
    settlement_set.add_argument("--event-slug", required=True)
    settlement_set.add_argument("--final-temp", type=float, default=None)
    settlement_set.add_argument("--winning-bucket-label", default=None)
    settlement_set.add_argument("--source", default="manual")
    settlement_set.set_defaults(func=cmd_weather_settlement_set)

    day_report = sub.add_parser("weather-day-report", help="Generate end-of-day forensic basket report")
    day_report.add_argument("--event-slug", required=True)
    day_report.add_argument("--limit", type=int, default=80)
    day_report.add_argument("--no-store-pnl", action="store_true")
    day_report.set_defaults(func=cmd_weather_day_report)

    sub.add_parser("weather-report", help="Generate weather arb report").set_defaults(func=cmd_weather_report)

    forecast_capture = sub.add_parser("weather-forecast-capture", help="Capture Open-Meteo forecast snapshots for discovered weather baskets")
    forecast_capture.add_argument("--discover-first", action="store_true")
    forecast_capture.set_defaults(func=cmd_weather_forecast_capture)

    observation_capture = sub.add_parser("weather-observation-capture", help="Capture Open-Meteo observation/provisional high snapshots")
    observation_capture.add_argument("--discover-first", action="store_true")
    observation_capture.set_defaults(func=cmd_weather_observation_capture)

    weather_forecasts = sub.add_parser("weather-forecasts", help="Show latest stored weather forecasts")
    weather_forecasts.add_argument("--limit", type=int, default=50)
    weather_forecasts.set_defaults(func=cmd_weather_forecasts)

    strategy = sub.add_parser("strategy", help="Weather strategy research reports")
    strategy_sub = strategy.add_subparsers(dest="strategy_command", required=True)

    strategy_timing = strategy_sub.add_parser("timing", help="Analyze first-buy timing by city lifecycle")
    strategy_timing.add_argument("--limit", type=int, default=20)
    strategy_timing.set_defaults(func=cmd_strategy_timing)

    strategy_buckets = strategy_sub.add_parser("buckets", help="Analyze forecast-centered bucket ladder construction")
    strategy_buckets.add_argument("--limit", type=int, default=20)
    strategy_buckets.set_defaults(func=cmd_strategy_buckets)

    strategy_sub.add_parser("orders", help="Analyze fill, cluster, and basket-level order size").set_defaults(func=cmd_strategy_orders)
    strategy_sub.add_parser("pnl", help="Analyze resolved city-level win rate and PnL").set_defaults(func=cmd_strategy_pnl)

    strategy_metar = strategy_sub.add_parser("metar", help="Analyze buy timing versus captured METAR releases")
    strategy_metar.add_argument("--limit", type=int, default=30)
    strategy_metar.set_defaults(func=cmd_strategy_metar)

    strategy_sub.add_parser("full-report", help="Generate the full weather strategy research report").set_defaults(func=cmd_strategy_full_report)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
