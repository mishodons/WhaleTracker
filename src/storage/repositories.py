from __future__ import annotations

import json
import sqlite3
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable

from src.storage.database import Database
from src.utils.dedupe import raw_json, trade_dedupe_key
from src.utils.time import to_iso, utc_now, utc_now_iso
from src.utils.quality import EXACT_TRADE_TIMESTAMP, DETECTION_TIMESTAMP, join_flags


def _float_or_none(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


class Repository:
    def __init__(self, database: Database | str | Path):
        self.database = database if isinstance(database, Database) else Database(database)
        self.database.initialize()

    def connect(self) -> sqlite3.Connection:
        return self.database.connect()

    def upsert_trader(self, wallet_address: str, label: str | None = None) -> int:
        wallet = wallet_address.lower()
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO traders(wallet_address, label, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(wallet_address) DO UPDATE SET label=COALESCE(excluded.label, traders.label)
                """,
                (wallet, label, now),
            )
            row = conn.execute("SELECT id FROM traders WHERE wallet_address=?", (wallet,)).fetchone()
            return int(row["id"])

    def upsert_market(self, market: dict[str, Any]) -> int | None:
        condition_id = (market.get("condition_id") or market.get("conditionId") or market.get("condition_id".lower()) or "").lower()
        if not condition_id:
            return None
        now_raw = market.get("created_at") or market.get("createdAt") or market.get("startDate")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO markets(
                  market_id, condition_id, slug, title, category, event_title, event_slug,
                  resolution_status, created_at, end_date, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(condition_id) DO UPDATE SET
                  market_id=COALESCE(excluded.market_id, markets.market_id),
                  slug=COALESCE(excluded.slug, markets.slug),
                  title=COALESCE(excluded.title, markets.title),
                  category=COALESCE(excluded.category, markets.category),
                  event_title=COALESCE(excluded.event_title, markets.event_title),
                  event_slug=COALESCE(excluded.event_slug, markets.event_slug),
                  resolution_status=COALESCE(excluded.resolution_status, markets.resolution_status),
                  created_at=COALESCE(excluded.created_at, markets.created_at),
                  end_date=COALESCE(excluded.end_date, markets.end_date),
                  raw_json=excluded.raw_json
                """,
                (
                    str(market.get("market_id") or market.get("id") or ""),
                    condition_id,
                    market.get("slug"),
                    market.get("title") or market.get("question"),
                    market.get("category") or market.get("categorySlug"),
                    market.get("event_title") or market.get("eventTitle"),
                    market.get("event_slug") or market.get("eventSlug"),
                    market.get("resolution_status") or ("closed" if market.get("closed") else "open"),
                    to_iso(now_raw),
                    to_iso(market.get("end_date") or market.get("endDate") or market.get("endDateIso")),
                    raw_json(market),
                ),
            )
            row = conn.execute("SELECT id FROM markets WHERE condition_id=?", (condition_id,)).fetchone()
            return int(row["id"]) if row else None

    def normalize_trade(self, trader_id: int, raw: dict[str, Any], detected_at: str | None = None) -> dict[str, Any]:
        price = _float_or_none(raw.get("price"))
        size = _float_or_none(raw.get("size"))
        notional = _float_or_none(raw.get("usdcSize") or raw.get("notional"))
        if notional is None and price is not None and size is not None:
            notional = price * size
        timestamp = to_iso(raw.get("timestamp") or raw.get("matchtime") or raw.get("trade_timestamp"))
        return {
            "trader_id": trader_id,
            "dedupe_key": trade_dedupe_key(raw),
            "external_trade_id": raw.get("id") or raw.get("trade_id"),
            "tx_hash": raw.get("transactionHash") or raw.get("transaction_hash") or raw.get("tx_hash"),
            "market_id": raw.get("market") or raw.get("market_id") or raw.get("conditionId"),
            "condition_id": (raw.get("conditionId") or raw.get("condition_id") or raw.get("market") or "").lower(),
            "token_id": str(raw.get("asset") or raw.get("asset_id") or raw.get("token_id") or ""),
            "market_slug": raw.get("slug") or raw.get("market_slug"),
            "market_title": raw.get("title") or raw.get("market_title"),
            "event_slug": raw.get("eventSlug") or raw.get("event_slug"),
            "outcome": raw.get("outcome"),
            "side": str(raw.get("side") or "").upper(),
            "price": price,
            "size": size,
            "notional": notional,
            "trade_timestamp": timestamp,
            "detected_at": detected_at or utc_now_iso(),
            "data_confidence": join_flags(EXACT_TRADE_TIMESTAMP if timestamp else None, DETECTION_TIMESTAMP),
            "raw_json": raw_json(raw),
            "created_at": utc_now_iso(),
        }

    def insert_trade(self, trader_id: int, raw: dict[str, Any], detected_at: str | None = None) -> tuple[int, bool]:
        trade = self.normalize_trade(trader_id, raw, detected_at)
        columns = list(trade.keys())
        placeholders = ",".join("?" for _ in columns)
        with self.connect() as conn:
            cursor = conn.execute(
                f"INSERT OR IGNORE INTO trades({','.join(columns)}) VALUES ({placeholders})",
                tuple(trade[column] for column in columns),
            )
            inserted = cursor.rowcount > 0
            row = conn.execute("SELECT id FROM trades WHERE dedupe_key=?", (trade["dedupe_key"],)).fetchone()
            return int(row["id"]), inserted

    def insert_orderbook_snapshot(self, snapshot: dict[str, Any]) -> int:
        columns = list(snapshot.keys())
        placeholders = ",".join("?" for _ in columns)
        with self.connect() as conn:
            cursor = conn.execute(
                f"INSERT INTO orderbook_snapshots({','.join(columns)}) VALUES ({placeholders})",
                tuple(snapshot[column] for column in columns),
            )
            return int(cursor.lastrowid)

    def schedule_followups(self, trade_id: int, schedules: Iterable[tuple[str, str]]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO followup_snapshots(trade_id, interval_label, scheduled_for)
                VALUES (?, ?, ?)
                """,
                [(trade_id, label, scheduled_for) for label, scheduled_for in schedules],
            )

    def get_trade(self, trade_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone()

    def list_recent_trades(self, limit: int = 20) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(conn.execute("SELECT * FROM trades ORDER BY trade_timestamp DESC, id DESC LIMIT ?", (limit,)))

    def list_trades(self, trader_id: int | None = None) -> list[sqlite3.Row]:
        query = "SELECT * FROM trades"
        params: tuple[Any, ...] = ()
        if trader_id is not None:
            query += " WHERE trader_id=?"
            params = (trader_id,)
        query += " ORDER BY trade_timestamp ASC, id ASC"
        with self.connect() as conn:
            return list(conn.execute(query, params))

    def pending_followups(self, now_iso: str, limit: int = 100) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT f.*, t.token_id, t.price AS trade_price, t.side AS trade_side, t.market_id
                    FROM followup_snapshots f
                    JOIN trades t ON t.id=f.trade_id
                    WHERE f.captured_at IS NULL AND f.scheduled_for <= ?
                    ORDER BY f.scheduled_for ASC
                    LIMIT ?
                    """,
                    (now_iso, limit),
                )
            )

    def complete_followup(self, followup_id: int, data: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE followup_snapshots
                SET captured_at=?, best_bid=?, best_ask=?, midpoint=?, spread=?,
                    price_change_from_trade=?, favorable_move_boolean=?, raw_json=?
                WHERE id=?
                """,
                (
                    data.get("captured_at"),
                    data.get("best_bid"),
                    data.get("best_ask"),
                    data.get("midpoint"),
                    data.get("spread"),
                    data.get("price_change_from_trade"),
                    1 if data.get("favorable_move_boolean") else 0 if data.get("favorable_move_boolean") is not None else None,
                    raw_json(data.get("raw_json", data)),
                    followup_id,
                ),
            )

    def replace_positions(self, trader_id: int, positions: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM positions WHERE trader_id=?", (trader_id,))
            if not positions:
                return
            columns = list(positions[0].keys())
            placeholders = ",".join("?" for _ in columns)
            conn.executemany(
                f"INSERT INTO positions({','.join(columns)}) VALUES ({placeholders})",
                [tuple(row[column] for column in columns) for row in positions],
            )

    def upsert_strategy_metric(self, metric: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO strategy_metrics(
                  trade_id, spread_at_entry, liquidity_score, trade_size_vs_depth, price_bucket,
                  market_age_at_trade, time_to_resolution, category, entry_type_hypothesis,
                  confidence_score, notes, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                  spread_at_entry=excluded.spread_at_entry,
                  liquidity_score=excluded.liquidity_score,
                  trade_size_vs_depth=excluded.trade_size_vs_depth,
                  price_bucket=excluded.price_bucket,
                  market_age_at_trade=excluded.market_age_at_trade,
                  time_to_resolution=excluded.time_to_resolution,
                  category=excluded.category,
                  entry_type_hypothesis=excluded.entry_type_hypothesis,
                  confidence_score=excluded.confidence_score,
                  notes=excluded.notes,
                  raw_json=excluded.raw_json
                """,
                (
                    metric.get("trade_id"),
                    metric.get("spread_at_entry"),
                    metric.get("liquidity_score"),
                    metric.get("trade_size_vs_depth"),
                    metric.get("price_bucket"),
                    metric.get("market_age_at_trade"),
                    metric.get("time_to_resolution"),
                    metric.get("category"),
                    metric.get("entry_type_hypothesis"),
                    metric.get("confidence_score"),
                    metric.get("notes"),
                    raw_json(metric),
                ),
            )

    def log(self, level: str, component: str, message: str, raw: dict[str, Any] | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO system_logs(timestamp, level, component, message, raw_json) VALUES (?, ?, ?, ?, ?)",
                (utc_now_iso(), level.upper(), component, message, raw_json(raw or {})),
            )

    def ws_event_storage_summary(self) -> dict[str, Any]:
        linked_cte = """
            WITH linked(id) AS (
              SELECT ws_trade_event_id FROM trade_execution_context WHERE ws_trade_event_id IS NOT NULL
              UNION
              SELECT pre_book_event_id FROM trade_execution_context WHERE pre_book_event_id IS NOT NULL
              UNION
              SELECT post_book_event_id FROM trade_execution_context WHERE post_book_event_id IS NOT NULL
            )
        """
        with self.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM ws_orderbook_events").fetchone()[0]
            linked = conn.execute(f"{linked_cte} SELECT COUNT(*) FROM ws_orderbook_events WHERE id IN (SELECT id FROM linked)").fetchone()[0]
            by_type = [
                dict(row)
                for row in conn.execute(
                    "SELECT event_type, COUNT(*) AS count FROM ws_orderbook_events GROUP BY event_type ORDER BY count DESC"
                )
            ]
            contexts = [
                dict(row)
                for row in conn.execute(
                    "SELECT match_confidence, COUNT(*) AS count FROM trade_execution_context GROUP BY match_confidence ORDER BY count DESC"
                )
            ]
        return {
            "total_ws_events": int(total),
            "linked_ws_events": int(linked),
            "unlinked_ws_events": int(total) - int(linked),
            "event_types": by_type,
            "execution_contexts": contexts,
        }

    def prune_unlinked_ws_events(
        self,
        *,
        keep_recent_minutes: float = 0,
        execute: bool = False,
        vacuum: bool = False,
        batch_size: int = 10000,
    ) -> dict[str, Any]:
        cutoff_iso = None
        params: tuple[Any, ...] = ()
        cutoff_clause = ""
        if keep_recent_minutes > 0:
            cutoff_iso = (utc_now() - timedelta(minutes=keep_recent_minutes)).isoformat()
            cutoff_clause = " AND local_received_at < ?"
            params = (cutoff_iso,)

        with self.connect() as conn:
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exec_context_ws_trade ON trade_execution_context(ws_trade_event_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exec_context_pre_book ON trade_execution_context(pre_book_event_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_exec_context_post_book ON trade_execution_context(post_book_event_id)")
            before_total = conn.execute("SELECT COUNT(*) FROM ws_orderbook_events").fetchone()[0]
            conn.execute("DROP TABLE IF EXISTS temp.keep_ws_event_ids")
            conn.execute("DROP TABLE IF EXISTS temp.prune_ws_event_ids")
            conn.execute("CREATE TEMP TABLE keep_ws_event_ids(id INTEGER PRIMARY KEY)")
            conn.execute("CREATE TEMP TABLE prune_ws_event_ids(id INTEGER PRIMARY KEY)")
            conn.execute(
                """
                INSERT OR IGNORE INTO keep_ws_event_ids(id)
                SELECT ws_trade_event_id FROM trade_execution_context WHERE ws_trade_event_id IS NOT NULL
                UNION
                SELECT pre_book_event_id FROM trade_execution_context WHERE pre_book_event_id IS NOT NULL
                UNION
                SELECT post_book_event_id FROM trade_execution_context WHERE post_book_event_id IS NOT NULL
                """
            )
            conn.execute(
                f"""
                INSERT INTO prune_ws_event_ids(id)
                SELECT w.id
                FROM ws_orderbook_events w
                LEFT JOIN keep_ws_event_ids k ON k.id=w.id
                WHERE k.id IS NULL{cutoff_clause}
                """,
                params,
            )
            eligible = conn.execute("SELECT COUNT(*) FROM prune_ws_event_ids").fetchone()[0]
            deleted = 0
            if execute and eligible:
                chunk_size = max(1, int(batch_size))
                while True:
                    ids = [int(row["id"]) for row in conn.execute("SELECT id FROM prune_ws_event_ids LIMIT ?", (chunk_size,)).fetchall()]
                    if not ids:
                        break
                    id_params = [(row_id,) for row_id in ids]
                    conn.executemany("DELETE FROM ws_orderbook_events WHERE id=?", id_params)
                    conn.executemany("DELETE FROM prune_ws_event_ids WHERE id=?", id_params)
                    deleted += len(ids)
                    conn.commit()
            after_total = conn.execute("SELECT COUNT(*) FROM ws_orderbook_events").fetchone()[0] if execute else before_total

        if execute and vacuum:
            with self.connect() as conn:
                conn.execute("VACUUM")
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

        return {
            "execute": execute,
            "vacuum": vacuum,
            "keep_recent_minutes": keep_recent_minutes,
            "cutoff_iso": cutoff_iso,
            "batch_size": batch_size,
            "before_total_ws_events": int(before_total),
            "eligible_unlinked_ws_events": int(eligible),
            "deleted_ws_events": int(deleted),
            "after_total_ws_events": int(after_total),
        }

    def upsert_weather_basket(self, basket: dict[str, Any]) -> int:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO weather_baskets(
                  event_slug, city, forecast_date, unit, event_title, status,
                  discovered_at, last_seen_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_slug) DO UPDATE SET
                  city=COALESCE(excluded.city, weather_baskets.city),
                  forecast_date=COALESCE(excluded.forecast_date, weather_baskets.forecast_date),
                  unit=COALESCE(excluded.unit, weather_baskets.unit),
                  event_title=COALESCE(excluded.event_title, weather_baskets.event_title),
                  status=COALESCE(excluded.status, weather_baskets.status),
                  last_seen_at=excluded.last_seen_at,
                  raw_json=excluded.raw_json
                """,
                (
                    basket.get("event_slug"),
                    basket.get("city"),
                    basket.get("forecast_date"),
                    basket.get("unit"),
                    basket.get("event_title"),
                    basket.get("status", "active"),
                    now,
                    now,
                    raw_json(basket),
                ),
            )
            row = conn.execute("SELECT id FROM weather_baskets WHERE event_slug=?", (basket.get("event_slug"),)).fetchone()
            return int(row["id"])

    def upsert_weather_bucket_market(self, bucket: dict[str, Any]) -> int:
        now = utc_now_iso()
        basket_id = bucket.get("basket_id")
        if not basket_id:
            basket_id = self.upsert_weather_basket(bucket)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO weather_bucket_markets(
                  basket_id, condition_id, token_id, market_slug, market_title, outcome,
                  bucket_label, lower_temp, upper_temp, bound_type, active, closed,
                  discovered_at, last_seen_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(token_id) DO UPDATE SET
                  basket_id=excluded.basket_id,
                  condition_id=COALESCE(excluded.condition_id, weather_bucket_markets.condition_id),
                  market_slug=COALESCE(excluded.market_slug, weather_bucket_markets.market_slug),
                  market_title=COALESCE(excluded.market_title, weather_bucket_markets.market_title),
                  outcome=COALESCE(excluded.outcome, weather_bucket_markets.outcome),
                  bucket_label=COALESCE(excluded.bucket_label, weather_bucket_markets.bucket_label),
                  lower_temp=COALESCE(excluded.lower_temp, weather_bucket_markets.lower_temp),
                  upper_temp=COALESCE(excluded.upper_temp, weather_bucket_markets.upper_temp),
                  bound_type=COALESCE(excluded.bound_type, weather_bucket_markets.bound_type),
                  active=COALESCE(excluded.active, weather_bucket_markets.active),
                  closed=COALESCE(excluded.closed, weather_bucket_markets.closed),
                  last_seen_at=excluded.last_seen_at,
                  raw_json=excluded.raw_json
                """,
                (
                    basket_id,
                    bucket.get("condition_id"),
                    str(bucket.get("token_id")),
                    bucket.get("market_slug"),
                    bucket.get("market_title"),
                    bucket.get("outcome"),
                    bucket.get("bucket_label"),
                    _float_or_none(bucket.get("lower_temp")),
                    _float_or_none(bucket.get("upper_temp")),
                    bucket.get("bound_type"),
                    1 if bucket.get("active", True) else 0,
                    1 if bucket.get("closed", False) else 0,
                    now,
                    now,
                    raw_json(bucket),
                ),
            )
            row = conn.execute("SELECT id FROM weather_bucket_markets WHERE token_id=?", (str(bucket.get("token_id")),)).fetchone()
            return int(row["id"])

    def list_weather_token_ids(self, *, active_only: bool = True) -> list[str]:
        query = "SELECT token_id FROM weather_bucket_markets"
        params: tuple[Any, ...] = ()
        if active_only:
            query += " WHERE COALESCE(active, 1)=1 AND COALESCE(closed, 0)=0"
        query += " ORDER BY token_id"
        with self.connect() as conn:
            return [str(row["token_id"]) for row in conn.execute(query, params).fetchall()]

    def get_weather_bucket_by_token(self, token_id: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT bm.*, b.event_slug, b.city, b.forecast_date, b.unit
                FROM weather_bucket_markets bm
                JOIN weather_baskets b ON b.id=bm.basket_id
                WHERE bm.token_id=?
                """,
                (str(token_id),),
            ).fetchone()

    def list_weather_bucket_markets_for_basket(self, basket_id: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT bm.*, b.event_slug, b.city, b.forecast_date, b.unit
                    FROM weather_bucket_markets bm
                    JOIN weather_baskets b ON b.id=bm.basket_id
                    WHERE bm.basket_id=?
                    ORDER BY bm.lower_temp, bm.upper_temp, bm.bucket_label, bm.outcome
                    """,
                    (basket_id,),
                )
            )

    def get_weather_basket_by_event_slug(self, event_slug: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM weather_baskets WHERE event_slug=?", (event_slug,)).fetchone()

    def insert_ws_orderbook_event(self, event: dict[str, Any]) -> int:
        columns = [
            "event_type",
            "token_id",
            "market_id",
            "event_exchange_timestamp_ms",
            "event_exchange_timestamp",
            "local_received_at",
            "message_hash",
            "transaction_hash",
            "side",
            "price",
            "size",
            "best_bid",
            "best_ask",
            "spread",
            "midpoint",
            "full_book_json",
            "raw_json",
        ]
        with self.connect() as conn:
            cursor = conn.execute(
                f"INSERT INTO ws_orderbook_events({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                tuple(event.get(column) for column in columns),
            )
            return int(cursor.lastrowid)

    def find_ws_trade_match(self, trade: dict[str, Any], *, window_ms: int = 5000) -> sqlite3.Row | None:
        tx_hash = (trade.get("tx_hash") or trade.get("transactionHash") or "").lower()
        token_id = str(trade.get("token_id") or trade.get("asset") or "")
        price = _float_or_none(trade.get("price"))
        size = _float_or_none(trade.get("size"))
        trade_ms = _int_or_none(trade.get("trade_timestamp_ms"))
        with self.connect() as conn:
            if tx_hash:
                row = conn.execute(
                    """
                    SELECT * FROM ws_orderbook_events
                    WHERE event_type='last_trade_price' AND LOWER(transaction_hash)=?
                    ORDER BY ABS(COALESCE(event_exchange_timestamp_ms, 0) - COALESCE(?, 0))
                    LIMIT 1
                    """,
                    (tx_hash, trade_ms),
                ).fetchone()
                if row:
                    return row
            if not token_id or trade_ms is None:
                return None
            return conn.execute(
                """
                SELECT * FROM ws_orderbook_events
                WHERE event_type='last_trade_price'
                  AND token_id=?
                  AND ABS(event_exchange_timestamp_ms - ?) <= ?
                  AND (? IS NULL OR ABS(price - ?) <= 0.000001)
                  AND (? IS NULL OR ABS(size - ?) <= 0.000001)
                ORDER BY ABS(event_exchange_timestamp_ms - ?) ASC
                LIMIT 1
                """,
                (token_id, trade_ms, window_ms, price, price, size, size, trade_ms),
            ).fetchone()

    def nearest_ws_book_events(self, token_id: str, execution_ms: int) -> tuple[sqlite3.Row | None, sqlite3.Row | None]:
        with self.connect() as conn:
            pre = conn.execute(
                """
                SELECT * FROM ws_orderbook_events
                WHERE token_id=? AND full_book_json IS NOT NULL AND event_exchange_timestamp_ms <= ?
                ORDER BY event_exchange_timestamp_ms DESC, id DESC
                LIMIT 1
                """,
                (str(token_id), execution_ms),
            ).fetchone()
            post = conn.execute(
                """
                SELECT * FROM ws_orderbook_events
                WHERE token_id=? AND full_book_json IS NOT NULL AND event_exchange_timestamp_ms >= ?
                ORDER BY event_exchange_timestamp_ms ASC, id ASC
                LIMIT 1
                """,
                (str(token_id), execution_ms),
            ).fetchone()
            return pre, post

    def upsert_trade_execution_context(self, context: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO trade_execution_context(
                  trade_id, token_id, execution_timestamp_ms, execution_timestamp,
                  execution_timestamp_source, ws_trade_event_id, pre_book_event_id,
                  post_book_event_id, pre_book_delta_ms, post_book_delta_ms,
                  match_confidence, quality_flags, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                  token_id=excluded.token_id,
                  execution_timestamp_ms=excluded.execution_timestamp_ms,
                  execution_timestamp=excluded.execution_timestamp,
                  execution_timestamp_source=excluded.execution_timestamp_source,
                  ws_trade_event_id=excluded.ws_trade_event_id,
                  pre_book_event_id=excluded.pre_book_event_id,
                  post_book_event_id=excluded.post_book_event_id,
                  pre_book_delta_ms=excluded.pre_book_delta_ms,
                  post_book_delta_ms=excluded.post_book_delta_ms,
                  match_confidence=excluded.match_confidence,
                  quality_flags=excluded.quality_flags,
                  raw_json=excluded.raw_json
                """,
                (
                    context.get("trade_id"),
                    str(context.get("token_id")),
                    context.get("execution_timestamp_ms"),
                    context.get("execution_timestamp"),
                    context.get("execution_timestamp_source"),
                    context.get("ws_trade_event_id"),
                    context.get("pre_book_event_id"),
                    context.get("post_book_event_id"),
                    context.get("pre_book_delta_ms"),
                    context.get("post_book_delta_ms"),
                    context.get("match_confidence"),
                    context.get("quality_flags"),
                    raw_json(context),
                ),
            )

    def replace_weather_positions(self, trader_id: int, rows: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM weather_positions WHERE trader_id=?", (trader_id,))
            if not rows:
                return
            columns = list(rows[0].keys())
            conn.executemany(
                f"INSERT INTO weather_positions({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                [tuple(row.get(column) for column in columns) for row in rows],
            )

    def replace_weather_basket_pnl(self, trader_id: int, rows: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM weather_basket_pnl WHERE trader_id=?", (trader_id,))
            if not rows:
                return
            columns = list(rows[0].keys())
            conn.executemany(
                f"INSERT INTO weather_basket_pnl({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                [tuple(row.get(column) for column in rows[0].keys()) for row in rows],
            )

    def get_weather_city_geocode(self, city: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM weather_city_geocodes WHERE city=?", (city,)).fetchone()

    def upsert_weather_city_geocode(self, row: dict[str, Any]) -> int:
        now = utc_now_iso()
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO weather_city_geocodes(
                  city, provider, provider_location_id, matched_name, country_code,
                  country, admin1, latitude, longitude, timezone, population,
                  confidence, created_at, updated_at, raw_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(city) DO UPDATE SET
                  provider=excluded.provider,
                  provider_location_id=excluded.provider_location_id,
                  matched_name=excluded.matched_name,
                  country_code=excluded.country_code,
                  country=excluded.country,
                  admin1=excluded.admin1,
                  latitude=excluded.latitude,
                  longitude=excluded.longitude,
                  timezone=excluded.timezone,
                  population=excluded.population,
                  confidence=excluded.confidence,
                  updated_at=excluded.updated_at,
                  raw_json=excluded.raw_json
                """,
                (
                    row.get("city"),
                    row.get("provider", "open-meteo"),
                    row.get("provider_location_id"),
                    row.get("matched_name"),
                    row.get("country_code"),
                    row.get("country"),
                    row.get("admin1"),
                    _float_or_none(row.get("latitude")),
                    _float_or_none(row.get("longitude")),
                    row.get("timezone"),
                    _int_or_none(row.get("population")),
                    row.get("confidence"),
                    now,
                    now,
                    row.get("raw_json") or raw_json(row),
                ),
            )
            geocode = conn.execute("SELECT id FROM weather_city_geocodes WHERE city=?", (row.get("city"),)).fetchone()
            return int(geocode["id"])

    def list_weather_baskets_for_forecasts(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT id, event_slug, city, forecast_date, unit, event_title, status
                    FROM weather_baskets
                    WHERE city IS NOT NULL AND forecast_date IS NOT NULL
                    ORDER BY forecast_date DESC, city
                    """
                )
            )

    def insert_weather_forecast_snapshot(self, snapshot: dict[str, Any]) -> int:
        columns = [
            "basket_id",
            "source",
            "city",
            "forecast_date",
            "unit",
            "latitude",
            "longitude",
            "provider_timezone",
            "captured_at",
            "forecast_generated_at",
            "predicted_high",
            "daily_high",
            "hourly_high",
            "model",
            "quality_flags",
            "raw_json",
        ]
        with self.connect() as conn:
            cursor = conn.execute(
                f"INSERT INTO weather_forecast_snapshots({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                tuple(snapshot.get(column) for column in columns),
            )
            return int(cursor.lastrowid)

    def latest_weather_forecasts(self, limit: int = 50) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    WITH latest AS (
                      SELECT basket_id, MAX(captured_at) captured_at
                      FROM weather_forecast_snapshots
                      GROUP BY basket_id
                    )
                    SELECT b.event_slug, f.city, f.forecast_date, f.unit, f.predicted_high,
                           f.daily_high, f.hourly_high, f.provider_timezone, f.captured_at,
                           f.quality_flags, g.matched_name, g.country_code, g.confidence
                    FROM latest l
                    JOIN weather_forecast_snapshots f
                      ON f.basket_id=l.basket_id AND f.captured_at=l.captured_at
                    JOIN weather_baskets b ON b.id=f.basket_id
                    LEFT JOIN weather_city_geocodes g ON g.city=f.city
                    ORDER BY f.captured_at DESC, f.forecast_date DESC, f.city
                    LIMIT ?
                    """,
                    (limit,),
                )
            )

    def insert_weather_basket_snapshot(self, snapshot: dict[str, Any]) -> int:
        columns = [
            "trade_id",
            "basket_id",
            "snapshot_type",
            "execution_timestamp",
            "execution_timestamp_ms",
            "captured_at",
            "snapshot_source",
            "token_count",
            "matched_token_count",
            "missing_token_count",
            "complete_yes_ask_cost",
            "complete_yes_bid_value",
            "one_share_yes_ask_edge",
            "one_share_yes_bid_edge",
            "traded_token_id",
            "traded_bucket_label",
            "traded_price",
            "traded_side",
            "quality_flags",
            "bucket_prices_json",
            "metrics_json",
            "raw_json",
        ]
        with self.connect() as conn:
            cursor = conn.execute(
                f"""
                INSERT OR REPLACE INTO weather_basket_snapshots({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                """,
                tuple(snapshot.get(column) for column in columns),
            )
            return int(cursor.lastrowid)

    def schedule_weather_followups(self, trade_id: int, basket_id: int, schedules: Iterable[tuple[str, str]]) -> None:
        with self.connect() as conn:
            conn.executemany(
                """
                INSERT OR IGNORE INTO weather_followup_snapshots(trade_id, basket_id, interval_label, scheduled_for)
                VALUES (?, ?, ?, ?)
                """,
                [(trade_id, basket_id, label, scheduled_for) for label, scheduled_for in schedules],
            )

    def pending_weather_followups(self, now_iso: str, limit: int = 200) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT f.*, t.token_id, t.price AS trade_price, t.side AS trade_side
                    FROM weather_followup_snapshots f
                    JOIN trades t ON t.id=f.trade_id
                    WHERE f.captured_at IS NULL AND f.scheduled_for <= ?
                    ORDER BY f.scheduled_for ASC, f.id ASC
                    LIMIT ?
                    """,
                    (now_iso, limit),
                )
            )

    def complete_weather_followup(self, followup_id: int, data: dict[str, Any]) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE weather_followup_snapshots
                SET captured_at=?, snapshot_source=?, traded_token_id=?, traded_price=?,
                    traded_midpoint=?, price_change_from_trade=?, favorable_move_boolean=?,
                    complete_yes_ask_cost=?, complete_yes_bid_value=?,
                    one_share_yes_ask_edge=?, one_share_yes_bid_edge=?,
                    matched_token_count=?, missing_token_count=?, quality_flags=?,
                    bucket_prices_json=?, raw_json=?
                WHERE id=?
                """,
                (
                    data.get("captured_at"),
                    data.get("snapshot_source"),
                    data.get("traded_token_id"),
                    data.get("traded_price"),
                    data.get("traded_midpoint"),
                    data.get("price_change_from_trade"),
                    1 if data.get("favorable_move_boolean") else 0 if data.get("favorable_move_boolean") is not None else None,
                    data.get("complete_yes_ask_cost"),
                    data.get("complete_yes_bid_value"),
                    data.get("one_share_yes_ask_edge"),
                    data.get("one_share_yes_bid_edge"),
                    data.get("matched_token_count"),
                    data.get("missing_token_count"),
                    data.get("quality_flags"),
                    data.get("bucket_prices_json"),
                    raw_json(data.get("raw_json", data)),
                    followup_id,
                ),
            )

    def insert_weather_observation(self, observation: dict[str, Any]) -> int:
        columns = [
            "basket_id",
            "source",
            "city",
            "forecast_date",
            "unit",
            "latitude",
            "longitude",
            "provider_timezone",
            "captured_at",
            "observation_time",
            "current_temperature",
            "intraday_high",
            "daily_high",
            "observed_high",
            "observation_status",
            "quality_flags",
            "raw_json",
        ]
        with self.connect() as conn:
            cursor = conn.execute(
                f"INSERT INTO weather_observations({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                tuple(observation.get(column) for column in columns),
            )
            return int(cursor.lastrowid)

    def latest_weather_observations(self, limit: int = 50) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    WITH latest AS (
                      SELECT basket_id, MAX(captured_at) captured_at
                      FROM weather_observations
                      GROUP BY basket_id
                    )
                    SELECT b.event_slug, o.city, o.forecast_date, o.unit, o.current_temperature,
                           o.intraday_high, o.daily_high, o.observed_high, o.observation_status,
                           o.captured_at, o.quality_flags
                    FROM latest l
                    JOIN weather_observations o
                      ON o.basket_id=l.basket_id AND o.captured_at=l.captured_at
                    JOIN weather_baskets b ON b.id=o.basket_id
                    ORDER BY o.captured_at DESC, o.forecast_date DESC, o.city
                    LIMIT ?
                    """,
                    (limit,),
                )
            )

    def upsert_weather_settlement(self, settlement: dict[str, Any]) -> int:
        columns = [
            "basket_id",
            "event_slug",
            "city",
            "forecast_date",
            "unit",
            "final_temp",
            "winning_bucket_market_id",
            "winning_token_id",
            "winning_bucket_label",
            "winning_market_slug",
            "settlement_status",
            "source",
            "confidence",
            "captured_at",
            "settled_at",
            "quality_flags",
            "raw_json",
        ]
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO weather_settlements({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                ON CONFLICT(basket_id) DO UPDATE SET
                  event_slug=excluded.event_slug,
                  city=excluded.city,
                  forecast_date=excluded.forecast_date,
                  unit=excluded.unit,
                  final_temp=excluded.final_temp,
                  winning_bucket_market_id=excluded.winning_bucket_market_id,
                  winning_token_id=excluded.winning_token_id,
                  winning_bucket_label=excluded.winning_bucket_label,
                  winning_market_slug=excluded.winning_market_slug,
                  settlement_status=excluded.settlement_status,
                  source=excluded.source,
                  confidence=excluded.confidence,
                  captured_at=excluded.captured_at,
                  settled_at=excluded.settled_at,
                  quality_flags=excluded.quality_flags,
                  raw_json=excluded.raw_json
                """,
                tuple(settlement.get(column) for column in columns),
            )
            row = conn.execute("SELECT id FROM weather_settlements WHERE basket_id=?", (settlement.get("basket_id"),)).fetchone()
            return int(row["id"])

    def get_weather_settlement_by_event_slug(self, event_slug: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute("SELECT * FROM weather_settlements WHERE event_slug=?", (event_slug,)).fetchone()

    def replace_weather_bucket_final_pnl(self, settlement_id: int, rows: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            conn.execute("DELETE FROM weather_bucket_final_pnl WHERE settlement_id=?", (settlement_id,))
            if not rows:
                return
            columns = [
                "settlement_id",
                "basket_id",
                "bucket_market_id",
                "token_id",
                "bucket_label",
                "outcome",
                "net_size",
                "avg_entry_price",
                "cost_basis",
                "realized_pnl",
                "final_payout",
                "final_pnl",
                "trade_count",
                "first_trade_at",
                "last_trade_at",
                "winning_bucket",
                "confidence",
                "computed_at",
                "raw_json",
            ]
            conn.executemany(
                f"INSERT INTO weather_bucket_final_pnl({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                [tuple(row.get(column) for column in columns) for row in rows],
            )

    def upsert_weather_station_mapping(self, mapping: dict[str, Any]) -> int:
        now = utc_now_iso()
        columns = [
            "city",
            "station_id",
            "station_name",
            "latitude",
            "longitude",
            "timezone",
            "mapping_confidence",
            "source",
            "notes",
            "created_at",
            "updated_at",
            "raw_json",
        ]
        row = {**mapping, "created_at": now, "updated_at": now, "raw_json": mapping.get("raw_json") or raw_json(mapping)}
        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO weather_station_mappings({','.join(columns)})
                VALUES ({','.join('?' for _ in columns)})
                ON CONFLICT(city) DO UPDATE SET
                  station_id=excluded.station_id,
                  station_name=excluded.station_name,
                  latitude=excluded.latitude,
                  longitude=excluded.longitude,
                  timezone=excluded.timezone,
                  mapping_confidence=excluded.mapping_confidence,
                  source=excluded.source,
                  notes=excluded.notes,
                  updated_at=excluded.updated_at,
                  raw_json=excluded.raw_json
                """,
                tuple(row.get(column) for column in columns),
            )
            saved = conn.execute("SELECT id FROM weather_station_mappings WHERE city=?", (mapping.get("city"),)).fetchone()
            return int(saved["id"])

    def list_weather_station_mappings_for_active_baskets(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT DISTINCT m.*
                    FROM weather_station_mappings m
                    JOIN weather_baskets b ON LOWER(b.city)=LOWER(m.city)
                    WHERE b.city IS NOT NULL AND b.forecast_date IS NOT NULL
                    ORDER BY m.city
                    """
                )
            )

    def insert_weather_metar_report(self, report: dict[str, Any]) -> tuple[int, bool]:
        columns = [
            "station_id",
            "city",
            "source",
            "report_type",
            "report_time",
            "first_seen_at",
            "raw_text",
            "temperature_c",
            "dewpoint_c",
            "wind_direction",
            "wind_speed_kt",
            "visibility_statute_mi",
            "altimeter_in_hg",
            "quality_flags",
            "raw_json",
        ]
        with self.connect() as conn:
            cursor = conn.execute(
                f"INSERT OR IGNORE INTO weather_metar_reports({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})",
                tuple(report.get(column) for column in columns),
            )
            row = conn.execute(
                """
                SELECT id FROM weather_metar_reports
                WHERE station_id=? AND COALESCE(report_time, '')=COALESCE(?, '') AND raw_text=?
                """,
                (report.get("station_id"), report.get("report_time"), report.get("raw_text")),
            ).fetchone()
            return int(row["id"]), cursor.rowcount > 0

    def latest_weather_metars(self, limit: int = 50) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return list(
                conn.execute(
                    """
                    SELECT *
                    FROM weather_metar_reports
                    ORDER BY first_seen_at DESC, report_time DESC, station_id
                    LIMIT ?
                    """,
                    (limit,),
                )
            )
