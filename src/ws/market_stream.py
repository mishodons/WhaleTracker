from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

import websockets

from src.utils.dedupe import raw_json
from src.utils.time import utc_now_iso

LOG = logging.getLogger(__name__)


def _num(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def exchange_ts_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        number = int(float(value))
    except (TypeError, ValueError):
        return None
    if number < 10_000_000_000:
        number *= 1000
    return number


def exchange_ts_iso(value: Any) -> str | None:
    ms = exchange_ts_ms(value)
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _trade_ts_ms(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return int(parsed.timestamp() * 1000)
        except ValueError:
            pass
    return exchange_ts_ms(value)


@dataclass
class BookState:
    token_id: str
    market_id: str | None = None
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    timestamp_ms: int | None = None
    message_hash: str | None = None

    def apply_book(self, event: dict[str, Any]) -> None:
        self.market_id = event.get("market") or self.market_id
        self.timestamp_ms = exchange_ts_ms(event.get("timestamp"))
        self.message_hash = event.get("hash") or self.message_hash
        self.bids = _levels_to_map(event.get("bids") or [])
        self.asks = _levels_to_map(event.get("asks") or [])

    def apply_price_change(self, change: dict[str, Any], *, market_id: str | None, timestamp: Any) -> None:
        self.market_id = market_id or self.market_id
        self.timestamp_ms = exchange_ts_ms(timestamp) or self.timestamp_ms
        self.message_hash = change.get("hash") or self.message_hash
        side = str(change.get("side") or "").upper()
        price = _num(change.get("price"))
        size = _num(change.get("size"))
        if price is None or size is None:
            return
        book_side = self.bids if side == "BUY" else self.asks
        if size <= 0:
            book_side.pop(price, None)
        else:
            book_side[price] = size

    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    def spread(self) -> float | None:
        bid = self.best_bid()
        ask = self.best_ask()
        return ask - bid if bid is not None and ask is not None else None

    def midpoint(self) -> float | None:
        bid = self.best_bid()
        ask = self.best_ask()
        return (bid + ask) / 2 if bid is not None and ask is not None else None

    def as_book_json(self) -> str:
        payload = {
            "market": self.market_id,
            "asset_id": self.token_id,
            "timestamp": self.timestamp_ms,
            "hash": self.message_hash,
            "bids": [{"price": price, "size": size} for price, size in sorted(self.bids.items(), reverse=True)],
            "asks": [{"price": price, "size": size} for price, size in sorted(self.asks.items())],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _levels_to_map(levels: list[dict[str, Any]]) -> dict[float, float]:
    mapped: dict[float, float] = {}
    for level in levels:
        price = _num(level.get("price"))
        size = _num(level.get("size"))
        if price is not None and size is not None and size > 0:
            mapped[price] = size
    return mapped


def event_row_from_message(message: dict[str, Any], *, local_received_at: str, state: BookState | None = None) -> dict[str, Any]:
    event_type = message.get("event_type") or "unknown"
    token_id = str(message.get("asset_id") or message.get("token_id") or "")
    best_bid = _num(message.get("best_bid")) if "best_bid" in message else state.best_bid() if state else None
    best_ask = _num(message.get("best_ask")) if "best_ask" in message else state.best_ask() if state else None
    spread = _num(message.get("spread")) if "spread" in message else (best_ask - best_bid if best_bid is not None and best_ask is not None else None)
    midpoint = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    timestamp = message.get("timestamp")
    return {
        "event_type": event_type,
        "token_id": token_id or None,
        "market_id": message.get("market"),
        "event_exchange_timestamp_ms": exchange_ts_ms(timestamp),
        "event_exchange_timestamp": exchange_ts_iso(timestamp),
        "local_received_at": local_received_at,
        "message_hash": message.get("hash"),
        "transaction_hash": message.get("transaction_hash") or message.get("transactionHash"),
        "side": str(message.get("side")).upper() if message.get("side") else None,
        "price": _num(message.get("price")),
        "size": _num(message.get("size")),
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "midpoint": midpoint,
        "full_book_json": state.as_book_json() if state else None,
        "raw_json": raw_json(message),
    }


def normalize_ws_payload(payload: Any, *, states: dict[str, BookState], local_received_at: str | None = None) -> list[dict[str, Any]]:
    received_at = local_received_at or utc_now_iso()
    messages = payload if isinstance(payload, list) else [payload]
    rows: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict) or not message:
            continue
        event_type = message.get("event_type")
        if event_type == "book":
            token_id = str(message.get("asset_id") or "")
            if not token_id:
                continue
            state = states.setdefault(token_id, BookState(token_id=token_id))
            state.apply_book(message)
            rows.append(event_row_from_message(message, local_received_at=received_at, state=state))
        elif event_type == "price_change":
            for change in message.get("price_changes") or []:
                token_id = str(change.get("asset_id") or "")
                if not token_id:
                    continue
                state = states.setdefault(token_id, BookState(token_id=token_id))
                state.apply_price_change(change, market_id=message.get("market"), timestamp=message.get("timestamp"))
                child = dict(change)
                child["event_type"] = "price_change"
                child["market"] = message.get("market")
                child["timestamp"] = message.get("timestamp")
                rows.append(event_row_from_message(child, local_received_at=received_at, state=state))
        else:
            token_id = str(message.get("asset_id") or message.get("token_id") or "")
            state = states.get(token_id)
            rows.append(event_row_from_message(message, local_received_at=received_at, state=state))
    return rows


class MarketStreamRecorder:
    def __init__(
        self,
        repository: Any,
        *,
        websocket_url: str,
        subscription_batch_size: int = 200,
        reconnect_backoff_seconds: float = 2.0,
        persist_raw_events: bool = False,
        buffer_seconds: float = 900,
        buffer_rows_per_token: int = 5000,
    ):
        self.repository = repository
        self.websocket_url = websocket_url
        self.subscription_batch_size = subscription_batch_size
        self.reconnect_backoff_seconds = reconnect_backoff_seconds
        self.persist_raw_events = persist_raw_events
        self.buffer_ms = int(buffer_seconds * 1000)
        self.buffer_rows_per_token = buffer_rows_per_token
        self.states: dict[str, BookState] = {}
        self.assets: set[str] = set()
        self.recent_rows: dict[str, deque[dict[str, Any]]] = defaultdict(deque)
        self._updates: asyncio.Queue[set[str]] = asyncio.Queue()

    async def subscribe_more(self, token_ids: Iterable[str]) -> None:
        tokens = {str(token_id) for token_id in token_ids if token_id}
        if tokens:
            await self._updates.put(tokens)

    async def run_forever(self, token_ids: Iterable[str]) -> None:
        self.assets.update(str(token_id) for token_id in token_ids if token_id)
        while True:
            try:
                await self._connect_and_record()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("market websocket disconnected: %s", exc)
                self.repository.log("WARN", "market_stream", "websocket disconnected", {"error": str(exc)})
                await asyncio.sleep(self.reconnect_backoff_seconds)

    async def _connect_and_record(self) -> None:
        async with websockets.connect(self.websocket_url, ping_interval=20, ping_timeout=20) as ws:
            await self._send_subscriptions(ws, self.assets, initial=True)
            receiver = asyncio.create_task(self._receive_loop(ws))
            updater = asyncio.create_task(self._update_loop(ws))
            done, pending = await asyncio.wait({receiver, updater}, return_when=asyncio.FIRST_EXCEPTION)
            for task in pending:
                task.cancel()
            for task in done:
                task.result()

    async def _send_subscriptions(self, ws: Any, token_ids: Iterable[str], *, initial: bool = False) -> None:
        tokens = sorted({str(token_id) for token_id in token_ids if token_id})
        for index in range(0, len(tokens), self.subscription_batch_size):
            batch = tokens[index : index + self.subscription_batch_size]
            payload: dict[str, Any] = {
                "assets_ids": batch,
                "type": "market",
                "custom_feature_enabled": True,
            }
            if not initial:
                payload["operation"] = "subscribe"
            await ws.send(json.dumps(payload))

    async def _update_loop(self, ws: Any) -> None:
        while True:
            tokens = await self._updates.get()
            new_tokens = tokens - self.assets
            if not new_tokens:
                continue
            self.assets.update(new_tokens)
            await self._send_subscriptions(ws, new_tokens, initial=False)

    async def _receive_loop(self, ws: Any) -> None:
        async for raw_message in ws:
            try:
                payload = json.loads(raw_message)
            except ValueError:
                continue
            rows = normalize_ws_payload(payload, states=self.states)
            for row in rows:
                self._remember(row)
                if self.persist_raw_events:
                    row["_db_id"] = self.repository.insert_ws_orderbook_event(row)

    def _remember(self, row: dict[str, Any]) -> None:
        token_id = str(row.get("token_id") or "")
        if not token_id:
            return
        bucket = self.recent_rows[token_id]
        bucket.append(row)
        self._trim_bucket(bucket)

    def _trim_bucket(self, bucket: deque[dict[str, Any]]) -> None:
        if self.buffer_rows_per_token > 0:
            while len(bucket) > self.buffer_rows_per_token:
                bucket.popleft()
        if self.buffer_ms <= 0 or not bucket:
            return
        latest = max((row.get("event_exchange_timestamp_ms") or 0 for row in bucket), default=0)
        if latest <= 0:
            return
        cutoff = latest - self.buffer_ms
        while bucket and (bucket[0].get("event_exchange_timestamp_ms") or latest) < cutoff:
            bucket.popleft()

    def execution_rows_for_trade(self, trade: dict[str, Any], *, window_ms: int = 5000) -> dict[str, dict[str, Any] | None]:
        token_id = str(trade.get("token_id") or trade.get("asset") or "")
        if not token_id:
            return {"ws_trade": None, "pre_book": None, "post_book": None}
        trade_ms = _trade_ts_ms(trade.get("trade_timestamp") or trade.get("timestamp"))
        rows = list(self.recent_rows.get(token_id, ()))
        ws_trade = self._find_trade_row(rows, trade, trade_ms, window_ms=window_ms)
        execution_ms = ws_trade.get("event_exchange_timestamp_ms") if ws_trade else trade_ms
        pre_book = post_book = None
        if execution_ms is not None:
            book_rows = [row for row in rows if row.get("full_book_json") and row.get("event_exchange_timestamp_ms") is not None]
            pre_candidates = [row for row in book_rows if int(row["event_exchange_timestamp_ms"]) <= int(execution_ms)]
            post_candidates = [row for row in book_rows if int(row["event_exchange_timestamp_ms"]) >= int(execution_ms)]
            pre_book = max(pre_candidates, key=lambda row: (int(row["event_exchange_timestamp_ms"]), int(row.get("_db_id") or 0)), default=None)
            post_book = min(post_candidates, key=lambda row: (int(row["event_exchange_timestamp_ms"]), int(row.get("_db_id") or 0)), default=None)
        return {"ws_trade": ws_trade, "pre_book": pre_book, "post_book": post_book}

    def persist_execution_rows(
        self,
        trade: dict[str, Any],
        *,
        window_ms: int = 5000,
        book_window_ms: int | None = None,
    ) -> dict[str, dict[str, Any] | None]:
        selected = self.execution_rows_for_trade(trade, window_ms=window_ms)
        if book_window_ms is not None:
            execution_ms = selected["ws_trade"].get("event_exchange_timestamp_ms") if selected.get("ws_trade") else _trade_ts_ms(trade.get("trade_timestamp") or trade.get("timestamp"))
            if execution_ms is not None:
                for key in ("pre_book", "post_book"):
                    row = selected.get(key)
                    event_ms = row.get("event_exchange_timestamp_ms") if row else None
                    if event_ms is not None and abs(int(event_ms) - int(execution_ms)) > book_window_ms:
                        selected[key] = None
        persisted: dict[str, dict[str, Any] | None] = {}
        for key, row in selected.items():
            persisted[key] = self._persist_cached_row(row) if row else None
        return persisted

    def basket_book_rows(
        self,
        token_ids: Iterable[str],
        *,
        execution_ms: int | None,
        max_delta_ms: int | None = 300000,
    ) -> dict[str, dict[str, Any] | None]:
        selected: dict[str, dict[str, Any] | None] = {}
        for token_id in {str(token_id) for token_id in token_ids if token_id}:
            rows = [row for row in self.recent_rows.get(token_id, ()) if row.get("full_book_json") and row.get("event_exchange_timestamp_ms") is not None]
            if not rows:
                selected[token_id] = None
                continue
            if execution_ms is None:
                selected[token_id] = rows[-1]
                continue
            pre = [
                row
                for row in rows
                if int(row["event_exchange_timestamp_ms"]) <= execution_ms
                and (max_delta_ms is None or execution_ms - int(row["event_exchange_timestamp_ms"]) <= max_delta_ms)
            ]
            if pre:
                selected[token_id] = max(pre, key=lambda row: int(row["event_exchange_timestamp_ms"]))
                continue
            post = [
                row
                for row in rows
                if int(row["event_exchange_timestamp_ms"]) >= execution_ms
                and (max_delta_ms is None or int(row["event_exchange_timestamp_ms"]) - execution_ms <= max_delta_ms)
            ]
            selected[token_id] = min(post, key=lambda row: int(row["event_exchange_timestamp_ms"]), default=None)
        return selected

    def _persist_cached_row(self, row: dict[str, Any]) -> dict[str, Any]:
        if not row.get("_db_id"):
            row["_db_id"] = self.repository.insert_ws_orderbook_event(row)
        copy = dict(row)
        copy["id"] = copy["_db_id"]
        return copy

    def _find_trade_row(
        self,
        rows: list[dict[str, Any]],
        trade: dict[str, Any],
        trade_ms: int | None,
        *,
        window_ms: int,
    ) -> dict[str, Any] | None:
        tx_hash = str(trade.get("tx_hash") or trade.get("transactionHash") or "").lower()
        if tx_hash:
            tx_matches = [
                row
                for row in rows
                if row.get("event_type") == "last_trade_price"
                and str(row.get("transaction_hash") or "").lower() == tx_hash
            ]
            if tx_matches:
                return min(tx_matches, key=lambda row: abs((row.get("event_exchange_timestamp_ms") or trade_ms or 0) - (trade_ms or 0)))

        if trade_ms is None:
            return None
        price = _num(trade.get("price"))
        size = _num(trade.get("size"))
        candidates = []
        for row in rows:
            event_ms = row.get("event_exchange_timestamp_ms")
            if row.get("event_type") != "last_trade_price" or event_ms is None:
                continue
            if abs(int(event_ms) - trade_ms) > window_ms:
                continue
            if price is not None and row.get("price") is not None and abs(float(row["price"]) - price) > 0.000001:
                continue
            if size is not None and row.get("size") is not None and abs(float(row["size"]) - size) > 0.000001:
                continue
            candidates.append(row)
        return min(candidates, key=lambda row: abs(int(row["event_exchange_timestamp_ms"]) - trade_ms), default=None)
