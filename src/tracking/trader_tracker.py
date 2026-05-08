from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.analysis.metrics import build_strategy_metric
from src.analysis.positions import refresh_positions
from src.api.gamma import GammaClient
from src.api.polymarket_clob import PolymarketClobClient
from src.api.polymarket_data import PolymarketDataClient
from src.storage.repositories import Repository
from src.tracking.followup_scheduler import build_followup_schedule, process_due_followups
from src.tracking.orderbook_snapshotter import OrderbookSnapshotter
from src.utils.config import AppConfig
from src.utils.time import utc_now_iso

LOG = logging.getLogger(__name__)


def _row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row) if not isinstance(row, dict) else row


class TraderTracker:
    def __init__(self, config: AppConfig, repository: Repository):
        self.config = config
        self.repository = repository
        self.data = PolymarketDataClient(config.data_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        self.gamma = GammaClient(config.gamma_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        self.clob = PolymarketClobClient(config.clob_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        self.snapshotter = OrderbookSnapshotter(
            self.clob,
            repository,
            slippage_notional_sizes=config.slippage_notional_sizes,
            liquidity_bands_pct=config.liquidity_bands_pct,
        )

    async def close(self) -> None:
        await self.data.close()
        await self.gamma.close()
        await self.clob.close()

    async def enrich_market(self, raw_trade: dict[str, Any]) -> dict[str, Any]:
        market: dict[str, Any] = {
            "condition_id": raw_trade.get("conditionId") or raw_trade.get("condition_id") or raw_trade.get("market"),
            "slug": raw_trade.get("slug"),
            "title": raw_trade.get("title"),
            "event_slug": raw_trade.get("eventSlug") or raw_trade.get("event_slug"),
        }
        gamma_market = None
        if raw_trade.get("slug"):
            try:
                gamma_market = await self.gamma.get_market_by_slug(str(raw_trade["slug"]))
            except Exception as exc:  # network enrichment should not block trade capture
                LOG.debug("Gamma market lookup failed: %s", exc)
        if gamma_market:
            market.update(
                {
                    "market_id": gamma_market.get("id"),
                    "condition_id": gamma_market.get("conditionId") or gamma_market.get("condition_id") or market.get("condition_id"),
                    "slug": gamma_market.get("slug") or market.get("slug"),
                    "title": gamma_market.get("question") or gamma_market.get("title") or market.get("title"),
                    "category": gamma_market.get("category") or gamma_market.get("categorySlug"),
                    "event_title": gamma_market.get("eventTitle"),
                    "event_slug": gamma_market.get("eventSlug") or market.get("event_slug"),
                    "created_at": gamma_market.get("createdAt") or gamma_market.get("startDate"),
                    "end_date": gamma_market.get("endDate") or gamma_market.get("endDateIso"),
                    "closed": gamma_market.get("closed"),
                    "raw_gamma": gamma_market,
                }
            )
        self.repository.upsert_market(market)
        return market

    async def process_raw_trade(self, trader_id: int, raw_trade: dict[str, Any], *, historical: bool = False) -> bool:
        detected_at = utc_now_iso()
        market = await self.enrich_market(raw_trade)
        trade_id, inserted = self.repository.insert_trade(trader_id, raw_trade, detected_at=detected_at)
        if not inserted:
            return False

        trade_row = self.repository.get_trade(trade_id)
        trade = _row_to_dict(trade_row)
        snapshot_record: dict[str, Any] | None = None
        if historical and not self.config.capture_current_books_on_backfill:
            self.snapshotter.mark_unavailable(trade)
        else:
            try:
                _, snapshot_record = await self.snapshotter.capture_for_trade(
                    trade,
                    snapshot_type="backfill_current" if historical else "entry",
                )
            except Exception as exc:
                LOG.warning("Orderbook capture failed for trade %s: %s", trade_id, exc)
                self.repository.log("WARN", "orderbook_snapshotter", "orderbook capture failed", {"trade_id": trade_id, "error": str(exc)})
                self.snapshotter.mark_unavailable(trade)

        if not historical:
            self.repository.schedule_followups(trade_id, build_followup_schedule(detected_at, self.config.followup_intervals))

        if snapshot_record:
            metric = build_strategy_metric(trade, snapshot_record, market)
            self.repository.upsert_strategy_metric(metric)
        refresh_positions(self.repository, trader_id)
        self.repository.log("INFO", "trader_tracker", "stored new trade", {"trade_id": trade_id, "historical": historical})
        return True

    async def run_once(self, wallet: str | None = None) -> int:
        target_wallet = wallet or self.config.target_wallet
        trader_id = self.repository.upsert_trader(target_wallet, self.config.trader_label)
        rows = await self.data.get_trades(target_wallet, limit=self.config.trade_page_limit, taker_only=False)
        new_count = 0
        for raw in sorted(rows, key=lambda item: item.get("timestamp", 0)):
            if await self.process_raw_trade(trader_id, raw, historical=False):
                new_count += 1
        await process_due_followups(
            self.repository,
            self.clob,
            slippage_notional_sizes=self.config.slippage_notional_sizes,
            liquidity_bands_pct=self.config.liquidity_bands_pct,
        )
        return new_count

    async def run_forever(self, wallet: str | None = None, *, interval_seconds: float | None = None) -> None:
        interval = interval_seconds or self.config.poll_interval_seconds
        LOG.info("tracking wallet %s every %.1fs", wallet or self.config.target_wallet, interval)
        while True:
            try:
                count = await self.run_once(wallet)
                if count:
                    LOG.info("captured %s new trade(s)", count)
            except Exception as exc:
                LOG.exception("tracking loop failed: %s", exc)
                self.repository.log("ERROR", "trader_tracker", "tracking loop failed", {"error": str(exc)})
            await asyncio.sleep(interval)

    async def backfill(self, wallet: str | None = None, *, max_pages: int | None = None) -> int:
        target_wallet = wallet or self.config.target_wallet
        trader_id = self.repository.upsert_trader(target_wallet, self.config.trader_label)
        count = 0
        async for raw in self.data.iter_trades(
            target_wallet,
            page_limit=self.config.backfill_page_limit,
            max_pages=max_pages or self.config.max_backfill_pages,
        ):
            if await self.process_raw_trade(trader_id, raw, historical=True):
                count += 1
        refresh_positions(self.repository, trader_id)
        return count

