from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.api.aviation_weather import AviationWeatherClient
from src.api.gamma import GammaClient
from src.api.open_meteo import OpenMeteoClient
from src.api.polymarket_clob import PolymarketClobClient
from src.api.polymarket_data import PolymarketDataClient
from src.storage.repositories import Repository
from src.utils.config import AppConfig
from src.utils.time import utc_now_iso
from src.weather.discovery import (
    discover_weather_markets,
    is_weather_trade,
    upsert_weather_event_markets,
    upsert_weather_trade_market,
)
from src.weather.execution import attach_execution_context
from src.weather.forecast import capture_forecasts_for_active_baskets
from src.weather.metar import capture_metars_for_active_baskets
from src.weather.observations import capture_observations_for_active_baskets
from src.weather.positions import recompute_weather_positions
from src.weather.snapshots import (
    current_basket_state_from_clob,
    entry_basket_snapshot_from_cache,
    entry_basket_snapshot_from_clob_state,
    process_due_weather_followups,
    schedule_weather_followups_for_trade,
)
from src.ws.market_stream import MarketStreamRecorder

LOG = logging.getLogger(__name__)


class WeatherTracker:
    def __init__(self, config: AppConfig, repository: Repository):
        self.config = config
        self.repository = repository
        self.data = PolymarketDataClient(config.data_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        self.gamma = GammaClient(config.gamma_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        self.clob = PolymarketClobClient(config.clob_base_url, config.timeout_seconds, config.max_retries, config.retry_backoff_seconds)
        self.open_meteo = OpenMeteoClient(
            forecast_base_url=config.open_meteo_forecast_base_url,
            geocoding_base_url=config.open_meteo_geocoding_base_url,
            timeout=config.timeout_seconds,
            max_retries=config.max_retries,
            backoff=config.retry_backoff_seconds,
        )
        self.aviation_weather = AviationWeatherClient(
            base_url=config.aviation_weather_base_url,
            timeout=config.timeout_seconds,
            max_retries=config.max_retries,
            backoff=config.retry_backoff_seconds,
        )
        self.recorder = MarketStreamRecorder(
            repository,
            websocket_url=config.weather_websocket_url,
            subscription_batch_size=config.weather_websocket_subscription_batch_size,
            persist_raw_events=config.weather_websocket_persist_raw_events,
            buffer_seconds=config.weather_websocket_buffer_seconds,
            buffer_rows_per_token=config.weather_websocket_buffer_rows_per_token,
        )
        self._event_refresh_cache: set[str] = set()

    async def close(self) -> None:
        await self.data.close()
        await self.gamma.close()
        await self.clob.close()
        await self.open_meteo.close()
        await self.aviation_weather.close()

    async def discover_once(self) -> int:
        buckets = await discover_weather_markets(
            self.gamma,
            self.repository,
            market_limit=self.config.weather_market_limit,
            max_pages=self.config.weather_max_market_pages,
        )
        tokens = self.repository.list_weather_token_ids()
        await self.recorder.subscribe_more(tokens)
        self.repository.log("INFO", "weather_discovery", "weather discovery completed", {"buckets": len(buckets), "tokens": len(tokens)})
        return len(buckets)

    async def capture_forecasts_once(self) -> int:
        if not self.config.weather_forecasts_enabled:
            return 0
        snapshots = await capture_forecasts_for_active_baskets(self.repository, self.open_meteo)
        return len(snapshots)

    async def capture_observations_once(self) -> int:
        if not self.config.weather_observations_enabled:
            return 0
        observations = await capture_observations_for_active_baskets(self.repository, self.open_meteo)
        return len(observations)

    async def capture_metars_once(self) -> int:
        if not self.config.weather_metar_enabled:
            return 0
        return await capture_metars_for_active_baskets(self.repository, self.aviation_weather)

    async def poll_trades_once(self, wallet: str | None = None) -> int:
        target_wallet = wallet or self.config.weather_target_wallet or self.config.target_wallet
        trader_id = self.repository.upsert_trader(target_wallet, self.config.trader_label)
        rows = await self.data.get_trades(target_wallet, limit=self.config.trade_page_limit, taker_only=False)
        inserted = 0
        for raw in sorted(rows, key=lambda item: item.get("timestamp", 0)):
            if not is_weather_trade(raw):
                continue
            bucket = upsert_weather_trade_market(self.repository, raw)
            if bucket:
                await self._refresh_trade_event_once(bucket.event_slug)
                await self.recorder.subscribe_more([bucket.token_id])
            trade_id, was_inserted = self.repository.insert_trade(trader_id, raw, detected_at=utc_now_iso())
            if not was_inserted:
                continue
            trade = self.repository.get_trade(trade_id)
            context = attach_execution_context(
                self.repository,
                trade,
                window_ms=self.config.weather_execution_match_window_ms,
                recorder=self.recorder,
                book_window_ms=int(self.config.weather_websocket_buffer_seconds * 1000),
            )
            trade_dict = dict(trade)
            if self.config.weather_basket_snapshots_enabled:
                snapshot = entry_basket_snapshot_from_cache(
                    self.repository,
                    self.recorder,
                    trade_dict,
                    context,
                    book_window_ms=int(self.config.weather_websocket_buffer_seconds * 1000),
                )
                if snapshot and snapshot.get("missing_token_count"):
                    try:
                        bucket_row = self.repository.get_weather_bucket_by_token(str(trade_dict.get("token_id") or ""))
                        if bucket_row:
                            state = await current_basket_state_from_clob(self.repository, self.clob, int(bucket_row["basket_id"]))
                            fallback = entry_basket_snapshot_from_clob_state(self.repository, trade_dict, context, state)
                            if fallback and (fallback.get("matched_token_count") or 0) >= (snapshot.get("matched_token_count") or 0):
                                snapshot = fallback
                    except Exception as exc:
                        self.repository.log("WARN", "weather_snapshots", "entry basket REST fallback failed", {"trade_id": trade_id, "error": str(exc)})
                if snapshot:
                    self.repository.insert_weather_basket_snapshot(snapshot)
            if self.config.weather_followups_enabled:
                bucket_row = self.repository.get_weather_bucket_by_token(str(trade_dict.get("token_id") or ""))
                if bucket_row:
                    schedule_weather_followups_for_trade(
                        self.repository,
                        trade_dict,
                        int(bucket_row["basket_id"]),
                        base_timestamp=context.get("execution_timestamp") or trade_dict.get("trade_timestamp"),
                        intervals=self.config.weather_followup_intervals,
                    )
            inserted += 1
        if inserted:
            recompute_weather_positions(self.repository, trader_id)
            self.repository.log("INFO", "weather_tracker", "stored weather trades", {"inserted": inserted})
        return inserted

    async def _refresh_trade_event_once(self, event_slug: str | None) -> None:
        if not event_slug or event_slug in self._event_refresh_cache:
            return
        self._event_refresh_cache.add(event_slug)
        try:
            event = await self.gamma.get_event_by_slug(event_slug)
            if not event:
                return
            buckets = upsert_weather_event_markets(self.repository, event)
            if buckets:
                await self.recorder.subscribe_more([bucket.token_id for bucket in buckets])
                self.repository.log("INFO", "weather_discovery", "refreshed trade event buckets", {"event_slug": event_slug, "buckets": len(buckets)})
        except Exception as exc:
            self.repository.log("WARN", "weather_discovery", "trade event refresh failed", {"event_slug": event_slug, "error": str(exc)})

    async def run_watch(self, wallet: str | None = None) -> None:
        await self.discover_once()
        if self.config.weather_forecasts_enabled:
            await self.capture_forecasts_once()
        if self.config.weather_observations_enabled:
            await self.capture_observations_once()
        if self.config.weather_metar_enabled:
            await self.capture_metars_once()
        initial_tokens = self.repository.list_weather_token_ids()
        LOG.info("starting weather websocket with %s token(s)", len(initial_tokens))
        stream_task = asyncio.create_task(self.recorder.run_forever(initial_tokens))
        discover_task = asyncio.create_task(self._discovery_loop())
        poll_task = asyncio.create_task(self._poll_loop(wallet))
        forecast_task = asyncio.create_task(self._forecast_loop()) if self.config.weather_forecasts_enabled else None
        observation_task = asyncio.create_task(self._observation_loop()) if self.config.weather_observations_enabled else None
        metar_task = asyncio.create_task(self._metar_loop()) if self.config.weather_metar_enabled else None
        followup_task = asyncio.create_task(self._followup_loop()) if self.config.weather_followups_enabled else None
        try:
            tasks = [stream_task, discover_task, poll_task]
            if forecast_task:
                tasks.append(forecast_task)
            if observation_task:
                tasks.append(observation_task)
            if metar_task:
                tasks.append(metar_task)
            if followup_task:
                tasks.append(followup_task)
            await asyncio.gather(*tasks)
        finally:
            for task in (stream_task, discover_task, poll_task, forecast_task, observation_task, metar_task, followup_task):
                if task:
                    task.cancel()

    async def _discovery_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.weather_discovery_refresh_seconds)
            try:
                await self.discover_once()
            except Exception as exc:
                LOG.warning("weather discovery failed: %s", exc)
                self.repository.log("WARN", "weather_discovery", "weather discovery failed", {"error": str(exc)})

    async def _forecast_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.weather_forecast_refresh_seconds)
            try:
                count = await self.capture_forecasts_once()
                LOG.info("captured %s weather forecast snapshot(s)", count)
            except Exception as exc:
                LOG.warning("weather forecast capture failed: %s", exc)
                self.repository.log("WARN", "weather_forecast", "weather forecast capture failed", {"error": str(exc)})

    async def _observation_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.weather_observation_refresh_seconds)
            try:
                count = await self.capture_observations_once()
                LOG.info("captured %s weather observation snapshot(s)", count)
            except Exception as exc:
                LOG.warning("weather observation capture failed: %s", exc)
                self.repository.log("WARN", "weather_observations", "weather observation capture failed", {"error": str(exc)})

    async def _metar_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.weather_metar_refresh_seconds)
            try:
                count = await self.capture_metars_once()
                if count:
                    LOG.info("captured %s METAR report(s)", count)
            except Exception as exc:
                LOG.warning("weather METAR capture failed: %s", exc)
                self.repository.log("WARN", "weather_metar", "weather METAR capture failed", {"error": str(exc)})

    async def _followup_loop(self) -> None:
        while True:
            await asyncio.sleep(self.config.weather_followup_poll_seconds)
            try:
                count = await process_due_weather_followups(self.repository, self.clob)
                if count:
                    LOG.info("captured %s weather followup snapshot(s)", count)
            except Exception as exc:
                LOG.warning("weather followup capture failed: %s", exc)
                self.repository.log("WARN", "weather_followups", "weather followup capture failed", {"error": str(exc)})

    async def _poll_loop(self, wallet: str | None) -> None:
        while True:
            try:
                count = await self.poll_trades_once(wallet)
                if count:
                    LOG.info("stored %s new weather trade(s)", count)
            except Exception as exc:
                LOG.warning("weather poll failed: %s", exc)
                self.repository.log("WARN", "weather_tracker", "weather poll failed", {"error": str(exc)})
            await asyncio.sleep(self.config.weather_trade_poll_seconds)
