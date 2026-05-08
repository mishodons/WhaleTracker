from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
import yaml


@dataclass(frozen=True)
class AppConfig:
    root_dir: Path
    database_path: Path
    target_wallet: str
    trader_label: str
    data_base_url: str
    gamma_base_url: str
    clob_base_url: str
    timeout_seconds: float
    max_retries: int
    retry_backoff_seconds: float
    poll_interval_seconds: float
    trade_page_limit: int
    backfill_page_limit: int
    max_backfill_pages: int
    capture_current_books_on_backfill: bool
    followup_intervals: list[str]
    slippage_notional_sizes: list[float]
    liquidity_bands_pct: list[float]
    log_level: str
    log_file: Path
    websocket_enabled: bool
    market_websocket_url: str
    weather_target_wallet: str
    weather_discovery_refresh_seconds: float
    weather_trade_poll_seconds: float
    weather_forecast_refresh_seconds: float
    weather_market_limit: int
    weather_max_market_pages: int
    weather_websocket_url: str
    weather_websocket_subscription_batch_size: int
    weather_websocket_persist_raw_events: bool
    weather_websocket_buffer_seconds: float
    weather_websocket_buffer_rows_per_token: int
    weather_execution_match_window_ms: int
    weather_forecasts_enabled: bool
    weather_basket_snapshots_enabled: bool
    weather_followups_enabled: bool
    weather_followup_intervals: list[str]
    weather_followup_poll_seconds: float
    weather_observations_enabled: bool
    weather_observation_refresh_seconds: float
    weather_metar_enabled: bool
    weather_metar_refresh_seconds: float
    aviation_weather_base_url: str
    open_meteo_forecast_base_url: str
    open_meteo_geocoding_base_url: str


def _deep_get(data: dict[str, Any], path: str, default: Any = None) -> Any:
    node: Any = data
    for part in path.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node


def _env(name: str, default: Any = None) -> Any:
    value = os.getenv(name)
    return default if value in (None, "") else value


def _resolve(root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else root / path


def load_config(config_path: str | Path = "config.yaml") -> AppConfig:
    load_dotenv()
    root = Path(config_path).resolve().parent
    with open(config_path, "r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    mode = str(_deep_get(raw, "tracking.default_mode", "normal"))
    intervals = _deep_get(raw, "tracking.poll_intervals", {}) or {}
    configured_interval = intervals.get(mode, 15)

    database_path = _resolve(root, _env("DATABASE_PATH", _deep_get(raw, "database.path", "data/whale_tracker.sqlite")))
    log_file = _resolve(root, _env("LOG_FILE", _deep_get(raw, "logging.file", "data/whale_tracker.log")))

    return AppConfig(
        root_dir=root,
        database_path=database_path,
        target_wallet=str(_env("TARGET_WALLET", _deep_get(raw, "trader.wallet", ""))).strip(),
        trader_label=str(_env("TRADER_LABEL", _deep_get(raw, "trader.label", "target-whale"))),
        data_base_url=str(_deep_get(raw, "api.data_base_url", "https://data-api.polymarket.com")).rstrip("/"),
        gamma_base_url=str(_deep_get(raw, "api.gamma_base_url", "https://gamma-api.polymarket.com")).rstrip("/"),
        clob_base_url=str(_deep_get(raw, "api.clob_base_url", "https://clob.polymarket.com")).rstrip("/"),
        timeout_seconds=float(_deep_get(raw, "api.timeout_seconds", 20)),
        max_retries=int(_deep_get(raw, "api.max_retries", 3)),
        retry_backoff_seconds=float(_deep_get(raw, "api.retry_backoff_seconds", 0.75)),
        poll_interval_seconds=float(_env("POLL_INTERVAL_SECONDS", configured_interval)),
        trade_page_limit=int(_deep_get(raw, "tracking.trade_page_limit", 100)),
        backfill_page_limit=int(_deep_get(raw, "tracking.backfill_page_limit", 500)),
        max_backfill_pages=int(_deep_get(raw, "tracking.max_backfill_pages", 20)),
        capture_current_books_on_backfill=bool(_deep_get(raw, "tracking.capture_current_books_on_backfill", True)),
        followup_intervals=[str(v) for v in _deep_get(raw, "snapshots.followup_intervals", ["0s", "1m", "5m", "15m", "1h", "4h", "24h"])],
        slippage_notional_sizes=[float(v) for v in _deep_get(raw, "snapshots.slippage_notional_sizes", [100, 500, 1000])],
        liquidity_bands_pct=[float(v) for v in _deep_get(raw, "snapshots.liquidity_bands_pct", [1, 2, 5, 10])],
        log_level=str(_env("LOG_LEVEL", _deep_get(raw, "logging.level", "INFO"))).upper(),
        log_file=log_file,
        websocket_enabled=bool(_deep_get(raw, "tracking.optional_market_websocket.enabled", False)),
        market_websocket_url=str(_deep_get(raw, "tracking.optional_market_websocket.url", "wss://ws-subscriptions-clob.polymarket.com/ws/market")),
        weather_target_wallet=str(_deep_get(raw, "weather.target_wallet", "")).strip(),
        weather_discovery_refresh_seconds=float(_deep_get(raw, "weather.discovery_refresh_seconds", 180)),
        weather_trade_poll_seconds=float(_deep_get(raw, "weather.trade_poll_seconds", 2)),
        weather_forecast_refresh_seconds=float(_deep_get(raw, "weather.forecast_refresh_seconds", 900)),
        weather_market_limit=int(_deep_get(raw, "weather.market_limit", 500)),
        weather_max_market_pages=int(_deep_get(raw, "weather.max_market_pages", 8)),
        weather_websocket_url=str(_deep_get(raw, "weather.websocket_url", "wss://ws-subscriptions-clob.polymarket.com/ws/market")),
        weather_websocket_subscription_batch_size=int(_deep_get(raw, "weather.websocket_subscription_batch_size", 200)),
        weather_websocket_persist_raw_events=bool(_deep_get(raw, "weather.websocket_persist_raw_events", False)),
        weather_websocket_buffer_seconds=float(_deep_get(raw, "weather.websocket_buffer_seconds", 300)),
        weather_websocket_buffer_rows_per_token=int(_deep_get(raw, "weather.websocket_buffer_rows_per_token", 1000)),
        weather_execution_match_window_ms=int(_deep_get(raw, "weather.execution_match_window_ms", 5000)),
        weather_forecasts_enabled=bool(_deep_get(raw, "weather.forecasts.enabled", True)),
        weather_basket_snapshots_enabled=bool(_deep_get(raw, "weather.basket_snapshots.enabled", True)),
        weather_followups_enabled=bool(_deep_get(raw, "weather.followups.enabled", True)),
        weather_followup_intervals=[str(v) for v in _deep_get(raw, "weather.followups.intervals", ["30s", "1m", "5m", "15m", "1h"])],
        weather_followup_poll_seconds=float(_deep_get(raw, "weather.followups.poll_seconds", 15)),
        weather_observations_enabled=bool(_deep_get(raw, "weather.observations.enabled", True)),
        weather_observation_refresh_seconds=float(_deep_get(raw, "weather.observations.refresh_seconds", 900)),
        weather_metar_enabled=bool(_deep_get(raw, "weather.metar.enabled", True)),
        weather_metar_refresh_seconds=float(_deep_get(raw, "weather.metar.refresh_seconds", 60)),
        aviation_weather_base_url=str(_deep_get(raw, "weather.metar.aviation_weather_base_url", "https://aviationweather.gov/api/data")).rstrip("/"),
        open_meteo_forecast_base_url=str(_deep_get(raw, "weather.forecasts.forecast_base_url", "https://api.open-meteo.com")).rstrip("/"),
        open_meteo_geocoding_base_url=str(_deep_get(raw, "weather.forecasts.geocoding_base_url", "https://geocoding-api.open-meteo.com")).rstrip("/"),
    )
