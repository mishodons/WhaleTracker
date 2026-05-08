# Polymarket Whale Tracker

Lightweight, local-first research tooling for observing one Polymarket trader or wallet. It records public trade activity, captures nearest available order book snapshots, schedules post-trade follow-ups, reconstructs approximate positions, and generates strategy reports.

This project is intentionally observation-only. It does not use private keys, does not authenticate to trading APIs, does not sign messages, and cannot place or cancel orders.

## What It Does

- Polls `data-api.polymarket.com/trades` for a target wallet.
- Deduplicates trades and stores them in SQLite.
- Enriches market metadata from Gamma when available.
- Captures CLOB order book snapshots from `clob.polymarket.com/book`.
- Computes spread, midpoint, depth, imbalance, liquidity bands, and slippage estimates.
- Schedules follow-up snapshots at configurable intervals.
- Reconstructs approximate positions from observed trades.
- Produces CLI queries and strategy reports for reverse engineering behavior.

## What It Does Not Do

- No copy trading.
- No private key handling.
- No exchange credentials.
- No order placement.
- No authenticated user WebSocket.
- No guarantee of exact historical order books.

## Install

```bash
cd polymarket_whale_tracker
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

On macOS/Linux, activate with:

```bash
source .venv/bin/activate
```

## Configure

Copy the example environment file and edit the public wallet address:

```bash
copy .env.example .env
```

Set:

```text
TARGET_WALLET=0x...
TRADER_LABEL=target-whale
DATABASE_PATH=data/whale_tracker.sqlite
POLL_INTERVAL_SECONDS=15
```

You can also edit `config.yaml` for polling modes, follow-up intervals, API URLs, and slippage sizes.

## Quick Start

Initialize/check local setup:

```bash
python main.py doctor
```

Backfill historical public trades:

```bash
python main.py backfill --wallet 0xABC...
```

Run one polling cycle:

```bash
python main.py track --wallet 0xABC... --once
```

Run continuous tracking:

```bash
python main.py track --wallet 0xABC... --interval 15
```

Capture due follow-up snapshots:

```bash
python main.py followups
```

## Query Examples

```bash
python main.py recent --limit 20
python main.py positions
python main.py market --slug example-market-slug
python main.py category --category politics
python main.py whale-size --amount 10000
python main.py wide-spread --spread 0.05
python main.py favorable --interval 1h
python main.py scaled-in
python main.py exits
python main.py profitable --limit 20
python main.py unresolved
python main.py summary
python main.py report --days 30
python main.py analyze-strategy
```

## Live Weather Basket Tracking

For the target weather-arb wallet, use the dedicated weather watcher:

```bash
python main.py weather-watch --wallet 0xb06a0eae498750ed0acac7e1f759f741c56e52f5
```

This runs three observation-only loops together:

- discovers active weather temperature buckets;
- subscribes to their public CLOB market WebSocket orderbooks;
- keeps a rolling in-memory orderbook buffer;
- polls the target wallet and records only weather trades plus the matched book rows around those trades.

Useful weather commands:

```bash
python main.py weather-discover
python main.py weather-recent --limit 20
python main.py weather-baskets
python main.py weather-basket --event-slug highest-temperature-in-buenos-aires-on-may-3-2026
python main.py weather-positions --refresh
python main.py weather-executions
python main.py weather-basket-snapshots
python main.py weather-followups
python main.py weather-observations
python main.py weather-observation-capture
python main.py weather-settlement-capture --event-slug highest-temperature-in-london-on-may-4-2026
python main.py weather-settlement-set --event-slug highest-temperature-in-london-on-may-4-2026 --final-temp 15
python main.py weather-day-report --event-slug highest-temperature-in-london-on-may-4-2026
python main.py weather-report
python main.py prune-db
python main.py prune-db --execute --vacuum
```

Execution context labels:

- `exact_ws_tx_match`: target trade matched a live WebSocket trade event by transaction hash.
- `probable_ws_match`: matched by token, price, size, side, and timestamp proximity.
- `data_api_only`: detected by REST but no live WebSocket trade event was available.
- `pre_trade_book_cached`: a book state was available before execution.
- `missed_pre_trade_book`: the token was discovered too late or the WebSocket was not running.

## Database

The default SQLite path is `data/whale_tracker.sqlite`.

Core tables:

- `traders`: target wallet and label.
- `trades`: deduplicated trade records with raw Data API JSON.
- `markets`: Gamma/Data market metadata.
- `orderbook_snapshots`: entry/backfill snapshots and computed microstructure metrics.
- `followup_snapshots`: scheduled and captured post-trade market states.
- `positions`: reconstructed per-token positions.
- `strategy_metrics`: derived trade-level strategy features.
- `system_logs`: persistent operational events.
- `weather_baskets`: weather city/date baskets.
- `weather_bucket_markets`: temperature bucket token IDs.
- `ws_orderbook_events`: matched public market WebSocket rows used by execution context. Set `weather.websocket_persist_raw_events: true` only for debugging; it grows quickly.
- `trade_execution_context`: target trade matched to WebSocket execution/book context.
- `weather_basket_snapshots`: all-bucket orderbook state around a target trade.
- `weather_followup_snapshots`: compact post-trade basket state and favorable-move checks.
- `weather_observations`: provisional current temperature/high observations from weather APIs.
- `weather_station_mappings`: city to likely/official ICAO station mapping for METAR timing analysis.
- `weather_metar_reports`: public METAR/SPECI reports with local `first_seen_at` timestamps.
- `weather_settlements`: final temp / winning bucket / confidence/source.
- `weather_bucket_final_pnl`: final per-bucket shares, cost, payout, and PnL.
- `weather_positions`: per-bucket weather positions.
- `weather_basket_pnl`: basket-level cost, payout, and edge estimates.

## Data Quality

The system stores explicit confidence labels:

- Trade timestamps come from Polymarket Data API.
- `detected_at` is local polling time.
- Live weather entry books are selected from the public WebSocket rolling buffer when available.
- If the buffer misses the execution window, the trade is labeled `data_api_only` or `missed_pre_trade_book`.
- METAR release timing is only exact from the moment this tracker first sees the public METAR; old trades cannot be reconstructed to public first-seen precision.
- ICAO station mappings are labeled likely until verified against the exact Polymarket settlement source.
- Backfilled historical trades cannot recover exact historical order books from public CLOB REST.
- PnL and positions are approximate when local history is incomplete.
- Maker/taker or passive/aggressive inference is low/medium confidence unless price aligns clearly with top of book.

## API Endpoints Used

- Data API: `GET https://data-api.polymarket.com/trades?user={address}`
- Data API: `GET https://data-api.polymarket.com/activity?user={address}`
- Data API: `GET https://data-api.polymarket.com/positions?user={address}`
- Data API: `GET https://data-api.polymarket.com/closed-positions?user={address}`
- Gamma API: `GET https://gamma-api.polymarket.com/markets?slug={slug}`
- Gamma API: `GET https://gamma-api.polymarket.com/events/slug/{slug}`
- CLOB API: `GET https://clob.polymarket.com/book?token_id={token_id}`
- CLOB API: `GET https://clob.polymarket.com/prices-history`
- AviationWeather API: `GET https://aviationweather.gov/api/data/metar?ids={stations}&format=json`

The optional public market WebSocket is represented in config but disabled by default. It is for orderbook/market data only.

## Interpreting Strategy Reports

The report aggregates observed behavior:

- Trade count and notional.
- Buy/sell flow.
- Category concentration.
- Price bucket preferences.
- Wide-spread entries.
- Scale-in candidates.
- Suspected exits.
- Approximate open exposure.
- A conservative strategy archetype hypothesis.

Treat the archetype as a research prompt, not a conclusion. Good reverse engineering comes from collecting enough examples and checking them against market context.

Weather strategy research commands:

```bash
python main.py strategy timing
python main.py strategy buckets
python main.py strategy orders
python main.py strategy pnl
python main.py strategy metar
python main.py strategy full-report
python main.py weather-metar-capture
python main.py weather-metars --limit 20
```

`weather-watch` captures METAR reports on the configured interval when `weather.metar.enabled` is true.

## Development

Run tests:

```bash
python -m pytest tests -q
```

The unit tests do not require live Polymarket network access.

## Known Limitations

- Public REST polling can miss sub-second sequencing.
- Data API trade rows may not expose every CLOB trade identifier.
- Historical order books are not available from the public CLOB REST API.
- Position reconstruction is only as complete as the ingested trade history.
- PnL estimates do not include all settlement/redeem edge cases.
- Category metadata depends on Gamma availability and market schema drift.
- Current implementation stores raw JSON for auditability but does not provide a GUI.

## Suggested Next Improvements

- Add configurable export of matched execution-context books to CSV/Parquet.
- Add Polygon receipt parsing for stronger maker/taker classification.
- Add CSV/Parquet export commands.
- Add a small read-only dashboard.
- Add richer market metadata caching by event.
- Add alerting for large new trades.
- Add more robust resolved-market PnL handling.
