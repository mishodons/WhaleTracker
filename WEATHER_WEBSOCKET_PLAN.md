# Weather WebSocket Tracking Plan

Goal: upgrade the tracker from general wallet observation to live, weather-only basket forensics for target wallet `0xb06a0eae498750ed0acac7e1f759f741c56e52f5`.

The important shift is to subscribe before the trader acts. REST polling can detect the wallet trade, but exact orderbook context requires a live WebSocket orderbook timeline already running for the relevant weather bucket token IDs.

## 1. Weather-Only Market Discovery

Add `src/weather/discovery.py`.

Responsibilities:

- Poll Gamma/Data API for active weather markets.
- Identify weather baskets by `eventSlug`, for example `highest-temperature-in-buenos-aires-on-may-3-2026`.
- Parse and store:
  - city
  - forecast date
  - unit: `C` or `F`
  - bucket label, such as `17C`, `22C or higher`, `80-81F`
  - lower and upper temperature bounds
  - token ID
  - condition ID
  - market slug
- Refresh every few minutes so new weather buckets are discovered and subscribed before the target trader enters.

## 2. Weather Database Tables

Extend `src/storage/schema.sql` with:

- `weather_baskets`: one city/date event.
- `weather_bucket_markets`: one temperature bucket leg.
- `weather_positions`: target trader position per bucket.
- `weather_basket_pnl`: cumulative cost, max payout, worst-case payout, best-case payout, guaranteed edge.
- `ws_orderbook_events`: raw and normalized WebSocket book/price/trade events.
- `trade_execution_context`: matched target trade plus nearest pre/post book context.

All tables should keep raw JSON where relevant for auditability.

## 3. WebSocket Orderbook Recorder

Add `src/ws/market_stream.py`.

Connect to:

```text
wss://ws-subscriptions-clob.polymarket.com/ws/market
```

Subscribe with token IDs:

```json
{
  "assets_ids": ["<token_id_1>", "<token_id_2>"],
  "type": "market",
  "custom_feature_enabled": true
}
```

The public market channel emits:

- `book`
- `price_change`
- `last_trade_price`
- `best_bid_ask`
- `new_market`
- `market_resolved`

Recorder behavior:

- Maintain an in-memory orderbook state per token.
- Persist every relevant WebSocket event with:
  - exchange timestamp
  - local receive timestamp
  - token ID
  - market/condition ID
  - event type
  - hash
  - best bid
  - best ask
  - spread
  - midpoint
  - full reconstructed book where available
  - raw JSON
- Reconnect with backoff.
- Resubscribe after reconnect.
- Periodically checkpoint current book states.

## 4. Match Whale Trades To WebSocket Executions

Keep REST polling for target wallet trades, but filter weather-only.

When a new target trade arrives, match it to WebSocket `last_trade_price` events by:

- transaction hash
- token ID
- side
- price
- size
- timestamp proximity

Store match confidence:

- `exact_ws_tx_match`
- `probable_ws_match`
- `data_api_only`

The WebSocket `last_trade_price` message includes millisecond timestamps and transaction hash in the current Polymarket docs. When available, this should be the preferred execution timestamp for live observations.

## 5. Attach Pre/Post Execution Book

For each matched target trade, attach:

- latest cached orderbook before execution
- first cached orderbook after execution
- latest `best_bid_ask` before execution
- first `best_bid_ask` after execution
- time delta between execution timestamp and each book timestamp
- spread
- midpoint
- depth near touch
- imbalance
- liquidity within 1%, 2%, 5%, and 10%
- estimated slippage
- crossed-spread/passive inference where possible

This becomes the best available answer to: what did the orderbook look like at execution?

Important caveat: if the token was discovered after the trade, the system must mark `missed_pre_trade_book`.

## 6. Weather Basket Position Engine

Add `src/weather/positions.py`.

Group all weather trades by `eventSlug`.

For each city/date basket, compute per bucket:

- bought size
- sold size
- net size
- average entry
- cost basis
- current mark
- realized PnL estimate
- unrealized PnL estimate

Compute basket-level metrics:

- total cumulative cost
- payout if each bucket wins
- worst-case PnL
- best-case PnL
- guaranteed arb edge if bucket set is complete
- ROI
- whether the trader has full-basket coverage, partial basket exposure, or a directional leg

## 7. Weather Arb Report

Add `src/weather/report.py`.

Report on the target trader only:

- times of day they enter weather trades
- city/date baskets traded
- bucket sequence and timing
- time from first bucket to last bucket in each basket
- execution timestamp source and confidence
- per-leg entry price and size
- per-leg orderbook context
- cumulative basket cost
- projected profit per possible winning bucket
- worst-case and best-case PnL
- whether they buy complete baskets, partial baskets, or directional legs
- whether they wait for liquidity, cross immediately, or trade into stale/wide books

## 8. Weather CLI Commands

Add commands:

```bash
python main.py weather-watch --wallet 0xb06a0eae498750ed0acac7e1f759f741c56e52f5
python main.py weather-discover
python main.py weather-recent
python main.py weather-baskets
python main.py weather-basket --event-slug highest-temperature-in-buenos-aires-on-may-3-2026
python main.py weather-positions
python main.py weather-executions
python main.py weather-report
```

`weather-watch` should run three async tasks together:

- weather market discovery
- WebSocket orderbook recorder
- target wallet weather trade poller

## 9. Tests

Add tests for:

- weather slug/event parsing
- bucket range parsing
- Celsius/Fahrenheit bucket parsing
- basket grouping by event slug
- basket PnL math
- WebSocket `book` event normalization
- WebSocket `price_change` orderbook updates
- WebSocket `last_trade_price` trade-event matching
- pre/post book context selection
- weather-only trade filtering

Add optional live smoke tests gated behind environment variables so regular tests remain offline.

## 10. Data Accuracy Labels

Use explicit labels in reports and database rows:

- `exact_ws_tx_match`: target trade matched to live WebSocket transaction hash.
- `probable_ws_match`: matched by token, price, size, side, and close timestamp.
- `data_api_only`: no matching live WebSocket trade event.
- `pre_trade_book_cached`: book state existed before execution.
- `post_trade_book_cached`: post-trade book update was captured.
- `data_api_second_precision`: only REST timestamp available.
- `ws_millisecond_precision`: WebSocket timestamp used.
- `missed_pre_trade_book`: token discovered too late or WebSocket disconnected.
- `basket_pnl_estimated`: unresolved or partial basket.
- `basket_complete`: bucket set appears complete for that city/date.
- `basket_partial`: trader has incomplete bucket coverage.

## 11. Implementation Order

1. Add schema tables and repository methods.
2. Add weather parsing and discovery.
3. Add WebSocket market recorder and in-memory book cache.
4. Add weather-only target trade poller.
5. Add execution matching and pre/post book attachment.
6. Add weather position and basket PnL engine.
7. Add weather report.
8. Add CLI commands.
9. Add tests and live smoke mode.

## Public API References

- Polymarket Market WebSocket: https://docs.polymarket.com/market-data/websocket/market-channel
- Polymarket CLOB orderbook REST: https://docs.polymarket.com/api-reference/market-data/get-order-book

