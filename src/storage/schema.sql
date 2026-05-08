PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS traders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  wallet_address TEXT NOT NULL UNIQUE,
  label TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  market_id TEXT,
  condition_id TEXT UNIQUE,
  slug TEXT,
  title TEXT,
  category TEXT,
  event_title TEXT,
  event_slug TEXT,
  resolution_status TEXT,
  created_at TEXT,
  end_date TEXT,
  raw_json TEXT
);

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trader_id INTEGER NOT NULL REFERENCES traders(id),
  dedupe_key TEXT NOT NULL UNIQUE,
  external_trade_id TEXT,
  tx_hash TEXT,
  market_id TEXT,
  condition_id TEXT,
  token_id TEXT,
  market_slug TEXT,
  market_title TEXT,
  event_slug TEXT,
  outcome TEXT,
  side TEXT,
  price REAL,
  size REAL,
  notional REAL,
  trade_timestamp TEXT,
  detected_at TEXT NOT NULL,
  data_confidence TEXT,
  raw_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_trader_ts ON trades(trader_id, trade_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_trades_market_slug ON trades(market_slug);
CREATE INDEX IF NOT EXISTS idx_trades_condition_token ON trades(condition_id, token_id);
CREATE INDEX IF NOT EXISTS idx_trades_token ON trades(token_id);
CREATE INDEX IF NOT EXISTS idx_trades_notional ON trades(notional);

CREATE TABLE IF NOT EXISTS orderbook_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER REFERENCES trades(id),
  market_id TEXT,
  token_id TEXT NOT NULL,
  snapshot_type TEXT NOT NULL,
  snapshot_timestamp TEXT NOT NULL,
  best_bid REAL,
  best_ask REAL,
  spread REAL,
  midpoint REAL,
  bid_depth_total REAL,
  ask_depth_total REAL,
  bid_depth_near_touch REAL,
  ask_depth_near_touch REAL,
  imbalance REAL,
  liquidity_1pct REAL,
  liquidity_2pct REAL,
  liquidity_5pct REAL,
  liquidity_10pct REAL,
  book_hash TEXT,
  latency_ms REAL,
  snapshot_source TEXT NOT NULL,
  depth_json TEXT,
  slippage_json TEXT,
  quality_flags TEXT,
  raw_orderbook_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orderbook_trade ON orderbook_snapshots(trade_id);
CREATE INDEX IF NOT EXISTS idx_orderbook_token_ts ON orderbook_snapshots(token_id, snapshot_timestamp DESC);

CREATE TABLE IF NOT EXISTS followup_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER NOT NULL REFERENCES trades(id),
  interval_label TEXT NOT NULL,
  scheduled_for TEXT NOT NULL,
  captured_at TEXT,
  best_bid REAL,
  best_ask REAL,
  midpoint REAL,
  spread REAL,
  price_change_from_trade REAL,
  favorable_move_boolean INTEGER,
  raw_json TEXT,
  UNIQUE(trade_id, interval_label)
);

CREATE INDEX IF NOT EXISTS idx_followups_due ON followup_snapshots(captured_at, scheduled_for);

CREATE TABLE IF NOT EXISTS positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trader_id INTEGER NOT NULL REFERENCES traders(id),
  market_id TEXT,
  condition_id TEXT,
  token_id TEXT NOT NULL,
  outcome TEXT,
  net_size REAL NOT NULL,
  avg_entry_price REAL,
  estimated_current_price REAL,
  estimated_unrealized_pnl REAL,
  estimated_realized_pnl REAL,
  total_bought REAL,
  total_sold REAL,
  direction TEXT,
  confidence TEXT,
  last_updated TEXT NOT NULL,
  UNIQUE(trader_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_positions_trader ON positions(trader_id, net_size);

CREATE TABLE IF NOT EXISTS strategy_metrics (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER NOT NULL UNIQUE REFERENCES trades(id),
  spread_at_entry REAL,
  liquidity_score REAL,
  trade_size_vs_depth REAL,
  price_bucket TEXT,
  market_age_at_trade TEXT,
  time_to_resolution TEXT,
  category TEXT,
  entry_type_hypothesis TEXT,
  confidence_score REAL,
  notes TEXT,
  raw_json TEXT
);

CREATE TABLE IF NOT EXISTS system_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp TEXT NOT NULL,
  level TEXT NOT NULL,
  component TEXT NOT NULL,
  message TEXT NOT NULL,
  raw_json TEXT
);

CREATE TABLE IF NOT EXISTS weather_baskets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_slug TEXT NOT NULL UNIQUE,
  city TEXT,
  forecast_date TEXT,
  unit TEXT,
  event_title TEXT,
  status TEXT,
  discovered_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_weather_baskets_date ON weather_baskets(forecast_date, city);

CREATE TABLE IF NOT EXISTS weather_bucket_markets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  condition_id TEXT,
  token_id TEXT NOT NULL UNIQUE,
  market_slug TEXT,
  market_title TEXT,
  outcome TEXT,
  bucket_label TEXT,
  lower_temp REAL,
  upper_temp REAL,
  bound_type TEXT,
  active INTEGER,
  closed INTEGER,
  discovered_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_weather_bucket_basket ON weather_bucket_markets(basket_id);
CREATE INDEX IF NOT EXISTS idx_weather_bucket_slug ON weather_bucket_markets(market_slug);

CREATE TABLE IF NOT EXISTS ws_orderbook_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_type TEXT NOT NULL,
  token_id TEXT,
  market_id TEXT,
  event_exchange_timestamp_ms INTEGER,
  event_exchange_timestamp TEXT,
  local_received_at TEXT NOT NULL,
  message_hash TEXT,
  transaction_hash TEXT,
  side TEXT,
  price REAL,
  size REAL,
  best_bid REAL,
  best_ask REAL,
  spread REAL,
  midpoint REAL,
  full_book_json TEXT,
  raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ws_events_token_ts ON ws_orderbook_events(token_id, event_exchange_timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_ws_events_tx ON ws_orderbook_events(transaction_hash);
CREATE INDEX IF NOT EXISTS idx_ws_events_type ON ws_orderbook_events(event_type);

CREATE TABLE IF NOT EXISTS trade_execution_context (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER NOT NULL UNIQUE REFERENCES trades(id),
  token_id TEXT NOT NULL,
  execution_timestamp_ms INTEGER,
  execution_timestamp TEXT,
  execution_timestamp_source TEXT,
  ws_trade_event_id INTEGER REFERENCES ws_orderbook_events(id),
  pre_book_event_id INTEGER REFERENCES ws_orderbook_events(id),
  post_book_event_id INTEGER REFERENCES ws_orderbook_events(id),
  pre_book_delta_ms INTEGER,
  post_book_delta_ms INTEGER,
  match_confidence TEXT NOT NULL,
  quality_flags TEXT,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_exec_context_token ON trade_execution_context(token_id, execution_timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_exec_context_ws_trade ON trade_execution_context(ws_trade_event_id);
CREATE INDEX IF NOT EXISTS idx_exec_context_pre_book ON trade_execution_context(pre_book_event_id);
CREATE INDEX IF NOT EXISTS idx_exec_context_post_book ON trade_execution_context(post_book_event_id);

CREATE TABLE IF NOT EXISTS weather_positions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trader_id INTEGER NOT NULL REFERENCES traders(id),
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  bucket_market_id INTEGER NOT NULL REFERENCES weather_bucket_markets(id),
  token_id TEXT NOT NULL,
  outcome TEXT,
  net_size REAL NOT NULL,
  avg_entry_price REAL,
  cost_basis REAL,
  realized_pnl REAL,
  current_midpoint REAL,
  mark_value REAL,
  unrealized_pnl REAL,
  last_trade_at TEXT,
  confidence TEXT,
  last_updated TEXT NOT NULL,
  UNIQUE(trader_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_weather_positions_basket ON weather_positions(trader_id, basket_id);

CREATE TABLE IF NOT EXISTS weather_basket_pnl (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trader_id INTEGER NOT NULL REFERENCES traders(id),
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  total_cost REAL NOT NULL,
  total_net_size REAL NOT NULL,
  min_payout REAL,
  max_payout REAL,
  worst_case_pnl REAL,
  best_case_pnl REAL,
  guaranteed_edge REAL,
  roi_worst_case REAL,
  coverage_type TEXT,
  computed_at TEXT NOT NULL,
  raw_json TEXT,
  UNIQUE(trader_id, basket_id)
);

CREATE INDEX IF NOT EXISTS idx_weather_pnl_trader ON weather_basket_pnl(trader_id, computed_at DESC);

CREATE TABLE IF NOT EXISTS weather_city_geocodes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  city TEXT NOT NULL UNIQUE,
  provider TEXT NOT NULL,
  provider_location_id TEXT,
  matched_name TEXT,
  country_code TEXT,
  country TEXT,
  admin1 TEXT,
  latitude REAL NOT NULL,
  longitude REAL NOT NULL,
  timezone TEXT,
  population INTEGER,
  confidence TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  raw_json TEXT
);

CREATE TABLE IF NOT EXISTS weather_forecast_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  source TEXT NOT NULL,
  city TEXT,
  forecast_date TEXT NOT NULL,
  unit TEXT,
  latitude REAL,
  longitude REAL,
  provider_timezone TEXT,
  captured_at TEXT NOT NULL,
  forecast_generated_at TEXT,
  predicted_high REAL,
  daily_high REAL,
  hourly_high REAL,
  model TEXT,
  quality_flags TEXT,
  raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_weather_forecasts_basket_date ON weather_forecast_snapshots(basket_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_city_date ON weather_forecast_snapshots(city, forecast_date, captured_at DESC);

CREATE TABLE IF NOT EXISTS weather_basket_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER REFERENCES trades(id),
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  snapshot_type TEXT NOT NULL,
  execution_timestamp TEXT,
  execution_timestamp_ms INTEGER,
  captured_at TEXT NOT NULL,
  snapshot_source TEXT NOT NULL,
  token_count INTEGER NOT NULL,
  matched_token_count INTEGER NOT NULL,
  missing_token_count INTEGER NOT NULL,
  complete_yes_ask_cost REAL,
  complete_yes_bid_value REAL,
  one_share_yes_ask_edge REAL,
  one_share_yes_bid_edge REAL,
  traded_token_id TEXT,
  traded_bucket_label TEXT,
  traded_price REAL,
  traded_side TEXT,
  quality_flags TEXT,
  bucket_prices_json TEXT NOT NULL,
  metrics_json TEXT,
  raw_json TEXT,
  UNIQUE(trade_id, snapshot_type)
);

CREATE INDEX IF NOT EXISTS idx_weather_basket_snapshots_basket ON weather_basket_snapshots(basket_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_basket_snapshots_trade ON weather_basket_snapshots(trade_id);

CREATE TABLE IF NOT EXISTS weather_followup_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trade_id INTEGER NOT NULL REFERENCES trades(id),
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  interval_label TEXT NOT NULL,
  scheduled_for TEXT NOT NULL,
  captured_at TEXT,
  snapshot_source TEXT,
  traded_token_id TEXT,
  traded_price REAL,
  traded_midpoint REAL,
  price_change_from_trade REAL,
  favorable_move_boolean INTEGER,
  complete_yes_ask_cost REAL,
  complete_yes_bid_value REAL,
  one_share_yes_ask_edge REAL,
  one_share_yes_bid_edge REAL,
  matched_token_count INTEGER,
  missing_token_count INTEGER,
  quality_flags TEXT,
  bucket_prices_json TEXT,
  raw_json TEXT,
  UNIQUE(trade_id, interval_label)
);

CREATE INDEX IF NOT EXISTS idx_weather_followups_due ON weather_followup_snapshots(captured_at, scheduled_for);
CREATE INDEX IF NOT EXISTS idx_weather_followups_trade ON weather_followup_snapshots(trade_id);
CREATE INDEX IF NOT EXISTS idx_weather_followups_basket ON weather_followup_snapshots(basket_id, scheduled_for);

CREATE TABLE IF NOT EXISTS weather_observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  source TEXT NOT NULL,
  city TEXT,
  forecast_date TEXT NOT NULL,
  unit TEXT,
  latitude REAL,
  longitude REAL,
  provider_timezone TEXT,
  captured_at TEXT NOT NULL,
  observation_time TEXT,
  current_temperature REAL,
  intraday_high REAL,
  daily_high REAL,
  observed_high REAL,
  observation_status TEXT,
  quality_flags TEXT,
  raw_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_weather_observations_basket ON weather_observations(basket_id, captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_weather_observations_city_date ON weather_observations(city, forecast_date, captured_at DESC);

CREATE TABLE IF NOT EXISTS weather_settlements (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  basket_id INTEGER NOT NULL UNIQUE REFERENCES weather_baskets(id),
  event_slug TEXT NOT NULL UNIQUE,
  city TEXT,
  forecast_date TEXT,
  unit TEXT,
  final_temp REAL,
  winning_bucket_market_id INTEGER REFERENCES weather_bucket_markets(id),
  winning_token_id TEXT,
  winning_bucket_label TEXT,
  winning_market_slug TEXT,
  settlement_status TEXT NOT NULL,
  source TEXT NOT NULL,
  confidence TEXT,
  captured_at TEXT NOT NULL,
  settled_at TEXT,
  quality_flags TEXT,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_weather_settlements_date ON weather_settlements(forecast_date, city);

CREATE TABLE IF NOT EXISTS weather_station_mappings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  city TEXT NOT NULL UNIQUE,
  station_id TEXT NOT NULL,
  station_name TEXT,
  latitude REAL,
  longitude REAL,
  timezone TEXT,
  mapping_confidence TEXT NOT NULL,
  source TEXT NOT NULL,
  notes TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_weather_station_mappings_station ON weather_station_mappings(station_id);

CREATE TABLE IF NOT EXISTS weather_metar_reports (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  station_id TEXT NOT NULL,
  city TEXT,
  source TEXT NOT NULL,
  report_type TEXT,
  report_time TEXT,
  first_seen_at TEXT NOT NULL,
  raw_text TEXT NOT NULL,
  temperature_c REAL,
  dewpoint_c REAL,
  wind_direction REAL,
  wind_speed_kt REAL,
  visibility_statute_mi REAL,
  altimeter_in_hg REAL,
  quality_flags TEXT,
  raw_json TEXT NOT NULL,
  UNIQUE(station_id, report_time, raw_text)
);

CREATE INDEX IF NOT EXISTS idx_weather_metar_station_time ON weather_metar_reports(station_id, report_time);
CREATE INDEX IF NOT EXISTS idx_weather_metar_city_seen ON weather_metar_reports(city, first_seen_at);

CREATE TABLE IF NOT EXISTS weather_bucket_final_pnl (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  settlement_id INTEGER NOT NULL REFERENCES weather_settlements(id),
  basket_id INTEGER NOT NULL REFERENCES weather_baskets(id),
  bucket_market_id INTEGER NOT NULL REFERENCES weather_bucket_markets(id),
  token_id TEXT NOT NULL,
  bucket_label TEXT,
  outcome TEXT,
  net_size REAL NOT NULL,
  avg_entry_price REAL,
  cost_basis REAL NOT NULL,
  realized_pnl REAL NOT NULL,
  final_payout REAL NOT NULL,
  final_pnl REAL NOT NULL,
  trade_count INTEGER NOT NULL,
  first_trade_at TEXT,
  last_trade_at TEXT,
  winning_bucket INTEGER NOT NULL,
  confidence TEXT,
  computed_at TEXT NOT NULL,
  raw_json TEXT,
  UNIQUE(settlement_id, token_id)
);

CREATE INDEX IF NOT EXISTS idx_weather_final_pnl_settlement ON weather_bucket_final_pnl(settlement_id, final_pnl DESC);
CREATE INDEX IF NOT EXISTS idx_weather_final_pnl_basket ON weather_bucket_final_pnl(basket_id, bucket_label);
