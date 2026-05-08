EXACT_TRADE_TIMESTAMP = "exact_trade_timestamp"
DETECTION_TIMESTAMP = "local_detection_timestamp"
POST_DETECTION_BOOK = "nearest_post_detection_orderbook"
WEBSOCKET_CACHED_BOOK = "websocket_cached_orderbook"
HISTORICAL_BOOK_UNAVAILABLE = "historical_orderbook_unavailable"
APPROXIMATE_POSITION = "approximate_position_reconstruction"
APPROXIMATE_PNL = "approximate_pnl"
UNKNOWN_RESOLUTION = "market_resolution_unknown"
LOW_CONFIDENCE_TAKER_PASSIVE = "low_confidence_taker_passive_inference"


def join_flags(*flags: str | None) -> str:
    return ",".join(flag for flag in flags if flag)

