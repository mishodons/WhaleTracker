from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from src.utils.time import utc_now_iso
from src.utils.quality import APPROXIMATE_POSITION


def _get(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, dict):
        return row.get(key, default)
    try:
        return row[key]
    except (KeyError, TypeError, IndexError):
        return getattr(row, key, default)


def _num(value: Any, default: float = 0.0) -> float:
    if value in (None, ""):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass
class PositionState:
    trader_id: int
    market_id: str | None
    condition_id: str | None
    token_id: str
    outcome: str | None
    net_size: float = 0.0
    avg_entry_price: float = 0.0
    estimated_realized_pnl: float = 0.0
    total_bought: float = 0.0
    total_sold: float = 0.0
    confidence: str = "local_trade_history"

    def apply(self, side: str, size: float, price: float) -> None:
        side = side.upper()
        if side == "BUY":
            self.total_bought += size
            if self.net_size < 0:
                cover = min(size, abs(self.net_size))
                self.estimated_realized_pnl += (self.avg_entry_price - price) * cover
                self.net_size += cover
                size -= cover
                self.confidence = APPROXIMATE_POSITION
            if size > 0:
                new_size = self.net_size + size
                if self.net_size > 0:
                    self.avg_entry_price = ((self.avg_entry_price * self.net_size) + (price * size)) / new_size
                else:
                    self.avg_entry_price = price
                self.net_size = new_size
        elif side == "SELL":
            self.total_sold += size
            if self.net_size > 0:
                matched = min(size, self.net_size)
                self.estimated_realized_pnl += (price - self.avg_entry_price) * matched
                self.net_size -= matched
                size -= matched
            if size > 0:
                self.net_size -= size
                if self.avg_entry_price == 0:
                    self.avg_entry_price = price
                self.confidence = APPROXIMATE_POSITION
            if abs(self.net_size) < 1e-12:
                self.net_size = 0.0
                self.avg_entry_price = 0.0

    def as_row(self, current_price: float | None = None) -> dict[str, Any]:
        unrealized = None
        if current_price is not None:
            if self.net_size >= 0:
                unrealized = (current_price - self.avg_entry_price) * self.net_size
            else:
                unrealized = (self.avg_entry_price - current_price) * abs(self.net_size)
        direction = "LONG" if self.net_size > 0 else "OBSERVED_NEGATIVE" if self.net_size < 0 else "FLAT"
        return {
            "trader_id": self.trader_id,
            "market_id": self.market_id,
            "condition_id": self.condition_id,
            "token_id": self.token_id,
            "outcome": self.outcome,
            "net_size": self.net_size,
            "avg_entry_price": self.avg_entry_price if self.avg_entry_price else None,
            "estimated_current_price": current_price,
            "estimated_unrealized_pnl": unrealized,
            "estimated_realized_pnl": self.estimated_realized_pnl,
            "total_bought": self.total_bought,
            "total_sold": self.total_sold,
            "direction": direction,
            "confidence": self.confidence,
            "last_updated": utc_now_iso(),
        }


def reconstruct_positions_from_trades(
    trades: list[Any],
    *,
    trader_id: int,
    current_prices: dict[str, float] | None = None,
) -> list[dict[str, Any]]:
    states: dict[str, PositionState] = {}
    current_prices = current_prices or {}
    for trade in sorted(trades, key=lambda row: (_get(row, "trade_timestamp", ""), _get(row, "id", 0))):
        token_id = str(_get(trade, "token_id", "") or "")
        if not token_id:
            continue
        state = states.get(token_id)
        if state is None:
            state = PositionState(
                trader_id=trader_id,
                market_id=_get(trade, "market_id"),
                condition_id=_get(trade, "condition_id"),
                token_id=token_id,
                outcome=_get(trade, "outcome"),
            )
            states[token_id] = state
        state.apply(str(_get(trade, "side", "")), _num(_get(trade, "size")), _num(_get(trade, "price")))
    return [state.as_row(current_prices.get(token_id)) for token_id, state in states.items()]


def refresh_positions(repository: Any, trader_id: int, current_prices: dict[str, float] | None = None) -> list[dict[str, Any]]:
    trades = repository.list_trades(trader_id)
    rows = reconstruct_positions_from_trades(trades, trader_id=trader_id, current_prices=current_prices)
    repository.replace_positions(trader_id, rows)
    return rows

