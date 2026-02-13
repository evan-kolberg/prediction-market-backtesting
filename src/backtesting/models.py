"""Unified, platform-agnostic data types for the backtesting engine.

All prices are normalized to float in [0.0, 1.0]. Kalshi cents are divided
by 100 during feed normalization. Polymarket prices are already in this range.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class Platform(str, Enum):
    KALSHI = "kalshi"
    POLYMARKET = "polymarket"


class Side(str, Enum):
    YES = "yes"
    NO = "no"


class OrderAction(str, Enum):
    BUY = "buy"
    SELL = "sell"


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    CANCELLED = "cancelled"


class MarketStatus(str, Enum):
    OPEN = "open"
    CLOSED = "closed"
    RESOLVED_YES = "resolved_yes"
    RESOLVED_NO = "resolved_no"


@dataclass
class MarketInfo:
    """Static metadata about a prediction market."""

    market_id: str
    platform: Platform
    title: str
    open_time: datetime | None
    close_time: datetime | None
    result: Side | None
    status: MarketStatus
    event_id: str | None = None
    token_id_map: dict[str, int] | None = None


@dataclass
class TradeEvent:
    """A single normalized historical trade from the data feed."""

    timestamp: datetime
    market_id: str
    platform: Platform
    yes_price: float
    no_price: float
    quantity: float
    taker_side: Side
    raw_id: str | None = None


@dataclass
class Order:
    """A limit order placed by a strategy."""

    order_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    market_id: str = ""
    action: OrderAction = OrderAction.BUY
    side: Side = Side.YES
    price: float = 0.0
    quantity: float = 0.0
    status: OrderStatus = OrderStatus.PENDING
    created_at: datetime | None = None
    filled_at: datetime | None = None
    fill_price: float | None = None
    filled_quantity: float = 0.0


@dataclass
class Fill:
    """Record of a filled order."""

    order_id: str
    market_id: str
    action: OrderAction
    side: Side
    price: float
    quantity: float
    timestamp: datetime
    commission: float = 0.0


@dataclass
class Position:
    """Current holding in a single market.

    quantity > 0 means long YES contracts.
    quantity < 0 means long NO contracts (equivalently, short YES).
    """

    market_id: str
    quantity: float = 0.0
    avg_entry_price: float = 0.0
    realized_pnl: float = 0.0


@dataclass
class PortfolioSnapshot:
    """Point-in-time snapshot of portfolio state."""

    timestamp: datetime
    cash: float
    total_equity: float
    unrealized_pnl: float
    num_positions: int


@dataclass
class BacktestResult:
    """Complete results from a backtest run."""

    equity_curve: list[PortfolioSnapshot]
    fills: list[Fill]
    metrics: dict[str, float]
    strategy_name: str
    platform: Platform
    start_time: datetime | None
    end_time: datetime | None
    initial_cash: float
    final_equity: float
    num_markets_traded: int
    num_markets_resolved: int
    event_log: list[str] = field(default_factory=list)
