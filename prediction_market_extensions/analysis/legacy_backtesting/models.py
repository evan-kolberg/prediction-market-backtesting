# Derived from or added to the NautilusTrader subtree in this repository.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-11.
# See the repository NOTICE file for provenance and licensing scope.

"""Unified, platform-agnostic data types for the backtesting engine.

All prices are normalized to float in [0.0, 1.0]. Kalshi cents are divided
by 100 during feed normalization. Polymarket prices are already in this range.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


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


PANEL_TOTAL_EQUITY = "total_equity"
PANEL_EQUITY = "equity"
PANEL_MARKET_PNL = "market_pnl"
PANEL_PERIODIC_PNL = "periodic_pnl"
PANEL_YES_PRICE = "yes_price"
PANEL_ALLOCATION = "allocation"
PANEL_DRAWDOWN = "drawdown"
PANEL_ROLLING_SHARPE = "rolling_sharpe"
PANEL_CASH_EQUITY = "cash_equity"
PANEL_MONTHLY_RETURNS = "monthly_returns"
PANEL_BRIER_ADVANTAGE = "brier_advantage"

ALL_PLOT_PANELS = (
    PANEL_TOTAL_EQUITY,
    PANEL_EQUITY,
    PANEL_MARKET_PNL,
    PANEL_PERIODIC_PNL,
    PANEL_YES_PRICE,
    PANEL_ALLOCATION,
    PANEL_DRAWDOWN,
    PANEL_ROLLING_SHARPE,
    PANEL_CASH_EQUITY,
    PANEL_MONTHLY_RETURNS,
    PANEL_BRIER_ADVANTAGE,
)

DEFAULT_DETAIL_PLOT_PANELS = (
    PANEL_EQUITY,
    PANEL_MARKET_PNL,
    PANEL_PERIODIC_PNL,
    PANEL_YES_PRICE,
    PANEL_ALLOCATION,
    PANEL_DRAWDOWN,
    PANEL_ROLLING_SHARPE,
    PANEL_CASH_EQUITY,
    PANEL_MONTHLY_RETURNS,
    PANEL_BRIER_ADVANTAGE,
)

DEFAULT_SUMMARY_PLOT_PANELS = (
    PANEL_TOTAL_EQUITY,
    PANEL_EQUITY,
    PANEL_PERIODIC_PNL,
    PANEL_ALLOCATION,
    PANEL_DRAWDOWN,
    PANEL_ROLLING_SHARPE,
    PANEL_CASH_EQUITY,
    PANEL_MONTHLY_RETURNS,
    PANEL_BRIER_ADVANTAGE,
)


def normalize_plot_panels(panels: Sequence[str] | None, *, default: Sequence[str]) -> tuple[str, ...]:
    requested = tuple(default if panels is None else panels)
    normalized: list[str] = []
    seen: set[str] = set()

    for panel in requested:
        panel_id = str(panel).strip()
        if not panel_id:
            continue
        if panel_id not in ALL_PLOT_PANELS:
            raise ValueError(f"Unknown plot panel {panel_id!r}. Valid panels: {', '.join(ALL_PLOT_PANELS)}.")
        if panel_id in seen:
            continue
        normalized.append(panel_id)
        seen.add(panel_id)

    return tuple(normalized)


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
    market_prices: dict[str, list[tuple[datetime, float]]] = field(default_factory=dict)
    market_pnls: dict[str, float] = field(default_factory=dict)
    overlay_series: dict[str, dict[str, Any]] = field(default_factory=dict)
    overlay_colors: dict[str, str] = field(default_factory=dict)
    hide_primary_panel_series: bool = False
    primary_series_name: str = "Strategy"
    prepend_total_equity_panel: bool = False
    total_equity_panel_label: str = "Total Equity"
    plot_monthly_returns: bool = True
    plot_panels: tuple[str, ...] = field(default_factory=tuple)

    def plot(self, **kwargs: Any) -> Any:
        """Render an interactive Bokeh chart of this backtest.

        Accepts all keyword arguments supported by
        :func:`prediction_market_extensions.analysis.legacy_backtesting.plotting.plot`.
        """
        from prediction_market_extensions.analysis.legacy_backtesting.plotting import plot as _plot

        return _plot(self, **kwargs)
