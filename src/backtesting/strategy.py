"""Abstract Strategy base class for prediction market backtesting.

Users subclass Strategy and implement on_trade() to define trading logic.
Order placement methods are injected by the Engine before simulation starts.
"""

from __future__ import annotations

import importlib
import inspect
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from src.backtesting.models import (
        Fill,
        MarketInfo,
        Order,
        PortfolioSnapshot,
        Side,
        TradeEvent,
    )


class Strategy(ABC):
    """Base class for backtesting strategies.

    Subclass this and implement ``on_trade()`` at minimum. Use the order
    placement methods (``buy_yes``, ``sell_no``, etc.) to interact with
    the simulated market. Access ``self.portfolio``, ``self.open_orders``,
    and ``self.markets`` for current state.
    """

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._place_order: Callable | None = None
        self._cancel_order_fn: Callable | None = None
        self._cancel_all_fn: Callable | None = None
        self._get_portfolio: Callable | None = None
        self._get_open_orders: Callable | None = None
        self._get_markets: Callable | None = None

    # -- Order placement API --

    def buy_yes(self, market_id: str, price: float, quantity: float) -> Order:
        """Place a limit order to buy YES contracts."""
        return self._place_order(market_id, "buy", "yes", price, quantity)

    def buy_no(self, market_id: str, price: float, quantity: float) -> Order:
        """Place a limit order to buy NO contracts."""
        return self._place_order(market_id, "buy", "no", price, quantity)

    def sell_yes(self, market_id: str, price: float, quantity: float) -> Order:
        """Place a limit order to sell YES contracts."""
        return self._place_order(market_id, "sell", "yes", price, quantity)

    def sell_no(self, market_id: str, price: float, quantity: float) -> Order:
        """Place a limit order to sell NO contracts."""
        return self._place_order(market_id, "sell", "no", price, quantity)

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a pending order by ID. Returns True if cancelled."""
        return self._cancel_order_fn(order_id)

    def cancel_all(self, market_id: str | None = None) -> int:
        """Cancel all pending orders, optionally filtered by market."""
        return self._cancel_all_fn(market_id)

    @property
    def portfolio(self) -> PortfolioSnapshot:
        """Current portfolio state."""
        return self._get_portfolio()

    @property
    def open_orders(self) -> list[Order]:
        """Currently pending orders."""
        return self._get_open_orders()

    @property
    def markets(self) -> dict[str, MarketInfo]:
        """All available market metadata."""
        return self._get_markets()

    # -- Lifecycle hooks --

    @abstractmethod
    def on_trade(self, trade: TradeEvent) -> None:
        """Called for each historical trade event. Primary strategy hook."""
        ...

    def on_fill(self, fill: Fill) -> None:  # noqa: B027
        """Called when one of this strategy's orders fills."""

    def on_market_open(self, market: MarketInfo) -> None:  # noqa: B027
        """Called when a market's open_time is reached."""

    def on_market_close(self, market: MarketInfo) -> None:  # noqa: B027
        """Called when a market's close_time is reached."""

    def on_market_resolve(self, market: MarketInfo, result: Side) -> None:  # noqa: B027
        """Called when a market resolves to YES or NO."""

    def initialize(self) -> None:  # noqa: B027
        """Called once before the simulation starts."""

    def finalize(self) -> None:  # noqa: B027
        """Called once after the simulation ends."""

    # -- Auto-discovery --

    @classmethod
    def load(cls, strategy_dir: Path | str | None = None) -> list[type[Strategy]]:
        """Scan directory for Strategy subclass implementations."""
        if strategy_dir is None:
            strategy_dir = Path(__file__).parent / "examples"
        strategy_dir = Path(strategy_dir)
        if not strategy_dir.exists():
            return []

        base_module = "src.backtesting.examples"
        strategies: list[type[Strategy]] = []

        for py_file in strategy_dir.glob("**/*.py"):
            if py_file.name.startswith("_"):
                continue
            relative_path = py_file.relative_to(strategy_dir)
            module_parts = relative_path.with_suffix("").parts
            module_name = base_module + "." + ".".join(module_parts)
            try:
                module = importlib.import_module(module_name)
            except ImportError:
                continue

            for _, obj in inspect.getmembers(module, inspect.isclass):
                if issubclass(obj, cls) and obj is not cls and not inspect.isabstract(obj):
                    strategies.append(obj)

        return strategies
