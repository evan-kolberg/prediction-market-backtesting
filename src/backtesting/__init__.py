"""Prediction market backtesting engine.

Provides an event-driven simulation loop for testing trading strategies
against historical Kalshi and Polymarket data.
"""

from src.backtesting.broker import Broker
from src.backtesting.engine import Engine
from src.backtesting.logger import BacktestLogger
from src.backtesting.models import (
    BacktestResult,
    Fill,
    MarketInfo,
    Order,
    OrderAction,
    OrderStatus,
    Platform,
    PortfolioSnapshot,
    Position,
    Side,
    TradeEvent,
)
from src.backtesting.portfolio import Portfolio
from src.backtesting.strategy import Strategy

__all__ = [
    "BacktestLogger",
    "BacktestResult",
    "Broker",
    "Engine",
    "Fill",
    "MarketInfo",
    "Order",
    "OrderAction",
    "OrderStatus",
    "Platform",
    "Portfolio",
    "PortfolioSnapshot",
    "Position",
    "Side",
    "Strategy",
    "TradeEvent",
]
