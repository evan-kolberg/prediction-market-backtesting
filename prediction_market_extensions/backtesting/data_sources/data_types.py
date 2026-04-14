from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketDataType:
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip().casefold())

    def __str__(self) -> str:
        return self.name


TradeTick = MarketDataType("trade_tick")
QuoteTick = MarketDataType("quote_tick")
Bar = MarketDataType("bar")

TRADE_TICK_DATA = TradeTick
QUOTE_TICK_DATA = QuoteTick
BAR_DATA = Bar


__all__ = [
    "BAR_DATA",
    "QUOTE_TICK_DATA",
    "TRADE_TICK_DATA",
    "Bar",
    "MarketDataType",
    "QuoteTick",
    "TradeTick",
]
