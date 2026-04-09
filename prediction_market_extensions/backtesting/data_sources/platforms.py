from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketPlatform:
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip().casefold())

    def __str__(self) -> str:
        return self.name


Kalshi = MarketPlatform("kalshi")
Polymarket = MarketPlatform("polymarket")

KALSHI_PLATFORM = Kalshi
POLYMARKET_PLATFORM = Polymarket


__all__ = [
    "Kalshi",
    "KALSHI_PLATFORM",
    "MarketPlatform",
    "Polymarket",
    "POLYMARKET_PLATFORM",
]
