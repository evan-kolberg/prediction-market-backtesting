from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketDataVendor:
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip().casefold())

    def __str__(self) -> str:
        return self.name


NATIVE_VENDOR = MarketDataVendor("native")
PMXT_VENDOR = MarketDataVendor("pmxt")
TELONEX_VENDOR = MarketDataVendor("telonex")


__all__ = [
    "MarketDataVendor",
    "NATIVE_VENDOR",
    "PMXT_VENDOR",
    "TELONEX_VENDOR",
]
