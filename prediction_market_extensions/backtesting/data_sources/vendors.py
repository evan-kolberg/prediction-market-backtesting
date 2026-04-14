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

Native = NATIVE_VENDOR
PMXT = PMXT_VENDOR


__all__ = ["NATIVE_VENDOR", "PMXT", "PMXT_VENDOR", "MarketDataVendor", "Native"]
