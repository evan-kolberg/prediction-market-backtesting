from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MarketDataVendor:
    name: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "name", self.name.strip().casefold())

    def __str__(self) -> str:
        return self.name


MARKETLENS_VENDOR = MarketDataVendor("marketlens")
NATIVE_VENDOR = MarketDataVendor("native")
PMXT_VENDOR = MarketDataVendor("pmxt")
TELONEX_VENDOR = MarketDataVendor("telonex")

Marketlens = MARKETLENS_VENDOR
Native = NATIVE_VENDOR
PMXT = PMXT_VENDOR
Telonex = TELONEX_VENDOR


__all__ = [
    "MARKETLENS_VENDOR",
    "NATIVE_VENDOR",
    "PMXT",
    "PMXT_VENDOR",
    "TELONEX_VENDOR",
    "MarketDataVendor",
    "Marketlens",
    "Native",
    "Telonex",
]
