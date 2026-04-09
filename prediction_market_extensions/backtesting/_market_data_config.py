from __future__ import annotations

from dataclasses import dataclass

from prediction_market_extensions.backtesting.data_sources import MarketDataType
from prediction_market_extensions.backtesting.data_sources import MarketDataVendor
from prediction_market_extensions.backtesting.data_sources import MarketPlatform


@dataclass(frozen=True)
class MarketDataConfig:
    platform: str | MarketPlatform
    data_type: str | MarketDataType
    vendor: str | MarketDataVendor
    sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "platform", _normalize_name(self.platform))
        object.__setattr__(self, "data_type", _normalize_name(self.data_type))
        object.__setattr__(self, "vendor", _normalize_name(self.vendor))
        object.__setattr__(
            self,
            "sources",
            tuple(source.strip() for source in self.sources if source.strip()),
        )


def _normalize_name(
    value: str | MarketPlatform | MarketDataType | MarketDataVendor,
) -> str:
    if isinstance(value, str):
        return value.strip().casefold()
    return value.name.strip().casefold()


__all__ = ["MarketDataConfig"]
