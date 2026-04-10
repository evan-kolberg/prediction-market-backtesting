from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd


type TimestampLike = pd.Timestamp | str | object


@dataclass(frozen=True)
class TradeReplay:
    market_slug: str | None = None
    market_ticker: str | None = None
    token_index: int = 0
    lookback_days: float | None = None
    start_time: TimestampLike | None = None
    end_time: TimestampLike | None = None
    outcome: str | None = None
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class QuoteReplay:
    market_slug: str
    token_index: int = 0
    lookback_hours: float | None = None
    start_time: TimestampLike | None = None
    end_time: TimestampLike | None = None
    outcome: str | None = None
    metadata: Mapping[str, Any] | None = None


type ReplaySpec = TradeReplay | QuoteReplay


@dataclass(frozen=True)
class MarketSimConfig:
    market_slug: str | None = None
    market_ticker: str | None = None
    token_index: int = 0
    lookback_days: float | None = None
    lookback_hours: float | None = None
    start_time: TimestampLike | None = None
    end_time: TimestampLike | None = None
    outcome: str | None = None
    metadata: Mapping[str, Any] | None = None


def coerce_legacy_market_sim_config(
    *, platform: str, data_type: str, vendor: str, sim: MarketSimConfig
) -> ReplaySpec:
    normalized_key = (
        platform.strip().casefold(),
        data_type.strip().casefold(),
        vendor.strip().casefold(),
    )

    if normalized_key == ("kalshi", "trade_tick", "native"):
        if sim.market_ticker is None:
            raise ValueError("market_ticker is required for Kalshi trade-tick replays.")
        return TradeReplay(
            market_ticker=sim.market_ticker,
            lookback_days=sim.lookback_days,
            start_time=sim.start_time,
            end_time=sim.end_time,
            outcome=sim.outcome,
            metadata=sim.metadata,
        )

    if normalized_key == ("polymarket", "trade_tick", "native"):
        if sim.market_slug is None:
            raise ValueError("market_slug is required for Polymarket trade-tick replays.")
        return TradeReplay(
            market_slug=sim.market_slug,
            token_index=sim.token_index,
            lookback_days=sim.lookback_days,
            start_time=sim.start_time,
            end_time=sim.end_time,
            outcome=sim.outcome,
            metadata=sim.metadata,
        )

    if normalized_key == ("polymarket", "quote_tick", "pmxt"):
        if sim.market_slug is None:
            raise ValueError("market_slug is required for quote-tick replays.")
        return QuoteReplay(
            market_slug=sim.market_slug,
            token_index=sim.token_index,
            lookback_hours=sim.lookback_hours,
            start_time=sim.start_time,
            end_time=sim.end_time,
            outcome=sim.outcome,
            metadata=sim.metadata,
        )

    raise NotImplementedError(
        "Unsupported replay selection for legacy MarketSimConfig: "
        f"platform={platform!r}, data_type={data_type!r}, vendor={vendor!r}."
    )


__all__ = [
    "MarketSimConfig",
    "QuoteReplay",
    "ReplaySpec",
    "TimestampLike",
    "TradeReplay",
    "coerce_legacy_market_sim_config",
]
