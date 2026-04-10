from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from prediction_market_extensions.adapters.prediction_market import HistoricalReplayAdapter

from prediction_market_extensions.backtesting._replay_specs import ReplaySpec
from prediction_market_extensions.backtesting.data_sources.replay_adapters import BUILTIN_REPLAY_ADAPTERS


type MarketDataKey = tuple[str, str, str]


@dataclass(frozen=True)
class MarketDataSupport:
    key: MarketDataKey
    adapter: HistoricalReplayAdapter


def _normalize_key_part(value: object) -> str:
    if isinstance(value, str):
        return value.strip().casefold()

    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name.strip().casefold()
    return str(value).strip().casefold()


def _normalize_lookup_key(*, platform: object, data_type: object, vendor: object) -> MarketDataKey:
    return (_normalize_key_part(platform), _normalize_key_part(data_type), _normalize_key_part(vendor))


def _support_from_adapter(adapter: HistoricalReplayAdapter) -> MarketDataSupport:
    return MarketDataSupport(key=(adapter.key.platform, adapter.key.data_type, adapter.key.vendor), adapter=adapter)


_BUILTIN_SUPPORTS: dict[MarketDataKey, MarketDataSupport] = {
    support.key: support for support in (_support_from_adapter(adapter) for adapter in BUILTIN_REPLAY_ADAPTERS)
}

_REGISTERED_SUPPORTS: dict[MarketDataKey, MarketDataSupport] = dict(_BUILTIN_SUPPORTS)


def register_market_data_support(support: MarketDataSupport) -> None:
    _REGISTERED_SUPPORTS[support.key] = support


def unregister_market_data_support(key: MarketDataKey) -> MarketDataSupport | None:
    return _REGISTERED_SUPPORTS.pop(key, None)


def resolve_market_data_support(*, platform: object, data_type: object, vendor: object) -> MarketDataSupport:
    key = _normalize_lookup_key(platform=platform, data_type=data_type, vendor=vendor)
    try:
        return _REGISTERED_SUPPORTS[key]
    except KeyError as exc:
        raise NotImplementedError(
            f"Unsupported backtest data selection: platform={key[0]!r}, data_type={key[1]!r}, vendor={key[2]!r}."
        ) from exc


def resolve_replay_adapter(*, platform: object, data_type: object, vendor: object) -> HistoricalReplayAdapter:
    return resolve_market_data_support(platform=platform, data_type=data_type, vendor=vendor).adapter


def supported_market_data_keys() -> tuple[MarketDataKey, ...]:
    return tuple(_REGISTERED_SUPPORTS)


def build_single_market_replay(*, support: MarketDataSupport, field_values: dict[str, Any]) -> ReplaySpec:
    replay = support.adapter.build_single_market_replay(field_values=field_values)
    return replay


__all__ = [
    "MarketDataKey",
    "MarketDataSupport",
    "build_single_market_replay",
    "register_market_data_support",
    "resolve_market_data_support",
    "resolve_replay_adapter",
    "supported_market_data_keys",
    "unregister_market_data_support",
]
