from __future__ import annotations

from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.adapters.polymarket.fee_model import PolymarketFeeModel
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue

from backtests._shared.data_sources.kalshi_native import (
    configured_kalshi_native_data_source,
)
from backtests._shared.data_sources.pmxt import configured_pmxt_data_source
from backtests._shared.data_sources.polymarket_native import (
    configured_polymarket_native_data_source,
)


type MarketDataKey = tuple[str, str, str]


@dataclass(frozen=True)
class EngineVenueSpec:
    venue: Venue
    oms_type: OmsType
    base_currency: Any
    fee_model_factory: Callable[[], Any]
    fill_model_mode: str = "taker"
    book_type: BookType = BookType.L1_MBP
    liquidity_consumption: bool = False


@dataclass(frozen=True)
class SingleMarketRunnerSpec:
    runner_path: str
    required_fields: tuple[str, ...]
    forwarded_fields: tuple[str, ...]


@dataclass(frozen=True)
class MarketDataSupport:
    key: MarketDataKey
    configure_data_source: Callable[..., Any]
    load_sim_method_name: str
    engine_spec: EngineVenueSpec
    single_market_runner: SingleMarketRunnerSpec


def _normalize_key_part(value: object) -> str:
    if isinstance(value, str):
        return value.strip().casefold()

    name = getattr(value, "name", None)
    if isinstance(name, str):
        return name.strip().casefold()
    return str(value).strip().casefold()


_SUPPORT_MATRIX: dict[MarketDataKey, MarketDataSupport] = {
    ("kalshi", "trade_tick", "native"): MarketDataSupport(
        key=("kalshi", "trade_tick", "native"),
        configure_data_source=configured_kalshi_native_data_source,
        load_sim_method_name="_load_kalshi_trade_tick_sim",
        engine_spec=EngineVenueSpec(
            venue=Venue("KALSHI"),
            oms_type=OmsType.NETTING,
            base_currency=USD,
            fee_model_factory=KalshiProportionalFeeModel,
        ),
        single_market_runner=SingleMarketRunnerSpec(
            runner_path=(
                "backtests._shared._kalshi_trade_tick_runner:"
                "run_single_market_trade_backtest"
            ),
            required_fields=("market_ticker", "lookback_days"),
            forwarded_fields=(
                "market_ticker",
                "lookback_days",
                "min_trades",
                "min_price_range",
                "chart_resample_rule",
                "emit_summary",
                "emit_html",
                "return_chart_layout",
                "end_time",
                "data_sources",
                "execution",
            ),
        ),
    ),
    ("polymarket", "trade_tick", "native"): MarketDataSupport(
        key=("polymarket", "trade_tick", "native"),
        configure_data_source=configured_polymarket_native_data_source,
        load_sim_method_name="_load_polymarket_trade_tick_sim",
        engine_spec=EngineVenueSpec(
            venue=POLYMARKET_VENUE,
            oms_type=OmsType.NETTING,
            base_currency=USDC_POS,
            fee_model_factory=PolymarketFeeModel,
        ),
        single_market_runner=SingleMarketRunnerSpec(
            runner_path=(
                "backtests._shared._polymarket_trade_tick_runner:"
                "run_single_market_trade_backtest"
            ),
            required_fields=("market_slug", "lookback_days"),
            forwarded_fields=(
                "market_slug",
                "token_index",
                "lookback_days",
                "min_trades",
                "min_price_range",
                "chart_resample_rule",
                "emit_summary",
                "emit_html",
                "return_chart_layout",
                "return_summary_series",
                "end_time",
                "data_sources",
                "execution",
            ),
        ),
    ),
    ("polymarket", "quote_tick", "pmxt"): MarketDataSupport(
        key=("polymarket", "quote_tick", "pmxt"),
        configure_data_source=configured_pmxt_data_source,
        load_sim_method_name="_load_polymarket_pmxt_quote_tick_sim",
        engine_spec=EngineVenueSpec(
            venue=POLYMARKET_VENUE,
            oms_type=OmsType.NETTING,
            base_currency=USDC_POS,
            fee_model_factory=PolymarketFeeModel,
            fill_model_mode="passive_book",
            book_type=BookType.L2_MBP,
            liquidity_consumption=True,
        ),
        single_market_runner=SingleMarketRunnerSpec(
            runner_path=(
                "backtests._shared._polymarket_quote_tick_pmxt_runner:"
                "run_single_market_pmxt_backtest"
            ),
            required_fields=("market_slug",),
            forwarded_fields=(
                "market_slug",
                "token_index",
                "lookback_hours",
                "min_quotes",
                "min_price_range",
                "chart_resample_rule",
                "emit_summary",
                "emit_html",
                "return_chart_layout",
                "return_summary_series",
                "start_time",
                "end_time",
                "data_sources",
                "execution",
            ),
        ),
    ),
}


def resolve_market_data_support(
    *,
    platform: object,
    data_type: object,
    vendor: object,
) -> MarketDataSupport:
    key = (
        _normalize_key_part(platform),
        _normalize_key_part(data_type),
        _normalize_key_part(vendor),
    )
    try:
        return _SUPPORT_MATRIX[key]
    except KeyError as exc:
        raise NotImplementedError(
            "Unsupported backtest data selection: "
            f"platform={key[0]!r}, data_type={key[1]!r}, vendor={key[2]!r}."
        ) from exc


def supported_market_data_keys() -> tuple[MarketDataKey, ...]:
    return tuple(_SUPPORT_MATRIX)


def load_single_market_runner(spec: SingleMarketRunnerSpec) -> Callable[..., Any]:
    module_name, _, attr_name = spec.runner_path.partition(":")
    module = import_module(module_name)
    return getattr(module, attr_name)


def build_single_market_runner_kwargs(
    *,
    spec: SingleMarketRunnerSpec,
    name: str,
    probability_window: int,
    strategy_factory: Callable[..., Any] | None,
    strategy_configs: Sequence[dict[str, Any]] | None,
    initial_cash: float,
    field_values: dict[str, Any],
) -> dict[str, Any]:
    for field_name in spec.required_fields:
        if field_values.get(field_name) is None:
            raise ValueError(f"{field_name} is required for this backtest selection.")

    runner_kwargs: dict[str, Any] = {
        "name": name,
        "probability_window": probability_window,
        "strategy_factory": strategy_factory,
        "strategy_configs": strategy_configs,
        "initial_cash": initial_cash,
    }
    for field_name in spec.forwarded_fields:
        value = field_values.get(field_name)
        if value is not None:
            runner_kwargs[field_name] = value
    return runner_kwargs


__all__ = [
    "EngineVenueSpec",
    "MarketDataKey",
    "MarketDataSupport",
    "SingleMarketRunnerSpec",
    "build_single_market_runner_kwargs",
    "load_single_market_runner",
    "resolve_market_data_support",
    "supported_market_data_keys",
]
