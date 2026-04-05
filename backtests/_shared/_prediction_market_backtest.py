from __future__ import annotations

import asyncio
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_trader.adapters.prediction_market import (
    research as prediction_market_research,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    build_brier_inputs,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    build_market_prices,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    extract_realized_pnl,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    extract_price_points,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    infer_realized_outcome,
)
from nautilus_trader.adapters.prediction_market.fill_model import (
    PredictionMarketTakerFillModel,
)
from nautilus_trader.adapters.prediction_market.research import print_backtest_summary
from nautilus_trader.adapters.prediction_market.research import (
    save_aggregate_backtest_report,
)
from nautilus_trader.adapters.prediction_market.research import (
    save_combined_backtest_report,
)
from nautilus_trader.analysis.legacy_plot_adapter import build_legacy_backtest_layout
from nautilus_trader.analysis.legacy_plot_adapter import save_legacy_backtest_layout
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.common.component import is_backtest_force_stop
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyFactory as NautilusStrategyFactory
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.risk.config import RiskEngineConfig
from nautilus_trader.trading.strategy import Strategy

from backtests._shared._backtest_runtime import apply_backtest_run_state
from backtests._shared._backtest_runtime import build_backtest_run_state
from backtests._shared._backtest_runtime import print_backtest_result_warnings
from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._market_data_support import resolve_market_data_support
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._strategy_configs import build_importable_strategy_configs
from backtests._shared._strategy_configs import StrategyConfigSpec
from backtests._shared.data_sources.kalshi_native import (
    RunnerKalshiDataLoader as KalshiDataLoader,
)
from backtests._shared.data_sources.pmxt import (
    RunnerPolymarketPMXTDataLoader as PolymarketPMXTDataLoader,
)
from backtests._shared.data_sources.polymarket_native import (
    RunnerPolymarketDataLoader as PolymarketDataLoader,
)


type StrategyFactory = Callable[[InstrumentId], Strategy]


@dataclass(frozen=True)
class MarketSimConfig:
    market_slug: str | None = None
    market_ticker: str | None = None
    token_index: int = 0
    lookback_days: int | None = None
    lookback_hours: float | None = None
    start_time: pd.Timestamp | datetime | str | None = None
    end_time: pd.Timestamp | datetime | str | None = None
    outcome: str | None = None
    strategy_configs: Sequence[StrategyConfigSpec] | None = None
    metadata: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class MarketReportConfig:
    count_key: str
    count_label: str
    pnl_label: str
    market_key: str = "slug"
    combined_report: bool = False
    combined_report_path: str | None = None
    summary_report: bool = False
    summary_report_path: str | None = None


@dataclass(frozen=True)
class _LoadedMarketSim:
    spec: MarketSimConfig
    instrument: Any
    records: list[Any]
    count: int
    count_key: str
    market_key: str
    market_id: str
    outcome: str
    realized_outcome: float | None
    prices: list[float]
    metadata: Mapping[str, Any]
    requested_start_ns: int | None
    requested_end_ns: int | None


class PredictionMarketBacktest:
    def __init__(
        self,
        *,
        name: str,
        data: MarketDataConfig,
        sims: Sequence[MarketSimConfig],
        strategy_configs: Sequence[StrategyConfigSpec] = (),
        strategy_factory: StrategyFactory | None = None,
        initial_cash: float,
        probability_window: int,
        min_trades: int = 0,
        min_quotes: int = 0,
        min_price_range: float = 0.0,
        default_lookback_days: int | None = None,
        default_lookback_hours: float | None = None,
        default_start_time: pd.Timestamp | datetime | str | None = None,
        default_end_time: pd.Timestamp | datetime | str | None = None,
        nautilus_log_level: str = "INFO",
        execution: ExecutionModelConfig | None = None,
        chart_resample_rule: str | None = None,
        emit_html: bool = True,
        return_chart_layout: bool = False,
        return_summary_series: bool = False,
    ) -> None:
        if strategy_factory is not None and strategy_configs:
            raise ValueError("Use strategy_factory or strategy_configs, not both.")
        if strategy_factory is None and not strategy_configs:
            raise ValueError(
                "strategy_configs is required when strategy_factory is not provided."
            )
        self.name = name
        self.data = data
        self.sims = tuple(sims)
        self.strategy_configs = tuple(strategy_configs)
        self.strategy_factory = strategy_factory
        self.initial_cash = float(initial_cash)
        self.probability_window = int(probability_window)
        self.min_trades = int(min_trades)
        self.min_quotes = int(min_quotes)
        self.min_price_range = float(min_price_range)
        self.default_lookback_days = default_lookback_days
        self.default_lookback_hours = default_lookback_hours
        self.default_start_time = default_start_time
        self.default_end_time = default_end_time
        self.nautilus_log_level = nautilus_log_level
        self.execution = execution if execution is not None else ExecutionModelConfig()
        self.chart_resample_rule = chart_resample_rule
        self.emit_html = emit_html
        self.return_chart_layout = return_chart_layout
        self.return_summary_series = return_summary_series

    def run(self) -> list[dict[str, Any]]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.run_async())

        raise RuntimeError(
            "run() cannot be called inside an active event loop; use await run_async() instead."
        )

    def run_backtest(self) -> list[dict[str, Any]]:
        return self.run()

    async def run_async(self) -> list[dict[str, Any]]:
        loaded_sims = await self._load_sims_async()
        if not loaded_sims:
            return []

        engine = self._build_engine()
        try:
            for loaded_sim in loaded_sims:
                engine.add_instrument(loaded_sim.instrument)
                engine.add_data(loaded_sim.records)

            if self.strategy_factory is not None:
                for loaded_sim in loaded_sims:
                    engine.add_strategy(self.strategy_factory(loaded_sim.instrument.id))
            else:
                for importable_config in self._build_importable_strategy_configs(
                    loaded_sims
                ):
                    engine.add_strategy(
                        NautilusStrategyFactory.create(importable_config)
                    )

            print(
                f"Starting {self.name} with {len(loaded_sims)} sims "
                f"and {len(self.strategy_configs)} strategy config(s)..."
            )
            engine.run()
            engine_result = engine.get_result()
            forced_stop = bool(is_backtest_force_stop())

            fills_report = engine.trader.generate_order_fills_report()
            positions_report = engine.trader.generate_positions_report()
            single_market_artifacts = self._build_single_market_artifacts(
                engine=engine,
                loaded_sims=loaded_sims,
                fills_report=fills_report,
            )
            return [
                self._build_result(
                    loaded_sim=loaded_sim,
                    fills_report=fills_report,
                    positions_report=positions_report,
                    single_market_artifacts=single_market_artifacts,
                    run_state=build_backtest_run_state(
                        data=loaded_sim.records,
                        backtest_end_ns=engine_result.backtest_end,
                        forced_stop=forced_stop,
                        requested_start_ns=loaded_sim.requested_start_ns,
                        requested_end_ns=loaded_sim.requested_end_ns,
                    ),
                )
                for loaded_sim in loaded_sims
            ]
        finally:
            engine.reset()
            engine.dispose()

    async def run_backtest_async(self) -> list[dict[str, Any]]:
        return await self.run_async()

    async def _load_sims_async(self) -> list[_LoadedMarketSim]:
        support = resolve_market_data_support(
            platform=self.data.platform,
            data_type=self.data.data_type,
            vendor=self.data.vendor,
        )
        load_sim = getattr(self, support.load_sim_method_name)
        with support.configure_data_source(sources=self.data.sources) as data_source:
            print(data_source.summary)
            loaded_sims: list[_LoadedMarketSim] = []
            for sim in self.sims:
                loaded_sim = await load_sim(sim)
                if loaded_sim is not None:
                    loaded_sims.append(loaded_sim)
            return loaded_sims

    async def _load_polymarket_trade_tick_sim(
        self, sim: MarketSimConfig
    ) -> _LoadedMarketSim | None:
        if sim.market_slug is None:
            raise ValueError("market_slug is required for Polymarket trade-tick sims.")

        lookback_days = sim.lookback_days or self.default_lookback_days
        if lookback_days is None:
            raise ValueError(
                "lookback_days is required for Polymarket trade-tick sims."
            )

        end = pd.Timestamp(
            sim.end_time if sim.end_time is not None else self.default_end_time
        )
        if pd.isna(end):
            end = pd.Timestamp(datetime.now(UTC))
        if end.tzinfo is None:
            end = end.tz_localize(UTC)
        else:
            end = end.tz_convert(UTC)
        start = end - pd.Timedelta(days=lookback_days)

        print(
            f"Loading Polymarket market {sim.market_slug} "
            f"(token_index={sim.token_index}, lookback={lookback_days}d, "
            f"window_end={end.isoformat()})..."
        )
        try:
            loader = await PolymarketDataLoader.from_market_slug(
                sim.market_slug, token_index=sim.token_index
            )
            trades = await loader.load_trades(start, end)
        except Exception as exc:
            print(f"Skip {sim.market_slug}: unable to load trades ({exc})")
            return None

        prices = [float(trade.price) for trade in trades]
        if len(trades) < self.min_trades:
            print(
                f"Skip {sim.market_slug}: {len(trades)} trades < {self.min_trades} required"
            )
            return None
        if prices and max(prices) - min(prices) < self.min_price_range:
            print(
                f"Skip {sim.market_slug}: price range {max(prices) - min(prices):.3f} "
                f"< {self.min_price_range:.3f}"
            )
            return None
        if not trades:
            print(f"Skip {sim.market_slug}: no trades returned")
            return None

        metadata = dict(sim.metadata or {})
        return _LoadedMarketSim(
            spec=sim,
            instrument=loader.instrument,
            records=list(trades),
            count=len(trades),
            count_key="trades",
            market_key="slug",
            market_id=sim.market_slug,
            outcome=str(loader.instrument.outcome or sim.outcome or ""),
            realized_outcome=infer_realized_outcome(loader.instrument),
            prices=prices,
            metadata=metadata,
            requested_start_ns=int(start.value),
            requested_end_ns=int(end.value),
        )

    async def _load_polymarket_pmxt_quote_tick_sim(
        self, sim: MarketSimConfig
    ) -> _LoadedMarketSim | None:
        if sim.market_slug is None:
            raise ValueError("market_slug is required for Polymarket quote-tick sims.")

        end = pd.Timestamp(
            sim.end_time if sim.end_time is not None else self.default_end_time
        )
        if pd.isna(end):
            end = pd.Timestamp(datetime.now(UTC))
        if end.tzinfo is None:
            end = end.tz_localize(UTC)
        else:
            end = end.tz_convert(UTC)

        start_candidate = (
            sim.start_time if sim.start_time is not None else self.default_start_time
        )
        if start_candidate is not None:
            start = pd.Timestamp(start_candidate)
            if start.tzinfo is None:
                start = start.tz_localize(UTC)
            else:
                start = start.tz_convert(UTC)
        else:
            lookback_hours = sim.lookback_hours or self.default_lookback_hours
            if lookback_hours is None:
                raise ValueError(
                    "start_time/end_time or lookback_hours is required for quote-tick sims."
                )
            start = end - pd.Timedelta(hours=lookback_hours)

        if start >= end:
            raise ValueError(
                f"start_time {start.isoformat()} must be earlier than end_time {end.isoformat()}"
            )

        print(
            f"Loading PMXT Polymarket market {sim.market_slug} "
            f"(token_index={sim.token_index}, window_start={start.isoformat()}, "
            f"window_end={end.isoformat()})..."
        )
        try:
            loader = await PolymarketPMXTDataLoader.from_market_slug(
                sim.market_slug, token_index=sim.token_index
            )
            records = list(loader.load_order_book_and_quotes(start, end))
        except Exception as exc:
            print(f"Skip {sim.market_slug}: unable to load PMXT quotes ({exc})")
            return None

        prices: list[float] = []
        quote_count = 0
        for record in records:
            if not isinstance(record, QuoteTick):
                continue
            quote_count += 1
            prices.append((float(record.bid_price) + float(record.ask_price)) / 2.0)

        if quote_count < self.min_quotes:
            print(
                f"Skip {sim.market_slug}: {quote_count} quotes < {self.min_quotes} required"
            )
            return None
        if prices and max(prices) - min(prices) < self.min_price_range:
            print(
                f"Skip {sim.market_slug}: price range {max(prices) - min(prices):.3f} "
                f"< {self.min_price_range:.3f}"
            )
            return None
        if not records:
            print(f"Skip {sim.market_slug}: no PMXT records returned")
            return None

        metadata = dict(sim.metadata or {})
        return _LoadedMarketSim(
            spec=sim,
            instrument=loader.instrument,
            records=records,
            count=quote_count,
            count_key="quotes",
            market_key="slug",
            market_id=sim.market_slug,
            outcome=str(loader.instrument.outcome or sim.outcome or ""),
            realized_outcome=infer_realized_outcome(loader.instrument),
            prices=prices,
            metadata=metadata,
            requested_start_ns=int(start.value),
            requested_end_ns=int(end.value),
        )

    async def _load_kalshi_trade_tick_sim(
        self, sim: MarketSimConfig
    ) -> _LoadedMarketSim | None:
        if sim.market_ticker is None:
            raise ValueError("market_ticker is required for Kalshi trade-tick sims.")

        lookback_days = sim.lookback_days or self.default_lookback_days
        if lookback_days is None:
            raise ValueError("lookback_days is required for Kalshi trade-tick sims.")

        end = pd.Timestamp(
            sim.end_time if sim.end_time is not None else self.default_end_time
        )
        if pd.isna(end):
            end = pd.Timestamp(datetime.now(UTC))
        if end.tzinfo is None:
            end = end.tz_localize(UTC)
        else:
            end = end.tz_convert(UTC)
        start = end - pd.Timedelta(days=lookback_days)

        print(
            f"Loading Kalshi market {sim.market_ticker} "
            f"(lookback={lookback_days}d, window_end={end.isoformat()})..."
        )
        try:
            loader = await KalshiDataLoader.from_market_ticker(sim.market_ticker)
            trades = await loader.load_trades(start, end)
        except Exception as exc:
            print(f"Skip {sim.market_ticker}: unable to load trades ({exc})")
            return None

        prices = [float(trade.price) for trade in trades]
        if len(trades) < self.min_trades:
            print(
                f"Skip {sim.market_ticker}: {len(trades)} trades < {self.min_trades} required"
            )
            return None
        if prices and max(prices) - min(prices) < self.min_price_range:
            print(
                f"Skip {sim.market_ticker}: price range {max(prices) - min(prices):.3f} "
                f"< {self.min_price_range:.3f}"
            )
            return None
        if not trades:
            print(f"Skip {sim.market_ticker}: no trades returned")
            return None

        metadata = dict(sim.metadata or {})
        return _LoadedMarketSim(
            spec=sim,
            instrument=loader.instrument,
            records=list(trades),
            count=len(trades),
            count_key="trades",
            market_key="ticker",
            market_id=sim.market_ticker,
            outcome=str(sim.outcome or ""),
            realized_outcome=infer_realized_outcome(loader.instrument),
            prices=prices,
            metadata=metadata,
            requested_start_ns=int(start.value),
            requested_end_ns=int(end.value),
        )

    def _build_engine(self) -> BacktestEngine:
        engine = BacktestEngine(
            config=BacktestEngineConfig(
                trader_id=TraderId("BACKTESTER-001"),
                logging=LoggingConfig(log_level=self.nautilus_log_level),
                risk_engine=RiskEngineConfig(),
            ),
        )
        latency_model = self.execution.build_latency_model()
        support = resolve_market_data_support(
            platform=self.data.platform,
            data_type=self.data.data_type,
            vendor=self.data.vendor,
        )
        fill_model = None
        if support.engine_spec.fill_model_mode == "taker":
            fill_model = PredictionMarketTakerFillModel()
        elif support.engine_spec.fill_model_mode != "passive_book":
            raise AssertionError(
                f"Unsupported fill model mode {support.engine_spec.fill_model_mode!r}"
            )
        engine.add_venue(
            venue=support.engine_spec.venue,
            oms_type=support.engine_spec.oms_type,
            account_type=AccountType.CASH,
            base_currency=support.engine_spec.base_currency,
            starting_balances=[
                Money(self.initial_cash, support.engine_spec.base_currency)
            ],
            fill_model=fill_model,
            fee_model=support.engine_spec.fee_model_factory(),
            book_type=support.engine_spec.book_type,
            latency_model=latency_model,
            liquidity_consumption=support.engine_spec.liquidity_consumption,
            queue_position=self.execution.queue_position,
        )
        return engine

    def _build_importable_strategy_configs(
        self, loaded_sims: Sequence[_LoadedMarketSim]
    ) -> list[Any]:
        if not loaded_sims:
            return []

        importable_configs: list[Any] = []
        all_instrument_ids = [loaded_sim.instrument.id for loaded_sim in loaded_sims]
        for strategy_spec in self.strategy_configs:
            batch_level = self._is_batch_strategy_config(strategy_spec)
            target_sims = loaded_sims[:1] if batch_level else loaded_sims
            for loaded_sim in target_sims:
                bound_spec = self._bind_strategy_spec(
                    strategy_spec=strategy_spec,
                    loaded_sim=loaded_sim,
                    all_instrument_ids=all_instrument_ids,
                )
                importable_configs.extend(
                    build_importable_strategy_configs(
                        strategy_configs=[bound_spec],
                        instrument_id=loaded_sim.instrument.id,
                    )
                )
        return importable_configs

    def _is_batch_strategy_config(self, strategy_spec: StrategyConfigSpec) -> bool:
        raw_config = strategy_spec.get("config", {})
        if self._contains_value(raw_config, "__ALL_SIM_INSTRUMENT_IDS__"):
            return True
        if not isinstance(raw_config, Mapping):
            return False
        instrument_ids = raw_config.get("instrument_ids")
        return instrument_ids not in (None, "__PRIMARY_INSTRUMENTS__")

    def _contains_value(self, value: Any, target: str) -> bool:
        if value == target:
            return True
        if isinstance(value, Mapping):
            return any(self._contains_value(inner, target) for inner in value.values())
        if isinstance(value, list | tuple):
            return any(self._contains_value(inner, target) for inner in value)
        return False

    def _bind_strategy_spec(
        self,
        *,
        strategy_spec: StrategyConfigSpec,
        loaded_sim: _LoadedMarketSim,
        all_instrument_ids: Sequence[InstrumentId],
    ) -> StrategyConfigSpec:
        raw_config = strategy_spec.get("config", {})
        if not isinstance(raw_config, Mapping):
            raise TypeError("strategy config payload must be a mapping")

        metadata = dict(loaded_sim.metadata)
        metadata.setdefault("market_slug", loaded_sim.spec.market_slug)
        metadata.setdefault("market_ticker", loaded_sim.spec.market_ticker)
        metadata.setdefault("token_index", loaded_sim.spec.token_index)
        metadata.setdefault("outcome", loaded_sim.outcome)

        return {
            "strategy_path": strategy_spec["strategy_path"],
            "config_path": strategy_spec["config_path"],
            "config": self._bind_value(
                raw_config,
                instrument_id=loaded_sim.instrument.id,
                all_instrument_ids=all_instrument_ids,
                metadata=metadata,
            ),
        }

    def _bind_value(
        self,
        value: Any,
        *,
        instrument_id: InstrumentId,
        all_instrument_ids: Sequence[InstrumentId],
        metadata: Mapping[str, Any],
    ) -> Any:
        if isinstance(value, Mapping):
            return {
                key: self._bind_value(
                    inner,
                    instrument_id=instrument_id,
                    all_instrument_ids=all_instrument_ids,
                    metadata=metadata,
                )
                for key, inner in value.items()
            }
        if isinstance(value, list):
            return [
                self._bind_value(
                    inner,
                    instrument_id=instrument_id,
                    all_instrument_ids=all_instrument_ids,
                    metadata=metadata,
                )
                for inner in value
            ]
        if isinstance(value, tuple):
            return tuple(
                self._bind_value(
                    inner,
                    instrument_id=instrument_id,
                    all_instrument_ids=all_instrument_ids,
                    metadata=metadata,
                )
                for inner in value
            )
        if value == "__SIM_INSTRUMENT_ID__":
            return instrument_id
        if value == "__ALL_SIM_INSTRUMENT_IDS__":
            return list(all_instrument_ids)
        if isinstance(value, str) and value.startswith("__SIM_METADATA__:"):
            key = value.removeprefix("__SIM_METADATA__:")
            return metadata[key]
        return value

    def _build_result(
        self,
        *,
        loaded_sim: _LoadedMarketSim,
        fills_report: pd.DataFrame,
        positions_report: pd.DataFrame,
        single_market_artifacts: Mapping[str, Any] | None = None,
        run_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        instrument_id = str(loaded_sim.instrument.id)
        instrument_fills = self._filter_report_rows(
            fills_report, instrument_id=instrument_id
        )
        instrument_positions = self._filter_report_rows(
            positions_report, instrument_id=instrument_id
        )

        pnl = extract_realized_pnl(instrument_positions)
        result: dict[str, Any] = {
            loaded_sim.market_key: loaded_sim.market_id,
            loaded_sim.count_key: loaded_sim.count,
            "fills": int(len(instrument_fills)),
            "pnl": float(pnl),
            "instrument_id": instrument_id,
            "outcome": loaded_sim.outcome,
            "realized_outcome": loaded_sim.realized_outcome,
            "token_index": loaded_sim.spec.token_index,
            "fill_events": self._serialize_fill_events(
                market_id=loaded_sim.market_id, fills_report=instrument_fills
            ),
        }
        if loaded_sim.spec.market_slug is not None:
            result["slug"] = loaded_sim.spec.market_slug
        if loaded_sim.spec.market_ticker is not None:
            result["ticker"] = loaded_sim.spec.market_ticker
        if loaded_sim.prices:
            result["entry_min"] = min(loaded_sim.prices)
            result["max"] = max(loaded_sim.prices)
            result["last"] = loaded_sim.prices[-1]
        if single_market_artifacts:
            result.update(single_market_artifacts)
        result.update(dict(loaded_sim.metadata))
        return apply_backtest_run_state(result=result, run_state=run_state or {})

    def _build_single_market_artifacts(
        self,
        *,
        engine: BacktestEngine,
        loaded_sims: Sequence[_LoadedMarketSim],
        fills_report: pd.DataFrame,
    ) -> dict[str, Any]:
        if len(loaded_sims) != 1:
            return {}

        return self._build_single_market_artifacts_for_loaded_sim(
            engine=engine,
            loaded_sim=loaded_sims[0],
            fills_report=fills_report,
        )

    def _build_single_market_artifacts_for_loaded_sim(
        self,
        *,
        engine: BacktestEngine,
        loaded_sim: _LoadedMarketSim,
        fills_report: pd.DataFrame,
    ) -> dict[str, Any]:
        price_points = extract_price_points(
            loaded_sim.records,
            price_attr="mid_price" if self.data.data_type == "quote_tick" else "price",
        )
        market_prices = build_market_prices(
            price_points,
            resample_rule=self.chart_resample_rule,
        )
        user_probabilities, market_probabilities, outcomes = build_brier_inputs(
            price_points,
            window=self.probability_window,
            realized_outcome=loaded_sim.realized_outcome,
        )
        artifacts: dict[str, Any] = {}

        chart_layout = None
        chart_title = f"{self.name}:{loaded_sim.market_id} legacy chart"
        if self.emit_html or self.return_chart_layout:
            output_path = (
                Path("output") / f"{self.name}_{loaded_sim.market_id}_legacy.html"
            ).resolve()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                chart_layout, chart_title = build_legacy_backtest_layout(
                    engine=engine,
                    output_path=str(output_path),
                    strategy_name=f"{self.name}:{loaded_sim.market_id}",
                    platform=self.data.platform,
                    initial_cash=self.initial_cash,
                    market_prices={str(loaded_sim.instrument.id): market_prices},
                    user_probabilities=user_probabilities,
                    market_probabilities=market_probabilities,
                    outcomes=outcomes,
                    open_browser=False,
                )
            except Exception as exc:
                print(f"Unable to save legacy chart for {loaded_sim.market_id}: {exc}")
            else:
                if self.emit_html:
                    artifacts["chart_path"] = save_legacy_backtest_layout(
                        chart_layout,
                        str(output_path),
                        chart_title,
                    )
                if self.return_chart_layout:
                    artifacts["chart_layout"] = chart_layout
                    artifacts["chart_title"] = chart_title

        if self.return_summary_series:
            artifacts.update(
                self._build_single_market_summary_series(
                    engine=engine,
                    loaded_sim=loaded_sim,
                    fills_report=fills_report,
                    market_prices=market_prices,
                    user_probabilities=user_probabilities,
                    market_probabilities=market_probabilities,
                    outcomes=outcomes,
                )
            )

        return artifacts

    def _build_single_market_summary_series(
        self,
        *,
        engine: BacktestEngine,
        loaded_sim: _LoadedMarketSim,
        fills_report: pd.DataFrame,
        market_prices: Any,
        user_probabilities: pd.Series,
        market_probabilities: pd.Series,
        outcomes: pd.Series,
    ) -> dict[str, Any]:
        legacy_models, _ = (
            prediction_market_research.legacy_plot_adapter._load_legacy_modules()
        )
        legacy_fills = prediction_market_research.legacy_plot_adapter._convert_fills(
            fills_report,
            legacy_models,
        )
        market_prices_with_fills = prediction_market_research.legacy_plot_adapter._market_prices_with_fill_points(
            {loaded_sim.market_id: market_prices},
            legacy_fills,
        ).get(loaded_sim.market_id, market_prices)
        dense_equity_series, dense_cash_series = (
            prediction_market_research._dense_account_series_from_engine(
                engine=engine,
                market_id=loaded_sim.market_id,
                market_prices=market_prices,
                initial_cash=self.initial_cash,
            )
        )
        pnl_series = (
            dense_equity_series - float(dense_equity_series.iloc[0])
            if not dense_equity_series.empty
            else prediction_market_research._extract_account_pnl_series(engine)
        )
        return {
            "price_series": prediction_market_research._series_to_iso_pairs(
                prediction_market_research._pairs_to_series(market_prices_with_fills)
            ),
            "pnl_series": prediction_market_research._series_to_iso_pairs(pnl_series)
            if not pnl_series.empty
            else [],
            "equity_series": prediction_market_research._series_to_iso_pairs(
                dense_equity_series
            )
            if not dense_equity_series.empty
            else [],
            "cash_series": prediction_market_research._series_to_iso_pairs(
                dense_cash_series
            )
            if not dense_cash_series.empty
            else [],
            "user_probability_series": prediction_market_research._series_to_iso_pairs(
                user_probabilities
            )
            if not user_probabilities.empty
            else [],
            "market_probability_series": prediction_market_research._series_to_iso_pairs(
                market_probabilities
            )
            if not market_probabilities.empty
            else [],
            "outcome_series": prediction_market_research._series_to_iso_pairs(outcomes)
            if not outcomes.empty
            else [],
            "fill_events": prediction_market_research._serialize_fill_events(
                market_id=loaded_sim.market_id,
                fills_report=fills_report,
            ),
        }

    def _filter_report_rows(
        self, report: pd.DataFrame, *, instrument_id: str
    ) -> pd.DataFrame:
        if report.empty or "instrument_id" not in report.columns:
            return pd.DataFrame()
        return report.loc[report["instrument_id"] == instrument_id].copy()

    def _serialize_fill_events(
        self, *, market_id: str, fills_report: pd.DataFrame
    ) -> list[dict[str, Any]]:
        if fills_report.empty:
            return []

        frame = fills_report.copy()
        if frame.index.name and frame.index.name not in frame.columns:
            frame = frame.reset_index()

        events: list[dict[str, Any]] = []
        for idx, (_, row) in enumerate(frame.iterrows(), start=1):
            quantity = self._parse_float_like(
                row.get("filled_qty", row.get("last_qty", row.get("quantity")))
            )
            if quantity <= 0.0:
                continue

            timestamp = pd.to_datetime(
                row.get("ts_last", row.get("ts_event", row.get("ts_init"))),
                utc=True,
                errors="coerce",
            )
            if pd.isna(timestamp):
                continue
            assert isinstance(timestamp, pd.Timestamp)

            events.append(
                {
                    "order_id": str(
                        row.get("client_order_id")
                        or row.get("venue_order_id")
                        or row.get("order_id")
                        or f"fill-{idx}"
                    ),
                    "market_id": market_id,
                    "action": str(row.get("side") or row.get("order_side") or "BUY")
                    .strip()
                    .lower(),
                    "side": "yes",
                    "price": self._parse_float_like(
                        row.get("avg_px", row.get("last_px", row.get("price")))
                    ),
                    "quantity": quantity,
                    "timestamp": timestamp.isoformat(),
                    "commission": self._parse_float_like(
                        row.get("commissions", row.get("commission", row.get("fees")))
                    ),
                },
            )

        events.sort(key=lambda event: event["timestamp"])
        return events

    def _parse_float_like(self, value: Any) -> float:
        if value is None:
            return 0.0
        if isinstance(value, int | float):
            return float(value)
        text = str(value).strip().replace("_", "").replace("\u2212", "-")
        if not text:
            return 0.0
        for token in text.split():
            try:
                return float(token)
            except ValueError:
                continue
        return 0.0


def finalize_market_results(
    *,
    name: str,
    results: Sequence[dict[str, Any]],
    report: MarketReportConfig,
) -> None:
    market_key = _resolve_report_market_key(
        results=results, configured_key=report.market_key
    )
    print_backtest_summary(
        results=list(results),
        market_key=market_key,
        count_key=report.count_key,
        count_label=report.count_label,
        pnl_label=report.pnl_label,
    )
    print_backtest_result_warnings(results=results, market_key=market_key)

    if len(results) == 1:
        chart_path = results[0].get("chart_path")
        if chart_path is not None:
            print(f"\nLegacy chart saved to {chart_path}")

    if report.combined_report and report.combined_report_path is not None:
        combined_path = save_combined_backtest_report(
            results=list(results),
            output_path=report.combined_report_path,
            title=f"{name} combined legacy chart",
            market_key=market_key,
            pnl_label=report.pnl_label,
        )
        if combined_path is not None:
            print(f"\nCombined legacy chart saved to {combined_path}")

    if report.summary_report and report.summary_report_path is not None:
        summary_path = save_aggregate_backtest_report(
            results=list(results),
            output_path=report.summary_report_path,
            title=f"{name} legacy multi-market chart",
            market_key=market_key,
            pnl_label=report.pnl_label,
        )
        if summary_path is not None:
            print(f"\nLegacy multi-market chart saved to {summary_path}")


def _resolve_report_market_key(
    *,
    results: Sequence[dict[str, Any]],
    configured_key: str,
) -> str:
    if not results:
        return configured_key

    first_result = results[0]
    if configured_key in first_result:
        return configured_key

    for fallback_key in ("slug", "ticker"):
        if fallback_key in first_result:
            return fallback_key

    return configured_key


def run_reported_backtest(
    *,
    backtest: PredictionMarketBacktest,
    report: MarketReportConfig,
    empty_message: str | None = None,
) -> list[dict[str, Any]]:
    results = backtest.run()
    if not results:
        if empty_message:
            print(empty_message)
        return []

    finalize_market_results(name=backtest.name, results=results, report=report)
    return results


__all__ = [
    "MarketReportConfig",
    "MarketSimConfig",
    "PredictionMarketBacktest",
    "_LoadedMarketSim",
    "finalize_market_results",
    "run_reported_backtest",
]
