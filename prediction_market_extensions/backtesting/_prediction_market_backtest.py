from __future__ import annotations

import asyncio
import warnings
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from typing import Any

import pandas as pd
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.common.component import is_backtest_force_stop
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyFactory as NautilusStrategyFactory
from nautilus_trader.model.identifiers import InstrumentId, TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.risk.config import RiskEngineConfig
from nautilus_trader.trading.strategy import Strategy

from prediction_market_extensions._runtime_log import log_info
from prediction_market_extensions import install_commission_patch
from prediction_market_extensions.adapters.prediction_market import (
    LoadedReplay,
    ReplayCoverageStats,
    ReplayLoadRequest,
    ReplayWindow,
)
from prediction_market_extensions.adapters.prediction_market.fill_model import (
    PredictionMarketTakerFillModel,
)
from prediction_market_extensions.backtesting._backtest_runtime import (
    add_engine_data_by_type,
    build_backtest_run_state,
)
from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._market_data_config import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import ReplaySpec
from prediction_market_extensions.backtesting._result_policies import (
    apply_joint_portfolio_settlement_pnl,
    apply_repo_research_disclosures,
)
from prediction_market_extensions.backtesting._strategy_configs import (
    StrategyConfigSpec,
    build_importable_strategy_configs,
)
from prediction_market_extensions.backtesting.data_sources.kalshi_native import (
    RunnerKalshiDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.pmxt import (
    RunnerPolymarketPMXTDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.polymarket_native import (
    RunnerPolymarketDataLoader,
)
from prediction_market_extensions.backtesting.data_sources.registry import resolve_replay_adapter
from prediction_market_extensions.backtesting.prediction_market import (
    MarketReportConfig,
    PredictionMarketArtifactBuilder,
    finalize_market_results,
    run_reported_backtest,
)

KalshiDataLoader = RunnerKalshiDataLoader
PolymarketDataLoader = RunnerPolymarketDataLoader
PolymarketPMXTDataLoader = RunnerPolymarketPMXTDataLoader


type StrategyFactory = Callable[[InstrumentId], Strategy]

LARGE_DATA_GAP_NS = 4 * 60 * 60 * 1_000_000_000
REPO_STATUS_TOPIC = "prediction_market.backtest.status"


def _record_ts_event(record: Any) -> int | None:
    value = getattr(record, "ts_event", None)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _largest_record_gap_ns(records: Sequence[Any]) -> int | None:
    previous_ts: int | None = None
    largest_gap: int | None = None
    for record in records:
        ts_event = _record_ts_event(record)
        if ts_event is None:
            continue
        if previous_ts is not None and ts_event > previous_ts:
            gap = ts_event - previous_ts
            largest_gap = gap if largest_gap is None else max(largest_gap, gap)
        previous_ts = ts_event
    return largest_gap


def _emit_engine_status(engine: BacktestEngine, message: str) -> None:
    try:
        msgbus = engine.kernel.msgbus
        logger = engine.kernel.logger
        if not getattr(engine, "_prediction_market_status_listener", False):
            msgbus.subscribe(REPO_STATUS_TOPIC, lambda msg: logger.info(str(msg)))
            setattr(engine, "_prediction_market_status_listener", True)
        msgbus.publish(REPO_STATUS_TOPIC, message, external_pub=False)
    except Exception:
        print(message)


def _serialize_engine_result_stats(engine_result: Any) -> dict[str, Any]:
    return {
        "iterations": int(getattr(engine_result, "iterations", 0) or 0),
        "total_events": int(getattr(engine_result, "total_events", 0) or 0),
        "total_orders": int(getattr(engine_result, "total_orders", 0) or 0),
        "total_positions": int(getattr(engine_result, "total_positions", 0) or 0),
        "elapsed_time": float(getattr(engine_result, "elapsed_time", 0.0) or 0.0),
        "stats_pnls": dict(getattr(engine_result, "stats_pnls", {}) or {}),
        "stats_returns": dict(getattr(engine_result, "stats_returns", {}) or {}),
    }


class PredictionMarketBacktest:
    def __init__(
        self,
        *,
        name: str,
        data: MarketDataConfig,
        replays: Sequence[ReplaySpec],
        strategy_configs: Sequence[StrategyConfigSpec] = (),
        strategy_factory: StrategyFactory | None = None,
        initial_cash: float,
        probability_window: int,
        min_book_events: int = 0,
        min_price_range: float = 0.0,
        default_lookback_days: int | None = None,
        default_lookback_hours: float | None = None,
        default_start_time: pd.Timestamp | datetime | str | None = None,
        default_end_time: pd.Timestamp | datetime | str | None = None,
        nautilus_log_level: str = "INFO",
        execution: ExecutionModelConfig | None = None,
        chart_resample_rule: str | None = None,
        return_summary_series: bool = False,
    ) -> None:
        if strategy_factory is not None and strategy_configs:
            raise ValueError("Use strategy_factory or strategy_configs, not both.")
        if strategy_factory is None and not strategy_configs:
            raise ValueError("strategy_configs is required when strategy_factory is not provided.")
        if not replays:
            raise ValueError("replays is required.")

        self.name = name
        self.data = data
        self.replays = self._normalize_replays(tuple(replays))
        self.strategy_configs = tuple(strategy_configs)
        self.strategy_factory = strategy_factory
        if initial_cash <= 0:
            raise ValueError(f"initial_cash must be positive, got {initial_cash}")
        self.initial_cash = float(initial_cash)
        self.probability_window = int(probability_window)
        self.min_book_events = int(min_book_events)
        self.min_price_range = float(min_price_range)
        self.default_lookback_days = default_lookback_days
        self.default_lookback_hours = default_lookback_hours
        self.default_start_time = default_start_time
        self.default_end_time = default_end_time
        self.nautilus_log_level = nautilus_log_level
        self.execution = execution if execution is not None else ExecutionModelConfig()
        self.chart_resample_rule = chart_resample_rule
        self.return_summary_series = return_summary_series

    def _strategy_summary_label(self) -> str:
        if self.strategy_configs:
            return f"{len(self.strategy_configs)} strategy config(s)"
        if self.strategy_factory is not None:
            return "a strategy factory"
        return "0 strategy config(s)"

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
        install_commission_patch()
        loaded_sims = await self._load_sims_async()
        if not loaded_sims:
            return []
        seen_instrument_ids: set[str] = set()
        for loaded_sim in loaded_sims:
            instrument_id = str(loaded_sim.instrument.id)
            if instrument_id in seen_instrument_ids:
                raise ValueError(
                    "Duplicate instruments are not allowed in a joint-portfolio run: "
                    f"{instrument_id}"
                )
            seen_instrument_ids.add(instrument_id)

        engine = self._build_engine()
        try:
            for loaded_sim in loaded_sims:
                engine.add_instrument(loaded_sim.instrument)
                add_engine_data_by_type(engine, list(loaded_sim.records))

            if self.strategy_factory is not None:
                for loaded_sim in loaded_sims:
                    engine.add_strategy(self.strategy_factory(loaded_sim.instrument.id))
            else:
                for importable_config in self._build_importable_strategy_configs(loaded_sims):
                    engine.add_strategy(NautilusStrategyFactory.create(importable_config))

            _emit_engine_status(
                engine,
                f"Starting {self.name} with {len(loaded_sims)} sims and {self._strategy_summary_label()}...",
            )
            engine.run()
            engine_result = engine.get_result()
            forced_stop = bool(is_backtest_force_stop())

            fills_report = engine.trader.generate_order_fills_report()
            positions_report = engine.trader.generate_positions_report()
            market_artifacts_by_instrument_id = self._build_market_artifacts(
                engine=engine, loaded_sims=loaded_sims, fills_report=fills_report
            )
            joint_portfolio_artifacts = self._build_joint_portfolio_artifacts(
                engine=engine, loaded_sims=loaded_sims
            )
            results = [
                self._build_result(
                    loaded_sim=loaded_sim,
                    fills_report=fills_report,
                    positions_report=positions_report,
                    market_artifacts=market_artifacts_by_instrument_id.get(
                        str(loaded_sim.instrument.id)
                    ),
                    joint_portfolio_artifacts=joint_portfolio_artifacts
                    if result_index == 0
                    else None,
                    run_state=build_backtest_run_state(
                        data=loaded_sim.records,
                        backtest_end_ns=engine_result.backtest_end,
                        forced_stop=forced_stop,
                        requested_start_ns=loaded_sim.requested_window.start_ns,
                        requested_end_ns=loaded_sim.requested_window.end_ns,
                    ),
                )
                for result_index, loaded_sim in enumerate(loaded_sims)
            ]
            apply_joint_portfolio_settlement_pnl(results)
            if results:
                results[0]["portfolio_stats"] = _serialize_engine_result_stats(engine_result)
            return apply_repo_research_disclosures(results)
        finally:
            engine.reset()
            engine.dispose()

    async def run_backtest_async(self) -> list[dict[str, Any]]:
        return await self.run_async()

    def _create_artifact_builder(self) -> PredictionMarketArtifactBuilder:
        return PredictionMarketArtifactBuilder(
            name=self.name,
            platform=self.data.platform,
            data_type=self.data.data_type,
            initial_cash=self.initial_cash,
            probability_window=self.probability_window,
            chart_resample_rule=self.chart_resample_rule,
            return_summary_series=self.return_summary_series,
            sim_count=len(self.replays),
        )

    def _build_result(
        self,
        *,
        loaded_sim: LoadedReplay,
        fills_report: pd.DataFrame,
        positions_report: pd.DataFrame,
        market_artifacts: Mapping[str, Any] | None = None,
        joint_portfolio_artifacts: Mapping[str, Any] | None = None,
        run_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self._create_artifact_builder().build_result(
            loaded_sim=loaded_sim,
            fills_report=fills_report,
            positions_report=positions_report,
            market_artifacts=market_artifacts,
            joint_portfolio_artifacts=joint_portfolio_artifacts,
            run_state=run_state,
        )

    def _build_market_artifacts(
        self,
        *,
        engine: BacktestEngine,
        loaded_sims: Sequence[LoadedReplay],
        fills_report: pd.DataFrame,
    ) -> dict[str, dict[str, Any]]:
        return self._create_artifact_builder().build_market_artifacts(
            engine=engine, loaded_sims=loaded_sims, fills_report=fills_report
        )

    def _build_joint_portfolio_artifacts(
        self, *, engine: BacktestEngine, loaded_sims: Sequence[LoadedReplay]
    ) -> dict[str, Any]:
        return self._create_artifact_builder().build_joint_portfolio_artifacts(
            engine=engine, loaded_sims=loaded_sims
        )

    def _normalize_replays(self, replays: Sequence[ReplaySpec]) -> tuple[ReplaySpec, ...]:
        adapter = resolve_replay_adapter(
            platform=self.data.platform, data_type=self.data.data_type, vendor=self.data.vendor
        )
        for replay in replays:
            if not isinstance(replay, adapter.replay_spec_type):
                raise TypeError(
                    "Replay spec does not match selected adapter. "
                    f"Expected {adapter.replay_spec_type.__name__}, "
                    f"received {type(replay).__name__}."
                )
        return tuple(replays)

    def _load_request(self) -> ReplayLoadRequest:
        return ReplayLoadRequest(
            min_record_count=self.min_book_events,
            min_price_range=self.min_price_range,
            default_lookback_days=self.default_lookback_days,
            default_lookback_hours=self.default_lookback_hours,
            default_start_time=self.default_start_time,
            default_end_time=self.default_end_time,
        )

    async def _load_sims_async(self) -> list[LoadedReplay]:
        adapter = resolve_replay_adapter(
            platform=self.data.platform, data_type=self.data.data_type, vendor=self.data.vendor
        )
        with adapter.configure_sources(sources=self.data.sources) as data_source:
            log_info(data_source.summary)
            loaded_sims: list[LoadedReplay] = []
            request = self._load_request()
            for replay in self.replays:
                loaded_sim = await adapter.load_replay(replay, request=request)
                if loaded_sim is not None:
                    largest_gap_ns = _largest_record_gap_ns(loaded_sim.records)
                    if largest_gap_ns is not None and largest_gap_ns > LARGE_DATA_GAP_NS:
                        warnings.warn(
                            f"Loaded replay {loaded_sim.market_id!r} contains a "
                            f"{largest_gap_ns / 1_000_000_000 / 3600:.2f} hour data gap. "
                            "Time-dependent strategy behavior across this window may be "
                            "less reliable.",
                            RuntimeWarning,
                            stacklevel=2,
                        )
                    loaded_sims.append(loaded_sim)
            return loaded_sims

    def _build_engine(self) -> BacktestEngine:
        engine = BacktestEngine(
            config=BacktestEngineConfig(
                trader_id=TraderId("BACKTESTER-001"),
                logging=LoggingConfig(log_level=self.nautilus_log_level),
                risk_engine=RiskEngineConfig(),
            )
        )
        latency_model = self.execution.build_latency_model()
        adapter = resolve_replay_adapter(
            platform=self.data.platform, data_type=self.data.data_type, vendor=self.data.vendor
        )
        engine_profile = adapter.engine_profile
        fill_model = None
        if engine_profile.fill_model_mode == "taker":
            fill_model = PredictionMarketTakerFillModel(**self.execution.build_fill_model_kwargs())
        elif engine_profile.fill_model_mode != "passive_book":
            raise AssertionError(f"Unsupported fill model mode {engine_profile.fill_model_mode!r}")
        engine.add_venue(
            venue=engine_profile.venue,
            oms_type=engine_profile.oms_type,
            account_type=engine_profile.account_type,
            base_currency=engine_profile.base_currency,
            starting_balances=[Money(self.initial_cash, engine_profile.base_currency)],
            fill_model=fill_model,
            fee_model=engine_profile.fee_model_factory(),
            book_type=engine_profile.book_type,
            latency_model=latency_model,
            liquidity_consumption=engine_profile.liquidity_consumption,
            queue_position=self.execution.queue_position,
            bar_execution=False,
            trade_execution=True,
        )
        return engine

    def _build_importable_strategy_configs(self, loaded_sims: Sequence[LoadedReplay]) -> list[Any]:
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
                        strategy_configs=[bound_spec], instrument_id=loaded_sim.instrument.id
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
        loaded_sim: LoadedReplay,
        all_instrument_ids: Sequence[InstrumentId],
    ) -> StrategyConfigSpec:
        raw_config = strategy_spec.get("config", {})
        if not isinstance(raw_config, Mapping):
            raise TypeError("strategy config payload must be a mapping")

        metadata = dict(loaded_sim.metadata)
        metadata.setdefault("market_slug", getattr(loaded_sim.spec, "market_slug", None))
        metadata.setdefault("market_ticker", getattr(loaded_sim.spec, "market_ticker", None))
        metadata.setdefault("token_index", getattr(loaded_sim.spec, "token_index", 0))
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


def _LoadedMarketSim(
    *,
    spec: ReplaySpec,
    instrument: Any,
    records: Sequence[Any],
    count: int,
    count_key: str,
    market_key: str,
    market_id: str,
    outcome: str,
    realized_outcome: float | None,
    prices: Sequence[float],
    metadata: Mapping[str, Any] | None,
    requested_start_ns: int | None,
    requested_end_ns: int | None,
) -> LoadedReplay:
    instrument_id = getattr(instrument, "id", None)
    return LoadedReplay(
        replay=spec,
        instrument=instrument,
        records=tuple(records),
        outcome=outcome,
        realized_outcome=realized_outcome,
        metadata=dict(metadata or {}),
        requested_window=ReplayWindow(start_ns=requested_start_ns, end_ns=requested_end_ns),
        loaded_window=None,
        coverage_stats=ReplayCoverageStats(
            count=count,
            count_key=count_key,
            market_key=market_key,
            market_id=market_id,
            prices=tuple(prices),
        ),
        instrument_ids=(instrument_id,) if instrument_id is not None else (),
    )


__all__ = [
    "KalshiDataLoader",
    "LoadedReplay",
    "MarketReportConfig",
    "PolymarketDataLoader",
    "PolymarketPMXTDataLoader",
    "PredictionMarketBacktest",
    "_LoadedMarketSim",
    "finalize_market_results",
    "run_reported_backtest",
]
