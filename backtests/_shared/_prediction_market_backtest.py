from __future__ import annotations

import asyncio
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_trader.adapters.kalshi.fee_model import KalshiProportionalFeeModel
from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.adapters.polymarket.fee_model import PolymarketFeeModel
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
from nautilus_trader.config import LoggingConfig
from nautilus_trader.config import StrategyFactory as NautilusStrategyFactory
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import BookType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.risk.config import RiskEngineConfig

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._strategy_configs import build_importable_strategy_configs
from backtests._shared._strategy_configs import StrategyConfigSpec
from backtests._shared.data_sources.kalshi_native import (
    RunnerKalshiDataLoader as KalshiDataLoader,
)
from backtests._shared.data_sources.kalshi_native import (
    configured_kalshi_native_data_source,
)
from backtests._shared.data_sources.pmxt import (
    RunnerPolymarketPMXTDataLoader as PolymarketPMXTDataLoader,
)
from backtests._shared.data_sources.pmxt import configured_pmxt_data_source
from backtests._shared.data_sources.polymarket_native import (
    RunnerPolymarketDataLoader as PolymarketDataLoader,
)
from backtests._shared.data_sources.polymarket_native import (
    configured_polymarket_native_data_source,
)


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


class PredictionMarketBacktest:
    def __init__(
        self,
        *,
        name: str,
        data: MarketDataConfig,
        sims: Sequence[MarketSimConfig],
        strategy_configs: Sequence[StrategyConfigSpec],
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
    ) -> None:
        self.name = name
        self.data = data
        self.sims = tuple(sims)
        self.strategy_configs = tuple(strategy_configs)
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

            for importable_config in self._build_importable_strategy_configs(
                loaded_sims
            ):
                engine.add_strategy(NautilusStrategyFactory.create(importable_config))

            print(
                f"Starting {self.name} with {len(loaded_sims)} sims "
                f"and {len(self.strategy_configs)} strategy config(s)..."
            )
            engine.run()

            fills_report = engine.trader.generate_order_fills_report()
            positions_report = engine.trader.generate_positions_report()
            chart_paths = self._build_single_market_chart_paths(
                engine=engine,
                loaded_sims=loaded_sims,
            )
            return [
                self._build_result(
                    loaded_sim=loaded_sim,
                    fills_report=fills_report,
                    positions_report=positions_report,
                    chart_path=chart_paths.get(str(loaded_sim.instrument.id)),
                )
                for loaded_sim in loaded_sims
            ]
        finally:
            engine.reset()
            engine.dispose()

    async def run_backtest_async(self) -> list[dict[str, Any]]:
        return await self.run_async()

    async def _load_sims_async(self) -> list[_LoadedMarketSim]:
        if (
            self.data.platform == "polymarket"
            and self.data.data_type == "trade_tick"
            and self.data.vendor == "native"
        ):
            with configured_polymarket_native_data_source(
                sources=self.data.sources
            ) as data_source:
                print(data_source.summary)
                loaded_sims: list[_LoadedMarketSim] = []
                for sim in self.sims:
                    loaded_sim = await self._load_polymarket_trade_tick_sim(sim)
                    if loaded_sim is not None:
                        loaded_sims.append(loaded_sim)
                return loaded_sims

        if (
            self.data.platform == "polymarket"
            and self.data.data_type == "quote_tick"
            and self.data.vendor == "pmxt"
        ):
            with configured_pmxt_data_source(sources=self.data.sources) as data_source:
                print(data_source.summary)
                loaded_sims: list[_LoadedMarketSim] = []
                for sim in self.sims:
                    loaded_sim = await self._load_polymarket_pmxt_quote_tick_sim(sim)
                    if loaded_sim is not None:
                        loaded_sims.append(loaded_sim)
                return loaded_sims

        if (
            self.data.platform == "kalshi"
            and self.data.data_type == "trade_tick"
            and self.data.vendor == "native"
        ):
            with configured_kalshi_native_data_source(
                sources=self.data.sources
            ) as data_source:
                print(data_source.summary)
                loaded_sims: list[_LoadedMarketSim] = []
                for sim in self.sims:
                    loaded_sim = await self._load_kalshi_trade_tick_sim(sim)
                    if loaded_sim is not None:
                        loaded_sims.append(loaded_sim)
                return loaded_sims

        raise NotImplementedError(
            "Unsupported backtest data selection: "
            f"platform={self.data.platform!r}, data_type={self.data.data_type!r}, "
            f"vendor={self.data.vendor!r}."
        )

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
        )

    def _build_engine(self) -> BacktestEngine:
        engine = BacktestEngine(
            config=BacktestEngineConfig(
                trader_id=TraderId("BACKTESTER-001"),
                logging=LoggingConfig(log_level=self.nautilus_log_level),
                risk_engine=RiskEngineConfig(bypass=True),
            ),
        )
        latency_model = self.execution.build_latency_model()

        if self.data.platform == "polymarket":
            engine.add_venue(
                venue=POLYMARKET_VENUE,
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                base_currency=USDC_POS,
                starting_balances=[Money(self.initial_cash, USDC_POS)],
                fill_model=None
                if self.data.data_type == "quote_tick"
                else PredictionMarketTakerFillModel(),
                fee_model=PolymarketFeeModel(),
                book_type=BookType.L2_MBP
                if self.data.data_type == "quote_tick"
                else BookType.L1_MBP,
                latency_model=latency_model,
                liquidity_consumption=self.data.data_type == "quote_tick",
                queue_position=self.execution.queue_position,
            )
            return engine

        if self.data.platform == "kalshi":
            engine.add_venue(
                venue=Venue("KALSHI"),
                oms_type=OmsType.NETTING,
                account_type=AccountType.CASH,
                base_currency=USD,
                starting_balances=[Money(self.initial_cash, USD)],
                fill_model=PredictionMarketTakerFillModel(),
                fee_model=KalshiProportionalFeeModel(),
                book_type=BookType.L1_MBP,
                latency_model=latency_model,
                queue_position=self.execution.queue_position,
            )
            return engine

        raise NotImplementedError(
            f"Unsupported platform for engine construction: {self.data.platform!r}"
        )

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
        chart_path: str | None = None,
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
        if chart_path is not None:
            result["chart_path"] = chart_path
        result.update(dict(loaded_sim.metadata))
        return result

    def _build_single_market_chart_paths(
        self,
        *,
        engine: BacktestEngine,
        loaded_sims: Sequence[_LoadedMarketSim],
    ) -> dict[str, str]:
        if len(loaded_sims) != 1:
            return {}

        loaded_sim = loaded_sims[0]
        chart_path = self._save_single_market_chart(
            engine=engine,
            loaded_sim=loaded_sim,
        )
        if chart_path is None:
            return {}
        return {str(loaded_sim.instrument.id): chart_path}

    def _save_single_market_chart(
        self,
        *,
        engine: BacktestEngine,
        loaded_sim: _LoadedMarketSim,
    ) -> str | None:
        price_points = extract_price_points(
            loaded_sim.records,
            price_attr="mid_price" if self.data.data_type == "quote_tick" else "price",
        )
        market_prices = build_market_prices(price_points)
        user_probabilities, market_probabilities, outcomes = build_brier_inputs(
            price_points,
            window=self.probability_window,
            realized_outcome=loaded_sim.realized_outcome,
        )
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
            return None

        return save_legacy_backtest_layout(
            chart_layout,
            str(output_path),
            chart_title,
        )

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
