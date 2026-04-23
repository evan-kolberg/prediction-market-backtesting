from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine

from prediction_market_extensions.adapters.prediction_market import LoadedReplay
from prediction_market_extensions.adapters.prediction_market import (
    research as prediction_market_research,
)
from prediction_market_extensions.adapters.prediction_market.backtest_utils import (
    build_brier_inputs,
    build_market_prices,
    downsample_price_points,
    extract_price_points,
    extract_realized_pnl,
)
from prediction_market_extensions.analysis.legacy_plot_adapter import (
    build_legacy_backtest_layout,
    save_legacy_backtest_layout,
)
from prediction_market_extensions.backtesting._backtest_runtime import apply_backtest_run_state
from prediction_market_extensions.backtesting._result_policies import (
    apply_binary_settlement_pnl,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def resolve_repo_relative_path(path_like: str | Path) -> Path:
    path = Path(path_like).expanduser()
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


@dataclass(frozen=True)
class PredictionMarketArtifactBuilder:
    name: str
    platform: str
    data_type: str
    initial_cash: float
    probability_window: int
    chart_resample_rule: str | None
    emit_html: bool
    chart_output_path: str | Path | None
    return_chart_layout: bool
    return_summary_series: bool
    detail_plot_panels: Sequence[str]
    sim_count: int

    def build_result(
        self,
        *,
        loaded_sim: LoadedReplay,
        fills_report: pd.DataFrame,
        positions_report: pd.DataFrame,
        market_artifacts: Mapping[str, Any] | None = None,
        joint_portfolio_artifacts: Mapping[str, Any] | None = None,
        run_state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        instrument_id = str(loaded_sim.instrument.id)
        instrument_fills = self._filter_report_rows(fills_report, instrument_id=instrument_id)
        instrument_positions = self._filter_report_rows(
            positions_report, instrument_id=instrument_id
        )
        fill_events = prediction_market_research._serialize_fill_events(
            market_id=loaded_sim.market_id,
            fills_report=instrument_fills,
        )

        pnl = extract_realized_pnl(instrument_positions)
        settlement_observable_ns = getattr(loaded_sim.instrument, "expiration_ns", None)
        result: dict[str, Any] = {
            loaded_sim.market_key: loaded_sim.market_id,
            loaded_sim.count_key: loaded_sim.count,
            "fills": len(instrument_fills),
            "pnl": float(pnl),
            "instrument_id": instrument_id,
            "outcome": loaded_sim.outcome,
            "realized_outcome": loaded_sim.realized_outcome,
            "token_index": getattr(loaded_sim.spec, "token_index", 0),
            "fill_events": fill_events,
            "settlement_observable_ns": settlement_observable_ns,
            "settlement_observable_time": (
                pd.Timestamp(settlement_observable_ns, unit="ns", tz="UTC").isoformat()
                if isinstance(settlement_observable_ns, int) and settlement_observable_ns > 0
                else None
            ),
        }
        market_slug = getattr(loaded_sim.spec, "market_slug", None)
        market_ticker = getattr(loaded_sim.spec, "market_ticker", None)
        if market_slug is not None:
            result["slug"] = market_slug
        if market_ticker is not None:
            result["ticker"] = market_ticker
        if loaded_sim.prices:
            result["entry_min"] = min(loaded_sim.prices)
            result["max"] = max(loaded_sim.prices)
            result["last"] = loaded_sim.prices[-1]
        if market_artifacts:
            result.update(market_artifacts)
        if joint_portfolio_artifacts:
            result.update(joint_portfolio_artifacts)
        result.update(dict(loaded_sim.metadata))
        result = apply_backtest_run_state(result=result, run_state=run_state or {})
        return apply_binary_settlement_pnl(result)

    def build_market_artifacts(
        self,
        *,
        engine: BacktestEngine,
        loaded_sims: Sequence[LoadedReplay],
        fills_report: pd.DataFrame,
    ) -> dict[str, dict[str, Any]]:
        include_portfolio_series = self.return_summary_series
        return {
            loaded_sim.market_id: self._build_market_artifacts_for_loaded_sim(
                engine=engine,
                loaded_sim=loaded_sim,
                fills_report=self._filter_report_rows(
                    fills_report, instrument_id=str(loaded_sim.instrument.id)
                ),
                include_portfolio_series=include_portfolio_series,
            )
            for loaded_sim in loaded_sims
        }

    def build_joint_portfolio_artifacts(
        self, *, engine: BacktestEngine, loaded_sims: Sequence[LoadedReplay]
    ) -> dict[str, Any]:
        if len(loaded_sims) <= 1 or not self.return_summary_series:
            return {}

        market_prices_by_instrument_id: dict[str, list[tuple[datetime, float]]] = {}
        for loaded_sim in loaded_sims:
            price_points = extract_price_points(
                loaded_sim.records,
                price_attr="mid_price" if self.data_type == "quote_tick" else "price",
            )
            price_points = downsample_price_points(price_points, max_points=5000)
            market_prices_by_instrument_id[str(loaded_sim.instrument.id)] = build_market_prices(
                price_points, resample_rule=self.chart_resample_rule
            )

        dense_equity_series, dense_cash_series = (
            prediction_market_research._dense_account_series_from_engine_for_markets(
                engine=engine,
                market_prices=market_prices_by_instrument_id,
                initial_cash=self.initial_cash,
            )
        )
        pnl_series = (
            dense_equity_series - float(dense_equity_series.iloc[0])
            if not dense_equity_series.empty
            else prediction_market_research._extract_account_pnl_series(engine)
        )
        return {
            "joint_portfolio_pnl_series": prediction_market_research._series_to_iso_pairs(
                pnl_series
            )
            if not pnl_series.empty
            else [],
            "joint_portfolio_equity_series": prediction_market_research._series_to_iso_pairs(
                dense_equity_series
            )
            if not dense_equity_series.empty
            else [],
            "joint_portfolio_cash_series": prediction_market_research._series_to_iso_pairs(
                dense_cash_series
            )
            if not dense_cash_series.empty
            else [],
        }

    def _build_market_artifacts_for_loaded_sim(
        self,
        *,
        engine: BacktestEngine,
        loaded_sim: LoadedReplay,
        fills_report: pd.DataFrame,
        include_portfolio_series: bool,
    ) -> dict[str, Any]:
        needs_detail_chart = self.emit_html or self.return_chart_layout
        summary_only = self.return_summary_series and not needs_detail_chart

        price_points = extract_price_points(
            loaded_sim.records,
            price_attr="mid_price" if self.data_type == "quote_tick" else "price",
        )
        if summary_only:
            price_points = downsample_price_points(price_points, max_points=5000)

        market_prices = build_market_prices(price_points, resample_rule=self.chart_resample_rule)
        artifact_warnings: list[str] = []
        user_probabilities, market_probabilities, outcomes = build_brier_inputs(
            price_points,
            window=self.probability_window,
            realized_outcome=loaded_sim.realized_outcome,
            warnings_out=artifact_warnings,
        )
        artifacts: dict[str, Any] = {}
        if artifact_warnings:
            artifacts["warnings"] = artifact_warnings

        chart_layout = None
        chart_title = f"{self.name}:{loaded_sim.market_id} legacy chart"
        if needs_detail_chart:
            output_path = self.resolve_chart_output_path(market_id=loaded_sim.market_id)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                chart_layout, chart_title = build_legacy_backtest_layout(
                    engine=engine,
                    output_path=str(output_path),
                    strategy_name=f"{self.name}:{loaded_sim.market_id}",
                    platform=self.platform,
                    initial_cash=self.initial_cash,
                    market_prices={str(loaded_sim.instrument.id): market_prices},
                    user_probabilities=user_probabilities,
                    market_probabilities=market_probabilities,
                    outcomes=outcomes,
                    open_browser=False,
                    plot_panels=self.detail_plot_panels,
                )
            except Exception as exc:
                print(f"Unable to save legacy chart for {loaded_sim.market_id}: {exc}")
            else:
                if self.emit_html:
                    artifacts["chart_path"] = save_legacy_backtest_layout(
                        chart_layout, str(output_path), chart_title
                    )
                if self.return_chart_layout:
                    artifacts["chart_layout"] = chart_layout
                    artifacts["chart_title"] = chart_title

        if self.return_summary_series:
            artifacts.update(
                self._build_market_summary_series(
                    engine=engine,
                    loaded_sim=loaded_sim,
                    fills_report=fills_report,
                    market_prices=market_prices,
                    user_probabilities=user_probabilities,
                    market_probabilities=market_probabilities,
                    outcomes=outcomes,
                    include_portfolio_series=include_portfolio_series,
                )
            )

        return artifacts

    def resolve_chart_output_path(self, *, market_id: str) -> Path:
        default_filename = f"{self.name}_{market_id}_legacy.html"
        configured_path = self.chart_output_path
        if configured_path is None:
            return resolve_repo_relative_path(Path("output") / default_filename)

        raw_path = str(configured_path)
        if "{" in raw_path:
            try:
                resolved = raw_path.format(name=self.name, market_id=market_id)
            except KeyError as exc:
                raise ValueError(
                    "chart_output_path may only reference {name} and {market_id}."
                ) from exc
            path = Path(resolved)
            if not path.suffix:
                path = path / default_filename
            return resolve_repo_relative_path(path)

        path = Path(raw_path)
        if path.suffix:
            if self.sim_count == 1:
                return resolve_repo_relative_path(path)
            return resolve_repo_relative_path(
                path.with_name(f"{path.stem}_{market_id}{path.suffix}")
            )
        return resolve_repo_relative_path(path / default_filename)

    def _build_market_summary_series(
        self,
        *,
        engine: BacktestEngine,
        loaded_sim: LoadedReplay,
        fills_report: pd.DataFrame,
        market_prices: Any,
        user_probabilities: pd.Series,
        market_probabilities: pd.Series,
        outcomes: pd.Series,
        include_portfolio_series: bool,
    ) -> dict[str, Any]:
        legacy_models, _ = prediction_market_research.legacy_plot_adapter._load_legacy_modules()
        legacy_fills = prediction_market_research.legacy_plot_adapter._convert_fills(
            fills_report, legacy_models
        )
        market_prices_with_fills = (
            prediction_market_research.legacy_plot_adapter._market_prices_with_fill_points(
                {loaded_sim.market_id: market_prices}, legacy_fills
            ).get(loaded_sim.market_id, market_prices)
        )
        fill_events = prediction_market_research._serialize_fill_events(
            market_id=loaded_sim.market_id, fills_report=fills_report
        )
        series_artifacts: dict[str, Any] = {
            "price_series": prediction_market_research._series_to_iso_pairs(
                prediction_market_research._pairs_to_series(market_prices_with_fills)
            ),
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
            "fill_events": fill_events,
        }
        if not include_portfolio_series:
            return series_artifacts

        dense_equity_series, dense_cash_series = (
            prediction_market_research._dense_market_account_series_from_fill_events(
                market_id=loaded_sim.market_id,
                market_prices=market_prices,
                fill_events=fill_events,
                initial_cash=self.initial_cash,
            )
        )
        pnl_series = (
            dense_equity_series - float(dense_equity_series.iloc[0])
            if not dense_equity_series.empty
            else prediction_market_research._extract_account_pnl_series(engine)
        )
        series_artifacts["pnl_series"] = (
            prediction_market_research._series_to_iso_pairs(pnl_series)
            if not pnl_series.empty
            else []
        )
        series_artifacts["equity_series"] = (
            prediction_market_research._series_to_iso_pairs(dense_equity_series)
            if not dense_equity_series.empty
            else []
        )
        series_artifacts["cash_series"] = (
            prediction_market_research._series_to_iso_pairs(dense_cash_series)
            if not dense_cash_series.empty
            else []
        )
        return series_artifacts

    @staticmethod
    def _filter_report_rows(report: pd.DataFrame, *, instrument_id: str) -> pd.DataFrame:
        if report.empty or "instrument_id" not in report.columns:
            return pd.DataFrame()
        return report.loc[report["instrument_id"] == instrument_id].copy()


__all__ = ["PredictionMarketArtifactBuilder", "resolve_repo_relative_path"]
