from __future__ import annotations

from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_trader.adapters.prediction_market import (
    research as prediction_market_research,
)
from nautilus_trader.adapters.prediction_market import LoadedReplay
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    build_brier_inputs,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    build_market_prices,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    extract_price_points,
)
from nautilus_trader.adapters.prediction_market.backtest_utils import (
    extract_realized_pnl,
)
from nautilus_trader.analysis.legacy_plot_adapter import build_legacy_backtest_layout
from nautilus_trader.analysis.legacy_plot_adapter import save_legacy_backtest_layout
from nautilus_trader.backtest.engine import BacktestEngine

from backtests._shared._backtest_runtime import apply_backtest_run_state


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
            "token_index": getattr(loaded_sim.spec, "token_index", 0),
            "fill_events": prediction_market_research._serialize_fill_events(
                market_id=loaded_sim.market_id,
                fills_report=instrument_fills,
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
        if single_market_artifacts:
            result.update(single_market_artifacts)
        result.update(dict(loaded_sim.metadata))
        return apply_backtest_run_state(result=result, run_state=run_state or {})

    def build_single_market_artifacts(
        self,
        *,
        engine: BacktestEngine,
        loaded_sims: Sequence[LoadedReplay],
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
        loaded_sim: LoadedReplay,
        fills_report: pd.DataFrame,
    ) -> dict[str, Any]:
        price_points = extract_price_points(
            loaded_sim.records,
            price_attr="mid_price" if self.data_type == "quote_tick" else "price",
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

    def _build_single_market_summary_series(
        self,
        *,
        engine: BacktestEngine,
        loaded_sim: LoadedReplay,
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

    @staticmethod
    def _filter_report_rows(
        report: pd.DataFrame, *, instrument_id: str
    ) -> pd.DataFrame:
        if report.empty or "instrument_id" not in report.columns:
            return pd.DataFrame()
        return report.loc[report["instrument_id"] == instrument_id].copy()


__all__ = [
    "PredictionMarketArtifactBuilder",
    "resolve_repo_relative_path",
]
