from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING

from prediction_market_extensions.adapters.prediction_market.research import (
    print_backtest_summary,
    save_aggregate_backtest_report,
    save_joint_portfolio_backtest_report,
)
from prediction_market_extensions.analysis.legacy_backtesting.models import (
    DEFAULT_SUMMARY_PLOT_PANELS,
)
from prediction_market_extensions.backtesting._backtest_runtime import (
    print_backtest_result_warnings,
)
from prediction_market_extensions.backtesting.prediction_market.artifacts import (
    resolve_repo_relative_path,
)

if TYPE_CHECKING:
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        PredictionMarketBacktest,
    )


@dataclass(frozen=True)
class MarketReportConfig:
    count_key: str
    count_label: str
    pnl_label: str
    market_key: str = "slug"
    summary_report: bool = False
    summary_report_path: str | None = None
    summary_plot_panels: Sequence[str] | None = None


def finalize_market_results(
    *,
    name: str,
    results: Sequence[dict[str, object]],
    report: MarketReportConfig,
    multi_replay_mode: str = "joint_portfolio",
) -> None:
    market_key = _resolve_report_market_key(results=results, configured_key=report.market_key)
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

    if report.summary_report and report.summary_report_path is not None:
        plot_panels = (
            DEFAULT_SUMMARY_PLOT_PANELS
            if report.summary_plot_panels is None
            else tuple(report.summary_plot_panels)
        )
        if multi_replay_mode == "joint_portfolio" and len(results) > 1:
            summary_path = save_joint_portfolio_backtest_report(
                results=list(results),
                output_path=resolve_repo_relative_path(report.summary_report_path),
                title=f"{name} legacy joint-portfolio chart",
                market_key=market_key,
                pnl_label=report.pnl_label,
                plot_panels=plot_panels,
            )
            saved_label = "Legacy joint-portfolio chart saved to"
        else:
            summary_path = save_aggregate_backtest_report(
                results=list(results),
                output_path=resolve_repo_relative_path(report.summary_report_path),
                title=f"{name} legacy independent aggregate chart",
                market_key=market_key,
                pnl_label=report.pnl_label,
                plot_panels=plot_panels,
            )
            saved_label = "Legacy independent aggregate chart saved to"
        if summary_path is not None:
            print(f"\n{saved_label} {summary_path}")


def run_reported_backtest(
    *,
    backtest: PredictionMarketBacktest,
    report: MarketReportConfig,
    empty_message: str | None = None,
    multi_replay_mode: str = "joint_portfolio",
) -> list[dict[str, object]]:
    results = backtest.run()
    if not results:
        if empty_message:
            print(empty_message)
        return []

    finalize_market_results(
        name=backtest.name, results=results, report=report, multi_replay_mode=multi_replay_mode
    )
    return results


def _resolve_report_market_key(*, results: Sequence[dict[str, object]], configured_key: str) -> str:
    if not results:
        return configured_key

    first_result = results[0]
    if configured_key in first_result:
        return configured_key

    for fallback_key in ("slug", "ticker"):
        if fallback_key in first_result:
            return fallback_key

    return configured_key


__all__ = ["MarketReportConfig", "finalize_market_results", "run_reported_backtest"]
