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
from prediction_market_extensions.backtesting._result_policies import (
    apply_joint_portfolio_settlement_pnl,
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
) -> None:
    results = apply_joint_portfolio_settlement_pnl(list(results))
    market_key = _resolve_report_market_key(results=results, configured_key=report.market_key)
    print_backtest_summary(
        results=list(results),
        market_key=market_key,
        count_key=report.count_key,
        count_label=report.count_label,
        pnl_label=report.pnl_label,
    )
    print_backtest_result_warnings(results=results, market_key=market_key)

    if not (report.summary_report and report.summary_report_path is not None):
        return

    plot_panels = (
        DEFAULT_SUMMARY_PLOT_PANELS
        if report.summary_plot_panels is None
        else tuple(report.summary_plot_panels)
    )
    if len(results) > 1:
        summary_path = save_joint_portfolio_backtest_report(
            results=list(results),
            output_path=resolve_repo_relative_path(report.summary_report_path),
            title=f"{name} joint-portfolio summary",
            market_key=market_key,
            pnl_label=report.pnl_label,
            plot_panels=plot_panels,
        )
    else:
        summary_path = save_aggregate_backtest_report(
            results=list(results),
            output_path=resolve_repo_relative_path(report.summary_report_path),
            title=f"{name} summary",
            market_key=market_key,
            pnl_label=report.pnl_label,
            plot_panels=plot_panels,
        )
    if summary_path is not None:
        print(f"\nSummary report saved to {summary_path}")


def run_reported_backtest(
    *,
    backtest: PredictionMarketBacktest,
    report: MarketReportConfig,
    empty_message: str | None = None,
) -> list[dict[str, object]]:
    results = backtest.run()
    if not results:
        if empty_message:
            print(empty_message)
        return []

    finalize_market_results(name=backtest.name, results=results, report=report)
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
