from __future__ import annotations

import asyncio
import importlib
from types import SimpleNamespace

import pytest

from prediction_market_extensions.backtesting import (
    _polymarket_trade_tick_multi_runner as multi_runner,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketReportConfig,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketSimConfig,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    PredictionMarketBacktest,
)
from prediction_market_extensions.backtesting._prediction_market_runner import (
    MarketDataConfig,
)


def test_multi_market_trade_runner_forwards_explicit_output_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[PredictionMarketBacktest] = []

    async def _fake_run_async(self):  # type: ignore[no-untyped-def]
        captured.append(self)
        return [
            {
                "slug": self.replays[0].market_slug,
                "trades": 2,
                "fills": 0,
                "pnl": 0.0,
                "chart_path": self.chart_output_path,
                "price_series": [],
                "pnl_series": [],
                "equity_series": [],
                "cash_series": [],
                "fill_events": [],
            }
        ]

    monkeypatch.setattr(
        PredictionMarketBacktest,
        "run_async",
        _fake_run_async,
    )

    backtest = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(
            platform="polymarket",
            data_type="trade_tick",
            vendor="native",
            sources=("demo-source",),
        ),
        sims=(
            MarketSimConfig(
                market_slug="demo-market",
                lookback_days=7,
                start_time="2026-03-01T00:00:00Z",
                end_time="2026-03-01T06:00:00Z",
                metadata={"sim_label": "sample-a"},
            ),
            MarketSimConfig(
                market_slug="demo-market",
                lookback_days=7,
                start_time="2026-03-01T06:00:00Z",
                end_time="2026-03-01T12:00:00Z",
                metadata={"sim_label": "sample-b"},
            ),
        ),
        strategy_factory=lambda instrument_id: SimpleNamespace(
            instrument_id=instrument_id
        ),
        initial_cash=100.0,
        probability_window=5,
        min_trades=10,
        emit_html=True,
        chart_output_path="output",
        return_summary_series=True,
    )

    results = asyncio.run(
        multi_runner.run_multi_market_trade_backtest_async(backtest=backtest)
    )

    assert len(results) == 2
    assert captured[0].data.sources == ("demo-source",)
    assert captured[0].replays[0].start_time == "2026-03-01T00:00:00Z"
    assert captured[0].replays[0].end_time == "2026-03-01T06:00:00Z"
    assert captured[0].return_summary_series is True
    assert captured[0].chart_output_path != captured[1].chart_output_path
    assert "sample-a" in str(captured[0].chart_output_path)
    assert "sample-b" in str(captured[1].chart_output_path)


def test_run_reported_multi_market_trade_backtest_applies_result_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    async def _fake_run_multi_market_trade_backtest_async(*, backtest):  # type: ignore[no-untyped-def]
        _ = backtest
        return [
            {"slug": "demo-a", "trades": 10, "fills": 1, "pnl": 1.0},
            {"slug": "demo-b", "trades": 12, "fills": 1, "pnl": 2.0},
        ]

    def _fake_finalize_market_results(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)

    class _Policy:
        def apply(self, results):  # type: ignore[no-untyped-def]
            return [
                {**result, "pnl": float(result["pnl"]) + 10.0} for result in results
            ]

    monkeypatch.setattr(
        multi_runner,
        "run_multi_market_trade_backtest_async",
        _fake_run_multi_market_trade_backtest_async,
    )
    monkeypatch.setattr(
        multi_runner,
        "finalize_market_results",
        _fake_finalize_market_results,
    )

    backtest = PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(
            platform="polymarket",
            data_type="trade_tick",
            vendor="native",
        ),
        sims=(MarketSimConfig(market_slug="demo-market", lookback_days=7),),
        strategy_factory=lambda instrument_id: SimpleNamespace(
            instrument_id=instrument_id
        ),
        initial_cash=100.0,
        probability_window=5,
    )
    report = MarketReportConfig(
        count_key="trades",
        count_label="Trades",
        pnl_label="PnL (USDC)",
    )

    results = multi_runner.run_reported_multi_market_trade_backtest(
        backtest=backtest,
        report=report,
        result_policy=_Policy(),
    )

    assert [result["pnl"] for result in results] == [11.0, 12.0]
    assert captured["name"] == "demo"
    assert captured["report"] == report
    assert captured["results"] == results


@pytest.mark.parametrize(
    "module_name",
    [
        "backtests.polymarket_trade_tick_multi_sim_runner",
    ],
)
def test_trade_tick_multi_runner_uses_reported_multi_runner(
    monkeypatch: pytest.MonkeyPatch,
    module_name: str,
) -> None:
    module = importlib.import_module(module_name)
    captured: dict[str, object] = {}

    def _fake_run_reported_multi_market_trade_backtest(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        return []

    monkeypatch.setattr(
        module,
        "run_reported_multi_market_trade_backtest",
        _fake_run_reported_multi_market_trade_backtest,
    )

    module.run()

    assert module.CHART_OUTPUT_PATH == "output"
    assert module.SUMMARY_REPORT_PATH == f"output/{module.NAME}_multi_market.html"
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.EXPERIMENT.chart_output_path == "output"
    assert module.EXPERIMENT.return_summary_series is True
    assert captured["report"] == module.REPORT
    assert captured["backtest"].name == module.NAME
    assert captured["backtest"].data == module.DATA
    assert captured["backtest"].replays == module.REPLAYS
    assert captured["backtest"].chart_output_path == "output"
    assert "result_policy" not in captured
