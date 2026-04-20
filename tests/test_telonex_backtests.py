from __future__ import annotations

import importlib

import pytest
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from prediction_market_extensions.backtesting._strategy_configs import build_strategies_from_configs
from strategies import QuoteTickDeepValueHoldConfig, QuoteTickDeepValueHoldStrategy

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))
FAKE_TELONEX_API_KEY = "test-telonex-key"
EXPECTED_TELONEX_SOURCES = (
    "local:/Volumes/LaCie/telonex_data",
    f"api:{FAKE_TELONEX_API_KEY}",
)
EXPECTED_DETAIL_PLOT_PANELS = (
    "total_equity",
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "total_drawdown",
    "drawdown",
    "total_rolling_sharpe",
    "rolling_sharpe",
    "total_cash_equity",
    "cash_equity",
    "monthly_returns",
    "total_brier_advantage",
    "brier_advantage",
)
EXPECTED_SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)


def test_telonex_joint_portfolio_runner_uses_local_first_sources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import sys

    monkeypatch.setenv("TELONEX_API_KEY", FAKE_TELONEX_API_KEY)
    sys.modules.pop("backtests.polymarket_telonex_quote_tick_joint_portfolio_runner", None)
    module = importlib.import_module(
        "backtests.polymarket_telonex_quote_tick_joint_portfolio_runner"
    )
    captured: dict[str, object] = {}

    def _fake_run_experiment(experiment):  # type: ignore[no-untyped-def]
        captured["experiment"] = experiment
        return []

    monkeypatch.setattr(module, "run_experiment", _fake_run_experiment)

    module.run()

    strategies = build_strategies_from_configs(
        strategy_configs=module.STRATEGY_CONFIGS, instrument_id=INSTRUMENT_ID
    )
    assert len(strategies) == 1
    strategy = strategies[0]

    assert isinstance(strategy, QuoteTickDeepValueHoldStrategy)
    assert isinstance(strategy.config, QuoteTickDeepValueHoldConfig)
    assert module.DATA.vendor == "telonex"
    assert module.DATA.sources == EXPECTED_TELONEX_SOURCES
    assert len(module.REPLAYS) == 5
    assert module.EXECUTION.queue_position is False
    assert module.DETAIL_PLOT_PANELS == EXPECTED_DETAIL_PLOT_PANELS
    assert module.SUMMARY_PLOT_PANELS == EXPECTED_SUMMARY_PLOT_PANELS
    assert module.REPORT.summary_report is True
    assert module.REPORT.summary_report_path == module.SUMMARY_REPORT_PATH
    assert module.EXPERIMENT.report == module.REPORT
    assert module.EXPERIMENT.return_summary_series is True
    assert module.EXPERIMENT.multi_replay_mode == "joint_portfolio"
    assert captured["experiment"] is module.EXPERIMENT
