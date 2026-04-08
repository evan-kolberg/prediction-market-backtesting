from __future__ import annotations

from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._replay_specs import MarketSimConfig


def _build_backtest(**kwargs) -> PredictionMarketBacktest:
    return PredictionMarketBacktest(
        name="demo",
        data=MarketDataConfig(
            platform="polymarket",
            data_type="quote_tick",
            vendor="pmxt",
        ),
        sims=(
            MarketSimConfig(
                market_slug="demo-market",
                start_time="2026-02-21T16:00:00Z",
                end_time="2026-02-21T17:00:00Z",
            ),
        ),
        initial_cash=100.0,
        probability_window=5,
        **kwargs,
    )


def test_strategy_summary_label_uses_config_count() -> None:
    backtest = _build_backtest(
        strategy_configs=(
            {
                "strategy_path": "strategies:QuoteTickVWAPReversionStrategy",
                "config_path": "strategies:QuoteTickVWAPReversionConfig",
                "config": {"vwap_window": 30},
            },
        )
    )

    assert backtest._strategy_summary_label() == "1 strategy config(s)"


def test_strategy_summary_label_reports_factory() -> None:
    backtest = _build_backtest(
        strategy_factory=lambda instrument_id: instrument_id,
    )

    assert backtest._strategy_summary_label() == "a strategy factory"
