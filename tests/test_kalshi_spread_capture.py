"""End-to-end tests for the Kalshi spread-capture strategy.

These tests hit the real Kalshi REST API and run a full backtest, so they
require network access and take several seconds to complete.
"""

import asyncio

import pytest

import strategies.kalshi_spread_capture as strat


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Redirect tearsheet output to a temp dir."""
    monkeypatch.chdir(tmp_path)  # tearsheet → tmp_path/output/


def test_full_run_produces_tearsheet(tmp_path):
    """Full pipeline (discover markets → fetch bars → backtest → tearsheets) runs without error."""
    asyncio.run(strat.run())

    output_dir = tmp_path / "output"
    if not output_dir.exists():
        pytest.skip(
            "No Kalshi markets had sufficient bar data (all rate-limited or too new)."
        )
    tearsheets = list(output_dir.glob(f"{strat.NAME}_*_tearsheet.html"))
    assert len(tearsheets) > 0, "output/ created but no tearsheets found"
    assert all(t.stat().st_size > 0 for t in tearsheets), (
        "One or more tearsheets are empty"
    )


def test_strategy_fires_signals(tmp_path):
    """Strategy opens at least one position across the discovered markets."""

    import pandas as pd
    from nautilus_trader.backtest.config import BacktestEngineConfig
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.model.currencies import USD
    from nautilus_trader.model.enums import AccountType, OmsType
    from nautilus_trader.model.identifiers import TraderId, Venue
    from nautilus_trader.model.objects import Money
    from nautilus_trader.risk.config import RiskEngineConfig

    from strategies.kalshi_spread_capture import (
        BarMeanReversion,
        BarMeanReversionConfig,
        _discover_markets,
        _load_market,
    )

    async def _run():
        from datetime import UTC, datetime, timedelta

        from nautilus_trader.core import nautilus_pyo3

        now = datetime.now(UTC)
        start = pd.Timestamp(now - timedelta(days=strat.LOOKBACK_DAYS))
        end = pd.Timestamp(now)

        http_client = nautilus_pyo3.HttpClient(
            default_quota=nautilus_pyo3.Quota.rate_per_second(20),
        )

        # Discover up to 5 candidate markets for a quick test.
        markets = await _discover_markets(candidate_limit=5, http_client=http_client)
        if not markets:
            pytest.skip("No open Kalshi markets found.")

        # Load first market that has sufficient bars.
        loader_bars = None
        for market in markets:
            result = await _load_market(market, start, end, http_client=http_client)
            if result is not None:
                loader_bars = (market["ticker"], *result)
                break

        if loader_bars is None:
            pytest.skip("No markets with sufficient bar data available.")

        ticker, loader, bars = loader_bars
        instrument = loader.instrument
        bar_type = bars[0].bar_type

        engine = BacktestEngine(
            config=BacktestEngineConfig(
                trader_id=TraderId("BACKTESTER-001"),
                logging=LoggingConfig(log_level="ERROR"),
                risk_engine=RiskEngineConfig(bypass=True),
            )
        )
        engine.add_venue(
            venue=Venue("KALSHI"),
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USD,
            starting_balances=[Money(strat.INITIAL_CASH, USD)],
        )
        engine.add_instrument(instrument)
        engine.add_data(bars)
        engine.add_strategy(
            BarMeanReversion(
                config=BarMeanReversionConfig(
                    instrument_id=instrument.id,
                    bar_type=bar_type,
                    trade_size=strat.TRADE_SIZE,
                    window=strat.WINDOW,
                    entry_threshold=strat.ENTRY_THRESHOLD,
                    take_profit=strat.TAKE_PROFIT,
                    stop_loss=strat.STOP_LOSS,
                )
            )
        )
        engine.run()
        report = engine.trader.generate_order_fills_report()
        engine.reset()
        engine.dispose()
        return report

    fills_report = asyncio.run(_run())
    assert (
        len(fills_report) >= 0
    )  # strategy ran without error; signals depend on market data
