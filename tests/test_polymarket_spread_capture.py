"""End-to-end tests for the Polymarket spread-capture strategy.

These tests hit the real Polymarket REST API and run a full backtest, so they
require network access and take a few seconds to complete.
"""

import asyncio

import pytest

import strategies.polymarket_spread_capture as strat


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Redirect tearsheet output to a temp dir."""
    monkeypatch.chdir(tmp_path)  # tearsheet → tmp_path/output/


def test_full_run_produces_tearsheet(tmp_path):
    """Full pipeline (discover markets → fetch trades → backtest → tearsheets) runs without error."""
    asyncio.run(strat.run())

    output_dir = tmp_path / "output"
    assert output_dir.exists(), "output/ directory not created"
    tearsheets = list(output_dir.glob(f"{strat.NAME}_*_tearsheet.html"))
    assert len(tearsheets) > 0, "No tearsheets created"
    assert all(t.stat().st_size > 0 for t in tearsheets), (
        "One or more tearsheets are empty"
    )


def test_strategy_fires_signals(tmp_path):
    """Strategy opens at least one position with the default thresholds."""

    from nautilus_trader.adapters.polymarket import PolymarketDataLoader
    from nautilus_trader.backtest.config import BacktestEngineConfig
    from nautilus_trader.backtest.engine import BacktestEngine
    from nautilus_trader.config import LoggingConfig
    from nautilus_trader.model.currencies import USDC_POS
    from nautilus_trader.model.enums import AccountType, OmsType
    from nautilus_trader.model.identifiers import TraderId
    from nautilus_trader.model.objects import Money

    from strategies.polymarket_spread_capture import (
        POLYMARKET_VENUE,
        SpreadCapture,
        SpreadCaptureConfig,
    )

    async def _run():
        loader = await PolymarketDataLoader.from_market_slug(strat.MARKET_SLUG)
        instrument = loader.instrument
        trades = await loader.load_trades()

        engine = BacktestEngine(
            config=BacktestEngineConfig(
                trader_id=TraderId("BACKTESTER-001"),
                logging=LoggingConfig(log_level="ERROR"),
            )
        )
        engine.add_venue(
            venue=POLYMARKET_VENUE,
            oms_type=OmsType.NETTING,
            account_type=AccountType.CASH,
            base_currency=USDC_POS,
            starting_balances=[Money(strat.INITIAL_CASH, USDC_POS)],
        )
        engine.add_instrument(instrument)
        engine.add_data(trades)
        engine.add_strategy(
            SpreadCapture(
                config=SpreadCaptureConfig(
                    instrument_id=instrument.id,
                    trade_size=strat.TRADE_SIZE,
                    vwap_window=strat.VWAP_WINDOW,
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
    assert len(fills_report) > 0, (
        "No orders were filled — strategy never entered a position. "
        "Check thresholds vs actual price variance."
    )
