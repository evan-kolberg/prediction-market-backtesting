"""EMA-cross strategy on Polymarket trade ticks.

Fetches trade data from the Polymarket REST API and replays it through a
NautilusTrader EMA-cross backtest using TICK-aggregated bars (no catalog needed).
"""

import os
from decimal import Decimal

import pandas as pd
from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.adapters.polymarket import PolymarketDataLoader
from nautilus_trader.analysis.config import TearsheetConfig
from nautilus_trader.analysis.tearsheet import create_tearsheet
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.strategies.ema_cross_long_only import EMACrossLongOnly
from nautilus_trader.examples.strategies.ema_cross_long_only import (
    EMACrossLongOnlyConfig,
)
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money

# ── Strategy metadata (shown in the menu) ────────────────────────────────────
NAME = "polymarket_ema_cross"
DESCRIPTION = "EMA-cross long-only on Polymarket trade ticks"

# ── Configure here ────────────────────────────────────────────────────────────
# High-volume active Polymarket market.
# Find others at: https://polymarket.com  or run active_markets.py
MARKET_SLUG = "gta-vi-released-before-june-2026"
TICK_BAR_SIZE = 10  # aggregate every N ticks into a bar
FAST_EMA = 5
SLOW_EMA = 15
TRADE_SIZE = Decimal("20")
INITIAL_CASH = 10_000.0
# ─────────────────────────────────────────────────────────────────────────────


async def run() -> None:
    print(f"Loading market: {MARKET_SLUG}...")
    loader = await PolymarketDataLoader.from_market_slug(MARKET_SLUG)
    instrument = loader.instrument

    print(f"Market:        {instrument.description or MARKET_SLUG}")
    print(f"Instrument ID: {instrument.id}")
    print(f"Outcome:       {instrument.outcome}\n")

    print("Fetching trade ticks...")
    trades = await loader.load_trades()
    print(f"Loaded {len(trades)} trade ticks\n")

    if not trades:
        raise ValueError("No historical data available for the specified market.")

    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(log_level="INFO"),
    )
    engine = BacktestEngine(config=engine_config)

    engine.add_venue(
        venue=POLYMARKET_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USDC_POS,
        starting_balances=[Money(INITIAL_CASH, USDC_POS)],
    )

    engine.add_instrument(instrument)
    engine.add_data(trades)

    # INTERNAL aggregation: NT builds bars from trade ticks on the fly —
    # no external bar data or catalog is required.
    bar_type = BarType.from_str(f"{instrument.id}-{TICK_BAR_SIZE}-TICK-LAST-INTERNAL")
    strategy_config = EMACrossLongOnlyConfig(
        instrument_id=instrument.id,
        bar_type=bar_type,
        trade_size=TRADE_SIZE,
        fast_ema_period=FAST_EMA,
        slow_ema_period=SLOW_EMA,
    )

    engine.add_strategy(EMACrossLongOnly(config=strategy_config))

    print("Running backtest...")
    engine.run()

    with pd.option_context(
        "display.max_rows", 100, "display.max_columns", None, "display.width", 300
    ):
        print(engine.trader.generate_account_report(POLYMARKET_VENUE))
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    tearsheet_path = f"output/{NAME}_tearsheet.html"
    os.makedirs("output", exist_ok=True)
    create_tearsheet(
        engine, tearsheet_path, config=TearsheetConfig(theme="nautilus_dark")
    )
    print(f"\nTearsheet saved to {tearsheet_path}")

    engine.reset()
    engine.dispose()
