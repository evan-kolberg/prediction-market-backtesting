"""EMA-cross strategy on Kalshi hourly bars.

Fetches bar data from the Kalshi REST API, writes it to a local Parquet
catalog, then replays it through a NautilusTrader EMA-cross backtest.
"""

from decimal import Decimal

import pandas as pd
from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.backtest.config import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestRunConfig,
    BacktestVenueConfig,
)
from nautilus_trader.analysis.config import TearsheetConfig
from nautilus_trader.analysis.tearsheet import create_tearsheet
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import ImportableStrategyConfig, LoggingConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.identifiers import TraderId, Venue
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.risk.config import RiskEngineConfig

# ── Strategy metadata (shown in the menu) ────────────────────────────────────
NAME = "kalshi_ema_cross"
DESCRIPTION = "EMA-cross long-only on Kalshi hourly bars"

# ── Configure here ────────────────────────────────────────────────────────────
# KXBTC-26MAR0617-B66000: "Will BTC close above $66,000 on March 6, 2026?"
# Highest-volume active Kalshi market (14,766 contracts traded).
# Opened 2026-02-27, closes 2026-03-06 — use Minutes1 for ~10K bars.
MARKET_TICKER = "KXBTC-26MAR0617-B66000"
BAR_INTERVAL = "Minutes1"  # Minutes1 | Hours1 | Days1
CATALOG_PATH = "./kalshi_catalog"
START = "2026-02-27"
END = "2026-03-07"  # day after expiry to capture all bars
FAST_EMA = 30  # 30-min fast EMA
SLOW_EMA = 100  # 100-min slow EMA
TRADE_SIZE = Decimal("1")
INITIAL_CASH = 10_000.0
# ─────────────────────────────────────────────────────────────────────────────


_INTERVAL_MINUTES = {"Minutes1": 1, "Hours1": 60, "Days1": 1440}
_KALSHI_MAX_CANDLES = 5_000  # API hard limit per request


async def _fetch_and_catalog() -> None:
    print(f"Fetching {BAR_INTERVAL} bars for {MARKET_TICKER} ({START} → {END})...")
    loader = await KalshiDataLoader.from_market_ticker(MARKET_TICKER)

    # Chunk the time range so no single request exceeds the API's 5 000-candle cap.
    chunk_delta = pd.Timedelta(
        minutes=_KALSHI_MAX_CANDLES * _INTERVAL_MINUTES[BAR_INTERVAL]
    )
    chunk_start = pd.Timestamp(START, tz="UTC")
    end_ts = pd.Timestamp(END, tz="UTC")

    all_bars = []
    while chunk_start < end_ts:
        chunk_end = min(chunk_start + chunk_delta, end_ts)
        chunk_bars = await loader.load_bars(
            start=chunk_start,
            end=chunk_end,
            interval=BAR_INTERVAL,
        )
        all_bars.extend(chunk_bars)
        chunk_start = chunk_end

    catalog = ParquetDataCatalog(CATALOG_PATH)
    catalog.write_data([loader.instrument])
    catalog.write_data(all_bars)
    print(f"Wrote {len(all_bars)} bars to {CATALOG_PATH}")


_BAR_SPEC_MAP = {
    "Minutes1": ("1-MINUTE-LAST", "1-MINUTE-LAST-EXTERNAL"),
    "Hours1": ("1-HOUR-LAST", "1-HOUR-LAST-EXTERNAL"),
    "Days1": ("1-DAY-LAST", "1-DAY-LAST-EXTERNAL"),
}


def _run_backtest() -> None:
    instrument_id = f"{MARKET_TICKER}.KALSHI"
    bar_spec, bar_type_suffix = _BAR_SPEC_MAP[BAR_INTERVAL]
    bar_type = f"{instrument_id}-{bar_type_suffix}"

    run_config = BacktestRunConfig(
        venues=[
            BacktestVenueConfig(
                name="KALSHI",
                oms_type="NETTING",
                account_type="CASH",
                base_currency="USD",
                starting_balances=[f"{INITIAL_CASH:.2f} USD"],
            )
        ],
        data=[
            BacktestDataConfig(
                catalog_path=CATALOG_PATH,
                data_cls=Bar,
                instrument_id=instrument_id,
                bar_spec=bar_spec,
                start_time=START,
                end_time=END,
            )
        ],
        engine=BacktestEngineConfig(
            trader_id=TraderId("BACKTESTER-001"),
            logging=LoggingConfig(log_level="INFO"),
            # bypass=True skips pre-trade risk price checks — required because
            # bar-only data leaves the risk engine's price cache empty, causing
            # false "Cannot check MARKET order risk" rejections.
            risk_engine=RiskEngineConfig(bypass=True),
            strategies=[
                ImportableStrategyConfig(
                    strategy_path="nautilus_trader.examples.strategies.ema_cross_long_only:EMACrossLongOnly",
                    config_path="nautilus_trader.examples.strategies.ema_cross_long_only:EMACrossLongOnlyConfig",
                    config={
                        "instrument_id": instrument_id,
                        "bar_type": bar_type,
                        "fast_ema_period": FAST_EMA,
                        "slow_ema_period": SLOW_EMA,
                        "trade_size": str(TRADE_SIZE),
                        # Disable pre-catalog historical bar request — the strategy
                        # would request bars from before the catalog window (empty),
                        # producing a noisy "<Bar[]> data with no bar" warning.
                        "request_historical_bars": False,
                    },
                )
            ],
        ),
        dispose_on_completion=False,
    )

    print("Running backtest...")
    node = BacktestNode(configs=[run_config])
    node.run()

    engine = node.get_engine(run_config.id)
    assert engine is not None, "BacktestEngine not found after node.run()"
    kalshi_venue = Venue("KALSHI")

    with pd.option_context(
        "display.max_rows", 100, "display.max_columns", None, "display.width", 300
    ):
        print(engine.trader.generate_account_report(kalshi_venue))
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    tearsheet_path = f"output/{NAME}_tearsheet.html"
    import os

    os.makedirs("output", exist_ok=True)
    create_tearsheet(
        engine, tearsheet_path, config=TearsheetConfig(theme="nautilus_dark")
    )
    print(f"Tearsheet saved to {tearsheet_path}")

    node.dispose()


async def run() -> None:
    await _fetch_and_catalog()
    _run_backtest()
