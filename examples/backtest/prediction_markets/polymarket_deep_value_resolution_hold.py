"""
Deep-value Polymarket strategy: buy low-priced outcomes and hold.

Runs on a single configured Polymarket market slug and renders a legacy chart
for side-by-side strategy comparison.
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.adapters.polymarket import PolymarketDataLoader
from nautilus_trader.adapters.polymarket.fee_model import PolymarketFeeModel
from nautilus_trader.adapters.prediction_market.backtest_utils import build_market_prices
from nautilus_trader.adapters.prediction_market.backtest_utils import extract_price_points
from nautilus_trader.adapters.prediction_market.backtest_utils import extract_realized_pnl
from nautilus_trader.adapters.prediction_market.backtest_utils import infer_realized_outcome
from nautilus_trader.analysis.legacy_plot_adapter import create_legacy_backtest_chart
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import LoggingConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickDeepValueHoldConfig
from nautilus_trader.examples.strategies.prediction_market import TradeTickDeepValueHoldStrategy
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.risk.config import RiskEngineConfig


try:
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG
except ModuleNotFoundError:
    _THIS_DIR = Path(__file__).resolve().parent
    if str(_THIS_DIR) not in sys.path:
        sys.path.insert(0, str(_THIS_DIR))
    from _defaults import DEFAULT_INITIAL_CASH
    from _defaults import DEFAULT_LOOKBACK_DAYS
    from _defaults import DEFAULT_POLYMARKET_MARKET_SLUG


NAME = "polymarket_deep_value_resolution_hold"
DESCRIPTION = "Buy below a configurable threshold and hold (single market)"

MARKET_SLUG = os.getenv(
    "MARKET_SLUG",
    DEFAULT_POLYMARKET_MARKET_SLUG,
)
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", str(DEFAULT_LOOKBACK_DAYS)))
MIN_TRADES = int(os.getenv("MIN_TRADES", "200"))
CHART_RESAMPLE_RULE = os.getenv("CHART_RESAMPLE_RULE")

ENTRY_PRICE_MAX = float(os.getenv("ENTRY_PRICE_MAX", "0.247"))

TRADE_SIZE = Decimal(os.getenv("TRADE_SIZE", "100"))
INITIAL_CASH = float(os.getenv("INITIAL_CASH", str(DEFAULT_INITIAL_CASH)))


def _build_probability_frame(
    trades: list[TradeTick],
    entry_price_max: float,
    realized_outcome: float | None,
) -> pd.DataFrame:
    rows: list[tuple[pd.Timestamp, float]] = []
    for tick in trades:
        ts = pd.to_datetime(int(tick.ts_event), unit="ns", utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        rows.append((ts, float(tick.price)))

    frame = pd.DataFrame(rows, columns=["ts", "market_probability"]) if rows else pd.DataFrame()
    if frame.empty:
        return frame

    frame = frame.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").set_index("ts")
    frame["market_probability"] = frame["market_probability"].clip(0.0, 1.0)

    signal_mask = frame["market_probability"] <= entry_price_max
    if signal_mask.any():
        first_signal_ts = frame.index[signal_mask.argmax()]
        user_probability = frame["market_probability"].copy()
        user_probability.loc[first_signal_ts:] = 1.0
    else:
        user_probability = frame["market_probability"].copy()

    frame["user_probability"] = user_probability
    frame = frame.dropna(subset=["user_probability", "market_probability"])
    if realized_outcome is not None:
        frame["outcome"] = float(realized_outcome)

    return frame


def _slugify(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in value)
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-")


def _run_backtest(
    slug: str,
    outcome: str,
    loader: PolymarketDataLoader,
    trades: list[TradeTick],
) -> dict[str, Any]:
    instrument = loader.instrument

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("BACKTESTER-001"),
            logging=LoggingConfig(log_level="WARNING"),
            risk_engine=RiskEngineConfig(bypass=True),
        ),
    )
    engine.add_venue(
        venue=POLYMARKET_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USDC_POS,
        starting_balances=[Money(INITIAL_CASH, USDC_POS)],
        fee_model=PolymarketFeeModel(),
    )
    engine.add_instrument(instrument)
    engine.add_data(trades)
    engine.add_strategy(
        TradeTickDeepValueHoldStrategy(
            TradeTickDeepValueHoldConfig(
                instrument_id=instrument.id,
                trade_size=TRADE_SIZE,
                entry_price_max=ENTRY_PRICE_MAX,
            ),
        ),
    )

    engine.run()

    fills = engine.trader.generate_order_fills_report()
    positions = engine.trader.generate_positions_report()
    pnl = extract_realized_pnl(positions)

    prob_frame = _build_probability_frame(
        trades=trades,
        entry_price_max=ENTRY_PRICE_MAX,
        realized_outcome=infer_realized_outcome(instrument),
    )
    price_points = extract_price_points(trades, price_attr="price")

    safe_outcome = _slugify(outcome) or "outcome"
    output_path = f"output/{NAME}_{slug}_{safe_outcome}_legacy.html"
    os.makedirs("output", exist_ok=True)
    create_legacy_backtest_chart(
        engine=engine,
        output_path=output_path,
        strategy_name=f"{NAME}:{slug}:{outcome}",
        platform="polymarket",
        initial_cash=INITIAL_CASH,
        market_prices={
            str(instrument.id): build_market_prices(
                price_points,
                resample_rule=CHART_RESAMPLE_RULE or None,
            )
        },
        user_probabilities=prob_frame.get("user_probability"),
        market_probabilities=prob_frame.get("market_probability"),
        outcomes=prob_frame.get("outcome"),
        open_browser=False,
        progress=False,
    )

    engine.reset()
    engine.dispose()

    prices = [float(t.price) for t in trades]
    return {
        "slug": slug,
        "outcome": outcome,
        "output_path": output_path,
        "trades": len(trades),
        "fills": len(fills),
        "pnl": pnl,
        "entry_min": min(prices),
        "max": max(prices),
        "last": prices[-1],
    }


def _print_summary(results: list[dict[str, Any]]) -> None:
    if not results:
        print("No qualifying resolved low-priced outcome markets were found.")
        return

    labels = [f"{r['slug']}:{r['outcome']}" for r in results]
    col_w = max(len(label) for label in labels) + 2
    header = (
        f"{'Market':<{col_w}} {'Trades':>7} {'Fills':>6} "
        f"{'Min Px':>8} {'Max Px':>8} {'Last Px':>8} {'PnL (USDC)':>12}"
    )
    sep = "-" * len(header)

    print(f"\n{sep}\n{header}\n{sep}")
    for row, label in zip(results, labels, strict=False):
        print(
            f"{label:<{col_w}} {row['trades']:>7} {row['fills']:>6} "
            f"{row['entry_min']:>8.3f} {row['max']:>8.3f} {row['last']:>8.3f} "
            f"{row['pnl']:>+12.4f}"
        )

    total_pnl = sum(float(r["pnl"]) for r in results)
    total_fills = sum(int(r["fills"]) for r in results)
    print(sep)
    print(
        f"{'TOTAL':<{col_w}} {'':>7} {total_fills:>6} {'':>8} {'':>8} {'':>8} {total_pnl:>+12.4f}"
    )
    print(sep)


async def run() -> None:
    now = datetime.now(UTC)
    start = pd.Timestamp(now - timedelta(days=LOOKBACK_DAYS))
    end = pd.Timestamp(now)

    print(f"Loading Polymarket market {MARKET_SLUG} (lookback={LOOKBACK_DAYS}d)...")
    try:
        loader = await PolymarketDataLoader.from_market_slug(MARKET_SLUG)
        trades = await loader.load_trades(start, end)
    except Exception as exc:
        print(f"Unable to load {MARKET_SLUG}: {exc}")
        return

    if len(trades) < MIN_TRADES:
        print(f"Skip {MARKET_SLUG}: {len(trades)} trades < {MIN_TRADES} required")
        return

    outcome = str(getattr(loader.instrument, "outcome", "yes"))
    print(f"  running backtest for {MARKET_SLUG}:{outcome}...")
    result = _run_backtest(
        slug=MARKET_SLUG,
        outcome=outcome,
        loader=loader,
        trades=trades,
    )

    _print_summary([result])
    print(f"\nLegacy chart saved to {result['output_path']}")


if __name__ == "__main__":
    asyncio.run(run())
