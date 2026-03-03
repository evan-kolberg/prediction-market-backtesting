"""Spread-capture strategy on Polymarket trade ticks.

Discovers the top active Polymarket markets by volume via the Gamma API,
fetches trade ticks for each in parallel, runs a SpreadCapture mean-reversion
strategy on every market with sufficient data, and prints an aggregate
performance table.
"""

from __future__ import annotations

import asyncio
import os
from collections import deque
from decimal import Decimal

import msgspec
import pandas as pd
from nautilus_trader.adapters.polymarket import POLYMARKET_VENUE
from nautilus_trader.adapters.polymarket import PolymarketDataLoader
from nautilus_trader.analysis.config import TearsheetConfig
from nautilus_trader.analysis.tearsheet import create_tearsheet
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import LoggingConfig
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.model.currencies import USDC_POS
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.trading.strategy import StrategyConfig

# ── Strategy metadata (shown in the menu) ────────────────────────────────────
NAME = "polymarket_spread_capture"
DESCRIPTION = "Mean-reversion spread capture across Polymarket markets"

# ── Configure here ────────────────────────────────────────────────────────────
# Kept for use in tests; the live run() discovers markets dynamically.
MARKET_SLUG = "gta-vi-released-before-june-2026"

MAX_MARKETS = 15  # cap total markets to keep runtime reasonable
MIN_TRADES = 50  # skip markets with fewer trade ticks

VWAP_WINDOW = 20  # rolling window for mid-price estimate
ENTRY_THRESHOLD = 0.001  # buy when price is 0.1¢ below rolling average (~1σ)
TAKE_PROFIT = 0.001  # close when price rises 0.1¢ above fill price
STOP_LOSS = 0.003  # close when price falls 0.3¢ below fill price (~3σ)
TRADE_SIZE = Decimal("20")
INITIAL_CASH = 10_000.0
# ─────────────────────────────────────────────────────────────────────────────

_GAMMA_API = "https://gamma-api.polymarket.com/markets"


class SpreadCaptureConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal("20")
    vwap_window: int = 20
    entry_threshold: float = 0.005
    take_profit: float = 0.005
    stop_loss: float = 0.015


class SpreadCapture(Strategy):
    """
    Mean-reversion spread capture strategy.

    Buys when price dips below a rolling average and exits on recovery
    or stop-loss.  Holds at most one position at a time.
    """

    def __init__(self, config: SpreadCaptureConfig) -> None:
        super().__init__(config)
        self._prices: deque[float] = deque(maxlen=config.vwap_window)
        self._entry_price: float | None = None
        self._pending: bool = False
        self._instrument = None

    def on_start(self) -> None:
        self._instrument = self.cache.instrument(self.config.instrument_id)
        if self._instrument is None:
            self.log.error(
                f"Instrument {self.config.instrument_id} not found — stopping."
            )
            self.stop()
            return
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        self._prices.append(price)

        # Wait for the rolling window to warm up, and for any pending order to settle.
        if len(self._prices) < self.config.vwap_window or self._pending:
            return

        rolling_avg = sum(self._prices) / len(self._prices)

        if self.portfolio.is_flat(self.config.instrument_id):
            # Entry: price dips below rolling average by the threshold.
            if price <= rolling_avg - self.config.entry_threshold:
                self._buy()
        else:
            assert self._entry_price is not None
            take_profit_hit = price >= self._entry_price + self.config.take_profit
            stop_loss_hit = price <= self._entry_price - self.config.stop_loss
            if take_profit_hit or stop_loss_hit:
                self.close_all_positions(self.config.instrument_id)
                self._pending = True

    def on_order_filled(self, event) -> None:
        if event.order_side == OrderSide.BUY:
            self._entry_price = float(event.last_px)
        else:
            self._entry_price = None
        self._pending = False

    def on_stop(self) -> None:
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)

    def _buy(self) -> None:
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self._instrument.make_qty(float(self.config.trade_size)),
            time_in_force=TimeInForce.IOC,
        )
        self.submit_order(order)
        self._pending = True


async def _discover_slugs(max_markets: int) -> list[str]:
    """Query Polymarket Gamma API for top active markets by volume."""
    client = nautilus_pyo3.HttpClient(
        default_quota=nautilus_pyo3.Quota.rate_per_second(20),
    )
    resp = await client.get(
        url=_GAMMA_API,
        params={
            "active": "true",
            "closed": "false",
            "archived": "false",
            "limit": "100",
        },
    )
    if resp.status != 200:
        return []
    markets: list[dict] = msgspec.json.decode(resp.body)

    def _vol(m: dict) -> float:
        try:
            return float(m.get("volume", 0) or 0)
        except (TypeError, ValueError):
            return 0.0

    markets.sort(key=_vol, reverse=True)
    slugs: list[str] = []
    for m in markets:
        slug = m.get("slug", "")
        if slug:
            slugs.append(slug)
        if len(slugs) >= max_markets:
            break
    return slugs


async def _load_market(
    slug: str,
) -> tuple[PolymarketDataLoader, list[TradeTick]] | None:
    """Fetch trades for one market slug.  Returns None if data is insufficient."""
    try:
        loader = await PolymarketDataLoader.from_market_slug(slug)
        trades = await loader.load_trades()
        if len(trades) < MIN_TRADES:
            return None
        return loader, trades
    except Exception as exc:
        print(f"  skip {slug}: {exc}")
        return None


def _extract_pnl(pos_report: pd.DataFrame) -> float:
    """Parse total realized PnL from a positions report DataFrame."""
    total = 0.0
    for _, row in pos_report.iterrows():
        pnl_str = str(row.get("realized_pnl", "")).strip()
        if pnl_str and pnl_str.lower() != "nan":
            try:
                # Money strings look like "−1.00 USDC_POS"; handle unicode minus.
                total += float(pnl_str.split()[0].replace("\u2212", "-"))
            except (ValueError, IndexError):
                pass
    return total


def _run_backtest(
    slug: str, loader: PolymarketDataLoader, trades: list[TradeTick]
) -> dict:
    """Run one market's backtest and return a results dict."""
    instrument = loader.instrument

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("BACKTESTER-001"),
            logging=LoggingConfig(log_level="INFO"),
        )
    )
    engine.add_venue(
        venue=POLYMARKET_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=USDC_POS,
        starting_balances=[Money(INITIAL_CASH, USDC_POS)],
    )
    engine.add_instrument(instrument)
    engine.add_data(trades)
    engine.add_strategy(
        SpreadCapture(
            config=SpreadCaptureConfig(
                instrument_id=instrument.id,
                trade_size=TRADE_SIZE,
                vwap_window=VWAP_WINDOW,
                entry_threshold=ENTRY_THRESHOLD,
                take_profit=TAKE_PROFIT,
                stop_loss=STOP_LOSS,
            )
        )
    )
    engine.run()

    fills = engine.trader.generate_order_fills_report()
    positions = engine.trader.generate_positions_report()
    pnl = _extract_pnl(positions)

    tearsheet_path = f"output/{NAME}_{slug}_tearsheet.html"
    os.makedirs("output", exist_ok=True)
    create_tearsheet(
        engine, tearsheet_path, config=TearsheetConfig(theme="nautilus_dark")
    )

    engine.reset()
    engine.dispose()

    return {
        "slug": slug,
        "trades": len(trades),
        "fills": len(fills),
        "pnl": pnl,
    }


def _print_summary(results: list[dict]) -> None:
    if not results:
        print("No markets had sufficient data.")
        return

    col_w = max(len(r["slug"]) for r in results) + 2
    header = f"{'Market':<{col_w}} {'Trades':>8} {'Fills':>6} {'PnL (USDC)':>12}"
    sep = "─" * len(header)
    print(f"\n{sep}\n{header}\n{sep}")
    for r in results:
        print(
            f"{r['slug']:<{col_w}} {r['trades']:>8} {r['fills']:>6} {r['pnl']:>+12.4f}"
        )
    total_pnl = sum(r["pnl"] for r in results)
    total_fills = sum(r["fills"] for r in results)
    print(sep)
    print(f"{'TOTAL':<{col_w}} {'':>8} {total_fills:>6} {total_pnl:>+12.4f}")
    print(sep)


async def run() -> None:
    print(f"Discovering top {MAX_MARKETS} active Polymarket markets by volume...")
    slugs = await _discover_slugs(MAX_MARKETS)
    print(f"Found {len(slugs)} markets → fetching trades in parallel...\n")

    loaded = await asyncio.gather(*[_load_market(s) for s in slugs])

    results: list[dict] = []
    for slug, market_data in zip(slugs, loaded):
        if market_data is None:
            print(f"  skip {slug}: fewer than {MIN_TRADES} trades")
            continue
        loader, trades = market_data
        print(f"  {slug}: {len(trades)} trades → running backtest...")
        result = _run_backtest(slug, loader, trades)
        results.append(result)

    _print_summary(results)
    print(f"\nTearsheets saved to output/{NAME}_<slug>_tearsheet.html")
