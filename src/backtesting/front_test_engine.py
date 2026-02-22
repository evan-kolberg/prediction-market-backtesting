"""Front-testing engine — paper trades strategies against live WebSocket data.

Wires a Strategy to a live feed (Kalshi or Polymarket) using the PaperBroker
for simulated order matching.  Strategies use the exact same API as backtesting.

Usage::

    engine = FrontTestEngine(feed=kalshi_live_feed, strategy=my_strategy)
    asyncio.run(engine.run())
"""

from __future__ import annotations

import asyncio
import signal
import time
from datetime import datetime

from src.backtesting.models import Fill, PortfolioSnapshot
from src.backtesting.paper_broker import PaperBroker
from src.backtesting.strategy import Strategy

DIM = "\033[2m"
BOLD = "\033[1m"
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
RESET = "\033[0m"


def _ts() -> str:
    return f"{DIM}{datetime.now().strftime('%H:%M:%S')}{RESET}"


class FrontTestEngine:
    """Paper-trading engine that runs strategies against live market data."""

    def __init__(
        self,
        feed,
        strategy: Strategy,
        commission_rate: float = 0.01,
        slippage: float = 0.005,
        liquidity_cap: bool = True,
        status_interval: int = 30,
    ):
        self.feed = feed
        self.strategy = strategy
        self.broker = PaperBroker(
            initial_cash=strategy.initial_cash,
            commission_rate=commission_rate,
            slippage=slippage,
            liquidity_cap=liquidity_cap,
        )
        self.status_interval = status_interval

        self._trade_count = 0
        self._fill_count = 0
        self._start_time: float | None = None
        self._first_trade_time: datetime | None = None
        self._last_trade_time: datetime | None = None
        self._stopped: bool = False
        self._equity_curve: list[PortfolioSnapshot] = []
        self._all_fills: list[Fill] = []

    def _wire_strategy(self) -> None:
        """Inject broker callbacks into the strategy (same pattern as Engine)."""
        all_markets = self.feed.markets()

        def _place_order(market_id, action, side, price, quantity, timestamp=None):
            order = self.broker.place_order(market_id, action, side, price, quantity)
            print(
                f"{_ts()}  {YELLOW}ORDER{RESET} {action.upper()} {side.upper()} "
                f"{market_id[:20]}... @ ${price:.2f} x{quantity:.0f}"
            )
            return order

        self.strategy._place_order = _place_order
        self.strategy._cancel_order_fn = self.broker.cancel_order
        self.strategy._cancel_all_fn = self.broker.cancel_all
        self.strategy._get_portfolio = self.broker.get_portfolio_snapshot
        self.strategy._get_open_orders = self.broker.get_pending_orders
        self.strategy._get_markets = lambda: all_markets

    def _print_status(self) -> None:
        """Print a periodic status update."""
        snap = self.broker.get_portfolio_snapshot()
        pnl = snap.total_equity - self.broker.initial_cash
        pnl_color = GREEN if pnl >= 0 else RED
        elapsed = time.monotonic() - self._start_time if self._start_time else 0
        mins = int(elapsed // 60)

        print(
            f"{_ts()}  {DIM}[{mins}m]{RESET} "
            f"Trades: {self._trade_count}  "
            f"Fills: {self._fill_count}  "
            f"Positions: {snap.num_positions}  "
            f"Cash: ${snap.cash:,.2f}  "
            f"Equity: {pnl_color}${snap.total_equity:,.2f}{RESET}  "
            f"P&L: {pnl_color}{'+' if pnl >= 0 else ''}${pnl:,.2f}{RESET}"
        )

    def _print_final_summary(self) -> None:
        """Print final results when the session ends."""
        snap = self.broker.get_portfolio_snapshot()
        pnl = snap.total_equity - self.broker.initial_cash
        pnl_pct = pnl / self.broker.initial_cash if self.broker.initial_cash else 0
        equity_color = GREEN if pnl >= 0 else RED

        print(f"\n{_ts()}  {BOLD}Front Test Summary: {self.strategy.name}{RESET}")
        print(f"{_ts()}  Period:        {self._first_trade_time} -> {self._last_trade_time}")
        print(f"{_ts()}  Initial cash:  ${self.broker.initial_cash:,.2f}")
        print(f"{_ts()}  Final equity:  {equity_color}${snap.total_equity:,.2f}{RESET}")
        print(f"{_ts()}  P&L:           {equity_color}{'+' if pnl >= 0 else ''}${pnl:,.2f} ({pnl_pct:+.2%}){RESET}")
        print(f"{_ts()}  Trades seen:   {self._trade_count}")
        print(f"{_ts()}  Orders filled: {self._fill_count}")
        print(f"{_ts()}  Positions:     {snap.num_positions}")

        if self._all_fills:
            wins = sum(1 for f in self._all_fills if f.price > 0)
            print(f"{_ts()}  Win rate:      {wins / len(self._all_fills):.2%}")
        print()

    async def run(self) -> None:
        """Run the front test until interrupted with Ctrl+C."""
        # Fetch market metadata
        print(f"{_ts()}  Connecting to live feed...")
        await self.feed.connect()

        markets = self.feed.markets()
        if not markets:
            print(f"{_ts()}  {RED}No markets found for the given IDs.{RESET}")
            return

        print(f"{_ts()}  {GREEN}Connected.{RESET} Watching {len(markets)} market(s):")
        for info in markets.values():
            print(f"{_ts()}    - {info.title[:60]}")

        self._wire_strategy()
        self.strategy.initialize()
        self._start_time = time.monotonic()

        print(f"\n{_ts()}  {BOLD}Front testing: {self.strategy.name}{RESET}")
        print(f"{_ts()}  Initial cash: ${self.strategy.initial_cash:,.2f}")
        print(f"{_ts()}  Press Ctrl+C to stop.")
        print(f"{_ts()}  {DIM}Waiting for live trades...{RESET}\n")

        last_status = time.monotonic()

        # Handle graceful shutdown — cancel the current task so the WebSocket
        # await is interrupted immediately rather than waiting for the next trade.
        loop = asyncio.get_event_loop()
        current_task = asyncio.current_task()

        def _signal_handler():
            self._stopped = True
            if current_task is not None:
                current_task.cancel()

        try:
            loop.add_signal_handler(signal.SIGINT, _signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            pass

        try:
            async for trade in self.feed.trades():
                if self._stopped:
                    break

                self._trade_count += 1
                if self._first_trade_time is None:
                    self._first_trade_time = trade.timestamp
                self._last_trade_time = trade.timestamp

                # Check fills against pending orders
                fills = self.broker.check_fills(trade)
                for fill in fills:
                    self._fill_count += 1
                    self._all_fills.append(fill)
                    fill_color = GREEN if fill.action.value == "buy" else RED
                    print(
                        f"{_ts()}  {fill_color}FILL{RESET} {fill.action.value.upper()} "
                        f"{fill.side.value.upper()} {fill.market_id[:20]}... "
                        f"@ ${fill.price:.4f} x{fill.quantity:.0f} "
                        f"(commission: ${fill.commission:.4f})"
                    )
                    self.strategy.on_fill(fill)

                # Record equity snapshot
                snap = self.broker.get_portfolio_snapshot()
                self._equity_curve.append(snap)

                # Strategy processes the trade
                self.strategy.on_trade(trade)

                # Periodic status
                now = time.monotonic()
                if now - last_status >= self.status_interval:
                    self._print_status()
                    last_status = now

        except asyncio.CancelledError:
            pass
        except KeyboardInterrupt:
            pass
        finally:
            try:
                loop.remove_signal_handler(signal.SIGINT)
            except Exception:
                pass
            self.strategy.finalize()
            self._print_final_summary()
