"""NautilusTrader-inspired event logger for backtest runs.

Formats and accumulates timestamped log lines for every significant
simulation event: orders, fills, market lifecycle, and portfolio changes.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.backtesting.models import Fill, MarketInfo, Order, Side


# ANSI color codes
_GREEN = "\033[32m"
_RED = "\033[31m"
_BOLD = "\033[1m"
_DIM = "\033[2m"
_RESET = "\033[0m"


class BacktestLogger:
    """Accumulates and optionally prints timestamped backtest events.

    Always stores plain-text lines in ``self.lines`` for later persistence.
    When ``print_live=True``, also writes ANSI-colored output to stdout
    using ``write_fn`` (which can be ``tqdm.write`` to avoid clobbering
    a progress bar).
    """

    def __init__(self, print_live: bool = False, write_fn=None):
        self.print_live = print_live
        self.write_fn = write_fn or print
        self.lines: list[str] = []

    # -- Core helper --

    def _log(self, timestamp: datetime | None, component: str, message: str, color: str = "") -> None:
        ts_str = timestamp.strftime("%Y-%m-%d %H:%M:%S") if timestamp else ""
        plain = f"{ts_str}  {component}  {message}"
        self.lines.append(plain)
        if self.print_live:
            colored = f"{_DIM}{ts_str}{_RESET}  {_BOLD}{component}{_RESET}  {color}{message}{_RESET}"
            self.write_fn(colored)

    # -- Lifecycle events --

    def start(
        self,
        timestamp: datetime | None,
        strategy_name: str,
        platform: str,
        num_markets: int,
        initial_cash: float,
    ) -> None:
        self._log(timestamp, "Engine", f"Backtest start: {strategy_name} on {platform}")
        self._log(timestamp, "Engine", f"{num_markets} markets loaded, initial cash: ${initial_cash:,.2f}")
        self._log(timestamp, "Portfolio", f"READY, cash=${initial_cash:,.2f}")
        self._log(timestamp, "Strategy", f"Initialized {strategy_name}")

    def end(
        self,
        timestamp: datetime | None,
        final_cash: float,
        final_equity: float,
        elapsed_seconds: float,
    ) -> None:
        if elapsed_seconds >= 60:
            elapsed_str = f"{elapsed_seconds / 60:.1f}m"
        else:
            elapsed_str = f"{elapsed_seconds:.1f}s"
        self._log(timestamp, "Engine", f"Backtest complete in {elapsed_str}")
        eq_color = _GREEN if final_equity >= final_cash else _RED
        self._log(timestamp, "Portfolio", f"cash=${final_cash:,.2f}, equity=${final_equity:,.2f}", eq_color)

    # -- Market events --

    def market_open(self, timestamp: datetime, market: MarketInfo) -> None:
        self._log(timestamp, "Market.OPEN", f'{market.market_id} "{market.title}"')

    def market_close(self, timestamp: datetime, market: MarketInfo) -> None:
        self._log(timestamp, "Market.CLOSE", market.market_id)

    def market_resolve(
        self,
        timestamp: datetime,
        market: MarketInfo,
        result: Side,
        pnl: float,
    ) -> None:
        result_str = result.value.upper()
        color = _GREEN if pnl >= 0 else _RED
        self._log(
            timestamp,
            "Market.RESOLVE",
            f"{market.market_id} -> {result_str} | P&L={_fmt_pnl(pnl)}",
            color,
        )

    # -- Order events --

    def order_submitted(self, timestamp: datetime, order: Order) -> None:
        action = order.action.value.upper()
        side = order.side.value.upper()
        short_id = order.order_id[:8]
        self._log(
            timestamp,
            "Order.SUBMITTED",
            f"{action} {order.quantity:.1f} {side} {order.market_id} @ {order.price:.2f} (id={short_id})",
        )

    def order_filled(self, timestamp: datetime, fill: Fill, cash_after: float) -> None:
        action = fill.action.value.upper()
        side = fill.side.value.upper()
        short_id = fill.order_id[:8]
        color = _GREEN if action == "SELL" else ""
        self._log(
            timestamp,
            "Order.FILLED",
            f"{action} {fill.quantity:.1f} {side} {fill.market_id} @ {fill.price:.2f} "
            f"(id={short_id}) | commission=${fill.commission:.2f}",
            color,
        )

    def order_cancelled(self, timestamp: datetime, order: Order) -> None:
        short_id = order.order_id[:8]
        self._log(timestamp, "Order.CANCELLED", f"{order.market_id} (id={short_id})")

    # -- Position events --

    def position_opened(
        self,
        timestamp: datetime,
        market_id: str,
        quantity: float,
        price: float,
        cash: float,
    ) -> None:
        sign = "+" if quantity >= 0 else ""
        side = "YES" if quantity >= 0 else "NO"
        self._log(
            timestamp,
            "Position.OPENED",
            f"{market_id} {sign}{abs(quantity):.1f} {side} @ {price:.2f} | cash=${cash:,.2f}",
        )

    def position_closed(
        self,
        timestamp: datetime,
        market_id: str,
        realized_pnl: float,
    ) -> None:
        color = _GREEN if realized_pnl >= 0 else _RED
        self._log(
            timestamp,
            "Position.CLOSED",
            f"{market_id} | realized P&L={_fmt_pnl(realized_pnl)}",
            color,
        )

    # -- Portfolio snapshots --

    def portfolio_update(
        self,
        timestamp: datetime,
        cash: float,
        equity: float,
        num_positions: int,
    ) -> None:
        self._log(
            timestamp,
            "Portfolio",
            f"cash=${cash:,.2f}, equity=${equity:,.2f}, positions={num_positions}",
        )


def _format_ts(ts: datetime) -> str:
    """Format a datetime as ISO 8601 with seconds precision."""
    if ts.tzinfo is not None and ts.tzinfo.utcoffset(ts) == timezone.utc.utcoffset(None):
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    if ts.tzinfo is not None:
        return ts.isoformat(timespec="seconds")
    return ts.strftime("%Y-%m-%dT%H:%M:%S")


def _fmt_pnl(value: float) -> str:
    """Format a P&L value with sign and dollar sign."""
    if value >= 0:
        return f"+${value:,.2f}"
    return f"-${abs(value):,.2f}"
