"""Smoke tests for the plotting module."""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from src.backtesting.models import (
    BacktestResult,
    Fill,
    OrderAction,
    Platform,
    PortfolioSnapshot,
    Side,
)


def _make_result(
    n_snaps: int = 100,
    n_fills: int = 40,
    n_markets: int = 8,
    include_prices: bool = True,
    include_pnls: bool = True,
) -> BacktestResult:
    """Build a synthetic BacktestResult with rich data for testing."""
    rng = random.Random(42)
    base = datetime(2025, 1, 1)
    cash = 10_000.0

    market_ids = [f"MKT-{i:03d}" for i in range(n_markets)]

    # Equity snapshots
    snaps: list[PortfolioSnapshot] = []
    for i in range(n_snaps):
        ts = base + timedelta(hours=i * 6)
        equity = cash + rng.gauss(0, 50) + i * 2
        snaps.append(
            PortfolioSnapshot(
                timestamp=ts,
                cash=cash - i * 1.5,
                total_equity=equity,
                unrealized_pnl=equity - cash + i * 1.5,
                num_positions=min(i % 6, n_markets),
            )
        )

    # Fills spread across markets
    fills: list[Fill] = []
    for j in range(n_fills):
        fills.append(
            Fill(
                order_id=f"o{j}",
                market_id=market_ids[j % n_markets],
                action=OrderAction.BUY if j % 3 != 0 else OrderAction.SELL,
                side=Side.YES,
                price=round(rng.uniform(0.10, 0.90), 2),
                quantity=float(rng.randint(1, 20)),
                timestamp=base + timedelta(hours=j * (n_snaps * 6 // n_fills)),
                commission=0.01,
            )
        )

    # Market prices (simulated)
    market_prices: dict[str, list[tuple[datetime, float]]] = {}
    if include_prices:
        for mid in market_ids:
            prices = []
            p = rng.uniform(0.2, 0.8)
            for i in range(n_snaps):
                ts = base + timedelta(hours=i * 6)
                p = max(0.01, min(0.99, p + rng.gauss(0, 0.02)))
                prices.append((ts, p))
            market_prices[mid] = prices

    # Per-market P&L
    market_pnls: dict[str, float] = {}
    if include_pnls:
        for mid in market_ids:
            market_pnls[mid] = rng.gauss(0, 5)

    return BacktestResult(
        equity_curve=snaps,
        fills=fills,
        metrics={"total_return": 0.05, "max_drawdown": 0.03},
        strategy_name="test_strat",
        platform=Platform.KALSHI,
        start_time=snaps[0].timestamp,
        end_time=snaps[-1].timestamp,
        initial_cash=10_000.0,
        final_equity=snaps[-1].total_equity,
        num_markets_traded=n_markets,
        num_markets_resolved=n_markets // 2,
        market_prices=market_prices,
        market_pnls=market_pnls,
    )


def test_plot_full():
    """Full plot with market prices plus trade connectors."""
    from src.backtesting.plotting import plot

    fig = plot(_make_result(), open_browser=False, filename="/tmp/test_plot_full")
    assert fig is not None


def test_plot_no_market_prices():
    """Fallback to equity chart when no market prices."""
    from src.backtesting.plotting import plot

    fig = plot(_make_result(include_prices=False), open_browser=False, filename="/tmp/test_plot_no_prices")
    assert fig is not None


def test_plot_no_fills():
    """Should still work with zero fills."""
    from src.backtesting.plotting import plot

    fig = plot(
        _make_result(n_fills=0, include_prices=False, include_pnls=False),
        open_browser=False,
        filename="/tmp/test_plot_no_fills",
    )
    assert fig is not None


def test_plot_minimal():
    """Very few snapshots."""
    from src.backtesting.plotting import plot

    fig = plot(_make_result(n_snaps=3, n_fills=1, n_markets=1), open_browser=False, filename="/tmp/test_plot_minimal")
    assert fig is not None


def test_plot_absolute_equity():
    """relative_equity=False mode."""
    from src.backtesting.plotting import plot

    fig = plot(_make_result(), open_browser=False, relative_equity=False, filename="/tmp/test_plot_absolute")
    assert fig is not None


def test_result_plot_method():
    """BacktestResult.plot() shortcut."""
    fig = _make_result().plot(open_browser=False, filename="/tmp/test_result_plot")
    assert fig is not None


def test_plot_no_pnls():
    """Fallback per-fill P&L scatter when market_pnls empty."""
    from src.backtesting.plotting import plot

    fig = plot(_make_result(include_pnls=False), open_browser=False, filename="/tmp/test_plot_no_pnls")
    assert fig is not None
