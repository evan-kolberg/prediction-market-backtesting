# Derived from NautilusTrader prediction-market test code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-10.
# See the repository NOTICE file for provenance and licensing scope.

"""Tests that verify downsampling actually bounds HTML chart size.

These tests would have caught the 31 MB bloat that shipped when 446 K equity
bars were serialised into Bokeh JSON without any reduction.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd

from prediction_market_extensions.analysis.legacy_backtesting.models import (
    BacktestResult,
    Fill,
    OrderAction,
    Platform,
    PortfolioSnapshot,
    Side,
)
from prediction_market_extensions.analysis.legacy_backtesting.plotting import (
    _downsample,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_large_result(
    n_bars: int = 100_000,
    n_fills: int = 50,
    n_markets: int = 5,
    initial_cash: float = 1000.0,
) -> BacktestResult:
    """Build a synthetic BacktestResult with *n_bars* equity snapshots."""
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rng = np.random.default_rng(42)

    equity = initial_cash + np.cumsum(rng.normal(0.01, 0.5, n_bars))
    equity = np.maximum(equity, 10.0)  # keep positive

    snapshots = [
        PortfolioSnapshot(
            timestamp=start + timedelta(seconds=i),
            cash=float(equity[i] * 0.7),
            total_equity=float(equity[i]),
            unrealized_pnl=float(equity[i] * 0.3 - initial_cash * 0.3),
            num_positions=min(i, n_markets),
        )
        for i in range(n_bars)
    ]

    market_ids = [f"market-{j}" for j in range(n_markets)]
    fill_indices = np.linspace(10, n_bars - 10, n_fills, dtype=int)
    fills = [
        Fill(
            order_id=f"fill-{k}",
            market_id=market_ids[k % n_markets],
            action=OrderAction.BUY if k % 2 == 0 else OrderAction.SELL,
            side=Side.YES,
            price=float(rng.uniform(0.2, 0.8)),
            quantity=float(rng.integers(1, 20)),
            timestamp=start + timedelta(seconds=int(fill_indices[k])),
        )
        for k in range(n_fills)
    ]

    market_prices: dict[str, list[tuple[datetime, float]]] = {}
    for mid in market_ids:
        prices = np.clip(0.5 + np.cumsum(rng.normal(0, 0.001, n_bars)), 0.01, 0.99)
        market_prices[mid] = [
            (start + timedelta(seconds=i), float(prices[i]))
            for i in range(0, n_bars, max(1, n_bars // 2000))
        ]

    return BacktestResult(
        equity_curve=snapshots,
        fills=fills,
        metrics={},
        strategy_name="test-bloat",
        platform=Platform.POLYMARKET,
        start_time=start,
        end_time=start + timedelta(seconds=n_bars),
        initial_cash=initial_cash,
        final_equity=float(equity[-1]),
        num_markets_traded=n_markets,
        num_markets_resolved=n_markets,
        market_prices=market_prices,
        market_pnls={mid: float(rng.normal(0, 5)) for mid in market_ids},
    )


# ---------------------------------------------------------------------------
# _downsample unit tests
# ---------------------------------------------------------------------------


class TestDownsample:
    def test_large_eq_reduced_to_max_points(self) -> None:
        n = 200_000
        eq = pd.DataFrame(
            {
                "datetime": pd.date_range("2026-01-01", periods=n, freq="s"),
                "cash": np.linspace(100, 90, n),
                "equity": np.linspace(100, 110, n),
                "drawdown_pct": np.zeros(n),
            }
        )
        fills_df = pd.DataFrame(
            columns=[
                "datetime",
                "market_id",
                "action",
                "side",
                "price",
                "quantity",
                "commission",
                "bar",
            ]
        )
        market_df = pd.DataFrame(index=eq.index)

        eq_ds, _, _, _ = _downsample(eq, fills_df, market_df, max_points=3000)

        # Allow small overshoot from must-keep points (fills, peaks, endpoints)
        assert len(eq_ds) <= 3100

    def test_small_eq_not_touched(self) -> None:
        n = 500
        eq = pd.DataFrame(
            {
                "datetime": pd.date_range("2026-01-01", periods=n, freq="s"),
                "cash": np.ones(n) * 100,
                "equity": np.ones(n) * 100,
                "drawdown_pct": np.zeros(n),
            }
        )
        fills_df = pd.DataFrame(
            columns=[
                "datetime",
                "market_id",
                "action",
                "side",
                "price",
                "quantity",
                "commission",
                "bar",
            ]
        )
        market_df = pd.DataFrame(index=eq.index)

        eq_ds, _, _, _ = _downsample(eq, fills_df, market_df, max_points=5000)

        assert len(eq_ds) == n

    def test_fill_bars_preserved_and_remapped(self) -> None:
        n = 50_000
        eq = pd.DataFrame(
            {
                "datetime": pd.date_range("2026-01-01", periods=n, freq="s"),
                "cash": np.ones(n) * 100,
                "equity": np.linspace(100, 200, n),
                "drawdown_pct": np.zeros(n),
            }
        )
        fills_df = pd.DataFrame(
            {
                "datetime": [eq["datetime"].iloc[25000]],
                "market_id": ["mkt-a"],
                "action": ["buy"],
                "side": ["yes"],
                "price": [0.5],
                "quantity": [10],
                "commission": [0.01],
                "bar": [25000],
            }
        )
        market_df = pd.DataFrame(index=eq.index)

        eq_ds, fills_ds, _, _ = _downsample(eq, fills_df, market_df, max_points=2000)

        assert len(eq_ds) <= 2100
        assert len(fills_ds) == 1
        bar = int(fills_ds.iloc[0]["bar"])
        assert 0 <= bar < len(eq_ds)

    def test_market_df_downsampled_in_sync(self) -> None:
        n = 30_000
        eq = pd.DataFrame(
            {
                "datetime": pd.date_range("2026-01-01", periods=n, freq="s"),
                "cash": np.ones(n) * 100,
                "equity": np.ones(n) * 100,
                "drawdown_pct": np.zeros(n),
            }
        )
        fills_df = pd.DataFrame(
            columns=[
                "datetime",
                "market_id",
                "action",
                "side",
                "price",
                "quantity",
                "commission",
                "bar",
            ]
        )
        prices = np.random.default_rng(0).uniform(0.3, 0.7, n)
        market_df = pd.DataFrame({"mkt-a": prices}, index=eq.index)

        eq_ds, _, mkt_ds, _ = _downsample(eq, fills_df, market_df, max_points=2000)

        assert len(mkt_ds) == len(eq_ds)

    def test_alloc_df_downsampled_in_sync(self) -> None:
        n = 30_000
        eq = pd.DataFrame(
            {
                "datetime": pd.date_range("2026-01-01", periods=n, freq="s"),
                "cash": np.ones(n) * 100,
                "equity": np.ones(n) * 100,
                "drawdown_pct": np.zeros(n),
            }
        )
        fills_df = pd.DataFrame(
            columns=[
                "datetime",
                "market_id",
                "action",
                "side",
                "price",
                "quantity",
                "commission",
                "bar",
            ]
        )
        market_df = pd.DataFrame(index=eq.index)
        alloc_df = pd.DataFrame({"Cash": np.ones(n) * 100}, index=eq.index)

        eq_ds, _, _, alloc_ds = _downsample(
            eq, fills_df, market_df, max_points=2000, alloc_df=alloc_df
        )

        assert alloc_ds is not None
        assert len(alloc_ds) == len(eq_ds)

    def test_equity_peak_preserved(self) -> None:
        n = 50_000
        equity = np.linspace(100, 200, n)
        peak_idx = 37_777
        equity[peak_idx] = 999.0  # artificial peak

        eq = pd.DataFrame(
            {
                "datetime": pd.date_range("2026-01-01", periods=n, freq="s"),
                "cash": np.ones(n) * 100,
                "equity": equity,
                "drawdown_pct": np.zeros(n),
            }
        )
        fills_df = pd.DataFrame(
            columns=[
                "datetime",
                "market_id",
                "action",
                "side",
                "price",
                "quantity",
                "commission",
                "bar",
            ]
        )
        market_df = pd.DataFrame(index=eq.index)

        eq_ds, _, _, _ = _downsample(eq, fills_df, market_df, max_points=2000)

        assert 999.0 in eq_ds["equity"].values


# ---------------------------------------------------------------------------
# End-to-end HTML size test
# ---------------------------------------------------------------------------


class TestHTMLChartSize:
    def test_large_backtest_html_under_5mb(self, tmp_path) -> None:
        """A 100 K-bar backtest must produce an HTML file under 5 MB.

        This is the test that would have caught the 31 MB regression.
        """
        result = _make_large_result(n_bars=100_000, n_fills=50, n_markets=3)
        output = tmp_path / "large_test.html"

        from prediction_market_extensions.analysis.legacy_backtesting.plotting import plot

        plot(
            result,
            filename=str(output),
            open_browser=False,
            progress=False,
            max_markets=3,
            plot_panels=("total_equity", "equity", "yes_price", "drawdown", "cash_equity"),
        )

        size_mb = output.stat().st_size / (1024 * 1024)
        assert size_mb < 5.0, (
            f"HTML chart is {size_mb:.1f} MB — must be under 5 MB. "
            f"Downsampling is likely broken or not being applied."
        )

    def test_small_backtest_renders_without_downsampling(self, tmp_path) -> None:
        """A small backtest (< 5000 bars) should still render correctly."""
        result = _make_large_result(n_bars=500, n_fills=5, n_markets=2)
        output = tmp_path / "small_test.html"

        from prediction_market_extensions.analysis.legacy_backtesting.plotting import plot

        plot(
            result,
            filename=str(output),
            open_browser=False,
            progress=False,
            max_markets=2,
            plot_panels=("total_equity", "equity", "drawdown"),
        )

        assert output.exists()
        size_mb = output.stat().st_size / (1024 * 1024)
        # Small file should be well under 5 MB
        assert size_mb < 3.0

    def test_html_size_does_not_scale_linearly_with_bars(self, tmp_path) -> None:
        """Doubling the bar count should NOT double the HTML size.

        This catches the case where downsampling is present but broken.
        """
        from prediction_market_extensions.analysis.legacy_backtesting.plotting import plot

        sizes = {}
        for n_bars in (10_000, 100_000):
            result = _make_large_result(n_bars=n_bars, n_fills=20, n_markets=2)
            output = tmp_path / f"scale_{n_bars}.html"
            plot(
                result,
                filename=str(output),
                open_browser=False,
                progress=False,
                max_markets=2,
                plot_panels=("total_equity", "equity"),
            )
            sizes[n_bars] = output.stat().st_size

        ratio = sizes[100_000] / sizes[10_000]
        assert ratio < 2.0, (
            f"10x more bars produced {ratio:.1f}x larger HTML — "
            f"expected <2x due to downsampling capping output size."
        )
