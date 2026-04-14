# Derived from NautilusTrader prediction-market test code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import warnings
from datetime import UTC, datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from prediction_market_extensions.analysis import legacy_plot_adapter as adapter
from prediction_market_extensions.analysis.legacy_backtesting import plotting
from prediction_market_extensions.analysis.legacy_backtesting.models import (
    PANEL_BRIER_ADVANTAGE,
    PANEL_EQUITY,
    PANEL_TOTAL_BRIER_ADVANTAGE,
    PANEL_TOTAL_CASH_EQUITY,
    PANEL_TOTAL_DRAWDOWN,
    PANEL_TOTAL_ROLLING_SHARPE,
    BacktestResult,
    Platform,
    PortfolioSnapshot,
)


class _DummyLayout:
    def __init__(self, children: list[object] | None = None) -> None:
        self.children = list(children or [])


def test_to_naive_utc_truncates_nanoseconds_without_warning() -> None:
    ts = pd.Timestamp("2026-02-22T12:55:24.290235905Z")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        converted = adapter._to_naive_utc(ts)

    assert converted == datetime(2026, 2, 22, 12, 55, 24, 290235)
    assert not any("Discarding nonzero nanoseconds" in str(warning.message) for warning in caught)


def test_build_portfolio_snapshots_truncates_nanoseconds_without_warning() -> None:
    account_report = pd.DataFrame(
        {"total": [100.0], "free": [100.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-02-22T12:55:24.290235905Z")]),
    )
    models_module = SimpleNamespace(PortfolioSnapshot=lambda **kwargs: SimpleNamespace(**kwargs))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        snapshots = adapter._build_portfolio_snapshots(models_module, account_report, fills=[])

    assert snapshots[0].timestamp == datetime(2026, 2, 22, 12, 55, 24, 290235)
    assert not any("Discarding nonzero nanoseconds" in str(warning.message) for warning in caught)


@pytest.mark.parametrize("fill_count", [250, 251, 1_667])
def test_build_legacy_backtest_layout_never_auto_limits_yes_price_fill_markers(
    monkeypatch: pytest.MonkeyPatch, tmp_path, fill_count: int
) -> None:
    base_layout = _DummyLayout()
    plotting_module = SimpleNamespace(plot=lambda *args, **kwargs: base_layout)
    apply_calls: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    engine = SimpleNamespace(trader=SimpleNamespace(generate_order_fills_report=list))

    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (SimpleNamespace(BacktestResult=_BacktestResult), plotting_module),
    )

    monkeypatch.setattr(adapter, "_extract_account_report", lambda *_: object())
    monkeypatch.setattr(
        adapter,
        "_convert_fills",
        lambda *_: [SimpleNamespace(market_id="test-market") for _ in range(fill_count)],
    )
    monkeypatch.setattr(adapter, "_build_portfolio_snapshots", lambda *args, **kwargs: [])
    monkeypatch.setattr(adapter, "_market_prices_with_fill_points", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        adapter,
        "_build_dense_portfolio_snapshots",
        lambda *args, **kwargs: [
            SimpleNamespace(timestamp=datetime(2025, 1, 1, tzinfo=UTC), total_equity=100.0),
            SimpleNamespace(timestamp=datetime(2025, 1, 2, tzinfo=UTC), total_equity=125.0),
        ],
    )
    monkeypatch.setattr(adapter, "_build_metrics", lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, "_platform_enum", lambda *args, **kwargs: "KALSHI")
    monkeypatch.setattr(
        adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: apply_calls.append(kwargs) or layout,
    )
    monkeypatch.setattr(
        adapter, "prepare_cumulative_brier_advantage", lambda **kwargs: pd.DataFrame()
    )

    layout, title = adapter.build_legacy_backtest_layout(
        engine=engine,
        output_path=tmp_path / "legacy.html",
        strategy_name="Test Strategy",
        platform="kalshi",
        initial_cash=100.0,
    )

    assert layout is base_layout
    assert title == "Test Strategy legacy chart"
    assert apply_calls == [{}]


def test_build_legacy_backtest_layout_skips_brier_when_not_requested(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    plotting_calls: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    def _fake_plot(*_args, **kwargs):  # type: ignore[no-untyped-def]
        plotting_calls.append(kwargs)
        return _DummyLayout()

    engine = SimpleNamespace(trader=SimpleNamespace(generate_order_fills_report=list))

    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (
            SimpleNamespace(BacktestResult=_BacktestResult),
            SimpleNamespace(plot=_fake_plot),
        ),
    )

    monkeypatch.setattr(adapter, "_extract_account_report", lambda *_: object())
    monkeypatch.setattr(adapter, "_convert_fills", lambda *_: [])
    monkeypatch.setattr(adapter, "_build_portfolio_snapshots", lambda *args, **kwargs: [])
    monkeypatch.setattr(adapter, "_market_prices_with_fill_points", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        adapter,
        "_build_dense_portfolio_snapshots",
        lambda *args, **kwargs: [
            SimpleNamespace(timestamp=datetime(2025, 1, 1, tzinfo=UTC), total_equity=100.0),
            SimpleNamespace(timestamp=datetime(2025, 1, 2, tzinfo=UTC), total_equity=125.0),
        ],
    )
    monkeypatch.setattr(adapter, "_build_metrics", lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, "_platform_enum", lambda *args, **kwargs: "KALSHI")
    monkeypatch.setattr(
        adapter, "_apply_layout_overrides", lambda layout, initial_cash, **kwargs: layout
    )
    monkeypatch.setattr(
        adapter,
        "prepare_cumulative_brier_advantage",
        lambda **kwargs: pytest.fail(
            "Brier inputs should not be prepared when the panel is not requested"
        ),
    )

    adapter.build_legacy_backtest_layout(
        engine=engine,
        output_path=tmp_path / "legacy.html",
        strategy_name="Test Strategy",
        platform="kalshi",
        initial_cash=100.0,
        plot_panels=(PANEL_EQUITY,),
    )

    assert plotting_calls == [
        {
            "filename": str((tmp_path / "legacy.html").resolve()),
            "max_markets": 30,
            "open_browser": False,
            "progress": False,
            "plot_panels": (PANEL_EQUITY,),
            "extra_panels": {},
        }
    ]


def test_build_legacy_backtest_layout_rejects_unknown_plot_panels(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    engine = SimpleNamespace(trader=SimpleNamespace(generate_order_fills_report=list))

    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (
            SimpleNamespace(BacktestResult=_BacktestResult),
            SimpleNamespace(plot=lambda *args, **kwargs: None),
        ),
    )

    monkeypatch.setattr(adapter, "_extract_account_report", lambda *_: object())
    monkeypatch.setattr(adapter, "_convert_fills", lambda *_: [])
    monkeypatch.setattr(adapter, "_build_portfolio_snapshots", lambda *args, **kwargs: [])
    monkeypatch.setattr(adapter, "_market_prices_with_fill_points", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        adapter,
        "_build_dense_portfolio_snapshots",
        lambda *args, **kwargs: [
            SimpleNamespace(timestamp=datetime(2025, 1, 1, tzinfo=UTC), total_equity=100.0),
            SimpleNamespace(timestamp=datetime(2025, 1, 2, tzinfo=UTC), total_equity=125.0),
        ],
    )
    monkeypatch.setattr(adapter, "_build_metrics", lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, "_platform_enum", lambda *args, **kwargs: "KALSHI")

    with pytest.raises(ValueError, match="Unknown plot panel"):
        adapter.build_legacy_backtest_layout(
            engine=engine,
            output_path=tmp_path / "legacy.html",
            strategy_name="Test Strategy",
            platform="kalshi",
            initial_cash=100.0,
            plot_panels=(PANEL_BRIER_ADVANTAGE, "not_a_panel"),
        )


def test_build_legacy_backtest_layout_adds_total_brier_panel_when_requested(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    plotting_calls: list[dict[str, object]] = []
    total_brier_panel = object()

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    def _fake_plot(*_args, **kwargs):  # type: ignore[no-untyped-def]
        plotting_calls.append(kwargs)
        return _DummyLayout()

    engine = SimpleNamespace(trader=SimpleNamespace(generate_order_fills_report=list))

    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (
            SimpleNamespace(BacktestResult=_BacktestResult),
            SimpleNamespace(plot=_fake_plot),
        ),
    )
    monkeypatch.setattr(adapter, "_extract_account_report", lambda *_: object())
    monkeypatch.setattr(adapter, "_convert_fills", lambda *_: [])
    monkeypatch.setattr(adapter, "_build_portfolio_snapshots", lambda *args, **kwargs: [])
    monkeypatch.setattr(adapter, "_market_prices_with_fill_points", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        adapter,
        "_build_dense_portfolio_snapshots",
        lambda *args, **kwargs: [
            SimpleNamespace(timestamp=datetime(2025, 1, 1, tzinfo=UTC), total_equity=100.0),
            SimpleNamespace(timestamp=datetime(2025, 1, 2, tzinfo=UTC), total_equity=125.0),
        ],
    )
    monkeypatch.setattr(adapter, "_build_metrics", lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, "_platform_enum", lambda *args, **kwargs: "KALSHI")
    monkeypatch.setattr(
        adapter, "_apply_layout_overrides", lambda layout, initial_cash, **kwargs: layout
    )
    monkeypatch.setattr(
        adapter,
        "prepare_cumulative_brier_advantage",
        lambda **kwargs: pd.DataFrame(
            {
                "brier_advantage": [0.1, -0.05],
                "cumulative_brier_advantage": [0.1, 0.05],
            },
            index=pd.to_datetime(["2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"]),
        ),
    )
    monkeypatch.setattr(adapter, "_build_total_brier_panel", lambda frame: total_brier_panel)

    adapter.build_legacy_backtest_layout(
        engine=engine,
        output_path=tmp_path / "legacy.html",
        strategy_name="Test Strategy",
        platform="kalshi",
        initial_cash=100.0,
        plot_panels=(PANEL_TOTAL_BRIER_ADVANTAGE,),
    )

    assert plotting_calls == [
        {
            "filename": str((tmp_path / "legacy.html").resolve()),
            "max_markets": 30,
            "open_browser": False,
            "progress": False,
            "plot_panels": (PANEL_TOTAL_BRIER_ADVANTAGE,),
            "extra_panels": {PANEL_TOTAL_BRIER_ADVANTAGE: total_brier_panel},
        }
    ]


def test_total_aggregate_only_panels_render_for_single_market_results(tmp_path) -> None:
    pytest.importorskip("bokeh")

    timestamps = pd.date_range("2025-01-01T00:00:00Z", periods=120, freq="h")
    equity_values = pd.Series(
        [100.0 + (idx * 0.15) + ((idx % 7) - 3) * 0.35 for idx in range(len(timestamps))],
        index=timestamps,
        dtype=float,
    )
    cash_values = equity_values - 2.5

    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=ts.to_pydatetime(),
                cash=float(cash_values.loc[ts]),
                total_equity=float(equity_values.loc[ts]),
                unrealized_pnl=float(equity_values.loc[ts] - cash_values.loc[ts]),
                num_positions=1,
            )
            for ts in timestamps
        ],
        fills=[],
        metrics={},
        strategy_name="single-market-total-panels",
        platform=Platform.KALSHI,
        start_time=timestamps[0].to_pydatetime(),
        end_time=timestamps[-1].to_pydatetime(),
        initial_cash=float(equity_values.iloc[0]),
        final_equity=float(equity_values.iloc[-1]),
        num_markets_traded=1,
        num_markets_resolved=1,
        market_prices={},
        market_pnls={},
    )

    output_path = tmp_path / "total_panels.html"
    layout = plotting.plot(
        result,
        filename=str(output_path),
        open_browser=False,
        progress=False,
        plot_panels=(
            PANEL_TOTAL_DRAWDOWN,
            PANEL_TOTAL_ROLLING_SHARPE,
            PANEL_TOTAL_CASH_EQUITY,
        ),
    )

    assert layout is not None
    assert output_path.exists()
