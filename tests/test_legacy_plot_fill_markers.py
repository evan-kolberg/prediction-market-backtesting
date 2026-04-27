# Derived from NautilusTrader prediction-market test code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import warnings
from datetime import UTC, datetime
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from prediction_market_extensions.analysis import legacy_plot_adapter as adapter
from prediction_market_extensions.analysis.legacy_backtesting import plotting
from prediction_market_extensions.analysis.legacy_backtesting.models import (
    PANEL_BRIER_ADVANTAGE,
    PANEL_DRAWDOWN,
    PANEL_EQUITY,
    PANEL_MARKET_PNL,
    PANEL_TOTAL_BRIER_ADVANTAGE,
    PANEL_TOTAL_CASH_EQUITY,
    PANEL_TOTAL_DRAWDOWN,
    PANEL_TOTAL_ROLLING_SHARPE,
    PANEL_YES_PRICE,
    BacktestResult,
    Fill,
    OrderAction,
    Platform,
    PortfolioSnapshot,
    Side,
)


class _DummyLayout:
    def __init__(self, children: list[object] | None = None) -> None:
        self.children = list(children or [])


def test_select_display_markets_includes_price_only_markets_within_limit() -> None:
    market_df = pd.DataFrame(
        {
            "filled-market": [0.40, 0.41],
            "nothing-ever-happens-2026": [0.90, 0.91],
        }
    )
    fills_df = pd.DataFrame({"market_id": ["filled-market"]})

    display_markets = plotting._select_display_markets(
        market_df,
        fills_df,
        max_markets=10,
    )

    assert display_markets == ["filled-market", "nothing-ever-happens-2026"]


def test_select_display_markets_prioritizes_filled_markets_when_limited() -> None:
    market_df = pd.DataFrame(
        {
            "filled-market": [0.40, 0.41],
            "price-only-volatile": [0.10, 0.90],
        }
    )
    fills_df = pd.DataFrame({"market_id": ["filled-market"]})

    display_markets = plotting._select_display_markets(
        market_df,
        fills_df,
        max_markets=1,
    )

    assert display_markets == ["filled-market"]


def test_allocation_values_no_token_at_its_own_price() -> None:
    start = pd.Timestamp("2026-03-14T18:00:00")
    eq = pd.DataFrame(
        {
            "datetime": [start, start + pd.Timedelta(minutes=1)],
            "cash": [97.0, 97.0],
            "equity": [100.0, 101.0],
            "drawdown_pct": [0.0, 0.0],
        }
    )
    fills_df = pd.DataFrame(
        {
            "datetime": [start],
            "market_id": ["no-token"],
            "action": ["buy"],
            "side": ["no"],
            "price": [0.30],
            "quantity": [10.0],
            "commission": [0.0],
            "bar": [0],
        }
    )

    allocation = plotting._build_allocation_data(
        eq,
        fills_df,
        {"no-token": [(start, 0.30), (start + pd.Timedelta(minutes=1), 0.40)]},
    )

    assert allocation["no-token"].tolist() == pytest.approx([3.0, 4.0])


def test_allocation_preserves_short_liability_sign() -> None:
    start = pd.Timestamp("2026-04-01T00:00:00")
    eq = pd.DataFrame(
        {
            "datetime": [start, start + pd.Timedelta(minutes=1)],
            "cash": [106.0, 106.0],
            "equity": [100.0, 100.0],
            "drawdown_pct": [0.0, 0.0],
        }
    )
    fills_df = pd.DataFrame(
        {
            "datetime": [start],
            "market_id": ["short-yes-market"],
            "action": ["sell"],
            "side": ["yes"],
            "price": [0.60],
            "quantity": [10.0],
            "commission": [0.0],
            "bar": [0],
        }
    )

    allocation = plotting._build_allocation_data(
        eq,
        fills_df,
        {"short-yes-market": [(start, 0.60), (start + pd.Timedelta(minutes=1), 0.60)]},
    )

    assert allocation["short-yes-market"].tolist() == pytest.approx([-6.0, -6.0])
    assert allocation["Cash"].tolist() == pytest.approx([106.0, 106.0])


def test_allocation_zeroes_settled_position_on_price_feed_final_bar() -> None:
    start = pd.Timestamp("2026-04-01T00:00:00")
    settlement = start + pd.Timedelta(minutes=5)
    eq = pd.DataFrame(
        {
            "datetime": [start, settlement],
            "cash": [995.25, 1000.25],
            "equity": [1000.0, 1000.25],
            "drawdown_pct": [0.0, 0.0],
        }
    )
    fills_df = pd.DataFrame(
        {
            "datetime": [start],
            "market_id": ["yes-winner"],
            "action": ["buy"],
            "side": ["yes"],
            "price": [0.95],
            "quantity": [5.0],
            "commission": [0.0],
            "bar": [0],
        }
    )

    allocation = plotting._build_allocation_data(
        eq,
        fills_df,
        {"yes-winner": [(start, 0.95), (settlement, 0.95)]},
    )

    assert allocation["yes-winner"].tolist() == pytest.approx([4.75, 0.0])
    assert allocation["Cash"].tolist() == pytest.approx([995.25, 1000.25])


def test_market_pnl_panel_plots_one_final_marker_per_market(tmp_path) -> None:
    pytest.importorskip("bokeh")
    from bokeh.models import ColumnDataSource

    start = datetime(2026, 4, 1, tzinfo=UTC)
    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=start,
                cash=99.0,
                total_equity=100.0,
                unrealized_pnl=1.0,
                num_positions=1,
            ),
            PortfolioSnapshot(
                timestamp=start.replace(minute=1),
                cash=98.0,
                total_equity=102.0,
                unrealized_pnl=4.0,
                num_positions=1,
            ),
        ],
        fills=[
            Fill(
                order_id="fill-1",
                market_id="repeat-market",
                action=OrderAction.BUY,
                side=Side.YES,
                price=0.40,
                quantity=2.0,
                timestamp=start,
            ),
            Fill(
                order_id="fill-2",
                market_id="repeat-market",
                action=OrderAction.BUY,
                side=Side.YES,
                price=0.45,
                quantity=2.0,
                timestamp=start.replace(minute=1),
            ),
        ],
        metrics={},
        strategy_name="market-pnl-repeat",
        platform=Platform.POLYMARKET,
        start_time=start,
        end_time=start.replace(minute=1),
        initial_cash=100.0,
        final_equity=102.0,
        num_markets_traded=1,
        num_markets_resolved=1,
        market_prices={
            "repeat-market": [
                (start, 0.40),
                (start.replace(minute=1), 0.45),
            ]
        },
        market_pnls={"repeat-market": 2.0},
    )

    layout = plotting.plot(
        result,
        filename=str(tmp_path / "market_pnl_repeat.html"),
        open_browser=False,
        progress=False,
        plot_panels=("market_pnl",),
    )

    marker_sources = [
        source
        for source in layout.select({"type": ColumnDataSource})
        if {"pnl_long", "pnl_short", "market_id"}.issubset(source.data)
    ]

    assert len(marker_sources) == 1
    assert marker_sources[0].data["market_id"].tolist() == ["repeat-market"]
    assert marker_sources[0].data["pnl_long"].tolist() == pytest.approx([2.0])


def test_dense_adapter_replays_no_token_buy_as_long_token_quantity() -> None:
    assert adapter._signed_quantity("buy", "no", 10.0) == pytest.approx(10.0)
    assert adapter._signed_quantity("sell", "no", 4.0) == pytest.approx(-4.0)


def test_monthly_returns_use_prior_month_end_for_sparse_equity(tmp_path) -> None:
    pytest.importorskip("bokeh")
    from bokeh.models import ColumnDataSource

    snapshots = [
        ("2026-01-31T23:59:00Z", 100.0),
        ("2026-02-28T23:59:00Z", 110.0),
        ("2026-03-31T23:59:00Z", 99.0),
    ]
    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=pd.Timestamp(ts).to_pydatetime(),
                cash=equity,
                total_equity=equity,
                unrealized_pnl=0.0,
                num_positions=0,
            )
            for ts, equity in snapshots
        ],
        fills=[],
        metrics={},
        strategy_name="monthly-sparse",
        platform=Platform.POLYMARKET,
        start_time=pd.Timestamp(snapshots[0][0]).to_pydatetime(),
        end_time=pd.Timestamp(snapshots[-1][0]).to_pydatetime(),
        initial_cash=100.0,
        final_equity=99.0,
        num_markets_traded=0,
        num_markets_resolved=0,
        market_prices={},
    )

    layout = plotting.plot(
        result,
        filename=str(tmp_path / "monthly_sparse.html"),
        open_browser=False,
        progress=False,
        plot_panels=("monthly_returns",),
    )

    source = next(
        source
        for source in layout.select({"type": ColumnDataSource})
        if {"month", "year", "ret"}.issubset(source.data)
    )
    returns_by_month = dict(zip(source.data["month"], source.data["ret"], strict=True))

    assert returns_by_month["Feb"] == pytest.approx(0.10)
    assert returns_by_month["Mar"] == pytest.approx(-0.10)


def test_yes_price_plot_renders_market_with_prices_but_no_fills(tmp_path) -> None:
    start = datetime(2026, 3, 14, 18, tzinfo=UTC)
    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=start,
                cash=100.0,
                total_equity=100.0,
                unrealized_pnl=0.0,
                num_positions=0,
            ),
            PortfolioSnapshot(
                timestamp=start.replace(minute=1),
                cash=99.5,
                total_equity=100.2,
                unrealized_pnl=0.2,
                num_positions=1,
            ),
        ],
        fills=[
            Fill(
                order_id="fill-1",
                market_id="filled-market",
                action=OrderAction.BUY,
                side=Side.YES,
                price=0.40,
                quantity=1.0,
                timestamp=start,
            )
        ],
        metrics={},
        strategy_name="test",
        platform=Platform.POLYMARKET,
        start_time=start,
        end_time=start.replace(minute=1),
        initial_cash=100.0,
        final_equity=100.2,
        num_markets_traded=1,
        num_markets_resolved=0,
        market_prices={
            "filled-market": [(start, 0.40), (start.replace(minute=1), 0.41)],
            "nothing-ever-happens-2026": [(start, 0.90), (start.replace(minute=1), 0.91)],
        },
    )
    output_path = tmp_path / "yes_price_price_only_market.html"

    plotting.plot(
        result,
        filename=str(output_path),
        open_browser=False,
        progress=False,
        max_markets=10,
        plot_panels=(PANEL_YES_PRICE,),
    )

    assert "price_nothing-ever-happens-2026" in output_path.read_text(encoding="utf-8")


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


def test_dense_portfolio_snapshots_apply_fill_cash_and_position_atomically() -> None:
    start = datetime(2026, 4, 26, 18, 4, 42)
    fill_time = datetime(2026, 4, 26, 18, 4, 42, 993000)
    cash_report_time = datetime(2026, 4, 26, 18, 4, 43, 43000)
    models_module = SimpleNamespace(PortfolioSnapshot=lambda **kwargs: SimpleNamespace(**kwargs))
    sparse_snapshots = [
        SimpleNamespace(timestamp=start, cash=100.0),
        SimpleNamespace(timestamp=cash_report_time, cash=95.25),
    ]
    fills = [
        Fill(
            order_id="fill-1",
            market_id="late-favorite",
            action=OrderAction.BUY,
            side=Side.YES,
            price=0.95,
            quantity=5.0,
            timestamp=fill_time,
        )
    ]

    snapshots = adapter._build_dense_portfolio_snapshots(
        models_module=models_module,
        sparse_snapshots=sparse_snapshots,
        fills=fills,
        market_prices={
            "late-favorite": [
                (start, 0.95),
                (fill_time, 0.95),
                (cash_report_time, 0.95),
            ]
        },
        initial_cash=100.0,
    )

    fill_snapshot = next(snapshot for snapshot in snapshots if snapshot.timestamp == fill_time)
    assert fill_snapshot.cash == pytest.approx(95.25)
    assert fill_snapshot.total_equity == pytest.approx(100.0)
    assert max(snapshot.total_equity for snapshot in snapshots) == pytest.approx(100.0)


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


def test_apply_layout_overrides_limits_yes_price_fill_markers() -> None:
    class _Axis:
        axis_label = "YES Price"

    class _Glyph:
        pass

    class _Source:
        def __init__(self) -> None:
            self.data = {
                "index": np.arange(6),
                "datetime": pd.date_range("2026-01-01", periods=6).to_numpy(),
                "price": np.arange(6, dtype=float),
                "fill_color": np.array(["1", "0", "1", "0", "1", "0"]),
                "market_id": np.array(["m"] * 6),
                "action": np.array(["buy"] * 6),
                "side": np.array(["yes"] * 6),
                "quantity": np.arange(6, dtype=float),
            }

    class _Renderer:
        def __init__(self, source: _Source) -> None:
            self.data_source = source
            self.glyph = _Glyph()

    class _LegendItem:
        def __init__(self, renderer: _Renderer) -> None:
            self.label = {"value": "Fills (6)"}
            self.renderers = [renderer]

    class _Legend:
        def __init__(self, item: _LegendItem) -> None:
            self.items = [item]

    source = _Source()
    renderer = _Renderer(source)
    legend_item = _LegendItem(renderer)
    fig = SimpleNamespace(
        title=SimpleNamespace(text="YES Price"),
        yaxis=[_Axis()],
        renderers=[renderer],
        legend=[_Legend(legend_item)],
        tools=[],
    )

    adapter._apply_layout_overrides(
        _DummyLayout(children=[fig]),
        initial_cash=100.0,
        max_yes_price_fill_markers=3,
    )

    assert source.data["price"].tolist() == [0.0, 2.0, 5.0]
    assert np.issubdtype(source.data["datetime"].dtype, np.datetime64)
    assert np.array_equal(
        source.data["datetime"],
        np.array(
            [
                pd.Timestamp("2026-01-01T00:00:00").to_datetime64(),
                pd.Timestamp("2026-01-03T00:00:00").to_datetime64(),
                pd.Timestamp("2026-01-06T00:00:00").to_datetime64(),
            ]
        ),
    )
    assert legend_item.label == {"value": "Fills (3 of 6)"}


def test_apply_layout_overrides_limits_market_pnl_fill_markers() -> None:
    class _Axis:
        axis_label = "Profit / Loss"

    class _Glyph:
        pass

    class _Source:
        def __init__(self) -> None:
            self.data = {
                "index": np.arange(6),
                "datetime": pd.date_range("2026-01-01", periods=6).to_numpy(),
                "pnl_long": np.arange(6, dtype=float),
                "pnl_short": np.arange(6, dtype=float),
                "positive": np.array(["1", "0", "1", "0", "1", "0"]),
                "market_id": np.array(["m"] * 6),
                "size_marker": np.arange(6, dtype=float),
            }

    class _Renderer:
        def __init__(self, source: _Source) -> None:
            self.data_source = source
            self.glyph = _Glyph()

    source = _Source()
    renderer = _Renderer(source)
    fig = SimpleNamespace(
        title=SimpleNamespace(text="Profit / Loss"),
        yaxis=[_Axis()],
        renderers=[renderer],
        legend=[],
        tools=[],
    )

    adapter._apply_layout_overrides(
        _DummyLayout(children=[fig]),
        initial_cash=100.0,
        max_market_pnl_fill_markers=3,
    )

    assert source.data["index"].tolist() == [0, 2, 5]
    assert np.issubdtype(source.data["datetime"].dtype, np.datetime64)
    assert np.array_equal(
        source.data["datetime"],
        np.array(
            [
                pd.Timestamp("2026-01-01T00:00:00").to_datetime64(),
                pd.Timestamp("2026-01-03T00:00:00").to_datetime64(),
                pd.Timestamp("2026-01-06T00:00:00").to_datetime64(),
            ]
        ),
    )
    assert source.data["pnl_long"].tolist() == [0.0, 2.0, 5.0]


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


def test_zero_fill_zero_cash_report_panels_do_not_crash(tmp_path) -> None:
    pytest.importorskip("bokeh")

    timestamps = pd.date_range("2025-01-01T00:00:00Z", periods=3, freq="h")
    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=ts.to_pydatetime(),
                cash=0.0,
                total_equity=0.0,
                unrealized_pnl=0.0,
                num_positions=0,
            )
            for ts in timestamps
        ],
        fills=[],
        metrics={},
        strategy_name="zero-fill-zero-cash",
        platform=Platform.KALSHI,
        start_time=timestamps[0].to_pydatetime(),
        end_time=timestamps[-1].to_pydatetime(),
        initial_cash=0.0,
        final_equity=0.0,
        num_markets_traded=0,
        num_markets_resolved=1,
        market_prices={},
        market_pnls={},
    )

    output_path = tmp_path / "zero_fill.html"
    layout = plotting.plot(
        result,
        filename=str(output_path),
        open_browser=False,
        progress=False,
        plot_panels=(PANEL_EQUITY, PANEL_DRAWDOWN, PANEL_TOTAL_DRAWDOWN),
    )

    assert layout is not None
    assert output_path.exists()


def test_market_pnl_panel_uses_resolved_market_pnl_for_fill_markers(tmp_path) -> None:
    pytest.importorskip("bokeh")
    from bokeh.models import ColumnDataSource

    start = datetime(2026, 3, 14, 18, tzinfo=UTC)
    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=start,
                cash=99.75,
                total_equity=100.0,
                unrealized_pnl=0.25,
                num_positions=1,
            ),
            PortfolioSnapshot(
                timestamp=start.replace(minute=1),
                cash=104.75,
                total_equity=104.75,
                unrealized_pnl=0.0,
                num_positions=0,
            ),
        ],
        fills=[
            Fill(
                order_id="fill-1",
                market_id="winning-market",
                action=OrderAction.BUY,
                side=Side.YES,
                price=0.05,
                quantity=5.0,
                timestamp=start,
            )
        ],
        metrics={},
        strategy_name="resolved-pnl-marker",
        platform=Platform.POLYMARKET,
        start_time=start,
        end_time=start.replace(minute=1),
        initial_cash=100.0,
        final_equity=104.75,
        num_markets_traded=1,
        num_markets_resolved=1,
        market_prices={"winning-market": [(start, 0.05), (start.replace(minute=1), 1.0)]},
        market_pnls={"winning-market": 4.75},
    )

    layout = plotting.plot(
        result,
        filename=str(tmp_path / "resolved_pnl_marker.html"),
        open_browser=False,
        progress=False,
        plot_panels=(PANEL_MARKET_PNL,),
    )

    sources = list(layout.select({"type": ColumnDataSource}))
    pnl_source = next(source for source in sources if "pnl_long" in source.data)
    assert pnl_source.data["pnl_long"].tolist() == [4.75]
    assert np.isnan(pnl_source.data["pnl_short"][0])
    assert pnl_source.data["positive"].tolist() == ["1"]


def test_build_dataframes_does_not_compute_percent_returns_from_negative_initial_cash() -> None:
    start = datetime(2026, 3, 14, 18, tzinfo=UTC)
    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=start,
                cash=-100.0,
                total_equity=-100.0,
                unrealized_pnl=0.0,
                num_positions=0,
            ),
            PortfolioSnapshot(
                timestamp=start.replace(minute=1),
                cash=-90.0,
                total_equity=-90.0,
                unrealized_pnl=0.0,
                num_positions=0,
            ),
        ],
        fills=[],
        metrics={},
        strategy_name="negative-initial",
        platform=Platform.POLYMARKET,
        start_time=start,
        end_time=start.replace(minute=1),
        initial_cash=-100.0,
        final_equity=-90.0,
        num_markets_traded=0,
        num_markets_resolved=1,
    )

    eq, _, _, _ = plotting._build_dataframes(result)

    assert eq["equity_pct"].tolist() == [1.0, 1.0]
    assert eq["return_pct"].tolist() == [0.0, 0.0]


def test_total_rolling_sharpe_uses_equity_timestamps(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    pytest.importorskip("bokeh")

    captured: dict[str, object] = {}

    def _fake_rolling_sharpe_array(
        values, annualize=True, annualization_factor=None, datetimes=None
    ):  # type: ignore[no-untyped-def]
        captured["datetimes"] = datetimes
        return np.zeros(len(values), dtype=float), 20

    monkeypatch.setattr(plotting, "_rolling_sharpe_array", _fake_rolling_sharpe_array)

    timestamps = pd.date_range("2025-01-01T00:00:00Z", periods=80, freq="h")
    result = BacktestResult(
        equity_curve=[
            PortfolioSnapshot(
                timestamp=ts.to_pydatetime(),
                cash=100.0,
                total_equity=100.0 + idx,
                unrealized_pnl=float(idx),
                num_positions=1,
            )
            for idx, ts in enumerate(timestamps)
        ],
        fills=[],
        metrics={},
        strategy_name="total-sharpe-datetimes",
        platform=Platform.KALSHI,
        start_time=timestamps[0].to_pydatetime(),
        end_time=timestamps[-1].to_pydatetime(),
        initial_cash=100.0,
        final_equity=179.0,
        num_markets_traded=1,
        num_markets_resolved=1,
        market_prices={},
        market_pnls={},
    )

    plotting.plot(
        result,
        filename=str(tmp_path / "total_sharpe.html"),
        open_browser=False,
        progress=False,
        plot_panels=(PANEL_TOTAL_ROLLING_SHARPE,),
    )

    assert isinstance(captured["datetimes"], pd.DatetimeIndex)
    assert list(captured["datetimes"]) == list(pd.DatetimeIndex(timestamps))
