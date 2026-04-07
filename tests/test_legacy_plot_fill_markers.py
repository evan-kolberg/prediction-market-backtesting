# Derived from NautilusTrader prediction-market test code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-03-16.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

import warnings
from datetime import UTC
from datetime import datetime
from types import SimpleNamespace

import pandas as pd
import pytest

from nautilus_trader.analysis import legacy_plot_adapter as adapter
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_BRIER_ADVANTAGE
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_EQUITY


class _DummyLayout:
    def __init__(self, children: list[object] | None = None) -> None:
        self.children = list(children or [])


def test_to_naive_utc_truncates_nanoseconds_without_warning() -> None:
    ts = pd.Timestamp("2026-02-22T12:55:24.290235905Z")

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        converted = adapter._to_naive_utc(ts)

    assert converted == datetime(2026, 2, 22, 12, 55, 24, 290235)
    assert not any(
        "Discarding nonzero nanoseconds" in str(warning.message) for warning in caught
    )


def test_build_portfolio_snapshots_truncates_nanoseconds_without_warning() -> None:
    account_report = pd.DataFrame(
        {"total": [100.0], "free": [100.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-02-22T12:55:24.290235905Z")]),
    )
    models_module = SimpleNamespace(
        PortfolioSnapshot=lambda **kwargs: SimpleNamespace(**kwargs)
    )

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        snapshots = adapter._build_portfolio_snapshots(
            models_module,
            account_report,
            fills=[],
        )

    assert snapshots[0].timestamp == datetime(2026, 2, 22, 12, 55, 24, 290235)
    assert not any(
        "Discarding nonzero nanoseconds" in str(warning.message) for warning in caught
    )


def test_should_hide_yes_price_fill_markers_only_when_fill_count_exceeds_marker_budget() -> (
    None
):
    assert (
        adapter._should_hide_yes_price_fill_markers(fill_count=250, max_points=5_000)
        is False
    )
    assert (
        adapter._should_hide_yes_price_fill_markers(fill_count=251, max_points=5_000)
        is True
    )
    assert (
        adapter._should_hide_yes_price_fill_markers(fill_count=1_667, max_points=5_000)
        is True
    )
    assert (
        adapter._should_hide_yes_price_fill_markers(fill_count=250, max_points=0)
        is False
    )
    assert (
        adapter._should_hide_yes_price_fill_markers(fill_count=251, max_points=0)
        is True
    )


def test_apply_layout_overrides_removes_yes_price_fill_markers_when_enabled() -> None:
    bokeh_models = pytest.importorskip("bokeh.models")
    bokeh_plotting = pytest.importorskip("bokeh.plotting")

    fig = bokeh_plotting.figure(title="YES Price")
    fig.yaxis.axis_label = "YES Price"
    price_source = bokeh_models.ColumnDataSource(
        {
            "index": [0, 1],
            "datetime": [
                pd.Timestamp("2026-03-02T14:05:00Z"),
                pd.Timestamp("2026-03-02T14:10:00Z"),
            ],
            "price_test_market": [0.48, 0.52],
        },
    )
    price_renderer = fig.line(x="index", y="price_test_market", source=price_source)
    fill_source = bokeh_models.ColumnDataSource(
        {
            "index": [0],
            "datetime": [pd.Timestamp("2026-03-02T14:07:00Z")],
            "action": ["BUY"],
            "side": ["YES"],
            "quantity": [1],
            "price": [0.49],
            "market_id": ["test-market"],
        },
    )
    fill_renderer = fig.scatter(
        x="index",
        y="price",
        source=fill_source,
        size=8,
        color="green",
        legend_label="Fills (1)",
    )
    fig.add_tools(
        bokeh_models.HoverTool(
            renderers=[price_renderer, fill_renderer],
            tooltips=[("Date", "@datetime{%F %T}")],
            formatters={"@datetime": "datetime"},
        ),
    )

    adapter._apply_layout_overrides(
        _DummyLayout(children=[fig]),
        initial_cash=1_000.0,
        hide_yes_price_fill_markers=True,
    )

    glyph_renderers = [
        renderer for renderer in fig.renderers if hasattr(renderer, "data_source")
    ]
    assert price_renderer in glyph_renderers
    assert fill_renderer not in glyph_renderers
    assert all(
        "fills" not in adapter._legend_item_label_text(item).lower()
        for legend in fig.legend
        for item in legend.items
    )


@pytest.mark.parametrize(
    ("fill_count", "expected_hide_markers"),
    [
        (250, False),
        (251, True),
        (1_667, True),
    ],
)
def test_build_legacy_backtest_layout_auto_hides_yes_price_fill_markers_for_high_fill_counts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    fill_count: int,
    expected_hide_markers: bool,
) -> None:
    base_layout = _DummyLayout()
    plotting_module = SimpleNamespace(plot=lambda *args, **kwargs: base_layout)
    apply_calls: list[bool] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    engine = SimpleNamespace(
        trader=SimpleNamespace(generate_order_fills_report=list),
    )

    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (SimpleNamespace(BacktestResult=_BacktestResult), plotting_module),
    )
    monkeypatch.setattr(
        adapter, "_configure_legacy_downsampling", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(adapter, "_extract_account_report", lambda *_: object())
    monkeypatch.setattr(
        adapter,
        "_convert_fills",
        lambda *_: [
            SimpleNamespace(market_id="test-market") for _ in range(fill_count)
        ],
    )
    monkeypatch.setattr(
        adapter, "_build_portfolio_snapshots", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        adapter, "_market_prices_with_fill_points", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        adapter,
        "_build_dense_portfolio_snapshots",
        lambda *args, **kwargs: [
            SimpleNamespace(
                timestamp=datetime(2025, 1, 1, tzinfo=UTC), total_equity=100.0
            ),
            SimpleNamespace(
                timestamp=datetime(2025, 1, 2, tzinfo=UTC), total_equity=125.0
            ),
        ],
    )
    monkeypatch.setattr(adapter, "_build_metrics", lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, "_platform_enum", lambda *args, **kwargs: "KALSHI")
    monkeypatch.setattr(
        adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: (
            apply_calls.append(
                kwargs.get("hide_yes_price_fill_markers", False),
            )
            or layout
        ),
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
    assert apply_calls == [expected_hide_markers]


def test_build_legacy_backtest_layout_skips_brier_when_not_requested(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    plotting_calls: list[dict[str, object]] = []

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    def _fake_plot(*_args, **kwargs):  # type: ignore[no-untyped-def]
        plotting_calls.append(kwargs)
        return _DummyLayout()

    engine = SimpleNamespace(
        trader=SimpleNamespace(generate_order_fills_report=list),
    )

    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (
            SimpleNamespace(BacktestResult=_BacktestResult),
            SimpleNamespace(plot=_fake_plot),
        ),
    )
    monkeypatch.setattr(
        adapter, "_configure_legacy_downsampling", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(adapter, "_extract_account_report", lambda *_: object())
    monkeypatch.setattr(adapter, "_convert_fills", lambda *_: [])
    monkeypatch.setattr(
        adapter, "_build_portfolio_snapshots", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        adapter, "_market_prices_with_fill_points", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        adapter,
        "_build_dense_portfolio_snapshots",
        lambda *args, **kwargs: [
            SimpleNamespace(
                timestamp=datetime(2025, 1, 1, tzinfo=UTC), total_equity=100.0
            ),
            SimpleNamespace(
                timestamp=datetime(2025, 1, 2, tzinfo=UTC), total_equity=125.0
            ),
        ],
    )
    monkeypatch.setattr(adapter, "_build_metrics", lambda *args, **kwargs: {})
    monkeypatch.setattr(adapter, "_platform_enum", lambda *args, **kwargs: "KALSHI")
    monkeypatch.setattr(
        adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
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
        },
    ]


def test_build_legacy_backtest_layout_rejects_unknown_plot_panels(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    engine = SimpleNamespace(
        trader=SimpleNamespace(generate_order_fills_report=list),
    )

    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (
            SimpleNamespace(BacktestResult=_BacktestResult),
            SimpleNamespace(plot=lambda *args, **kwargs: None),
        ),
    )
    monkeypatch.setattr(
        adapter, "_configure_legacy_downsampling", lambda *_args, **_kwargs: None
    )
    monkeypatch.setattr(adapter, "_extract_account_report", lambda *_: object())
    monkeypatch.setattr(adapter, "_convert_fills", lambda *_: [])
    monkeypatch.setattr(
        adapter, "_build_portfolio_snapshots", lambda *args, **kwargs: []
    )
    monkeypatch.setattr(
        adapter, "_market_prices_with_fill_points", lambda *args, **kwargs: {}
    )
    monkeypatch.setattr(
        adapter,
        "_build_dense_portfolio_snapshots",
        lambda *args, **kwargs: [
            SimpleNamespace(
                timestamp=datetime(2025, 1, 1, tzinfo=UTC), total_equity=100.0
            ),
            SimpleNamespace(
                timestamp=datetime(2025, 1, 2, tzinfo=UTC), total_equity=125.0
            ),
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
