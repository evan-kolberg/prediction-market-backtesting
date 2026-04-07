# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

import json
from datetime import UTC
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from nautilus_trader.analysis import legacy_plot_adapter as adapter
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_BRIER_ADVANTAGE
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_CASH_EQUITY
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_EQUITY
from nautilus_trader.analysis.legacy_plot_adapter import prepare_cumulative_brier_advantage


class _DummyAxis:
    def __init__(self, axis_label: str) -> None:
        self.axis_label = axis_label


class _DummyFigure:
    def __init__(self, axis_label: str, title: str) -> None:
        self.yaxis = [_DummyAxis(axis_label)]
        self.title = title
        self.renderers: list[object] = []
        self.children: list[object] = []


class _DummyDiv:
    def __init__(self, text: str) -> None:
        self.text = text


class _DummyLayout:
    def __init__(self, children: list[object] | None = None) -> None:
        self.children = list(children or [])


def test_prepare_cumulative_brier_advantage_calculates_expected_values() -> None:
    index = pd.date_range("2025-01-01", periods=3, freq="D")
    user_probabilities = pd.Series([0.8, 0.4, 0.3], index=index)
    market_probabilities = pd.Series([0.6, 0.5, 0.7], index=index)
    outcomes = pd.Series([1.0, 0.0, 0.0], index=index)

    result = prepare_cumulative_brier_advantage(
        user_probabilities=user_probabilities,
        market_probabilities=market_probabilities,
        outcomes=outcomes,
    )

    assert not result.empty
    assert result["brier_advantage"].iloc[0] == pytest.approx(0.12)
    assert result["brier_advantage"].iloc[1] == pytest.approx(0.09)
    assert result["brier_advantage"].iloc[2] == pytest.approx(0.40)
    assert result["cumulative_brier_advantage"].iloc[-1] == pytest.approx(0.61)


def test_prepare_cumulative_brier_advantage_returns_empty_when_inputs_missing() -> None:
    result = prepare_cumulative_brier_advantage(
        user_probabilities=None,
        market_probabilities=pd.Series([0.5]),
        outcomes=pd.Series([1.0]),
    )

    assert result.empty


def test_apply_layout_overrides_removes_nested_data_banner_and_preserves_panels() -> None:
    monthly_returns = _DummyFigure(axis_label="Monthly Returns", title="Monthly Returns")
    periodic_pnl = _DummyFigure(axis_label="Periodic PnL", title="Periodic PnL")
    nested = _DummyLayout(
        children=[
            (_DummyDiv("<b>Data:</b> downsampled to 10,000 points"), 0, 0),
            monthly_returns,
            periodic_pnl,
        ],
    )
    layout = _DummyLayout(children=[nested])

    result = adapter._apply_layout_overrides(layout, initial_cash=1_000.0)

    assert result is layout
    assert nested.children == [monthly_returns, periodic_pnl]
    assert [fig.yaxis[0].axis_label for fig in nested.children] == [
        "Monthly Returns",
        "Periodic PnL",
    ]


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("2026-03-02T14:05:00Z", "2026-03-02T18:45:00Z", "Mar 02, 2026"),
        ("2026-03-02T14:05:00Z", "2026-03-09T18:45:00Z", "Mar 02 - 09, 2026"),
        ("2026-03-30T14:05:00Z", "2026-04-02T18:45:00Z", "Mar 30, 2026 - Apr 02, 2026"),
    ],
)
def test_format_period_label_uses_day_level_ranges(start: str, end: str, expected: str) -> None:
    assert adapter._format_period_label(start, end) == expected


def test_standardize_periodic_pnl_panel_replaces_dual_hover_with_single_bar_renderer() -> None:
    bokeh_models = pytest.importorskip("bokeh.models")
    bokeh_plotting = pytest.importorskip("bokeh.plotting")

    fig = bokeh_plotting.figure(title="P&L (periodic)")
    fig.yaxis.axis_label = "P&L (periodic)"
    source = bokeh_models.ColumnDataSource(
        {
            "x": [1.0, 2.0],
            "pnl_pos": [7.5, 0.0],
            "pnl_neg": [0.0, -3.25],
            "pnl": [7.5, -3.25],
            "dt_start": [
                pd.Timestamp("2026-03-02T14:05:00Z"),
                pd.Timestamp("2026-03-03T11:00:00Z"),
            ],
            "dt_end": [pd.Timestamp("2026-03-02T18:45:00Z"), pd.Timestamp("2026-03-03T17:30:00Z")],
        },
    )
    gain = fig.vbar(x="x", top="pnl_pos", width=0.8, source=source)
    loss = fig.vbar(x="x", top="pnl_neg", width=0.8, source=source)
    fig.add_tools(
        bokeh_models.HoverTool(
            renderers=[gain, loss],
            tooltips=[("Period", "@dt_start{%b %Y} - @dt_end{%b %Y}")],
            formatters={"@dt_start": "datetime", "@dt_end": "datetime"},
        ),
    )

    adapter._standardize_periodic_pnl_panel(_DummyLayout(children=[fig]))

    hover_tools = [tool for tool in fig.tools if tool.__class__.__name__ == "HoverTool"]
    assert len(hover_tools) == 1
    assert len(hover_tools[0].renderers) == 1
    assert hover_tools[0].tooltips == [
        ("Period", "@period_label"),
        ("Start", "@dt_start{%F %T}"),
        ("End", "@dt_end{%F %T}"),
        ("P&L", "@pnl{$0,0.00}"),
    ]

    glyph_renderers = [renderer for renderer in fig.renderers if hasattr(renderer, "data_source")]
    assert len(glyph_renderers) == 1
    data = glyph_renderers[0].data_source.data
    assert data["period_label"] == ["Mar 02, 2026", "Mar 03, 2026"]


def test_standardize_periodic_pnl_panel_removes_legacy_hover_from_merged_toolbar() -> None:
    bokeh_embed = pytest.importorskip("bokeh.embed")
    bokeh_layouts = pytest.importorskip("bokeh.layouts")
    bokeh_models = pytest.importorskip("bokeh.models")
    bokeh_plotting = pytest.importorskip("bokeh.plotting")

    fig = bokeh_plotting.figure(title="P&L (periodic)")
    fig.yaxis.axis_label = "P&L (periodic)"
    source = bokeh_models.ColumnDataSource(
        {
            "x": [1.0],
            "pnl_pos": [7.5],
            "pnl_neg": [0.0],
            "pnl": [7.5],
            "dt_start": [pd.Timestamp("2026-03-02T14:05:00Z")],
            "dt_end": [pd.Timestamp("2026-03-02T18:45:00Z")],
        },
    )
    gain = fig.vbar(x="x", top="pnl_pos", width=0.8, source=source)
    loss = fig.vbar(x="x", top="pnl_neg", width=0.8, source=source)
    fig.add_tools(
        bokeh_models.HoverTool(
            renderers=[gain, loss],
            tooltips=[("Period", "@dt_start{%b %Y} - @dt_end{%b %Y}")],
            formatters={"@dt_start": "datetime", "@dt_end": "datetime"},
        ),
    )
    layout = bokeh_layouts.gridplot([[fig]], merge_tools=True)

    adapter._standardize_periodic_pnl_panel(layout)

    payload = json.dumps(bokeh_embed.json_item(layout))

    assert "@dt_start{%b %Y}" not in payload
    assert "@period_label" in payload
    assert "@dt_start{%F %T}" in payload
    assert "@dt_end{%F %T}" in payload


def test_standardize_yes_price_hover_shows_timestamp_and_yes_price() -> None:
    bokeh_models = pytest.importorskip("bokeh.models")
    bokeh_plotting = pytest.importorskip("bokeh.plotting")

    fig = bokeh_plotting.figure(title="YES Price")
    fig.yaxis.axis_label = "YES Price"
    source = bokeh_models.ColumnDataSource(
        {
            "index": [0, 1],
            "datetime": [
                pd.Timestamp("2026-03-02T14:05:00Z"),
                pd.Timestamp("2026-03-02T14:10:00Z"),
            ],
            "price_test_market": [0.48, 0.52],
        },
    )
    renderer = fig.line(x="index", y="price_test_market", source=source)
    fig.add_tools(bokeh_models.HoverTool(renderers=[renderer], tooltips=[("x, y", "$x, $y")]))

    adapter._standardize_yes_price_hover(_DummyLayout(children=[fig]))

    hover_tools = [tool for tool in fig.tools if tool.__class__.__name__ == "HoverTool"]
    assert len(hover_tools) == 1
    assert hover_tools[0].renderers == [renderer]
    assert hover_tools[0].tooltips == [
        ("Date", "@datetime{%F %T}"),
        ("YES Price", "@{price_test_market}{0.[00]%}"),
    ]


def test_append_brier_panel_uses_left_axis_label_instead_of_top_title() -> None:
    frame = pd.DataFrame(
        {
            "brier_advantage": [0.1, -0.05],
            "cumulative_brier_advantage": [0.1, 0.05],
        },
        index=pd.to_datetime(["2026-03-01T00:00:00Z", "2026-03-02T00:00:00Z"], utc=True),
    )
    layout = _DummyLayout()

    result = adapter._append_brier_panel(layout, frame)

    assert result is layout
    assert len(layout.children) == 1
    figure = layout.children[0]
    assert figure.title is None
    assert figure.yaxis[0].axis_label == "Cumulative Brier Advantage"
    assert figure.legend[0].location == "top_left"
    assert figure.legend[0].orientation == "horizontal"
    assert figure.legend[0].label_text_font_size == "8pt"
    assert figure.legend[0].border_line_alpha == 0
    assert figure.x_range.bounds is not None


def test_append_brier_placeholder_panel_uses_left_axis_label_instead_of_top_title() -> None:
    layout = _DummyLayout()

    result = adapter._append_brier_placeholder_panel(
        layout, "Unavailable until the market resolves."
    )

    assert result is layout
    assert len(layout.children) == 1
    figure = layout.children[0]
    assert figure.title is None
    assert figure.yaxis[0].axis_label == "Cumulative Brier Advantage"


def test_remove_panels_by_yaxis_labels_prunes_matching_figures() -> None:
    keep = _DummyFigure(axis_label="YES Price", title="YES Price")
    drop = _DummyFigure(axis_label="Cash / Equity", title="Cash / Equity")
    nested = _DummyLayout(children=[keep, drop])
    layout = _DummyLayout(children=[nested])

    adapter._remove_panels_by_yaxis_labels(layout, {"Cash / Equity"})

    assert nested.children == [keep]


def test_append_multi_market_brier_panel_builds_one_line_per_market() -> None:
    layout = _DummyLayout()
    brier_frames = {
        "market-a": pd.DataFrame(
            {
                "brier_advantage": [0.1, -0.05],
                "cumulative_brier_advantage": [0.1, 0.05],
            },
            index=pd.to_datetime(["2026-03-01T00:00:00Z", "2026-03-02T00:00:00Z"], utc=True),
        ),
        "market-b": pd.DataFrame(
            {
                "brier_advantage": [0.2, 0.1],
                "cumulative_brier_advantage": [0.2, 0.3],
            },
            index=pd.to_datetime(["2026-03-01T12:00:00Z", "2026-03-02T12:00:00Z"], utc=True),
        ),
    }

    result = adapter._append_multi_market_brier_panel(layout, brier_frames)

    assert result is layout
    assert len(layout.children) == 1
    figure = layout.children[0]
    assert figure.yaxis[0].axis_label == "Cumulative Brier Advantage"
    assert figure.legend[0].location == "top_left"
    assert figure.legend[0].orientation == "horizontal"
    assert figure.legend[0].label_text_font_size == "8pt"
    assert figure.legend[0].border_line_alpha == 0
    assert figure.x_range.bounds is not None
    line_renderers = [
        renderer
        for renderer in figure.renderers
        if getattr(getattr(renderer, "glyph", None), "__class__", None).__name__ == "Line"
    ]
    assert len(line_renderers) == 2


@pytest.mark.parametrize("with_brier_panel", [False, True])
def test_create_legacy_backtest_chart_saves_final_layout_without_requiring_brier_panel(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    with_brier_panel: bool,
) -> None:
    base_layout = _DummyLayout()
    built_brier_panel = object()
    plotting_calls: list[dict[str, object]] = []
    downsampling_calls: list[tuple[object, bool, int]] = []
    save_calls: list[tuple[object, Path, str]] = []

    def _fake_plot(*_args, **kwargs):  # type: ignore[no-untyped-def]
        plotting_calls.append(kwargs)
        return base_layout

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    engine = SimpleNamespace(
        trader=SimpleNamespace(generate_order_fills_report=list),
    )

    legacy_repo = tmp_path / "legacy_repo"
    monkeypatch.setattr(adapter, "resolve_legacy_plot_repo", lambda *_: legacy_repo)
    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (
            SimpleNamespace(BacktestResult=_BacktestResult),
            SimpleNamespace(plot=_fake_plot),
        ),
    )
    monkeypatch.setattr(
        adapter,
        "_configure_legacy_downsampling",
        lambda module, adaptive, max_points: downsampling_calls.append(
            (module, adaptive, max_points),
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
        adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        adapter,
        "prepare_cumulative_brier_advantage",
        lambda **kwargs: (
            pd.DataFrame({"cumulative_brier_advantage": [0.1]})
            if with_brier_panel
            else pd.DataFrame()
        ),
    )
    monkeypatch.setattr(
        adapter,
        "_build_brier_panel",
        lambda frame: built_brier_panel,
    )
    monkeypatch.setattr(
        adapter,
        "_save_layout",
        lambda layout, output_path, title: save_calls.append((layout, output_path, title)),
    )

    output_path = tmp_path / "legacy.html"
    result = adapter.create_legacy_backtest_chart(
        engine=engine,
        output_path=output_path,
        strategy_name="Test Strategy",
        platform="kalshi",
        initial_cash=100.0,
    )

    assert result == str(output_path.resolve())
    assert len(downsampling_calls) == 1
    assert downsampling_calls[0][1:] == (True, 5000)
    assert plotting_calls == [
        {
            "filename": str(output_path.resolve()),
            "max_markets": 30,
            "open_browser": False,
            "progress": False,
            "plot_panels": adapter.DEFAULT_DETAIL_PLOT_PANELS,
            "extra_panels": (
                {PANEL_BRIER_ADVANTAGE: built_brier_panel} if with_brier_panel else {}
            ),
        },
    ]
    assert save_calls == [
        (
            base_layout,
            output_path.resolve(),
            "Test Strategy legacy chart",
        ),
    ]


def test_create_legacy_backtest_chart_saves_placeholder_brier_panel_when_outcome_unresolved(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    base_layout = _DummyLayout()
    placeholder_panel = object()
    plotting_calls: list[dict[str, object]] = []
    save_calls: list[tuple[object, Path, str]] = []

    def _fake_plot(*_args, **kwargs):  # type: ignore[no-untyped-def]
        plotting_calls.append(kwargs)
        return base_layout

    class _BacktestResult:
        def __init__(self, **kwargs) -> None:
            self.kwargs = kwargs

    engine = SimpleNamespace(
        trader=SimpleNamespace(generate_order_fills_report=list),
    )

    legacy_repo = tmp_path / "legacy_repo"
    monkeypatch.setattr(adapter, "resolve_legacy_plot_repo", lambda *_: legacy_repo)
    monkeypatch.setattr(
        adapter,
        "_load_legacy_modules",
        lambda *_: (
            SimpleNamespace(BacktestResult=_BacktestResult),
            SimpleNamespace(plot=_fake_plot),
        ),
    )
    monkeypatch.setattr(adapter, "_configure_legacy_downsampling", lambda *_args, **_kwargs: None)
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
        adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        adapter,
        "prepare_cumulative_brier_advantage",
        lambda **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(
        adapter,
        "_build_brier_placeholder_panel",
        lambda message: placeholder_panel,
    )
    monkeypatch.setattr(
        adapter,
        "_save_layout",
        lambda layout, output_path, title: save_calls.append((layout, output_path, title)),
    )

    output_path = tmp_path / "legacy.html"
    adapter.create_legacy_backtest_chart(
        engine=engine,
        output_path=output_path,
        strategy_name="Test Strategy",
        platform="kalshi",
        initial_cash=100.0,
        user_probabilities=pd.Series(
            [0.45, 0.55], index=pd.date_range("2025-01-01", periods=2, freq="D")
        ),
        market_probabilities=pd.Series(
            [0.5, 0.6], index=pd.date_range("2025-01-01", periods=2, freq="D")
        ),
        outcomes=pd.Series(dtype=float),
    )

    assert plotting_calls == [
        {
            "filename": str(output_path.resolve()),
            "max_markets": 30,
            "open_browser": False,
            "progress": False,
            "plot_panels": adapter.DEFAULT_DETAIL_PLOT_PANELS,
            "extra_panels": {PANEL_BRIER_ADVANTAGE: placeholder_panel},
        },
    ]
    assert save_calls == [
        (
            base_layout,
            output_path.resolve(),
            "Test Strategy legacy chart",
        ),
    ]


def test_build_legacy_backtest_layout_omits_brier_when_panel_not_requested(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    plotting_calls: list[dict[str, object]] = []

    def _fake_plot(*_args, **kwargs):  # type: ignore[no-untyped-def]
        plotting_calls.append(kwargs)
        return _DummyLayout()

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
            SimpleNamespace(plot=_fake_plot),
        ),
    )
    monkeypatch.setattr(adapter, "_configure_legacy_downsampling", lambda *_args, **_kwargs: None)
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
        adapter,
        "_apply_layout_overrides",
        lambda layout, initial_cash, **kwargs: layout,
    )
    monkeypatch.setattr(
        adapter,
        "prepare_cumulative_brier_advantage",
        lambda **kwargs: pytest.fail("Brier data should not be prepared when the panel is omitted"),
    )

    adapter.build_legacy_backtest_layout(
        engine=engine,
        output_path=tmp_path / "legacy.html",
        strategy_name="Test Strategy",
        platform="kalshi",
        initial_cash=100.0,
        plot_panels=(PANEL_EQUITY, PANEL_CASH_EQUITY),
    )

    assert plotting_calls == [
        {
            "filename": str((tmp_path / "legacy.html").resolve()),
            "max_markets": 30,
            "open_browser": False,
            "progress": False,
            "plot_panels": (PANEL_EQUITY, PANEL_CASH_EQUITY),
            "extra_panels": {},
        },
    ]
