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

from __future__ import annotations

import warnings
from types import SimpleNamespace

import pandas as pd
import pytest

from nautilus_trader.analysis import legacy_plot_adapter as adapter
from nautilus_trader.analysis.legacy_backtesting import plotting
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_CASH_EQUITY
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_EQUITY
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_MONTHLY_RETURNS
from nautilus_trader.analysis.legacy_backtesting.models import PANEL_YES_PRICE


def _result() -> SimpleNamespace:
    return SimpleNamespace(
        strategy_name="test-strategy",
        platform=SimpleNamespace(value="kalshi"),
        fills=[],
        market_prices={},
        overlay_series={},
        overlay_colors={},
        plot_monthly_returns=True,
        hide_primary_panel_series=False,
        primary_series_name="Strategy",
        total_equity_panel_label="Total Equity",
    )


def _eq_frame() -> pd.DataFrame:
    datetimes = pd.date_range("2025-01-01", periods=4, freq="h")
    equity = pd.Series([100.0, 102.0, 101.0, 103.0], dtype=float)
    equity_peak = equity.cummax()
    return pd.DataFrame(
        {
            "datetime": datetimes,
            "cash": [100.0, 99.5, 99.0, 101.0],
            "equity": equity,
            "unrealized_pnl": [0.0, 2.5, 2.0, 2.0],
            "num_positions": [0.0, 1.0, 1.0, 0.0],
            "equity_pct": equity / 100.0,
            "equity_peak": equity_peak,
            "equity_pct_peak": (equity / 100.0).cummax(),
            "drawdown_pct": ((equity_peak - equity) / equity_peak).fillna(0.0),
            "return_pct": (equity - 100.0) / 100.0,
        },
    )


def _fills_frame() -> pd.DataFrame:
    return pd.DataFrame(
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


def _figure_names(layout: object) -> list[str]:
    return [str(getattr(fig, "name", "")) for fig in adapter._iter_figures(layout)]


def test_plot_skips_allocation_builder_when_panel_not_requested(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    pytest.importorskip("bokeh")
    allocation_calls: list[bool] = []

    monkeypatch.setattr(
        plotting,
        "_build_dataframes",
        lambda *args, **kwargs: (_eq_frame(), _fills_frame(), pd.DataFrame(index=range(4)), 4),
    )
    monkeypatch.setattr(
        plotting,
        "_build_allocation_data",
        lambda *args, **kwargs: allocation_calls.append(True),
    )
    monkeypatch.setattr(plotting, "show", lambda *args, **kwargs: None)

    layout = plotting.plot(
        _result(),
        filename=str(tmp_path / "chart.html"),
        open_browser=False,
        progress=False,
        plot_panels=(PANEL_EQUITY, PANEL_CASH_EQUITY),
    )

    assert allocation_calls == []
    figures = list(adapter._iter_figures(layout))
    assert _figure_names(layout) == [PANEL_EQUITY, PANEL_CASH_EQUITY]
    assert figures[0].xaxis[0].visible is False
    assert figures[1].xaxis[0].visible is True


def test_plot_honors_requested_panel_order_and_skips_missing_yes_price(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    pytest.importorskip("bokeh")

    monkeypatch.setattr(
        plotting,
        "_build_dataframes",
        lambda *args, **kwargs: (_eq_frame(), _fills_frame(), pd.DataFrame(index=range(4)), 4),
    )
    monkeypatch.setattr(plotting, "show", lambda *args, **kwargs: None)

    layout = plotting.plot(
        _result(),
        filename=str(tmp_path / "chart.html"),
        open_browser=False,
        progress=False,
        plot_panels=(PANEL_MONTHLY_RETURNS, PANEL_YES_PRICE, PANEL_EQUITY),
    )

    assert _figure_names(layout) == [PANEL_MONTHLY_RETURNS, PANEL_EQUITY]


def test_plot_merges_toolbar_without_active_tool_warnings(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    pytest.importorskip("bokeh")

    monkeypatch.setattr(
        plotting,
        "_build_dataframes",
        lambda *args, **kwargs: (_eq_frame(), _fills_frame(), pd.DataFrame(index=range(4)), 4),
    )
    monkeypatch.setattr(plotting, "show", lambda *args, **kwargs: None)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        plotting.plot(
            _result(),
            filename=str(tmp_path / "chart.html"),
            open_browser=False,
            progress=False,
            plot_panels=(PANEL_EQUITY, PANEL_CASH_EQUITY),
        )

    messages = [str(warning.message) for warning in caught]
    assert not any("toolbar.active_drag" in message for message in messages)
    assert not any("toolbar.active_scroll" in message for message in messages)


def test_plot_raises_when_requested_panels_render_nothing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    pytest.importorskip("bokeh")

    monkeypatch.setattr(
        plotting,
        "_build_dataframes",
        lambda *args, **kwargs: (_eq_frame(), _fills_frame(), pd.DataFrame(index=range(4)), 4),
    )
    monkeypatch.setattr(plotting, "show", lambda *args, **kwargs: None)

    with pytest.raises(ValueError, match="No chart panels were rendered"):
        plotting.plot(
            _result(),
            filename=str(tmp_path / "chart.html"),
            open_browser=False,
            progress=False,
            plot_panels=(PANEL_YES_PRICE,),
        )
