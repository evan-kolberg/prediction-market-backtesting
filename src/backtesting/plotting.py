"""Interactive Bokeh-based plotting for prediction-market backtest results.

Produces a minitrade-style multi-panel interactive chart:

    1. Equity curve (relative %) with drawdown shading, peak/final markers
    2. Per-trade P&L (aggregated bar chart or scatter)
    3. Market prices (main panel) with per-market YES price lines,
       fill markers, and trade-connector dotted lines
    4. Drawdown percentage
    5. Cash balance and open-position count

All panels share a linked x-axis and crosshair, with auto-scaling y-axes,
hover tooltips, and click-to-hide legends.
"""
# pyright: reportArgumentType=false, reportCallIssue=false, reportOperatorIssue=false

from __future__ import annotations

import os
import sys
from colorsys import hls_to_rgb, rgb_to_hls
from functools import partial
from itertools import cycle
from typing import TYPE_CHECKING, Any, Optional

import numpy as np
import pandas as pd  # type: ignore[import-untyped]
from bokeh.colors import RGB
from bokeh.colors.named import lime as BULL_COLOR
from bokeh.colors.named import tomato as BEAR_COLOR
from bokeh.io import output_file, output_notebook, show
from bokeh.io.state import curstate
from bokeh.layouts import gridplot
from bokeh.models import (  # type: ignore[attr-defined]
    ColumnDataSource,
    CrosshairTool,
    CustomJS,
    DatetimeTickFormatter,
    HoverTool,
    Legend,
    NumeralTickFormatter,
    Range1d,
    Span,
    WheelZoomTool,
)
from bokeh.palettes import Category10
from bokeh.plotting import figure as _figure
from bokeh.transform import factor_cmap

from src.backtesting.progress import PinnedProgress

try:
    from bokeh.models import CustomJSTickFormatter
except ImportError:
    from bokeh.models import FuncTickFormatter as CustomJSTickFormatter  # type: ignore[no-redef, attr-defined]

if TYPE_CHECKING:
    from src.backtesting.models import BacktestResult

IS_JUPYTER_NOTEBOOK = "ipykernel" in sys.modules
if IS_JUPYTER_NOTEBOOK:
    output_notebook(hide_banner=True)


def _is_notebook() -> bool:
    """Re-check at call time whether we're in a Jupyter kernel."""
    return IS_JUPYTER_NOTEBOOK or "ipykernel" in sys.modules


def set_bokeh_output(notebook: bool = False) -> None:
    """Force Bokeh output mode."""
    global IS_JUPYTER_NOTEBOOK
    IS_JUPYTER_NOTEBOOK = notebook


COLORS = [BEAR_COLOR, BULL_COLOR]
NBSP = "\N{NBSP}" * 4

_AUTOSCALE_JS_TEMPLATE = """
if (!window._bt_scale_range) {{
    window._bt_scale_range = function (range, min, max, pad) {{
        "use strict";
        if (min !== Infinity && max !== -Infinity) {{
            pad = pad ? (max - min) * .03 : 0;
            range.start = min - pad;
            range.end = max + pad;
        }}
    }};
}}
clearTimeout(window._bt_autoscale_timeout);
window._bt_autoscale_timeout = setTimeout(function () {{
    "use strict";
    let i = Math.max(Math.floor(cb_obj.start), 0),
        j = Math.min(Math.ceil(cb_obj.end), source.data['{high_key}'].length);
    let max = Math.max.apply(null, source.data['{high_key}'].slice(i, j)),
        min = Math.min.apply(null, source.data['{low_key}'].slice(i, j));
    _bt_scale_range({range_var}, min, max, true);
}}, 50);
"""


def _bokeh_reset(filename: Optional[str] = None) -> None:
    """Reset Bokeh state and configure output target."""
    curstate().reset()
    if filename:
        if not filename.endswith(".html"):
            filename += ".html"
        output_file(filename, title=filename)
    elif _is_notebook():
        output_notebook(hide_banner=True)


def colorgen():
    """Yield an infinite cycle of Category10 colors."""
    yield from cycle(Category10[10])


def lightness(color: Any, light: float = 0.94) -> RGB:
    """Return *color* adjusted to the given lightness."""
    rgb = np.array([color.r, color.g, color.b]) / 255
    h, _, s = rgb_to_hls(*rgb)
    rgb = np.array(hls_to_rgb(h, light, s)) * 255.0
    return RGB(*rgb)


def _build_dataframes(
    result: "BacktestResult",
    bar: PinnedProgress[None] | None = None,
    max_markets: int = 10,
):
    """Convert a :class:`BacktestResult` into plotting-ready DataFrames.

    Only the top *max_markets* markets (by price range among traded markets)
    are fully aligned to the equity timeline.  This avoids building a
    20 000-column DataFrame that would consume tens of GB of RAM.

    Returns
    -------
    eq : pd.DataFrame
        Per-snapshot equity, cash, drawdown, etc.
    fills_df : pd.DataFrame
        Individual fill events mapped to the nearest equity-bar index.
    market_df : pd.DataFrame
        Per-market YES price series (NaN outside each market's active window).
    """
    snaps = result.equity_curve
    if not snaps:
        raise ValueError("Cannot plot an empty equity curve.")

    records = [
        {
            "datetime": s.timestamp,
            "cash": s.cash,
            "equity": s.total_equity,
            "unrealized_pnl": s.unrealized_pnl,
            "num_positions": float(s.num_positions),
        }
        for s in snaps
    ]
    eq = pd.DataFrame.from_records(records)
    eq["datetime"] = pd.to_datetime(eq["datetime"])
    eq = eq.sort_values("datetime").reset_index(drop=True)

    initial = result.initial_cash
    eq["equity_pct"] = eq["equity"] / initial
    eq["equity_peak"] = eq["equity"].cummax()
    eq["equity_pct_peak"] = eq["equity_pct"].cummax()
    dd_raw = (eq["equity_peak"] - eq["equity"]) / eq["equity_peak"].replace(0, np.nan)
    eq["drawdown_pct"] = dd_raw.fillna(0.0)
    eq["return_pct"] = (eq["equity"] - initial) / initial

    fill_records = [
        {
            "datetime": f.timestamp,
            "market_id": f.market_id,
            "action": f.action.value,
            "side": f.side.value,
            "price": f.price,
            "quantity": f.quantity,
            "commission": f.commission,
        }
        for f in result.fills
    ]
    if fill_records:
        fills_df = pd.DataFrame.from_records(fill_records)
        fills_df["datetime"] = pd.to_datetime(fills_df["datetime"])
        fills_df = fills_df.sort_values("datetime").reset_index(drop=True)
        eq_times = eq["datetime"].values
        bar_idx = np.searchsorted(eq_times, fills_df["datetime"].values, side="right") - 1
        fills_df["bar"] = np.clip(bar_idx, 0, len(eq) - 1)
    else:
        fills_df = pd.DataFrame(
            columns=["datetime", "market_id", "action", "side", "price", "quantity", "commission", "bar"]
        )

    market_prices = getattr(result, "market_prices", {})
    market_series: dict[str, np.ndarray] = {}
    if market_prices:
        # --- cheap pre-filter: pick the top max_markets by price range ---
        traded_ids = set(fills_df["market_id"]) if not fills_df.empty else set()
        range_map: dict[str, float] = {}
        for mid, recs in market_prices.items():
            if not recs:
                continue
            prices_raw = [p for _, p in recs]
            if not prices_raw:
                continue
            pmin, pmax = min(prices_raw), max(prices_raw)
            # prefer traded markets; bias their range so they sort first
            bonus = 1e6 if (mid in traded_ids) else 0.0
            range_map[mid] = (pmax - pmin) + bonus
        # sort descending by range and keep only top N
        selected = sorted(range_map, key=range_map.get, reverse=True)[:max_markets]  # type: ignore[arg-type]
        selected_set = set(selected)

        n_selected = len(selected_set)
        if bar:
            bar.set_desc(f"Processing {n_selected:,}/{len(market_prices):,} markets")
        eq_dt = pd.DataFrame({"datetime": eq["datetime"], "_idx": eq.index})
        eq_dt_sorted = eq_dt.sort_values("datetime")
        eq_dts = pd.DatetimeIndex(eq["datetime"])
        for mid in selected:
            if bar:
                bar.advance()
            recs = market_prices[mid]
            if not recs:
                continue
            ts_list, price_list = zip(*recs)
            dt_arr = pd.to_datetime(list(ts_list))
            mkt = pd.DataFrame({"datetime": dt_arr, "price": list(price_list)})
            mkt = mkt.sort_values("datetime").drop_duplicates("datetime", keep="last")
            merged = pd.merge_asof(eq_dt_sorted, mkt, on="datetime")
            merged = merged.sort_values("_idx")
            prices = merged["price"].values.copy().astype(float)
            first_ts, last_ts = dt_arr.min(), dt_arr.max()
            prices[eq_dts < first_ts] = np.nan
            prices[eq_dts > last_ts] = np.nan
            if np.isnan(prices).all():
                continue
            market_series[mid] = prices
    if market_series:
        market_df = pd.DataFrame(market_series, index=eq.index)
    else:
        market_df = pd.DataFrame(index=eq.index)

    return eq, fills_df, market_df


# ---------------------------------------------------------------------------
# Main plot function
# ---------------------------------------------------------------------------

def plot(
    result: "BacktestResult",
    *,
    filename: str = "",
    plot_width: Optional[int] = None,
    plot_equity: bool = True,
    plot_drawdown: bool = True,
    plot_pl: bool = True,
    plot_cash: bool = True,
    plot_market_prices: bool = True,
    show_legend: bool = True,
    open_browser: bool = True,
    relative_equity: bool = True,
    max_markets: int = 10,
    progress: bool = True,
) -> object:
    """Render an interactive Bokeh chart for *result*.

    Parameters
    ----------
    result : BacktestResult
        Output of ``Engine.run()``.
    filename : str
        Save to this HTML path. Empty string = auto-generate into ``output/``.
    max_markets : int
        Maximum number of market price lines to display (ranked by price range).
    open_browser : bool
        Open the chart in the default browser after rendering.
    """
    if not filename and not _is_notebook():
        filename = f"output/backtest_{result.strategy_name}_{result.platform.value}"
    elif filename and not filename.startswith("output/") and not filename.startswith("/"):
        filename = f"output/{filename}"
    if filename:
        os.makedirs(os.path.dirname(filename) or "output", exist_ok=True)
    _bokeh_reset(filename)

    n_markets = len(getattr(result, "market_prices", {}))
    chart_steps = 6  # setup, equity, P&L, market/fallback, sub-panels, layout
    # Only max_markets are fully processed; the rest are skipped cheaply
    total_steps = min(n_markets, max_markets) + chart_steps

    use_bar = progress and not _is_notebook()
    bar: PinnedProgress[None] | None = None
    if use_bar:
        bar = PinnedProgress(
            iter([]),
            total=total_steps,
            desc="Rendering chart",
            unit=" steps",
        )
        bar._setup()  # activate the pinned bar immediately

    if bar:
        bar.set_desc("Building dataframes")
    eq, fills_df, market_df = _build_dataframes(result, bar=bar, max_markets=max_markets)
    if bar:
        bar.write(
            f"  {len(eq):,} bars, {len(fills_df):,} fills, "
            f"{len(market_df.columns):,} markets"
        )
    index = eq.index
    initial = result.initial_cash

    # Rank markets by observable price range
    if not market_df.empty:
        traded_cols = [c for c in market_df.columns
                       if c in set(fills_df["market_id"]) if not fills_df.empty]
        if not traded_cols:
            traded_cols = list(market_df.columns)
        price_range = (
            market_df[traded_cols].max() - market_df[traded_cols].min()
        ).sort_values(ascending=False)
        display_markets = price_range.head(max_markets).index.tolist()
    else:
        display_markets = []
    has_market_lines = plot_market_prices and len(display_markets) > 0

    new_figure = partial(
        _figure,  # type: ignore[call-arg]
        x_axis_type="linear",
        width=plot_width,
        height=400,
        tools="xpan,xwheel_zoom,box_zoom,undo,redo,reset,save",
        active_drag="xpan",
        active_scroll="xwheel_zoom",
    )

    pad = (index[-1] - index[0]) / 20 if len(index) > 1 else 1
    x_range_kw: dict[str, Any] = (
        dict(x_range=Range1d(index[0], index[-1], min_interval=10,  # type: ignore[call-arg]
                             bounds=(index[0] - pad, index[-1] + pad)))
        if len(index) > 1 else {}
    )

    fig_main = new_figure(height=400, **x_range_kw)  # type: ignore[call-arg]
    source = ColumnDataSource(eq)

    fig_main.xaxis.formatter = CustomJSTickFormatter(
        args=dict(
            axis=fig_main.xaxis[0],
            formatter=DatetimeTickFormatter(days="%a, %d %b", months="%m/%Y"),
            source=source,
        ),
        code="""
this.labels = this.labels || formatter.doFormat(ticks
    .map(i => source.data.datetime[i])
    .filter(t => t !== undefined));
return this.labels[index] || "";
        """,
    )

    figs_above: list = []
    figs_below: list = []

    def _set_tooltips(fig, tooltips=(), vline=True, renderers=()):
        """Attach a HoverTool with a prepended date tooltip."""
        tooltips = [('Date', '@datetime{%c}')] + list(tooltips)
        fig.add_tools(HoverTool(
            point_policy="follow_mouse",
            renderers=list(renderers),
            formatters={"@datetime": "datetime"},
            tooltips=tooltips,
            mode="vline" if vline else "mouse",
        ))

    def _new_sub(y_label: str, height: int = 90, **kwargs):
        """Create a sub-figure sharing *fig_main*'s x-range."""
        fig = new_figure(x_range=fig_main.x_range, height=height, **kwargs)  # type: ignore[call-arg]
        fig.xaxis.visible = False
        fig.yaxis.minor_tick_line_color = None
        fig.add_layout(Legend(), "center")
        fig.legend.orientation = "horizontal"
        fig.legend.background_fill_alpha = 0.8
        fig.legend.border_line_alpha = 0
        fig.yaxis.axis_label = y_label
        return fig

    def _plot_equity():
        equity = eq["equity_pct"].copy() if relative_equity else eq["equity"].copy()
        source.add(equity.values, "eq_plot")
        fig = _new_sub("Equity", height=180)

        hw = equity.cummax()
        fig.patch(
            "index", "eq_dd_patch",
            source=ColumnDataSource(dict(
                index=np.r_[index, index[::-1]],
                eq_dd_patch=np.r_[equity.values, hw.values[::-1]],
            )),
            fill_color="#ffffea", line_color="#ffcb66",
        )

        r = fig.line("index", "eq_plot", source=source, line_width=1.5,
                     line_alpha=1, legend_label="Strategy")

        if relative_equity:
            fmt_tip = "@eq_plot{+0,0.[000]%}"
            fmt_tick = "0,0.[00]%"
            fmt_legend = "{:,.0f}%"
        else:
            fmt_tip = "@eq_plot{$ 0,0}"
            fmt_tick = "$ 0.0 a"
            fmt_legend = "${:,.0f}"

        _set_tooltips(fig, [("Equity", fmt_tip)], renderers=[r])
        fig.yaxis.formatter = NumeralTickFormatter(format=fmt_tick)

        argmax = int(equity.idxmax())
        peak_val = equity.iloc[argmax]
        fig.scatter(argmax, peak_val, color="cyan", size=8,
                    legend_label="Peak ({})".format(
                        fmt_legend.format(peak_val * (100 if relative_equity else 1))))

        fig.scatter(index[-1], equity.iloc[-1], color="blue", size=8,
                    legend_label="Final ({})".format(
                        fmt_legend.format(equity.iloc[-1] * (100 if relative_equity else 1))))

        dd = eq["drawdown_pct"]
        dd_end = int(dd.idxmax())
        if dd.iloc[dd_end] > 0:
            dd_start = int(equity.iloc[:dd_end].idxmax())
            dd_dur = eq["datetime"].iloc[dd_end] - eq["datetime"].iloc[dd_start]
            label = f"Max Dd Dur. ({dd_dur})".replace(" 00:00:00", "").replace("(0 days ", "(")
            fig.line([dd_start, dd_end], equity.iloc[dd_start],
                     line_color="red", line_width=2, legend_label=label)

            if not plot_drawdown:
                fig.scatter(dd_end, equity.iloc[dd_end], color="red", size=8,
                            legend_label="Max Drawdown (-{:.1f}%)".format(
                                100 * dd.iloc[dd_end]))

        figs_above.append(fig)

    def _plot_pl():
        """Render P&L as time-bucketed bars (many trades) or scatter (few)."""
        fig = _new_sub("Profit / Loss", height=110)
        fig.add_layout(Span(location=0, dimension="width",
                            line_color="#666666", line_dash="dashed", line_width=1))

        market_pnls = getattr(result, "market_pnls", {})

        if market_pnls:
            pnl_records = []
            for mid, pnl_val in market_pnls.items():
                if pnl_val == 0.0:
                    continue
                mkt_fills = fills_df[fills_df["market_id"] == mid] if not fills_df.empty else pd.DataFrame()
                if mkt_fills.empty:
                    continue
                pnl_records.append({
                    "bar": int(mkt_fills["bar"].iloc[-1]),
                    "datetime": mkt_fills["datetime"].iloc[-1],
                    "pnl": pnl_val,
                    "market_id": mid,
                })
            if pnl_records:
                pnl_df = pd.DataFrame(pnl_records).sort_values("bar")

                if len(pnl_df) > 200:
                    n_buckets = min(200, len(eq))
                    bucket_size = max(1, len(eq) // n_buckets)
                    pnl_df["bucket"] = (pnl_df["bar"] // bucket_size) * bucket_size
                    agg = pnl_df.groupby("bucket").agg(
                        pnl=("pnl", "sum"),
                        count=("pnl", "count"),
                        wins=("pnl", lambda x: (x > 0).sum()),
                    ).reset_index()
                    bars_x = agg["bucket"].values
                    bars_y = agg["pnl"].values
                    bars_count = agg["count"].values
                    bars_wins = agg["wins"].values
                    bar_colors = [str(BULL_COLOR) if v > 0 else str(BEAR_COLOR) for v in bars_y]

                    bar_src = ColumnDataSource(dict(
                        index=bars_x,
                        pnl=bars_y,
                        color=bar_colors,
                        count=bars_count,
                        wins=bars_wins,
                    ))
                    r = fig.vbar("index", top="pnl", source=bar_src,
                                 width=max(bucket_size * 0.8, 0.8),
                                 fill_color="color", line_color="color",
                                 fill_alpha=0.7)
                    _set_tooltips(fig,
                                  [("Net P/L", "@pnl{+$0,0.00}"),
                                   ("Trades", "@count"), ("Wins", "@wins")],
                                  vline=False, renderers=[r])
                else:
                    sz = np.abs(pnl_df["pnl"].values).astype(float)
                    if sz.max() > sz.min():
                        sz = np.interp(sz, (sz.min(), sz.max()), (8, 20))
                    else:
                        sz = np.full_like(sz, 12.0)
                    pnl_long = np.where(pnl_df["pnl"].values > 0, pnl_df["pnl"].values, np.nan)
                    pnl_short = np.where(pnl_df["pnl"].values <= 0, pnl_df["pnl"].values, np.nan)
                    positive = np.where(pnl_df["pnl"].values > 0, "1", "0")
                    pnl_src = ColumnDataSource(dict(
                        index=pnl_df["bar"].values,
                        datetime=pnl_df["datetime"].values,
                        pnl_long=pnl_long,
                        pnl_short=pnl_short,
                        positive=positive,
                        market_id=pnl_df["market_id"].values,
                        size_marker=sz,
                    ))
                    cmap = factor_cmap("positive", COLORS, ["0", "1"])
                    r1 = fig.scatter("index", "pnl_long", source=pnl_src,
                                     fill_color=cmap, marker="triangle",
                                     line_color="black", size="size_marker")
                    r2 = fig.scatter("index", "pnl_short", source=pnl_src,
                                     fill_color=cmap, marker="inverted_triangle",
                                     line_color="black", size="size_marker")
                    _set_tooltips(fig, [("Market", "@market_id"), ("P/L", "@pnl_long{+$0,0.00}")],
                                  vline=False, renderers=[r1])
                    _set_tooltips(fig, [("Market", "@market_id"), ("P/L", "@pnl_short{+$0,0.00}")],
                                  vline=False, renderers=[r2])

        elif not fills_df.empty:
            relevant_fills = fills_df[fills_df["market_id"].isin(display_markets)] if display_markets else fills_df
            if relevant_fills.empty:
                relevant_fills = fills_df.head(200)
            pnl_vals = np.where(
                relevant_fills["action"] == "sell",
                relevant_fills["price"] * relevant_fills["quantity"],
                -relevant_fills["price"] * relevant_fills["quantity"],
            )
            positive = (pnl_vals > 0).astype(int).astype(str)
            sz = np.abs(pnl_vals).astype(float)
            if sz.max() > sz.min():
                sz = np.interp(sz, (sz.min(), sz.max()), (8, 20))
            else:
                sz = np.full_like(sz, 12.0)
            pnl_long = np.where(pnl_vals > 0, pnl_vals, np.nan)
            pnl_short = np.where(pnl_vals <= 0, pnl_vals, np.nan)
            fill_src = ColumnDataSource(dict(
                index=relevant_fills["bar"].values,
                datetime=relevant_fills["datetime"].values,
                pnl_long=pnl_long,
                pnl_short=pnl_short,
                positive=positive,
                market_id=relevant_fills["market_id"].values,
                size_marker=sz,
            ))
            cmap = factor_cmap("positive", COLORS, ["0", "1"])
            r1 = fig.scatter("index", "pnl_long", source=fill_src,
                             fill_color=cmap, marker="triangle",
                             line_color="black", size="size_marker")
            r2 = fig.scatter("index", "pnl_short", source=fill_src,
                             fill_color=cmap, marker="inverted_triangle",
                             line_color="black", size="size_marker")
            _set_tooltips(fig, [("Market", "@market_id"), ("Value", "@pnl_long{+$0,0.00}")],
                          vline=False, renderers=[r1])
            _set_tooltips(fig, [("Market", "@market_id"), ("Value", "@pnl_short{+$0,0.00}")],
                          vline=False, renderers=[r2])

        fig.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")
        return fig

    def _plot_market_prices():
        """Per-market YES price lines, fill markers, and trade connectors."""
        ohlc_colors = colorgen()
        label_tooltip_pairs: list[tuple[str, str]] = []
        price_extremes = pd.DataFrame(index=index)

        for mid in display_markets:
            color = next(ohlc_colors)
            arr = market_df[mid].values
            short = mid[:20] + "\u2026" if len(mid) > 20 else mid
            col = f"price_{mid}"
            source.add(arr, col)
            price_extremes[col] = pd.Series(arr).values
            label_tooltip_pairs.append((short, f"@{{{col}}}{{0.[00]%}}"))
            fig_main.line("index", col, source=source,
                          legend_label=short, line_color=color, line_width=2)

        if len(market_df.columns) > max_markets:
            hidden = len(market_df.columns) - max_markets
            fig_main.line(0, 0, legend_label=f"{hidden} more markets hidden",
                          line_color="black")

        _draw_trade_connectors()
        _draw_fill_markers()

        main_tooltips = [("x, y", NBSP.join(("$index", "$y{0,0.0[0000]}")))]
        main_tooltips.extend(label_tooltip_pairs)
        _set_tooltips(fig_main, main_tooltips, vline=True, renderers=[])

        fig_main.yaxis.axis_label = "YES Price"
        fig_main.yaxis.formatter = NumeralTickFormatter(format="0.[00]%")

        if not price_extremes.empty and price_extremes.shape[1] > 0:
            low_vals = price_extremes.min(axis=1).ffill().fillna(0).values
            high_vals = price_extremes.max(axis=1).ffill().fillna(1).values
            source.add(low_vals, "price_low")
            source.add(high_vals, "price_high")

            global_min = float(np.nanmin(low_vals))
            global_max = float(np.nanmax(high_vals))
            pad = max((global_max - global_min) * 0.05, 0.01)
            fig_main.y_range = Range1d(global_min - pad, global_max + pad)  # type: ignore[call-arg]

            fig_main.x_range.js_on_change(
                "end",
                CustomJS(
                    args=dict(price_range=fig_main.y_range, source=source),
                    code=_AUTOSCALE_JS_TEMPLATE.format(
                        high_key="price_high", low_key="price_low",
                        range_var="price_range",
                    ),
                ),
            )

        fig_main.legend.orientation = "horizontal"
        fig_main.legend.background_fill_alpha = 0.8
        fig_main.legend.border_line_alpha = 0

    def _draw_trade_connectors():
        """Dotted lines connecting fills on the same market.

        Green = profitable, red = losing.
        """
        if fills_df.empty:
            return

        market_pnls = getattr(result, "market_pnls", {})
        relevant = fills_df[fills_df["market_id"].isin(display_markets)].copy()
        if relevant.empty:
            return

        xs_profit: list[list] = []
        ys_profit: list[list] = []
        xs_loss: list[list] = []
        ys_loss: list[list] = []

        for mid in relevant["market_id"].unique():
            mkt = relevant[relevant["market_id"] == mid].sort_values("bar")
            if len(mkt) < 2:
                continue
            xs = mkt["bar"].values.tolist()
            ys = mkt["price"].values.tolist()

            profitable = market_pnls[mid] > 0 if mid in market_pnls else ys[-1] > ys[0]

            if profitable:
                xs_profit.append(xs)
                ys_profit.append(ys)
            else:
                xs_loss.append(xs)
                ys_loss.append(ys)

        colors_darker = [lightness(BEAR_COLOR, 0.35), lightness(BULL_COLOR, 0.35)]
        if xs_profit:
            fig_main.multi_line(
                xs_profit, ys_profit,
                line_color=str(colors_darker[1]),
                line_width=6, line_alpha=0.8, line_dash="dotted",
                legend_label=f"Profitable ({len(xs_profit)})",
            )
        if xs_loss:
            fig_main.multi_line(
                xs_loss, ys_loss,
                line_color=str(colors_darker[0]),
                line_width=6, line_alpha=0.8, line_dash="dotted",
                legend_label=f"Losing ({len(xs_loss)})",
            )

    def _draw_fill_markers():
        """Buy/sell markers on the main price chart."""
        if fills_df.empty:
            return

        relevant = fills_df[fills_df["market_id"].isin(display_markets)].copy()
        if relevant.empty:
            return

        fill_color_code = np.where(relevant["action"] == "buy", "1", "0")  # 1=green, 0=red

        marker_src = ColumnDataSource(dict(
            index=relevant["bar"].values,
            datetime=relevant["datetime"].values,
            price=relevant["price"].values,
            fill_color=fill_color_code,
            market_id=relevant["market_id"].values,
            action=relevant["action"].values,
            side=relevant["side"].values,
            quantity=relevant["quantity"].values,
        ))

        cmap = factor_cmap("fill_color", COLORS, ["0", "1"])
        fig_main.scatter(
            "index", "price", source=marker_src,
            fill_color=cmap, marker="circle", line_color="black",
            size=8, fill_alpha=0.7,
            legend_label=f"Fills ({len(relevant)})",
        )

    def _plot_main_fallback():
        """Fallback main chart showing equity when no market prices exist."""
        source.add(eq["equity"].values, "equity_abs")
        r = fig_main.line("index", "equity_abs", source=source,
                          line_width=1.5, line_color="#1f77b4",
                          legend_label="Equity")
        fig_main.yaxis.axis_label = "Equity ($)"
        fig_main.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")

        source.add(eq["equity"].values, "price_high")
        source.add(eq["equity"].values, "price_low")
        fig_main.x_range.js_on_change(
            "end",
            CustomJS(
                args=dict(price_range=fig_main.y_range, source=source),
                code=_AUTOSCALE_JS_TEMPLATE.format(
                    high_key="price_high", low_key="price_low",
                    range_var="price_range",
                ),
            ),
        )
        _set_tooltips(fig_main,
                      [("Equity", "@equity_abs{$0,0.00}"),
                       ("Cash", "@cash{$0,0.00}")],
                      vline=True, renderers=[r])

    def _plot_drawdown():
        fig = _new_sub("Drawdown", height=90)
        source.add(eq["drawdown_pct"].values, "dd_pct")
        r = fig.line("index", "dd_pct", source=source, line_width=1.3)
        argmax = int(eq["drawdown_pct"].idxmax())
        fig.scatter(argmax, eq["drawdown_pct"].iloc[argmax], color="red", size=8,
                    legend_label="Peak (-{:.1f}%)".format(
                        100 * eq["drawdown_pct"].iloc[argmax]))
        _set_tooltips(fig, [("Drawdown", "@dd_pct{-0.[0]%}")], renderers=[r])
        fig.yaxis.formatter = NumeralTickFormatter(format="-0.[0]%")
        return fig

    def _plot_cash():
        fig = _new_sub("Cash / Positions", height=90)
        r = fig.line("index", "cash", source=source, line_width=1.3,
                     line_color="#1f77b4", legend_label="Cash")
        max_pos = eq["num_positions"].max()
        if max_pos > 0:
            scale = eq["cash"].max() / max_pos if max_pos > 0 else 1
            source.add((eq["num_positions"] * scale).values, "pos_scaled")
            fig.line("index", "pos_scaled", source=source, line_width=1.3,
                     line_color="#ff7f0e", line_dash="dashed",
                     legend_label="Positions (scaled)")
        _set_tooltips(fig, [("Cash", "@cash{$0,0.00}"),
                            ("Positions", "@num_positions{0,0}")],
                      renderers=[r])
        fig.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")
        return fig

    if bar:
        bar.set_desc("Chart setup")
        bar.advance()

    if plot_equity:
        _plot_equity()
    if bar:
        bar.set_desc("Equity panel")
        bar.advance()

    if plot_pl:
        pl_fig = _plot_pl()
        if pl_fig is not None:
            figs_above.append(pl_fig)
    if bar:
        bar.set_desc("P&L panel")
        bar.advance()

    if has_market_lines:
        _plot_market_prices()
    else:
        _plot_main_fallback()
    if bar:
        bar.set_desc("Market prices")
        bar.advance()

    if plot_drawdown:
        figs_below.append(_plot_drawdown())
    if plot_cash:
        figs_below.append(_plot_cash())
    if bar:
        bar.set_desc("Sub-panels")
        bar.advance()

    # Hide x-axis on everything except fig_main
    for f in figs_above:
        f.xaxis.visible = False
    for f in figs_below:
        f.xaxis.visible = False

    plots = figs_above + [fig_main] + figs_below
    linked_crosshair = CrosshairTool(dimensions="both")

    for f in plots:
        if f.legend:
            f.legend.visible = show_legend
            f.legend.location = "top_left"
            f.legend.border_line_width = 1
            f.legend.border_line_color = "#333333"
            f.legend.padding = 5
            f.legend.spacing = 0
            f.legend.margin = 0
            f.legend.label_text_font_size = "8pt"
            f.legend.click_policy = "hide"
        f.min_border_left = 0
        f.min_border_top = 3
        f.min_border_bottom = 6
        f.min_border_right = 10
        f.outline_line_color = "#666666"
        f.toolbar.logo = None  # type: ignore[assignment]
        f.add_tools(linked_crosshair)
        wz = next((t for t in f.tools if isinstance(t, WheelZoomTool)), None)
        if wz is not None:
            wz.maintain_focus = False  # type: ignore[attr-defined]

    kwargs: dict[str, Any] = {}
    if plot_width is None:
        kwargs["sizing_mode"] = "stretch_width"

    grid = gridplot(
        plots,  # type: ignore[arg-type]
        ncols=1,
        toolbar_location="right",
        merge_tools=True,
        **kwargs,  # type: ignore[arg-type]
    )
    if bar:
        bar.set_desc("Layout assembled")
        bar.advance()

    try:
        show(grid, browser=None if open_browser else "none")
    finally:
        if bar:
            bar._refresh_bar()
            bar._teardown()

    return grid
