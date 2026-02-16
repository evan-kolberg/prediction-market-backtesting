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
# pyright: reportArgumentType=false, reportCallIssue=false, reportOperatorIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import os
import random
import sys
from colorsys import hls_to_rgb, rgb_to_hls
from functools import partial
from itertools import cycle
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd  # type: ignore[import-untyped]
from bokeh.colors.named import lime as BULL_COLOR
from bokeh.colors.named import tomato as BEAR_COLOR
from bokeh.io import output_file, output_notebook, show
from bokeh.io.state import curstate
from bokeh.layouts import column, gridplot
from bokeh.models import (  # type: ignore[attr-defined]
    ColumnDataSource,
    CrosshairTool,
    CustomJS,
    DatetimeTickFormatter,
    Div,
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


def _bokeh_reset(filename: str | None = None) -> None:
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


def lightness(color: Any, light: float = 0.94) -> str:
    """Return *color* adjusted to the given lightness as a hex string."""
    rgb = np.array([color.r, color.g, color.b]) / 255
    h, _, s = rgb_to_hls(*rgb)
    r_c, g_c, b_c = hls_to_rgb(h, light, s)
    return f"#{int(r_c * 255):02x}{int(g_c * 255):02x}{int(b_c * 255):02x}"


def _downsample(
    eq: pd.DataFrame,
    fills_df: pd.DataFrame,
    market_df: pd.DataFrame,
    max_points: int = 5000,
    alloc_df: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    """Downsample plotting data to *max_points* rows.

    Preserves first/last points, fill-event rows, equity peak, and max
    drawdown to keep key visual events intact.  Fills' ``bar`` column is
    remapped to match the new indices.

    If *alloc_df* is provided it is downsampled to the same rows.
    """
    n = len(eq)
    if n <= max_points:
        return eq, fills_df, market_df, alloc_df

    # Build set of "must keep" original indices
    keep: set[int] = {0, n - 1}  # first & last

    # Rows where fills occurred
    if not fills_df.empty:
        keep.update(fills_df["bar"].astype(int).tolist())

    # Equity peak and max drawdown
    keep.add(int(eq["equity"].idxmax()))
    keep.add(int(eq["drawdown_pct"].idxmax()))

    # Uniformly spaced samples to fill up to max_points
    budget = max_points - len(keep)
    if budget > 0:
        uniform = np.linspace(0, n - 1, budget, dtype=int)
        keep.update(uniform.tolist())

    keep_sorted = sorted(keep)

    # Build old→new index mapping
    old_to_new = {old_idx: new_idx for new_idx, old_idx in enumerate(keep_sorted)}

    # Downsample eq and market_df
    eq_ds = eq.iloc[keep_sorted].reset_index(drop=True)
    market_ds = market_df.iloc[keep_sorted].reset_index(drop=True) if not market_df.empty else market_df

    # Downsample alloc_df to same rows
    alloc_ds: pd.DataFrame | None = None
    if alloc_df is not None and not alloc_df.empty:
        alloc_ds = alloc_df.iloc[keep_sorted].reset_index(drop=True)

    # Remap fills bar indices
    if not fills_df.empty:
        fills_ds = fills_df.copy()
        fills_ds["bar"] = fills_ds["bar"].astype(int).map(old_to_new)
        # Drop any fills that didn't map (shouldn't happen since we kept them)
        fills_ds = fills_ds.dropna(subset=["bar"])
        fills_ds["bar"] = fills_ds["bar"].astype(int)
    else:
        fills_ds = fills_df

    return eq_ds, fills_ds, market_ds, alloc_ds


def _build_dataframes(
    result: BacktestResult,
    bar: PinnedProgress[None] | None = None,
    max_markets: int = 10,
    max_points: int = 5000,
):
    """Convert a :class:`BacktestResult` into plotting-ready DataFrames.

    Only the top *max_markets* markets (by price range among traded markets)
    are fully aligned to the equity timeline.  This avoids building a
    20 000-column DataFrame that would consume tens of GB of RAM.

    The result is downsampled to at most *max_points* bars so the
    serialised Bokeh JSON stays small enough for the browser to render.

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
        # --- select markets: all traded first, then random sample ---
        traded_ids = set(fills_df["market_id"]) if not fills_df.empty else set()
        # Always include every traded market
        traded_with_data = [mid for mid in traded_ids if mid in market_prices and market_prices[mid]]
        non_traded = [mid for mid in market_prices if mid not in traded_ids and market_prices[mid]]
        # Fill remaining budget with a random spread of non-traded markets
        budget = max(0, max_markets - len(traded_with_data))
        if budget > 0 and non_traded:
            sampled = random.sample(non_traded, min(budget, len(non_traded)))
        else:
            sampled = []
        selected = traded_with_data + sampled
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

    # Downsample to keep the Bokeh JSON small enough for the browser
    n_bars_original = len(eq)
    eq, fills_df, market_df, _ = _downsample(eq, fills_df, market_df, max_points=max_points)

    return eq, fills_df, market_df, n_bars_original


def _build_allocation_data(
    eq: pd.DataFrame,
    fills_df: pd.DataFrame,
    market_prices: dict[str, list[tuple]],
    top_n: int | None = None,
) -> pd.DataFrame:
    """Reconstruct position-value allocation over time from fills + prices.

    Returns a :class:`~pandas.DataFrame` with one column per traded market
    plus ``"Cash"``.  When *top_n* is ``None`` (default) every position gets
    its own column — no "Other" bucket — so that each market is visible in
    the allocation chart.  Values are **dollar amounts** (not percentages);
    the caller normalises.
    """
    n_bars = len(eq)
    eq_dts = eq["datetime"].values  # datetime64[ns]

    if fills_df.empty:
        return pd.DataFrame({"Cash": eq["cash"].values}, index=eq.index)

    # 1. Replay fills → cumulative position qty at each bar ----------------
    #    Only track traded markets (those with fills).
    pos_changes: dict[str, np.ndarray] = {}  # mid → delta array
    for _, f in fills_df.iterrows():
        mid = f["market_id"]
        bar_idx = int(f["bar"])
        if mid not in pos_changes:
            pos_changes[mid] = np.zeros(n_bars)
        if f["action"] == "buy" and f["side"] == "yes":
            pos_changes[mid][bar_idx] += f["quantity"]
        elif f["action"] == "sell" and f["side"] == "yes":
            pos_changes[mid][bar_idx] -= f["quantity"]
        elif f["action"] == "buy" and f["side"] == "no":
            pos_changes[mid][bar_idx] -= f["quantity"]
        elif f["action"] == "sell" and f["side"] == "no":
            pos_changes[mid][bar_idx] += f["quantity"]

    pos_qty: dict[str, np.ndarray] = {}
    for mid, deltas in pos_changes.items():
        pos_qty[mid] = np.cumsum(deltas)

    # 2. Forward-fill market prices onto the equity timeline ---------------
    #    Use the last fill price per market as a cheap fallback; only do
    #    the full price-history lookup for the top-N markets (by qty) to
    #    keep this fast even with thousands of traded markets.
    fill_price_map: dict[str, float] = {}
    if not fills_df.empty:
        for mid, grp in fills_df.groupby("market_id"):
            fill_price_map[str(mid)] = float(grp["price"].iloc[-1])

    # Pre-select top candidates by peak absolute qty so we limit expensive work
    peak_qty = {mid: float(np.max(np.abs(q))) for mid, q in pos_qty.items()}
    ranked_by_qty = sorted(peak_qty, key=peak_qty.get, reverse=True)  # type: ignore[arg-type]
    # Process full price history for top N only; rest use fill-price fallback
    if top_n is None:
        expensive_set = set(ranked_by_qty)  # all markets
    else:
        expensive_set = set(ranked_by_qty[: top_n * 2])

    # Pre-compute last price timestamp for each market from raw price data.
    # Used to zero out positions after the market's price feed ends
    # (i.e. market resolved / closed).
    market_last_ts: dict[str, np.datetime64] = {}
    for mid in pos_qty:
        recs = market_prices.get(mid, [])
        if recs:
            last_ts = max(ts for ts, _ in recs)
            market_last_ts[mid] = pd.Timestamp(last_ts).tz_localize(None).to_datetime64()

    price_on_bar: dict[str, np.ndarray] = {}
    for mid in pos_qty:
        recs = market_prices.get(mid, []) if mid in expensive_set else []
        if recs:
            ts_list, pr_list = zip(*recs)
            ts_arr = pd.to_datetime(list(ts_list)).values.astype("datetime64[ns]")
            pr_arr = np.array(pr_list, dtype=float)
            order = np.argsort(ts_arr)
            ts_arr, pr_arr = ts_arr[order], pr_arr[order]
            idx = np.searchsorted(ts_arr, eq_dts, side="right") - 1
            prices = np.full(n_bars, np.nan)
            valid = idx >= 0
            prices[valid] = pr_arr[idx[valid]]
            prices[eq_dts < ts_arr[0]] = np.nan
            prices[eq_dts > ts_arr[-1]] = np.nan
            price_on_bar[mid] = prices
        else:
            # Cheap fallback: constant price from last fill
            fp = fill_price_map.get(mid, 0.5)
            price_on_bar[mid] = np.full(n_bars, fp)

    # Zero out position qty after the market's price feed ends.
    # This handles market resolution: once there is no more price data,
    # the position was settled and should not contribute to allocation.
    for mid in pos_qty:
        last_ts = market_last_ts.get(mid)
        if last_ts is not None:
            # Zero out all bars after the last price observation
            cutoff = np.searchsorted(eq_dts, last_ts, side="right")
            if 0 < cutoff < n_bars:
                pos_qty[mid][cutoff:] = 0.0
        else:
            # No price data at all — use fills to find end of activity
            mid_fills = fills_df[fills_df["market_id"] == mid]
            if not mid_fills.empty:
                last_bar = int(mid_fills["bar"].max())
                if last_bar < n_bars - 1:
                    pos_qty[mid][last_bar + 1 :] = 0.0

    # 3. Compute mark-to-market position values ----------------------------
    pos_values: dict[str, np.ndarray] = {}
    for mid, qty in pos_qty.items():
        pr = price_on_bar.get(mid)
        if pr is None:
            continue
        safe_pr = np.nan_to_num(pr, nan=0.0)
        val = np.where(qty >= 0, qty * safe_pr, np.abs(qty) * (1.0 - safe_pr))
        val = np.maximum(val, 0.0)
        pos_values[mid] = val

    # 4. Keep all positions (or top-N with "Other" bucket) ----------------
    peak = {mid: float(np.max(v)) for mid, v in pos_values.items()}
    ranked = sorted(peak, key=peak.get, reverse=True)  # type: ignore[arg-type]

    # Keep individual columns for top markets, aggregate the rest into
    # numbered visual bands so the HTML stays small.  Default max_bands=50
    # means at most ~50 position columns regardless of how many markets
    # were traded.
    max_bands = 50 if top_n is None else top_n
    if len(ranked) <= max_bands:
        top_ids = ranked
        other_ids: list[str] = []
    else:
        top_ids = ranked[:max_bands]
        other_ids = ranked[max_bands:]

    # Build all columns at once to avoid DataFrame fragmentation warnings
    col_data: dict[str, np.ndarray] = {}
    for mid in top_ids:
        label = mid[:20] + "\u2026" if len(mid) > 20 else mid
        col_data[label] = pos_values[mid]
    if other_ids:
        col_data["Other"] = np.sum([pos_values[m] for m in other_ids], axis=0)
    col_data["Cash"] = np.maximum(eq["cash"].values, 0.0)
    alloc = pd.DataFrame(col_data, index=eq.index)
    return alloc


# ---------------------------------------------------------------------------
# Main plot function
# ---------------------------------------------------------------------------


def plot(
    result: BacktestResult,
    *,
    filename: str = "",
    plot_width: int | None = None,
    plot_equity: bool = True,
    plot_drawdown: bool = True,
    plot_pl: bool = True,
    plot_cash: bool = True,
    plot_market_prices: bool = True,
    plot_allocation: bool = True,
    show_legend: bool = True,
    open_browser: bool = True,
    relative_equity: bool = True,
    max_markets: int = 30,
    progress: bool = True,
) -> Any:
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

    use_bar = progress and not _is_notebook()
    bar: PinnedProgress[None] | None = None
    # We'll create the progress bar after we know actual step count.

    # Build dataframes — _build_dataframes already downsamples to max_points.
    eq, fills_df, market_df, n_bars_original = _build_dataframes(
        result,
        bar=None,
        max_markets=max_markets,
    )

    # Build allocation from the (already downsampled) data.
    # Every traded position gets its own column (no top-N).
    alloc_df: pd.DataFrame | None = None
    n_alloc_positions = 0
    if plot_allocation:
        alloc_df = _build_allocation_data(
            eq,
            fills_df,
            getattr(result, "market_prices", {}),
            top_n=None,
        )
        n_alloc_positions = len([c for c in alloc_df.columns if c not in ("Cash", "Other")])

    # Now create progress bar with accurate step count
    n_chart_steps = 6  # setup, equity, P&L, market/fallback, sub-panels, layout
    total_steps = n_chart_steps
    n_fills_total = len(result.fills)
    n_total_markets = len(getattr(result, "market_prices", {}))
    if use_bar:
        bar = PinnedProgress(
            iter([]),
            total=total_steps,
            desc="Rendering chart",
            unit=" steps",
        )
        bar._setup()
        bar.write(f"  {n_bars_original:,} bars, {n_fills_total:,} fills, {n_total_markets:,} markets")
        if n_bars_original > len(eq):
            bar.write(f"  Downsampled: {n_bars_original:,} → {len(eq):,} bars")
    index = eq.index

    # Rank markets by observable price range
    if not market_df.empty:
        traded_cols = [c for c in market_df.columns if c in set(fills_df["market_id"]) if not fills_df.empty]
        if not traded_cols:
            traded_cols = list(market_df.columns)
        price_range = (market_df[traded_cols].max() - market_df[traded_cols].min()).sort_values(ascending=False)
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
        **({} if plot_width else {"sizing_mode": "stretch_width"}),  # type: ignore[arg-type]
    )

    pad = (index[-1] - index[0]) / 20 if len(index) > 1 else 1
    x_range_kw: dict[str, Any] = (
        {
            "x_range": Range1d(
                index[0],
                index[-1],
                min_interval=10,  # type: ignore[call-arg]
                bounds=(index[0] - pad, index[-1] + pad),
            )
        }
        if len(index) > 1
        else {}
    )

    fig_main = new_figure(height=400, **x_range_kw)  # type: ignore[call-arg]
    source = ColumnDataSource(eq)

    fig_main.xaxis.formatter = CustomJSTickFormatter(
        args={
            "axis": fig_main.xaxis[0],
            "formatter": DatetimeTickFormatter(days="%a, %d %b", months="%m/%Y"),
            "source": source,
        },
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
        tooltips = [("Date", "@datetime{%c}")] + list(tooltips)
        fig.add_tools(
            HoverTool(
                point_policy="follow_mouse",
                renderers=list(renderers),
                formatters={"@datetime": "datetime"},
                tooltips=tooltips,
                mode="vline" if vline else "mouse",
            )
        )

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
            "index",
            "eq_dd_patch",
            source=ColumnDataSource(
                {
                    "index": np.r_[index, index[::-1]],
                    "eq_dd_patch": np.r_[equity.values, hw.values[::-1]],
                }
            ),
            fill_color="#ffffea",
            line_color="#ffcb66",
        )

        r = fig.line("index", "eq_plot", source=source, line_width=1.5, line_alpha=1, legend_label="Strategy")

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
        fig.scatter(
            argmax,
            peak_val,
            color="cyan",
            size=8,
            legend_label=f"Peak ({fmt_legend.format(peak_val * (100 if relative_equity else 1))})",
        )

        fig.scatter(
            index[-1],
            equity.iloc[-1],
            color="blue",
            size=8,
            legend_label=f"Final ({fmt_legend.format(equity.iloc[-1] * (100 if relative_equity else 1))})",
        )

        dd = eq["drawdown_pct"]
        dd_end = int(dd.idxmax())
        if dd.iloc[dd_end] > 0:
            dd_start = int(equity.iloc[:dd_end].idxmax())
            dd_dur = eq["datetime"].iloc[dd_end] - eq["datetime"].iloc[dd_start]
            label = f"Max Dd Dur. ({dd_dur})".replace(" 00:00:00", "").replace("(0 days ", "(")
            fig.line([dd_start, dd_end], equity.iloc[dd_start], line_color="red", line_width=2, legend_label=label)

            if not plot_drawdown:
                fig.scatter(
                    dd_end,
                    equity.iloc[dd_end],
                    color="red",
                    size=8,
                    legend_label=f"Max Drawdown (-{100 * dd.iloc[dd_end]:.1f}%)",
                )

        figs_above.append(fig)

    def _plot_pl():
        """Render per-market P&L as triangle tick markers."""
        fig = _new_sub("Profit / Loss", height=110)
        fig.add_layout(Span(location=0, dimension="width", line_color="#666666", line_dash="dashed", line_width=1))

        market_pnls = getattr(result, "market_pnls", {})

        if market_pnls:
            pnl_records = []
            for mid, pnl_val in market_pnls.items():
                if pnl_val == 0.0:
                    continue
                mkt_fills = fills_df[fills_df["market_id"] == mid] if not fills_df.empty else pd.DataFrame()
                if mkt_fills.empty:
                    continue
                pnl_records.append(
                    {
                        "bar": int(mkt_fills["bar"].iloc[-1]),
                        "datetime": mkt_fills["datetime"].iloc[-1],
                        "pnl": pnl_val,
                        "market_id": mid,
                    }
                )
            if pnl_records:
                pnl_df = pd.DataFrame(pnl_records).sort_values("bar")

                if False:  # always use scatter triangles (minitrade style)
                    pass
                else:
                    sz = np.abs(pnl_df["pnl"].values).astype(float)
                    if sz.max() > sz.min():
                        sz = np.interp(sz, (sz.min(), sz.max()), (8, 20))
                    else:
                        sz = np.full_like(sz, 12.0)
                    pnl_long = np.where(pnl_df["pnl"].values > 0, pnl_df["pnl"].values, np.nan)
                    pnl_short = np.where(pnl_df["pnl"].values <= 0, pnl_df["pnl"].values, np.nan)
                    positive = np.where(pnl_df["pnl"].values > 0, "1", "0")
                    pnl_src = ColumnDataSource(
                        {
                            "index": pnl_df["bar"].values,
                            "datetime": pnl_df["datetime"].values,
                            "pnl_long": pnl_long,
                            "pnl_short": pnl_short,
                            "positive": positive,
                            "market_id": pnl_df["market_id"].values,
                            "size_marker": sz,
                        }
                    )
                    cmap = factor_cmap("positive", COLORS, ["0", "1"])
                    r1 = fig.scatter(
                        "index",
                        "pnl_long",
                        source=pnl_src,
                        fill_color=cmap,
                        marker="triangle",
                        line_color="black",
                        size="size_marker",
                    )
                    r2 = fig.scatter(
                        "index",
                        "pnl_short",
                        source=pnl_src,
                        fill_color=cmap,
                        marker="inverted_triangle",
                        line_color="black",
                        size="size_marker",
                    )
                    _set_tooltips(
                        fig, [("Market", "@market_id"), ("P/L", "@pnl_long{+$0,0.00}")], vline=False, renderers=[r1]
                    )
                    _set_tooltips(
                        fig, [("Market", "@market_id"), ("P/L", "@pnl_short{+$0,0.00}")], vline=False, renderers=[r2]
                    )

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
            fill_src = ColumnDataSource(
                {
                    "index": relevant_fills["bar"].values,
                    "datetime": relevant_fills["datetime"].values,
                    "pnl_long": pnl_long,
                    "pnl_short": pnl_short,
                    "positive": positive,
                    "market_id": relevant_fills["market_id"].values,
                    "size_marker": sz,
                }
            )
            cmap = factor_cmap("positive", COLORS, ["0", "1"])
            r1 = fig.scatter(
                "index",
                "pnl_long",
                source=fill_src,
                fill_color=cmap,
                marker="triangle",
                line_color="black",
                size="size_marker",
            )
            r2 = fig.scatter(
                "index",
                "pnl_short",
                source=fill_src,
                fill_color=cmap,
                marker="inverted_triangle",
                line_color="black",
                size="size_marker",
            )
            _set_tooltips(
                fig, [("Market", "@market_id"), ("Value", "@pnl_long{+$0,0.00}")], vline=False, renderers=[r1]
            )
            _set_tooltips(
                fig, [("Market", "@market_id"), ("Value", "@pnl_short{+$0,0.00}")], vline=False, renderers=[r2]
            )

        fig.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")
        return fig

    def _plot_pnl_period():
        """Bar chart showing P&L aggregated over equal time intervals."""
        fig = _new_sub("P&L (periodic)", height=120)
        fig.add_layout(Span(location=0, dimension="width", line_color="#666666", line_dash="dashed", line_width=1))

        equity_vals = eq["equity"].values
        n = len(equity_vals)
        if n < 4:
            return fig

        # Divide the timeline into ~50-100 bins
        n_bins = max(10, min(100, n // 20))
        bin_edges = np.linspace(0, n - 1, n_bins + 1, dtype=int)

        bar_x: list[float] = []
        bar_pnl: list[float] = []
        bar_dt_start: list = []
        bar_dt_end: list = []

        for i in range(len(bin_edges) - 1):
            start, end = int(bin_edges[i]), int(bin_edges[i + 1])
            pnl = float(equity_vals[end] - equity_vals[start])
            bar_x.append((start + end) / 2.0)
            bar_pnl.append(pnl)
            bar_dt_start.append(eq["datetime"].iloc[start])
            bar_dt_end.append(eq["datetime"].iloc[end])

        pnl_pos = [max(0.0, p) for p in bar_pnl]
        pnl_neg = [min(0.0, p) for p in bar_pnl]
        bar_width = max(1, (bin_edges[1] - bin_edges[0]) * 0.8)

        pnl_src = ColumnDataSource(
            {
                "x": bar_x,
                "pnl_pos": pnl_pos,
                "pnl_neg": pnl_neg,
                "pnl": bar_pnl,
                "dt_start": bar_dt_start,
                "dt_end": bar_dt_end,
            }
        )

        r1 = fig.vbar(
            x="x",
            top="pnl_pos",
            source=pnl_src,
            width=bar_width,
            color=BULL_COLOR.to_hex(),
            alpha=0.7,
            legend_label="Gain",
        )
        r2 = fig.vbar(
            x="x",
            top="pnl_neg",
            source=pnl_src,
            width=bar_width,
            color=BEAR_COLOR.to_hex(),
            alpha=0.7,
            legend_label="Loss",
        )

        fig.add_tools(
            HoverTool(
                renderers=[r1, r2],
                tooltips=[
                    ("Period", "@dt_start{%b %Y} \u2013 @dt_end{%b %Y}"),
                    ("P&L", "@pnl{$0,0.00}"),
                ],
                formatters={"@dt_start": "datetime", "@dt_end": "datetime"},
                mode="vline",
            )
        )

        fig.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")
        return fig

    def _plot_monthly_returns():
        """Monthly returns heatmap — grid of months × years coloured by return %."""
        from bokeh.models import BasicTicker, ColorBar, LinearColorMapper, PrintfTickFormatter

        dts = pd.to_datetime(eq["datetime"])
        eqv = eq["equity"].values.copy()
        monthly = pd.DataFrame({"datetime": dts, "equity": eqv})
        monthly["year"] = monthly["datetime"].dt.year.astype(str)
        monthly["month"] = monthly["datetime"].dt.month

        # Compute return for each calendar month
        first_last = monthly.groupby(["year", "month"]).agg(
            eq_start=("equity", "first"),
            eq_end=("equity", "last"),
        )
        first_last["ret"] = (first_last["eq_end"] - first_last["eq_start"]) / first_last["eq_start"]  # type: ignore[reportIndexIssue]
        first_last = first_last.reset_index()

        if first_last.empty:
            return None

        month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        first_last["month_name"] = first_last["month"].map(lambda m: month_names[m - 1])

        years = sorted(first_last["year"].unique())
        months_used = sorted(first_last["month"].unique())
        month_labels = [month_names[m - 1] for m in months_used]

        max_abs = max(abs(first_last["ret"].max()), abs(first_last["ret"].min()), 0.001)
        mapper = LinearColorMapper(
            palette=["#d73027", "#f46d43", "#fdae61", "#fee08b", "#ffffbf", "#d9ef8b", "#a6d96a", "#66bd63", "#1a9850"],
            low=-max_abs,
            high=max_abs,
        )

        fig = _figure(
            x_range=month_labels,
            y_range=list(reversed(years)),
            x_axis_location="above",
            width=plot_width,
            height=max(80, 40 + 28 * len(years)),
            tools="hover,save",
            toolbar_location=None,
            **({} if plot_width else {"sizing_mode": "stretch_width"}),
        )

        heat_src = ColumnDataSource(
            {
                "month": first_last["month_name"].tolist(),
                "year": first_last["year"].tolist(),
                "ret": first_last["ret"].tolist(),
                "ret_pct": (first_last["ret"] * 100).round(2).tolist(),
            }
        )

        fig.rect(
            x="month",
            y="year",
            width=1,
            height=1,
            source=heat_src,
            fill_color={"field": "ret", "transform": mapper},
            line_color="white",
            line_width=1.5,
        )

        # Add return % as text labels on each cell
        from bokeh.models import LabelSet

        heat_src.add(
            [f"{v:+.1f}%" for v in (first_last["ret"] * 100).values],
            "label",
        )
        labels = LabelSet(
            x="month",
            y="year",
            text="label",
            source=heat_src,
            text_align="center",
            text_baseline="middle",
            text_font_size="9pt",
            text_color="#333333",
        )
        fig.add_layout(labels)

        color_bar = ColorBar(
            color_mapper=mapper,
            ticker=BasicTicker(desired_num_ticks=5),
            formatter=PrintfTickFormatter(format="%+.1f%%"),
            label_standoff=6,
            border_line_color=None,
            location=(0, 0),
            width=8,
        )
        fig.add_layout(color_bar, "right")

        fig.axis.axis_line_color = None
        fig.axis.major_tick_line_color = None
        fig.grid.grid_line_color = None
        fig.yaxis.axis_label = "Monthly Returns"

        hover = fig.select_one(HoverTool)
        if hover is not None:
            hover.tooltips = [
                ("Month", "@month @year"),
                ("Return", "@ret_pct{+0.00}%"),
            ]

        return fig

    def _plot_rolling_sharpe():
        """Rolling Sharpe ratio over the equity curve."""
        equity_vals = eq["equity"].values.copy()
        n = len(equity_vals)
        if n < 60:
            return None

        # Compute bar-to-bar returns
        returns = np.diff(equity_vals) / equity_vals[:-1]

        # Rolling window — use ~5% of total bars, clamped to [20, 500]
        window = max(20, min(500, n // 20))

        # Rolling Sharpe = rolling_mean / rolling_std (annualised later via tooltip)
        rolling_mean = pd.Series(returns).rolling(window, min_periods=window).mean().values
        rolling_std = pd.Series(returns).rolling(window, min_periods=window).std().values

        # Avoid division by zero
        with np.errstate(divide="ignore", invalid="ignore"):
            rolling_sharpe = np.where(rolling_std > 0, rolling_mean / rolling_std, 0.0)

        # Align to eq index (returns are 1 shorter, rolling clips more)
        sharpe_full = np.full(n, np.nan)
        sharpe_full[1:] = rolling_sharpe  # shift by 1 since returns start at index 1

        fig = _new_sub("Rolling Sharpe", height=100)
        source.add(sharpe_full, "rolling_sharpe")

        fig.add_layout(Span(location=0, dimension="width", line_color="#666666", line_dash="dashed", line_width=1))

        r = fig.line("index", "rolling_sharpe", source=source, line_width=1.3, line_color="#9467bd")

        # Shade positive/negative regions (only where sharpe data exists)
        safe_sharpe = np.nan_to_num(sharpe_full, nan=0.0)
        pos_sharpe = np.maximum(safe_sharpe, 0.0)
        neg_sharpe = np.minimum(safe_sharpe, 0.0)
        zero_line = np.zeros(n)

        idx_arr = np.arange(n, dtype=float)
        fig.patch(
            x=np.r_[idx_arr, idx_arr[::-1]].tolist(),
            y=np.r_[pos_sharpe, zero_line[::-1]].tolist(),
            fill_color=BULL_COLOR.to_hex(),
            fill_alpha=0.15,
            line_color=None,
        )
        fig.patch(
            x=np.r_[idx_arr, idx_arr[::-1]].tolist(),
            y=np.r_[neg_sharpe, zero_line[::-1]].tolist(),
            fill_color=BEAR_COLOR.to_hex(),
            fill_alpha=0.15,
            line_color=None,
        )

        _set_tooltips(
            fig,
            [("Rolling Sharpe", "@rolling_sharpe{0.000}")],
            renderers=[r],
        )
        fig.yaxis.axis_label = f"Sharpe ({window}-bar)"
        fig.legend.visible = False

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
            fig_main.line("index", col, source=source, legend_label=short, line_color=color, line_width=2)

        if len(market_df.columns) > max_markets:
            hidden = len(market_df.columns) - max_markets
            fig_main.line(0, 0, legend_label=f"{hidden} more markets hidden", line_color="black")

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
                    args={"price_range": fig_main.y_range, "source": source},
                    code=_AUTOSCALE_JS_TEMPLATE.format(
                        high_key="price_high",
                        low_key="price_low",
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
                xs_profit,
                ys_profit,
                line_color=colors_darker[1],
                line_width=6,
                line_alpha=0.8,
                line_dash="dotted",
                legend_label=f"Profitable ({len(xs_profit)})",
            )
        if xs_loss:
            fig_main.multi_line(
                xs_loss,
                ys_loss,
                line_color=colors_darker[0],
                line_width=6,
                line_alpha=0.8,
                line_dash="dotted",
                legend_label=f"Losing ({len(xs_loss)})",
            )

    def _draw_fill_markers():
        """Buy/sell markers on the main price chart (all traded markets)."""
        if fills_df.empty:
            return

        # Show fills for ALL traded markets, not just displayed price lines
        relevant = fills_df.copy()
        if relevant.empty:
            return

        fill_color_code = np.where(relevant["action"] == "buy", "1", "0")  # 1=green, 0=red

        marker_src = ColumnDataSource(
            {
                "index": relevant["bar"].values,
                "datetime": relevant["datetime"].values,
                "price": relevant["price"].values,
                "fill_color": fill_color_code,
                "market_id": relevant["market_id"].values,
                "action": relevant["action"].values,
                "side": relevant["side"].values,
                "quantity": relevant["quantity"].values,
            }
        )

        cmap = factor_cmap("fill_color", COLORS, ["0", "1"])
        fig_main.scatter(
            "index",
            "price",
            source=marker_src,
            fill_color=cmap,
            marker="circle",
            line_color="black",
            size=8,
            fill_alpha=0.7,
            legend_label=f"Fills ({len(relevant)})",
        )

    def _plot_main_fallback():
        """Fallback main chart showing equity when no market prices exist."""
        source.add(eq["equity"].values, "equity_abs")
        r = fig_main.line(
            "index", "equity_abs", source=source, line_width=1.5, line_color="#1f77b4", legend_label="Equity"
        )
        fig_main.yaxis.axis_label = "Equity ($)"
        fig_main.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")

        source.add(eq["equity"].values, "price_high")
        source.add(eq["equity"].values, "price_low")
        fig_main.x_range.js_on_change(
            "end",
            CustomJS(
                args={"price_range": fig_main.y_range, "source": source},
                code=_AUTOSCALE_JS_TEMPLATE.format(
                    high_key="price_high",
                    low_key="price_low",
                    range_var="price_range",
                ),
            ),
        )
        _set_tooltips(
            fig_main, [("Equity", "@equity_abs{$0,0.00}"), ("Cash", "@cash{$0,0.00}")], vline=True, renderers=[r]
        )

    def _plot_drawdown():
        fig = _new_sub("Drawdown", height=90)
        source.add(eq["drawdown_pct"].values, "dd_pct")
        r = fig.line("index", "dd_pct", source=source, line_width=1.3)
        argmax = int(eq["drawdown_pct"].idxmax())
        fig.scatter(
            argmax,
            eq["drawdown_pct"].iloc[argmax],
            color="red",
            size=8,
            legend_label="Peak (-{:.1f}%)".format(100 * eq["drawdown_pct"].iloc[argmax]),
        )
        _set_tooltips(fig, [("Drawdown", "@dd_pct{-0.[0]%}")], renderers=[r])
        fig.yaxis.formatter = NumeralTickFormatter(format="-0.[0]%")
        return fig

    def _plot_cash():
        fig = _new_sub("Cash / Equity", height=90)
        r = fig.line("index", "cash", source=source, line_width=1.3, line_color="#1f77b4", legend_label="Cash")

        # Show actual equity line so the gap = position value
        source.add(eq["equity"].values, "equity_dollar")
        fig.line(
            "index",
            "equity_dollar",
            source=source,
            line_width=1.3,
            line_color="#2ca02c",
            legend_label="Equity",
        )

        # Show actual dollar position value (equity - cash) as dashed line
        pos_value = (eq["equity"] - eq["cash"]).values
        source.add(pos_value, "pos_value")
        fig.line(
            "index",
            "pos_value",
            source=source,
            line_width=1.3,
            line_color="#ff7f0e",
            line_dash="dashed",
            legend_label="Positions ($)",
        )

        _set_tooltips(
            fig,
            [
                ("Cash", "@cash{$0,0.00}"),
                ("Equity", "@equity_dollar{$0,0.00}"),
                ("Position Value", "@pos_value{$0,0.00}"),
                ("# Positions", "@num_positions{0,0}"),
            ],
            renderers=[r],
        )
        fig.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")
        return fig

    def _plot_allocation():
        """Stacked area chart: grey = cash, coloured bands = positions.

        Every traded market gets its own random colour so allocation is
        visible even with hundreds of positions.  The allocation data uses
        the full (non-downsampled) equity timeline.
        """
        assert alloc_df is not None  # narrowing for type checker
        fig = _new_sub("Allocation", height=220)

        pos_cols = [c for c in alloc_df.columns if c not in ("Cash", "Other")]
        other_col = "Other" if "Other" in alloc_df.columns else None
        all_cols = pos_cols + ([other_col] if other_col else []) + ["Cash"]

        # Normalise against actual equity (from the equity curve) so allocation
        # fractions stay consistent with the Cash/Equity panel.
        equity_total = eq["equity"].values.copy()
        # Fallback: if equity is zero or unavailable, use sum of components
        component_total = alloc_df[all_cols].sum(axis=1).values
        row_total = pd.Series(
            np.where(equity_total > 0, np.maximum(equity_total, component_total), component_total),
            index=alloc_df.index,
        ).replace(0, 1.0)
        normed = alloc_df[all_cols].div(row_total, axis=0).fillna(0.0).clip(0.0, 1.0)

        # Allocation is downsampled to the same rows as eq, so use eq's index.
        alloc_src_data: dict[str, Any] = {"index": eq.index.values}

        # Stack order: positions first (coloured), then Other, then Cash (grey)
        stackers: list[str] = []
        stack_labels: list[str] = []
        for col in pos_cols:
            key = f"alloc_{col.replace(' ', '_').replace('.', '_')}"
            alloc_src_data[key] = normed[col].values
            stackers.append(key)
            stack_labels.append(col)
        if other_col:
            key = "alloc__Other"
            alloc_src_data[key] = normed[other_col].values
            stackers.append(key)
            stack_labels.append("Other")
        # Cash on top (grey)
        cash_key = "alloc__Cash"
        alloc_src_data[cash_key] = normed["Cash"].values
        stackers.append(cash_key)
        stack_labels.append("Cash")

        alloc_source = ColumnDataSource(alloc_src_data)

        # Generate random distinguishable colours for every position.
        # Use golden-angle hue spacing for maximal visual separation.
        n_pos = len(pos_cols) + (1 if other_col else 0)
        rng = random.Random(42)  # deterministic per run
        hue_offset = rng.random()
        palette: list[str] = []
        golden_ratio = 0.618033988749895
        for i in range(n_pos):
            h = (hue_offset + i * golden_ratio) % 1.0
            s = 0.55 + rng.random() * 0.3  # 0.55–0.85
            lit = 0.45 + rng.random() * 0.15  # 0.45–0.60
            r_c, g_c, b_c = hls_to_rgb(h, lit, s)
            palette.append(f"#{int(r_c * 255):02x}{int(g_c * 255):02x}{int(b_c * 255):02x}")
        # Cash = neutral grey
        palette.append("#cccccc")

        renderers = fig.varea_stack(
            stackers=stackers,
            x="index",
            source=alloc_source,
            color=palette[: len(stackers)],
            alpha=0.85,
        )

        # Only add legend entries for a manageable subset; skip if thousands
        from bokeh.models import LegendItem

        MAX_LEGEND = 15
        legend_items: list[Any] = []
        # Always show Cash
        legend_items.append(LegendItem(label="Cash", renderers=[renderers[-1]]))
        if other_col:
            legend_items.append(LegendItem(label="Other", renderers=[renderers[-2]]))
        # Show top positions by peak value
        for r_obj, lbl in list(zip(renderers, stack_labels))[: MAX_LEGEND - len(legend_items)]:
            if lbl in ("Cash", "Other"):
                continue
            legend_items.append(LegendItem(label=lbl, renderers=[r_obj]))
        if len(pos_cols) > MAX_LEGEND:
            n_hidden = len(pos_cols) - MAX_LEGEND + len(legend_items)
            legend_items.append(LegendItem(label=f"+{n_hidden} more", renderers=[]))
        fig.legend.items = legend_items

        fig.y_range = Range1d(0, 1)
        fig.yaxis.formatter = NumeralTickFormatter(format="0%")

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
        pnl_period_fig = _plot_pnl_period()
        if pnl_period_fig is not None:
            figs_above.append(pnl_period_fig)
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

    if plot_allocation and alloc_df is not None and len(alloc_df) > 0:
        figs_above.append(_plot_allocation())  # type: ignore[arg-type]
    if plot_drawdown:
        figs_below.append(_plot_drawdown())
    sharpe_fig = _plot_rolling_sharpe()
    if sharpe_fig is not None:
        figs_below.append(sharpe_fig)
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

    # Downsampling / data summary notice
    downsampled = n_bars_original > len(eq)
    n_price_markets = len(market_df.columns) if not market_df.empty else 0
    n_traded = len(set(fills_df["market_id"])) if not fills_df.empty else 0
    fills_pct = len(fills_df) / max(n_fills_total, 1) * 100
    mkt_pct = n_price_markets / max(n_total_markets, 1) * 100
    alloc_pct = n_alloc_positions / max(n_traded, 1) * 100
    banner: Div | None = None
    parts_txt: list[str] = []
    if downsampled:
        bar_pct = len(eq) / n_bars_original * 100
        parts_txt.append(f"Bars: {n_bars_original:,}\u2192{len(eq):,} ({bar_pct:.0f}%)")
    parts_txt.append(f"Fills: {n_fills_total:,}\u2192{len(fills_df):,} ({fills_pct:.0f}%)")
    parts_txt.append(f"Markets graphed: {n_price_markets}/{n_total_markets:,} ({mkt_pct:.0f}%)")
    if n_alloc_positions > 0:
        parts_txt.append(f"Alloc: {n_alloc_positions}/{n_traded} traded ({alloc_pct:.0f}%)")
    banner = Div(
        text=(
            f"<div style='background:#fff3cd;border:1px solid #ffc107;padding:4px 12px;"
            f"font-size:11px;color:#856404;border-radius:3px;margin-bottom:2px'>"
            f"\u26a0 <b>Data:</b> {' &middot; '.join(parts_txt)}</div>"
        ),
    )

    grid = gridplot(
        plots,  # type: ignore[arg-type]
        ncols=1,
        toolbar_location="right",
        merge_tools=True,
        **kwargs,  # type: ignore[arg-type]
    )

    # Monthly returns heatmap (separate axes, not linked to shared x-range)
    monthly_fig = _plot_monthly_returns()

    # Prepend scrollbar CSS into the banner so we don't need a separate Div
    scroll_style = "<style>html{overflow-y:scroll}body{margin:0 8px}</style>"

    layout: Any
    parts: list = []
    if banner:
        # Merge CSS into banner and give it a proper sizing mode
        banner.text = scroll_style + banner.text
        banner.sizing_mode = "stretch_width"
        parts.append(banner)
    else:
        # No banner — inject CSS via a minimal invisible Div
        parts.append(Div(text=scroll_style, sizing_mode="stretch_width", height=1, visible=False))
    parts.append(grid)
    if monthly_fig is not None:
        parts.append(monthly_fig)
    if len(parts) == 1:
        layout = parts[0]
    else:
        layout = column(*parts, sizing_mode="stretch_width")  # type: ignore[arg-type]
    if bar:
        bar.set_desc("Layout assembled")
        bar.advance()

    try:
        show(layout, browser=None if open_browser else "none")
    finally:
        if bar:
            bar._refresh_bar()
            bar._teardown()

    return layout
