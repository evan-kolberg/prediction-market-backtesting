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
#  Modified by Evan Kolberg in this repository on 2026-03-11 and 2026-03-16.
#  See the repository NOTICE file for provenance and licensing scope.
#
"""
Bridge Nautilus backtest results into the vendored legacy prediction-market plotting framework.

This adapter maps Nautilus reports into the `BacktestResult` expected by the
vendored legacy Bokeh charts and appends a cumulative Brier advantage panel.
"""

from __future__ import annotations

import importlib
import re
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from prediction_market_extensions.analysis.legacy_backtesting.models import DEFAULT_DETAIL_PLOT_PANELS
from prediction_market_extensions.analysis.legacy_backtesting.models import PANEL_BRIER_ADVANTAGE
from prediction_market_extensions.analysis.legacy_backtesting.models import normalize_plot_panels
from nautilus_trader.analysis.reporter import ReportProvider


LEGACY_BACKTESTING_ROOT = Path(__file__).resolve().parent / "legacy_backtesting"


def _yes_price_fill_marker_budget(max_points: int) -> int:
    """
    Return the readable YES-price fill-marker budget for one chart.
    """
    if max_points <= 0:
        return 250
    return max(50, min(250, max_points // 10))


def _yes_price_fill_marker_limit(fill_count: int, max_points: int) -> int | None:
    """
    Return the marker cap to apply on dense YES-price charts, if any.
    """
    marker_budget = _yes_price_fill_marker_budget(max_points)
    if fill_count <= marker_budget:
        return None
    return marker_budget


def _should_hide_yes_price_fill_markers(fill_count: int, max_points: int) -> bool:
    """
    Backward-compatible predicate for callers that still remove dense markers.
    """
    return _yes_price_fill_marker_limit(fill_count, max_points) is not None


def _parse_float(value: Any, default: float = 0.0) -> float:
    """
    Parse a float from numbers and money-like strings.
    """
    if value is None:
        return default

    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip()
    if not text:
        return default

    text = text.replace("_", "").replace("\u2212", "-")
    match = re.search(r"[-+]?\d*\.?\d+", text)
    if match is None:
        return default

    try:
        return float(match.group(0))
    except ValueError:
        return default


def _to_naive_utc(value: Any) -> datetime | None:
    """
    Convert a timestamp-like value to naive UTC datetime.
    """
    if value is None:
        return None

    # Handle raw nanosecond timestamps common in Nautilus reports.
    if isinstance(value, int | float) and abs(float(value)) > 1e12:
        ts = pd.to_datetime(int(value), unit="ns", utc=True, errors="coerce")
    else:
        ts = pd.to_datetime(value, utc=True, errors="coerce")

    if pd.isna(ts):
        return None

    if isinstance(ts, pd.DatetimeIndex):
        if len(ts) == 0:
            return None
        ts = ts[0]

    assert isinstance(ts, pd.Timestamp)
    return _timestamp_to_naive_utc_datetime(ts)


def _timestamp_to_naive_utc_datetime(ts: pd.Timestamp) -> datetime:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    ts = ts.tz_localize(None)
    if ts.nanosecond:
        ts = ts.floor("us")
    return ts.to_pydatetime()


def _first_value(row: pd.Series, *keys: str) -> Any:
    for key in keys:
        if key in row.index:
            value = row[key]
            if value is not None and not (isinstance(value, float) and pd.isna(value)):
                return value
    return None


def prepare_cumulative_brier_advantage(
    user_probabilities: pd.Series | None = None,
    market_probabilities: pd.Series | None = None,
    outcomes: pd.Series | None = None,
) -> pd.DataFrame:
    """
    Compute cumulative Brier advantage through time.

    Advantage is `market_brier - strategy_brier` where Brier score is
    `(p - y)^2`.
    """
    if user_probabilities is None or market_probabilities is None or outcomes is None:
        return pd.DataFrame()

    frame = pd.concat(
        [
            user_probabilities.rename("user_probability"),
            market_probabilities.rename("market_probability"),
            outcomes.rename("outcome"),
        ],
        axis=1,
        join="inner",
    ).dropna()

    if frame.empty:
        return frame

    for col in ("user_probability", "market_probability", "outcome"):
        frame[col] = pd.to_numeric(frame[col], errors="coerce")

    frame = frame.dropna()
    if frame.empty:
        return frame

    frame = frame.sort_index()
    frame["user_probability"] = frame["user_probability"].clip(0.0, 1.0)
    frame["market_probability"] = frame["market_probability"].clip(0.0, 1.0)
    frame["outcome"] = frame["outcome"].clip(0.0, 1.0)

    frame["user_brier"] = (frame["user_probability"] - frame["outcome"]) ** 2
    frame["market_brier"] = (frame["market_probability"] - frame["outcome"]) ** 2
    frame["brier_advantage"] = frame["market_brier"] - frame["user_brier"]
    frame["cumulative_brier_advantage"] = frame["brier_advantage"].cumsum()

    return frame


def resolve_legacy_plot_repo(explicit_path: str | Path | None = None) -> Path:
    """
    Return the vendored legacy plotting package root inside this repository.
    """
    _ = explicit_path
    plotting_path = LEGACY_BACKTESTING_ROOT / "plotting.py"
    if not plotting_path.exists():
        raise FileNotFoundError(f"Vendored legacy plotting module is missing: {plotting_path}")
    return LEGACY_BACKTESTING_ROOT


def _load_legacy_modules(repo_path: Path | None = None) -> tuple[Any, Any]:
    _ = repo_path
    importlib.invalidate_caches()
    models = importlib.import_module("prediction_market_extensions.analysis.legacy_backtesting.models")
    plotting = importlib.import_module("prediction_market_extensions.analysis.legacy_backtesting.plotting")
    return models, plotting


def _configure_legacy_downsampling(plotting_module: Any, *, adaptive: bool = True, max_points: int = 5000) -> None:
    """
    Configure legacy dataframe downsampling.
    """
    downsample_fn = getattr(plotting_module, "_downsample", None)
    if downsample_fn is None:
        return

    if not adaptive:

        def _identity_downsample(eq, fills_df, market_df, max_points=5000, alloc_df=None, keep_indices=None):
            return eq, fills_df, market_df, alloc_df

        plotting_module._downsample = _identity_downsample
        return

    requested_max_points = max(2, int(max_points))

    def _adaptive_downsample(eq, fills_df, market_df, max_points=5000, alloc_df=None, keep_indices=None):
        total_points = sum(
            len(frame)
            for frame in (eq, fills_df, market_df, alloc_df)
            if frame is not None and hasattr(frame, "__len__")
        )
        if total_points <= requested_max_points:
            return eq, fills_df, market_df, alloc_df

        return downsample_fn(
            eq, fills_df, market_df, max_points=requested_max_points, alloc_df=alloc_df, keep_indices=keep_indices
        )

    plotting_module._downsample = _adaptive_downsample


def _disable_legacy_downsampling(plotting_module: Any) -> None:
    """
    Backward-compatible helper for callers that explicitly want dense rendering.
    """

    def _identity_downsample(eq, fills_df, market_df, max_points=5000, alloc_df=None, keep_indices=None):
        return eq, fills_df, market_df, alloc_df

    plotting_module._downsample = _identity_downsample


def _extract_account_report(engine: Any) -> pd.DataFrame:
    accounts = []

    if hasattr(engine, "cache"):
        try:
            accounts = list(engine.cache.accounts())
        except Exception:  # pragma: no cover - defensive fallback
            accounts = []

    if not accounts and hasattr(engine, "kernel") and hasattr(engine.kernel, "cache"):
        try:
            accounts = list(engine.kernel.cache.accounts())
        except Exception:  # pragma: no cover - defensive fallback
            accounts = []

    if not accounts:
        raise ValueError("No accounts were found on the backtest engine cache.")

    report = ReportProvider.generate_account_report(accounts[0])
    if report.empty:
        raise ValueError("Account report is empty; cannot build chart.")

    frame = report.copy()
    frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce")
    frame = frame[~frame.index.isna()].sort_index()

    if frame.empty:
        raise ValueError("Account report has no valid timestamps.")

    frame.index = frame.index.tz_convert("UTC").tz_localize(None)
    frame = frame.groupby(frame.index).last().sort_index()
    return frame


def _infer_market_side(models_module: Any, market_id: str) -> Any:
    token = market_id.upper()
    if token.endswith("NO") or "-NO" in token or ".NO." in token or "_NO" in token:
        return models_module.Side.NO
    return models_module.Side.YES


def _signed_quantity(action: str, side: str, qty: float) -> float:
    if action == "buy" and side == "yes":
        return qty
    if action == "sell" and side == "yes":
        return -qty
    if action == "buy" and side == "no":
        return -qty
    if action == "sell" and side == "no":
        return qty
    return 0.0


def _convert_fills(fills_report: pd.DataFrame, models_module: Any) -> list[Any]:
    if fills_report is None or fills_report.empty:
        return []

    frame = fills_report.copy()
    if frame.index.name and frame.index.name not in frame.columns:
        frame = frame.reset_index()

    converted: list[Any] = []
    for idx, (_, row) in enumerate(frame.iterrows(), start=1):
        market_id = str(_first_value(row, "market_id", "instrument_id", "ticker", "symbol") or "").strip()
        if not market_id:
            continue

        timestamp = _to_naive_utc(_first_value(row, "ts_event", "ts_init", "ts_last", "timestamp", "datetime"))
        if timestamp is None:
            continue

        action_raw = str(_first_value(row, "order_side", "action", "side") or "").upper()
        action = models_module.OrderAction.BUY if action_raw == "BUY" else models_module.OrderAction.SELL

        side = _infer_market_side(models_module, market_id)
        price = _parse_float(_first_value(row, "last_px", "avg_px", "price"), default=0.0)
        quantity = _parse_float(_first_value(row, "last_qty", "filled_qty", "quantity", "qty"), default=0.0)
        if quantity <= 0:
            continue

        commission = _parse_float(_first_value(row, "commission", "commissions", "fee", "fees"), default=0.0)
        order_id = str(_first_value(row, "order_id", "client_order_id", "venue_order_id") or f"fill-{idx}")

        converted.append(
            models_module.Fill(
                order_id=order_id,
                market_id=market_id,
                action=action,
                side=side,
                price=price,
                quantity=quantity,
                timestamp=timestamp,
                commission=commission,
            )
        )

    converted.sort(key=lambda fill: fill.timestamp)
    return converted


def _position_count_by_snapshot(snapshot_times: list[datetime], fills: list[Any]) -> list[int]:
    if not snapshot_times:
        return []

    counts: list[int] = []
    position_qty: dict[str, float] = {}
    fill_idx = 0

    for snapshot_time in snapshot_times:
        while fill_idx < len(fills) and fills[fill_idx].timestamp <= snapshot_time:
            fill = fills[fill_idx]
            signed_qty = _signed_quantity(fill.action.value, fill.side.value, float(fill.quantity))
            if signed_qty != 0.0:
                market_qty = position_qty.get(fill.market_id, 0.0) + signed_qty
                if abs(market_qty) < 1e-12:
                    position_qty.pop(fill.market_id, None)
                else:
                    position_qty[fill.market_id] = market_qty
            fill_idx += 1

        counts.append(len(position_qty))

    return counts


def _build_portfolio_snapshots(models_module: Any, account_report: pd.DataFrame, fills: list[Any]) -> list[Any]:
    snapshot_times = [_timestamp_to_naive_utc_datetime(ts) for ts in account_report.index]
    num_positions = _position_count_by_snapshot(snapshot_times, fills)

    snapshots: list[Any] = []
    for idx, timestamp in enumerate(snapshot_times):
        row = account_report.iloc[idx]
        total_equity = _parse_float(row.get("total", row.get("equity", 0.0)), default=0.0)
        cash = _parse_float(row.get("free", row.get("cash", total_equity)), default=total_equity)
        unrealized_pnl = total_equity - cash

        snapshots.append(
            models_module.PortfolioSnapshot(
                timestamp=timestamp,
                cash=cash,
                total_equity=total_equity,
                unrealized_pnl=unrealized_pnl,
                num_positions=num_positions[idx] if idx < len(num_positions) else 0,
            )
        )

    return snapshots


def _build_dense_timeline(
    fills: list[Any], market_prices: Mapping[str, Sequence[tuple[datetime, float]]]
) -> pd.DatetimeIndex:
    timeline: set[datetime] = set()
    for points in market_prices.values():
        for ts, _ in points:
            timeline.add(ts)
    for fill in fills:
        timeline.add(fill.timestamp)
    return pd.DatetimeIndex(sorted(timeline))


def _dense_cash_series(sparse_snapshots: list[Any], dense_dt: pd.DatetimeIndex, initial_cash: float) -> np.ndarray:
    sparse_df = pd.DataFrame(
        {
            "datetime": pd.to_datetime([snapshot.timestamp for snapshot in sparse_snapshots]),
            "cash": [float(snapshot.cash) for snapshot in sparse_snapshots],
        }
    ).sort_values("datetime")
    sparse_df = sparse_df.drop_duplicates(subset=["datetime"], keep="last")

    dense_df = pd.DataFrame({"datetime": dense_dt})
    dense_df = pd.merge_asof(dense_df, sparse_df, on="datetime", direction="backward")
    dense_df["cash"] = dense_df["cash"].ffill().fillna(float(initial_cash))
    return dense_df["cash"].to_numpy(dtype=float)


def _replay_fill_position_deltas(
    fills: list[Any], dense_dts: np.ndarray
) -> tuple[dict[str, np.ndarray], dict[str, float]]:
    n_bars = len(dense_dts)
    pos_changes: dict[str, np.ndarray] = {}
    fill_price_map: dict[str, float] = {}

    for fill in sorted(fills, key=lambda f: f.timestamp):
        market_id = fill.market_id
        delta = _signed_quantity(fill.action.value, fill.side.value, float(fill.quantity))
        if delta == 0.0:
            continue
        if market_id not in pos_changes:
            pos_changes[market_id] = np.zeros(n_bars, dtype=float)

        ts64 = pd.Timestamp(fill.timestamp).to_datetime64()
        bar_idx = int(np.searchsorted(dense_dts, ts64, side="right") - 1)
        bar_idx = max(0, min(n_bars - 1, bar_idx))
        pos_changes[market_id][bar_idx] += delta
        fill_price_map[market_id] = float(fill.price)

    return pos_changes, fill_price_map


def _aligned_market_prices(
    market_id: str,
    market_prices: Mapping[str, Sequence[tuple[datetime, float]]],
    dense_dts: np.ndarray,
    n_bars: int,
    fallback_price: float,
) -> tuple[np.ndarray, np.datetime64 | None]:
    recs = market_prices.get(market_id, [])
    if not recs:
        return np.full(n_bars, fallback_price, dtype=float), None

    ts_arr = pd.to_datetime([ts for ts, _ in recs]).to_numpy(dtype="datetime64[ns]")
    pr_arr = np.asarray([price for _, price in recs], dtype=float)

    order = np.argsort(ts_arr)
    ts_arr = ts_arr[order]
    pr_arr = pr_arr[order]

    idx = np.searchsorted(ts_arr, dense_dts, side="right") - 1
    prices = np.full(n_bars, np.nan, dtype=float)
    valid = idx >= 0
    prices[valid] = pr_arr[idx[valid]]
    prices[dense_dts < ts_arr[0]] = np.nan
    prices[dense_dts > ts_arr[-1]] = np.nan
    return prices, ts_arr[-1]


def _apply_resolution_cutoffs(
    pos_qty: dict[str, np.ndarray],
    pos_changes: Mapping[str, np.ndarray],
    market_last_ts: Mapping[str, np.datetime64 | None],
    dense_dts: np.ndarray,
) -> None:
    n_bars = len(dense_dts)
    for market_id, qty in pos_qty.items():
        last_ts = market_last_ts.get(market_id)
        if last_ts is not None:
            cutoff = int(np.searchsorted(dense_dts, last_ts, side="right"))
            if 0 < cutoff < n_bars:
                qty[cutoff:] = 0.0
            continue

        change_idx = np.flatnonzero(np.abs(pos_changes[market_id]) > 1e-12)
        if change_idx.size:
            last_idx = int(change_idx.max())
            if last_idx < n_bars - 1:
                qty[last_idx + 1 :] = 0.0


def _mark_to_market(
    pos_qty: Mapping[str, np.ndarray], price_on_bar: Mapping[str, np.ndarray]
) -> tuple[np.ndarray, np.ndarray]:
    if not pos_qty:
        return np.array([], dtype=float), np.array([], dtype=int)

    n_bars = len(next(iter(pos_qty.values())))
    total_pos_value = np.zeros(n_bars, dtype=float)
    num_positions = np.zeros(n_bars, dtype=int)

    for market_id, qty in pos_qty.items():
        prices = np.nan_to_num(price_on_bar.get(market_id, np.zeros(n_bars, dtype=float)), nan=0.0)
        values = np.where(qty >= 0.0, qty * prices, np.abs(qty) * (1.0 - prices))
        values = np.maximum(values, 0.0)
        total_pos_value += values
        num_positions += (np.abs(qty) > 1e-12).astype(int)

    return total_pos_value, num_positions


def _build_dense_portfolio_snapshots(
    models_module: Any,
    sparse_snapshots: list[Any],
    fills: list[Any],
    market_prices: Mapping[str, Sequence[tuple[datetime, float]]],
    initial_cash: float,
) -> list[Any]:
    """
    Build a dense mark-to-market equity curve on market-price timestamps.

    Sparse account snapshots are usually emitted only when account state changes
    (typically around fills). This reconstructs per-timestamp portfolio value
    across market price updates so charts include movement between trades.
    """
    if not sparse_snapshots or not market_prices:
        return sparse_snapshots

    dense_dt = _build_dense_timeline(fills, market_prices)
    if len(dense_dt) == 0 or len(dense_dt) <= len(sparse_snapshots):
        return sparse_snapshots

    dense_dts = dense_dt.to_numpy(dtype="datetime64[ns]")
    n_bars = len(dense_dt)
    cash_series = _dense_cash_series(sparse_snapshots, dense_dt, initial_cash=initial_cash)
    pos_changes, fill_price_map = _replay_fill_position_deltas(fills, dense_dts)

    if not pos_changes:
        # No open positions; keep dense cash-only timeline.
        return [
            models_module.PortfolioSnapshot(
                timestamp=_timestamp_to_naive_utc_datetime(ts),
                cash=float(cash_series[i]),
                total_equity=float(cash_series[i]),
                unrealized_pnl=0.0,
                num_positions=0,
            )
            for i, ts in enumerate(dense_dt)
        ]

    pos_qty = {mid: np.cumsum(delta) for mid, delta in pos_changes.items()}
    price_on_bar: dict[str, np.ndarray] = {}
    market_last_ts: dict[str, np.datetime64 | None] = {}
    for market_id in pos_qty:
        prices, last_ts = _aligned_market_prices(
            market_id=market_id,
            market_prices=market_prices,
            dense_dts=dense_dts,
            n_bars=n_bars,
            fallback_price=fill_price_map.get(market_id, 0.5),
        )
        price_on_bar[market_id] = prices
        market_last_ts[market_id] = last_ts

    _apply_resolution_cutoffs(pos_qty, pos_changes, market_last_ts, dense_dts)
    total_pos_value, num_positions = _mark_to_market(pos_qty, price_on_bar)

    total_equity = cash_series + total_pos_value
    return [
        models_module.PortfolioSnapshot(
            timestamp=_timestamp_to_naive_utc_datetime(ts),
            cash=float(cash_series[i]),
            total_equity=float(total_equity[i]),
            unrealized_pnl=float(total_pos_value[i]),
            num_positions=int(num_positions[i]),
        )
        for i, ts in enumerate(dense_dt)
    ]


def _build_market_pnls(positions_report: pd.DataFrame) -> dict[str, float]:
    if positions_report is None or positions_report.empty:
        return {}

    frame = positions_report.copy()
    if frame.index.name and frame.index.name not in frame.columns:
        frame = frame.reset_index()

    pnls: dict[str, float] = {}
    for _, row in frame.iterrows():
        market_id = str(_first_value(row, "market_id", "instrument_id", "ticker", "symbol") or "").strip()
        if not market_id:
            continue
        realized = _parse_float(_first_value(row, "realized_pnl", "pnl"), default=0.0)
        pnls[market_id] = pnls.get(market_id, 0.0) + realized

    return pnls


def _normalize_market_prices(
    market_prices: Mapping[str, Sequence[tuple[Any, float]]] | None,
) -> dict[str, list[tuple[datetime, float]]]:
    if not market_prices:
        return {}

    normalized: dict[str, list[tuple[datetime, float]]] = {}
    for market_id, points in market_prices.items():
        values: list[tuple[datetime, float]] = []
        for ts_like, price_like in points:
            timestamp = _to_naive_utc(ts_like)
            if timestamp is None:
                continue
            values.append((timestamp, float(price_like)))

        if not values:
            continue

        # Keep the latest value for duplicate timestamps.
        frame = pd.DataFrame(values, columns=["ts", "price"]).sort_values("ts")
        frame = frame.drop_duplicates(subset=["ts"], keep="last")
        normalized[str(market_id)] = []
        for row in frame.itertuples(index=False):
            timestamp = _to_naive_utc(row.ts)
            if timestamp is None:
                continue
            normalized[str(market_id)].append((timestamp, float(row.price)))

    return normalized


def _market_prices_from_fills(fills: list[Any]) -> dict[str, list[tuple[datetime, float]]]:
    market_prices: dict[str, list[tuple[datetime, float]]] = {}
    for fill in fills:
        market_prices.setdefault(str(fill.market_id), []).append((fill.timestamp, float(fill.price)))
    return market_prices


def _merge_market_price_sources(
    primary: Mapping[str, Sequence[tuple[Any, float]]] | None,
    secondary: Mapping[str, Sequence[tuple[Any, float]]] | None,
) -> dict[str, list[tuple[datetime, float]]]:
    """
    Merge two market-price maps and normalize timestamps/prices.

    When fills are overlaid on the YES-price panel, this keeps fill timestamps
    present in the plotted market series so marker x/y coordinates align with
    the rendered line.
    """
    merged: dict[str, list[tuple[Any, float]]] = {}

    for source in (primary, secondary):
        if not source:
            continue
        for market_id, points in source.items():
            if not points:
                continue
            merged.setdefault(str(market_id), []).extend(points)

    return _normalize_market_prices(merged)


def _market_prices_with_fill_points(
    market_prices: Mapping[str, Sequence[tuple[Any, float]]] | None, fills: list[Any]
) -> dict[str, list[tuple[datetime, float]]]:
    normalized_market_prices = _normalize_market_prices(market_prices)
    fill_prices = _market_prices_from_fills(fills)
    if not normalized_market_prices:
        return fill_prices

    return _merge_market_price_sources(normalized_market_prices, fill_prices)


def _build_metrics(snapshots: list[Any], initial_cash: float) -> dict[str, float]:
    if not snapshots:
        return {}

    equity = pd.Series([float(snapshot.total_equity) for snapshot in snapshots])
    running_max = equity.cummax().replace(0, pd.NA)
    drawdown = (running_max - equity) / running_max

    final_equity = float(equity.iloc[-1])
    total_return = (final_equity - initial_cash) / initial_cash if initial_cash else 0.0

    return {
        "final_equity": final_equity,
        "total_return": total_return,
        "max_drawdown": float(drawdown.max(skipna=True) or 0.0),
    }


def _platform_enum(models_module: Any, platform: str) -> Any:
    platform_lower = platform.lower()
    if "poly" in platform_lower:
        return models_module.Platform.POLYMARKET
    return models_module.Platform.KALSHI


def _append_chart_panel(layout: Any, panel: Any) -> Any:
    try:
        from bokeh.layouts import column
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise ImportError("Bokeh is required for legacy chart rendering.") from exc

    if hasattr(layout, "children"):
        layout.children.append(panel)
        return layout

    return column(layout, panel, sizing_mode="stretch_width")


def _mark_panel_figure(fig: Any, panel_id: str) -> Any:
    fig.name = panel_id
    tags = list(getattr(fig, "tags", []))
    tags.append(f"panel:{panel_id}")
    fig.tags = tags
    return fig


def _brier_unavailable_reason(
    *, user_probabilities: pd.Series | None, market_probabilities: pd.Series | None, outcomes: pd.Series | None
) -> str | None:
    has_probability_inputs = any(
        series is not None and not series.empty for series in (user_probabilities, market_probabilities)
    )
    if not has_probability_inputs:
        return None

    if outcomes is None or outcomes.empty:
        return "Unavailable until the market resolves."

    return "Unavailable for the selected probability window."


def _build_brier_placeholder_panel(message: str) -> Any:
    try:
        from bokeh.models import Label
        from bokeh.models import Span
        from bokeh.plotting import figure
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise ImportError("Bokeh is required for legacy chart rendering.") from exc

    fig = figure(
        title=None,
        x_axis_type="datetime",
        height=220,
        tools="save",
        sizing_mode="stretch_width",
        toolbar_location="right",
    )
    fig.add_layout(Span(location=0, dimension="width", line_color="#666666", line_dash="dashed", line_width=1))
    fig.add_layout(
        Label(
            x=20, y=105, x_units="screen", y_units="screen", text=message, text_font_size="11pt", text_color="#666666"
        )
    )
    fig.xaxis.axis_label = "Date"
    fig.yaxis.axis_label = "Cumulative Brier Advantage"

    return _mark_panel_figure(fig, PANEL_BRIER_ADVANTAGE)


def _style_panel_legend(fig: Any) -> None:
    for legend in getattr(fig, "legend", []):
        legend.location = "top_left"
        legend.orientation = "horizontal"
        legend.border_line_alpha = 0
        legend.padding = 5
        legend.spacing = 0
        legend.margin = 0
        legend.label_text_font_size = "8pt"
        legend.click_policy = "hide"


def _build_brier_panel(brier_frame: pd.DataFrame) -> Any | None:
    if brier_frame.empty:
        return None

    try:
        from bokeh.models import ColumnDataSource
        from bokeh.models import HoverTool
        from bokeh.models import NumeralTickFormatter
        from bokeh.models import Range1d
        from bokeh.models import Span
        from bokeh.models import WheelZoomTool
        from bokeh.plotting import figure
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise ImportError("Bokeh is required for legacy chart rendering.") from exc

    frame = brier_frame.copy()
    frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce")
    frame = frame[~frame.index.isna()].sort_index()
    if frame.empty:
        return None

    frame.index = frame.index.tz_convert("UTC").tz_localize(None)
    frame = frame.drop_duplicates(keep="last")
    if len(frame.index) > 1:
        pad = (frame.index[-1] - frame.index[0]) / 20
        x_range: Any = Range1d(frame.index[0], frame.index[-1], bounds=(frame.index[0] - pad, frame.index[-1] + pad))
    else:
        x_range = None

    source = ColumnDataSource(
        {
            "datetime": frame.index,
            "cumulative_brier_advantage": frame["cumulative_brier_advantage"].to_numpy(),
            "brier_advantage": frame["brier_advantage"].to_numpy(),
        }
    )

    fig = figure(
        title=None,
        x_axis_type="datetime",
        height=220,
        tools="xpan,xwheel_zoom,box_zoom,undo,redo,reset,save",
        active_drag="xpan",
        active_scroll="xwheel_zoom",
        sizing_mode="stretch_width",
        toolbar_location="right",
        x_range=x_range,
    )

    fig.line(
        x="datetime",
        y="cumulative_brier_advantage",
        source=source,
        line_width=2.0,
        line_color="#2ca0f0",
        legend_label="Cum. Brier Advantage",
    )
    fig.add_layout(Span(location=0, dimension="width", line_color="#666666", line_dash="dashed", line_width=1))

    fig.add_tools(
        HoverTool(
            mode="vline",
            formatters={"@datetime": "datetime"},
            tooltips=[
                ("Date", "@datetime{%F %T}"),
                ("Cum Advantage", "@cumulative_brier_advantage{0.0000}"),
                ("Point Advantage", "@brier_advantage{0.0000}"),
            ],
        )
    )

    fig.xaxis.axis_label = "Date"
    fig.yaxis.axis_label = "Cumulative Brier Advantage"
    fig.yaxis.formatter = NumeralTickFormatter(format="0.0000")
    _style_panel_legend(fig)
    wheel_zoom = next((tool for tool in fig.tools if isinstance(tool, WheelZoomTool)), None)
    if wheel_zoom is not None:
        wheel_zoom.maintain_focus = False  # type: ignore[attr-defined]

    return _mark_panel_figure(fig, PANEL_BRIER_ADVANTAGE)


def _append_brier_placeholder_panel(layout: Any, message: str) -> Any:
    return _append_chart_panel(layout, _build_brier_placeholder_panel(message))


def _append_brier_panel(layout: Any, brier_frame: pd.DataFrame) -> Any:
    panel = _build_brier_panel(brier_frame)
    if panel is None:
        return layout
    return _append_chart_panel(layout, panel)


def _iter_layout_nodes(node: Any):
    yield node
    children = getattr(node, "children", None)
    if children is None:
        return

    for child in children:
        obj = child[0] if isinstance(child, tuple) else child
        if obj is not None:
            yield from _iter_layout_nodes(obj)


def _iter_figures(layout: Any):
    for node in _iter_layout_nodes(layout):
        if hasattr(node, "renderers") and hasattr(node, "title") and hasattr(node, "yaxis"):
            yield node


def _field_name(spec: Any) -> str | None:
    if isinstance(spec, str):
        return spec
    if isinstance(spec, dict):
        field = spec.get("field")
        return str(field) if field is not None else None

    field = getattr(spec, "field", None)
    return str(field) if field is not None else None


def _filter_tool_container(container: Any, tools_to_remove: set[Any]) -> None:
    if container is None or not tools_to_remove:
        return

    tools = getattr(container, "tools", None)
    if tools is None:
        return

    filtered_tools: list[Any] = []
    changed = False

    for tool in list(tools):
        proxy_tools = getattr(tool, "tools", None)
        if proxy_tools is not None:
            remaining_proxy_tools = [
                proxy_tool for proxy_tool in list(proxy_tools) if proxy_tool not in tools_to_remove
            ]
            if len(remaining_proxy_tools) != len(proxy_tools):
                tool.tools = remaining_proxy_tools
                changed = True
            if not remaining_proxy_tools:
                changed = True
                continue

        if tool in tools_to_remove:
            changed = True
            continue

        filtered_tools.append(tool)

    if changed:
        container.tools = filtered_tools


def _remove_tools_from_layout(layout: Any, tools_to_remove: set[Any]) -> None:
    if not tools_to_remove:
        return

    for node in _iter_layout_nodes(layout):
        _filter_tool_container(node, tools_to_remove)
        _filter_tool_container(getattr(node, "toolbar", None), tools_to_remove)


def _remove_hover_tools(fig: Any, *, layout: Any | None = None) -> set[Any]:
    removed = {tool for tool in getattr(fig, "tools", []) if tool.__class__.__name__ == "HoverTool"}
    if not removed:
        return set()

    fig.tools = [tool for tool in getattr(fig, "tools", []) if tool not in removed]
    _filter_tool_container(getattr(fig, "toolbar", None), removed)
    if layout is not None:
        _remove_tools_from_layout(layout, removed)
    return removed


def _format_period_label(start: Any, end: Any) -> str:
    start_dt = _to_naive_utc(start)
    end_dt = _to_naive_utc(end)
    if start_dt is None or end_dt is None:
        return ""

    if start_dt.date() == end_dt.date():
        return start_dt.strftime("%b %d, %Y")
    if start_dt.year == end_dt.year and start_dt.month == end_dt.month:
        return f"{start_dt.strftime('%b %d')} - {end_dt.strftime('%d, %Y')}"
    return f"{start_dt.strftime('%b %d, %Y')} - {end_dt.strftime('%b %d, %Y')}"


def _find_figure_with_yaxis_label(layout: Any, predicate: Any) -> Any | None:
    for fig in _iter_figures(layout):
        labels = [str(axis.axis_label or "") for axis in getattr(fig, "yaxis", [])]
        if any(predicate(label) for label in labels):
            return fig
    return None


def _periodic_pnl_panel_source(target: Any) -> tuple[dict[str, Any] | None, float | None]:
    source_data: dict[str, Any] | None = None
    bar_width: float | None = None

    for renderer in getattr(target, "renderers", []):
        source = getattr(renderer, "data_source", None)
        data = getattr(source, "data", None)
        if isinstance(data, dict) and {"x", "pnl", "dt_start", "dt_end"}.issubset(data):
            source_data = data

        glyph = getattr(renderer, "glyph", None)
        width = getattr(glyph, "width", None)
        if isinstance(width, int | float):
            bar_width = float(width)

    return source_data, bar_width


def _build_periodic_pnl_panel_source_data(source_data: dict[str, Any]) -> dict[str, Any] | None:
    x_values = np.asarray(source_data["x"], dtype=float)
    pnl_values = np.asarray(source_data["pnl"], dtype=float)
    dt_start = [_to_naive_utc(value) for value in source_data["dt_start"]]
    dt_end = [_to_naive_utc(value) for value in source_data["dt_end"]]
    if not len(x_values) or len(x_values) != len(pnl_values):
        return None

    return {
        "x": x_values,
        "pnl": pnl_values,
        "dt_start": dt_start,
        "dt_end": dt_end,
        "period_label": [_format_period_label(start, end) for start, end in zip(dt_start, dt_end, strict=False)],
        "color": np.where(pnl_values >= 0.0, "#2ecc71", "#e74c3c"),
    }


def _resolve_periodic_pnl_bar_width(x_values: np.ndarray, bar_width: float | None) -> float:
    if bar_width is not None:
        return bar_width

    diffs = np.diff(np.sort(x_values))
    return max(1.0, float(np.median(diffs)) * 0.8) if len(diffs) else 1.0


def _yes_price_line_renderers(target: Any) -> list[Any]:
    renderers: list[Any] = []
    for renderer in getattr(target, "renderers", []):
        glyph = getattr(renderer, "glyph", None)
        if glyph is None or glyph.__class__.__name__ != "Line":
            continue

        source = getattr(renderer, "data_source", None)
        data = getattr(source, "data", None)
        if not isinstance(data, dict) or "datetime" not in data:
            continue

        y_field = _field_name(getattr(glyph, "y", None))
        if not y_field or not y_field.startswith("price_"):
            continue

        renderer.name = y_field
        renderers.append(renderer)

    return renderers


def _remove_data_banner(layout: Any) -> Any:
    for node in _iter_layout_nodes(layout):
        children = getattr(node, "children", None)
        if not children:
            continue

        filtered_children: list[Any] = []
        for child in children:
            obj = child[0] if isinstance(child, tuple) else child
            text = getattr(obj, "text", "")
            if isinstance(text, str) and "<b>Data:</b>" in text:
                continue
            filtered_children.append(child)

        if len(filtered_children) != len(children):
            node.children = filtered_children
    return layout


def _legend_item_label_text(item: Any) -> str:
    label = getattr(item, "label", None)
    if isinstance(label, dict):
        return str(label.get("value", ""))
    value = getattr(label, "value", None)
    if value is not None:
        return str(value)
    return str(label)


def _set_legend_item_label_text(item: Any, text: str) -> None:
    item.label = {"value": text}


def _remove_yes_price_profitability_legend_items(fig: Any) -> set[Any]:
    renderers_to_drop: set[Any] = set()

    for legend in getattr(fig, "legend", []):
        kept_items = []
        for item in list(getattr(legend, "items", [])):
            lower = _legend_item_label_text(item).lower()
            if "profitable" in lower or "losing" in lower:
                for renderer in getattr(item, "renderers", []):
                    renderers_to_drop.add(renderer)
                continue
            kept_items.append(item)
        legend.items = kept_items

    return renderers_to_drop


def _remove_yes_price_profitability_connectors(layout: Any) -> None:
    """
    Remove profitable/losing connector overlays from the YES price panel.
    """
    yes_fig = None
    for fig in _iter_figures(layout):
        labels = [str(axis.axis_label or "") for axis in getattr(fig, "yaxis", [])]
        if any(label == "YES Price" for label in labels):
            yes_fig = fig
            break

    if yes_fig is None:
        return

    renderers_to_drop = _remove_yes_price_profitability_legend_items(yes_fig)

    # Drop any unlabeled multiline overlays as a safety net.
    for renderer in getattr(yes_fig, "renderers", []):
        glyph = getattr(renderer, "glyph", None)
        if glyph is not None and glyph.__class__.__name__ == "MultiLine":
            renderers_to_drop.add(renderer)

    if renderers_to_drop:
        yes_fig.renderers = [r for r in yes_fig.renderers if r not in renderers_to_drop]


def _remove_yes_price_fill_markers(layout: Any) -> None:  # noqa: C901
    """
    Remove synthetic fill marker overlays from the YES price panel.
    """
    yes_fig = None
    for fig in _iter_figures(layout):
        labels = [str(axis.axis_label or "") for axis in getattr(fig, "yaxis", [])]
        if any(label == "YES Price" for label in labels):
            yes_fig = fig
            break

    if yes_fig is None:
        return

    renderers_to_drop: set[Any] = set()

    for renderer in getattr(yes_fig, "renderers", []):
        glyph = getattr(renderer, "glyph", None)
        source = getattr(renderer, "data_source", None)
        data = getattr(source, "data", None)
        if glyph is None or not isinstance(data, dict):
            continue
        if glyph.__class__.__name__ not in {"Scatter", "Circle"}:
            continue
        keys = set(data.keys())
        if {"action", "side", "quantity", "price", "market_id"}.issubset(keys):
            renderers_to_drop.add(renderer)

    if renderers_to_drop:
        yes_fig.renderers = [r for r in yes_fig.renderers if r not in renderers_to_drop]
        for legend in getattr(yes_fig, "legend", []):
            kept_items = []
            for item in list(getattr(legend, "items", [])):
                lower = _legend_item_label_text(item).lower()
                item_renderers = [r for r in getattr(item, "renderers", []) if r not in renderers_to_drop]
                if "fills" in lower or not item_renderers:
                    continue
                item.renderers = item_renderers
                kept_items.append(item)
            legend.items = kept_items
        for tool in getattr(yes_fig, "tools", []):
            renderers = getattr(tool, "renderers", None)
            if isinstance(renderers, list | tuple):
                tool.renderers = [r for r in renderers if r not in renderers_to_drop]


def _is_yes_price_fill_renderer(renderer: Any) -> bool:
    glyph = getattr(renderer, "glyph", None)
    source = getattr(renderer, "data_source", None)
    data = getattr(source, "data", None)
    if glyph is None or not isinstance(data, dict):
        return False
    if glyph.__class__.__name__ not in {"Scatter", "Circle"}:
        return False
    keys = set(data.keys())
    return {"action", "side", "quantity", "price", "market_id"}.issubset(keys)


def _limit_yes_price_fill_markers(layout: Any, *, max_markers: int) -> None:  # noqa: C901
    """
    Keep a readable sample of YES-price fill markers instead of dropping them.
    """
    if max_markers <= 0:
        _remove_yes_price_fill_markers(layout)
        return

    yes_fig = _find_figure_with_yaxis_label(layout, lambda label: label == "YES Price")
    if yes_fig is None:
        return

    for renderer in getattr(yes_fig, "renderers", []):
        if not _is_yes_price_fill_renderer(renderer):
            continue

        source = getattr(renderer, "data_source", None)
        data = getattr(source, "data", None)
        if source is None or not isinstance(data, dict):
            continue

        frame = pd.DataFrame({key: list(value) for key, value in data.items()})
        total = len(frame)
        if total <= max_markers:
            continue

        limited = _downsample_frame_rows(frame, max_points=max_markers, extrema_columns=("price", "index"))
        source.data = {column: limited[column].tolist() for column in frame.columns}

        shown = len(limited)
        for legend in getattr(yes_fig, "legend", []):
            for item in list(getattr(legend, "items", [])):
                if renderer not in getattr(item, "renderers", []):
                    continue
                if "fills" not in _legend_item_label_text(item).lower():
                    continue
                _set_legend_item_label_text(item, f"Fills ({shown:,} shown of {total:,})")


def _standardize_periodic_pnl_panel(layout: Any) -> None:
    try:
        from bokeh.models import ColumnDataSource
        from bokeh.models import HoverTool
        from bokeh.models import NumeralTickFormatter
    except ImportError:
        return

    target = _find_figure_with_yaxis_label(layout, lambda label: "periodic" in label.lower())
    if target is None:
        return

    source_data, bar_width = _periodic_pnl_panel_source(target)
    if source_data is None:
        return

    panel_data = _build_periodic_pnl_panel_source_data(source_data)
    if panel_data is None:
        return

    panel_source = ColumnDataSource(panel_data)
    bar_width = _resolve_periodic_pnl_bar_width(panel_data["x"], bar_width)

    target.renderers = [renderer for renderer in target.renderers if not hasattr(renderer, "data_source")]
    for legend in getattr(target, "legend", []):
        legend.items = []
        legend.visible = False
    _remove_hover_tools(target, layout=layout)

    renderer = target.vbar(x="x", top="pnl", source=panel_source, width=bar_width, color="color", alpha=0.75)
    target.add_tools(
        HoverTool(
            renderers=[renderer],
            formatters={"@dt_start": "datetime", "@dt_end": "datetime"},
            tooltips=[
                ("Period", "@period_label"),
                ("Start", "@dt_start{%F %T}"),
                ("End", "@dt_end{%F %T}"),
                ("P&L", "@pnl{$0,0.00}"),
            ],
            mode="vline",
        )
    )
    target.yaxis.formatter = NumeralTickFormatter(format="$ 0,0")


def _relabel_market_pnl_panel(layout: Any, axis_label: str = "Market P&L") -> None:
    try:
        from bokeh.models import HoverTool
    except ImportError:
        return

    target = _find_figure_with_yaxis_label(layout, lambda label: label == "Profit / Loss")
    if target is None:
        return

    if getattr(target, "yaxis", None):
        target.yaxis[0].axis_label = axis_label

    for tool in getattr(target, "tools", []):
        if not isinstance(tool, HoverTool):
            continue
        if not tool.tooltips:
            continue
        updated = []
        for label, value in tool.tooltips:
            if label == "P/L":
                updated.append(("Final Market P&L", value))
            else:
                updated.append((label, value))
        tool.tooltips = updated


def _relabel_periodic_pnl_panel(layout: Any, axis_label: str) -> None:
    target = _find_figure_with_yaxis_label(layout, lambda label: "periodic" in label.lower())
    if target is None:
        return

    if getattr(target, "yaxis", None):
        target.yaxis[0].axis_label = axis_label


def _remove_panels_by_yaxis_labels(layout: Any, labels: set[str]) -> None:
    if not labels:
        return

    def _should_keep(obj: Any) -> bool:
        axes = getattr(obj, "yaxis", None)
        if not axes:
            return True
        axis_labels = {str(axis.axis_label or "") for axis in axes}
        return axis_labels.isdisjoint(labels)

    def _prune(node: Any) -> None:
        children = getattr(node, "children", None)
        if not children:
            return

        filtered_children: list[Any] = []
        for child in children:
            obj = child[0] if isinstance(child, tuple) else child
            if obj is not None and not _should_keep(obj):
                continue
            if obj is not None:
                _prune(obj)
            filtered_children.append(child)
        node.children = filtered_children

    _prune(layout)


def _extract_yes_price_colors(layout: Any) -> dict[str, Any]:
    target = _find_figure_with_yaxis_label(layout, lambda label: label == "YES Price")
    if target is None:
        return {}

    color_by_market: dict[str, Any] = {}
    for renderer in _yes_price_line_renderers(target):
        glyph = getattr(renderer, "glyph", None)
        if glyph is None:
            continue
        y_field = _field_name(getattr(glyph, "y", None))
        if not y_field or not y_field.startswith("price_"):
            continue
        color = getattr(glyph, "line_color", None)
        if color is None:
            continue
        color_by_market[y_field.removeprefix("price_")] = color

    return color_by_market


def _downsample_frame_rows(
    frame: pd.DataFrame, *, max_points: int, extrema_columns: Sequence[str] = ()
) -> pd.DataFrame:
    if frame.empty or len(frame) <= max_points:
        return frame

    keep: set[int] = {0, len(frame) - 1}
    for column in extrema_columns:
        if column not in frame:
            continue
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=float)
        if len(values) == 0 or np.isnan(values).all():
            continue
        keep.add(int(np.nanargmax(values)))
        keep.add(int(np.nanargmin(values)))

    budget = max(0, int(max_points) - len(keep))
    if budget > 0:
        keep.update(np.linspace(0, len(frame) - 1, num=budget, dtype=int).tolist())

    return frame.iloc[sorted(keep)].copy()


def _build_multi_market_brier_panel(  # noqa: C901
    brier_frames: Mapping[str, pd.DataFrame],
    *,
    axis_label: str = "Cumulative Brier Advantage",
    color_by_market: Mapping[str, Any] | None = None,
    max_points_per_market: int = 400,
) -> Any | None:
    valid_frames = {
        market_id: frame.copy() for market_id, frame in brier_frames.items() if frame is not None and not frame.empty
    }
    if not valid_frames:
        return None

    try:
        from bokeh.models import ColumnDataSource
        from bokeh.models import HoverTool
        from bokeh.models import NumeralTickFormatter
        from bokeh.models import Range1d
        from bokeh.models import Span
        from bokeh.models import WheelZoomTool
        from bokeh.palettes import Category10
        from bokeh.plotting import figure
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise ImportError("Bokeh is required for legacy chart rendering.") from exc

    datetime_bounds: list[pd.Timestamp] = []
    for frame in valid_frames.values():
        index = pd.to_datetime(frame.index, utc=True, errors="coerce")
        index = index[~index.isna()]
        if len(index) == 0:
            continue
        datetime_bounds.extend([index.min(), index.max()])

    if len(datetime_bounds) >= 2:
        start = min(datetime_bounds).tz_convert("UTC").tz_localize(None)
        end = max(datetime_bounds).tz_convert("UTC").tz_localize(None)
        pad = (end - start) / 20 if end > start else pd.Timedelta(hours=1)
        x_range: Any = Range1d(start, end, bounds=(start - pad, end + pad))
    else:
        x_range = None

    color_cycle = iter(Category10[10])
    fig = figure(
        title=None,
        x_axis_type="datetime",
        height=260,
        tools="xpan,xwheel_zoom,box_zoom,undo,redo,reset,save",
        active_drag="xpan",
        active_scroll="xwheel_zoom",
        sizing_mode="stretch_width",
        toolbar_location="right",
        x_range=x_range,
    )

    renderers: list[Any] = []
    for market_id, frame in valid_frames.items():
        frame.index = pd.to_datetime(frame.index, utc=True, errors="coerce")
        frame = frame[~frame.index.isna()].sort_index()
        if frame.empty:
            continue

        frame.index = frame.index.tz_convert("UTC").tz_localize(None)
        frame = frame[~frame.index.duplicated(keep="last")]
        frame = _downsample_frame_rows(
            frame, max_points=max_points_per_market, extrema_columns=("cumulative_brier_advantage", "brier_advantage")
        )

        source = ColumnDataSource(
            {
                "datetime": frame.index,
                "cumulative_brier_advantage": frame["cumulative_brier_advantage"].to_numpy(dtype=float),
                "brier_advantage": frame["brier_advantage"].to_numpy(dtype=float),
            }
        )
        line_color = None
        if color_by_market is not None:
            line_color = color_by_market.get(market_id)
        if line_color is None:
            try:
                line_color = next(color_cycle)
            except StopIteration:
                color_cycle = iter(Category10[10])
                line_color = next(color_cycle)

        renderer = fig.line(
            x="datetime",
            y="cumulative_brier_advantage",
            source=source,
            line_width=2.0,
            line_color=line_color,
            legend_label=market_id,
            muted_alpha=0.15,
        )
        renderer.name = market_id
        renderers.append(renderer)

    if not renderers:
        return None

    fig.add_layout(Span(location=0, dimension="width", line_color="#666666", line_dash="dashed", line_width=1))
    fig.add_tools(
        HoverTool(
            renderers=renderers,
            mode="vline",
            formatters={"@datetime": "datetime"},
            tooltips=[
                ("Market", "$name"),
                ("Date", "@datetime{%F %T}"),
                ("Cum Advantage", "@cumulative_brier_advantage{0.0000}"),
                ("Point Advantage", "@brier_advantage{0.0000}"),
            ],
        )
    )
    fig.xaxis.axis_label = "Date"
    fig.yaxis.axis_label = axis_label
    fig.yaxis.formatter = NumeralTickFormatter(format="0.0000")
    _style_panel_legend(fig)
    wheel_zoom = next((tool for tool in fig.tools if isinstance(tool, WheelZoomTool)), None)
    if wheel_zoom is not None:
        wheel_zoom.maintain_focus = False  # type: ignore[attr-defined]

    return _mark_panel_figure(fig, PANEL_BRIER_ADVANTAGE)


def _append_multi_market_brier_panel(
    layout: Any,
    brier_frames: Mapping[str, pd.DataFrame],
    *,
    axis_label: str = "Cumulative Brier Advantage",
    color_by_market: Mapping[str, Any] | None = None,
    max_points_per_market: int = 400,
) -> Any:
    panel = _build_multi_market_brier_panel(
        brier_frames,
        axis_label=axis_label,
        color_by_market=color_by_market,
        max_points_per_market=max_points_per_market,
    )
    if panel is None:
        return layout
    return _append_chart_panel(layout, panel)


def _standardize_yes_price_hover(layout: Any) -> None:
    try:
        from bokeh.models import HoverTool
    except ImportError:
        return

    target = _find_figure_with_yaxis_label(layout, lambda label: label == "YES Price")
    if target is None:
        return

    line_renderers = _yes_price_line_renderers(target)

    if not line_renderers:
        return

    if len(line_renderers) == 1:
        yes_price_tooltip = f"@{{{line_renderers[0].name}}}{{0.[00]%}}"
    else:
        yes_price_tooltip = "@$name{0.[00]%}"

    _remove_hover_tools(target, layout=layout)
    target.add_tools(
        HoverTool(
            renderers=line_renderers,
            formatters={"@datetime": "datetime"},
            tooltips=[("Date", "@datetime{%F %T}"), ("YES Price", yes_price_tooltip)],
            mode="vline",
        )
    )


def _focus_allocation_panel(layout: Any) -> None:
    try:
        from bokeh.models import Range1d
    except ImportError:
        return

    for fig in _iter_figures(layout):
        labels = [axis.axis_label for axis in getattr(fig, "yaxis", [])]
        if "Allocation" not in labels:
            continue

        glyph_renderers = [r for r in fig.renderers if hasattr(r, "data_source")]
        if not glyph_renderers:
            continue

        source = glyph_renderers[0].data_source
        data = getattr(source, "data", {})
        alloc_cols = [k for k in data if str(k).startswith("alloc_")]
        non_cash = [k for k in alloc_cols if "Cash" not in str(k)]
        if not non_cash:
            continue

        stacked = np.zeros(len(data[non_cash[0]]), dtype=float)
        for col in non_cash:
            stacked += np.nan_to_num(np.asarray(data[col], dtype=float))

        peak = float(np.nanmax(stacked)) if len(stacked) else 0.0
        upper = min(1.0, max(0.05, peak * 1.3))
        fig.y_range = Range1d(0.0, upper)
        fig.yaxis[0].axis_label = "Market Allocation (ex-cash)"

        # Hide the grey cash stack so market allocation is visible.
        if glyph_renderers:
            glyph_renderers[-1].visible = False
        break


def _apply_layout_overrides(
    layout: Any,
    initial_cash: float,
    *,
    hide_yes_price_fill_markers: bool = False,
    max_yes_price_fill_markers: int | None = None,
    relabel_market_pnl: bool = False,
) -> Any:
    _ = initial_cash  # Keep signature stable for callers and future layout transforms.
    layout = _remove_data_banner(layout)
    _focus_allocation_panel(layout)
    _remove_yes_price_profitability_connectors(layout)
    if hide_yes_price_fill_markers:
        _remove_yes_price_fill_markers(layout)
    elif max_yes_price_fill_markers is not None:
        _limit_yes_price_fill_markers(layout, max_markers=max_yes_price_fill_markers)
    _standardize_periodic_pnl_panel(layout)
    if relabel_market_pnl:
        _relabel_market_pnl_panel(layout)
    _standardize_yes_price_hover(layout)
    return layout


def _save_layout(layout: Any, output_path: Path, title: str) -> None:
    """
    Persist the final Bokeh layout after all adapter-level cleanup.
    """
    try:
        from bokeh.io import output_file
        from bokeh.io import save
    except ImportError as exc:  # pragma: no cover - runtime dependency
        raise ImportError("Bokeh is required for legacy chart rendering.") from exc

    output_file(str(output_path), title=title)
    save(layout, filename=str(output_path), title=title)


def save_legacy_backtest_layout(layout: Any, output_path: str | Path, title: str) -> str:
    """
    Save a pre-built legacy chart layout and return the absolute output path.
    """
    output_abs = Path(output_path).expanduser().resolve()
    output_abs.parent.mkdir(parents=True, exist_ok=True)
    _save_layout(layout, output_abs, title)
    return str(output_abs)


def build_legacy_backtest_layout(
    engine: Any,
    output_path: str | Path,
    strategy_name: str,
    platform: str,
    initial_cash: float,
    market_prices: Mapping[str, Sequence[tuple[Any, float]]] | None = None,
    user_probabilities: pd.Series | None = None,
    market_probabilities: pd.Series | None = None,
    outcomes: pd.Series | None = None,
    legacy_repo_path: str | Path | None = None,
    open_browser: bool = False,
    max_markets: int = 30,
    progress: bool = False,
    adaptive_downsampling: bool = True,
    max_downsample_points: int = 5000,
    plot_panels: Sequence[str] | None = None,
) -> tuple[Any, str]:
    """
    Build the final legacy-style Bokeh layout for a Nautilus backtest engine.
    """
    _ = legacy_repo_path  # Legacy code is vendored in-repo.
    models_module, plotting_module = _load_legacy_modules()
    _configure_legacy_downsampling(plotting_module, adaptive=adaptive_downsampling, max_points=max_downsample_points)

    account_report = _extract_account_report(engine)
    fills_report = engine.trader.generate_order_fills_report()

    fills = _convert_fills(fills_report, models_module)
    sparse_snapshots = _build_portfolio_snapshots(models_module, account_report, fills)
    normalized_market_prices = _market_prices_with_fill_points(market_prices, fills)

    snapshots = _build_dense_portfolio_snapshots(
        models_module=models_module,
        sparse_snapshots=sparse_snapshots,
        fills=fills,
        market_prices=normalized_market_prices,
        initial_cash=float(initial_cash),
    )
    if not snapshots:
        raise ValueError("No portfolio snapshots were built from the account report.")

    metrics = _build_metrics(snapshots, initial_cash)
    resolved_plot_panels = normalize_plot_panels(plot_panels, default=DEFAULT_DETAIL_PLOT_PANELS)

    result = models_module.BacktestResult(
        equity_curve=snapshots,
        fills=fills,
        metrics=metrics,
        strategy_name=strategy_name,
        platform=_platform_enum(models_module, platform),
        start_time=snapshots[0].timestamp,
        end_time=snapshots[-1].timestamp,
        initial_cash=float(initial_cash),
        final_equity=float(snapshots[-1].total_equity),
        num_markets_traded=len({fill.market_id for fill in fills}),
        num_markets_resolved=0,
        market_prices=normalized_market_prices,
        # Leave this empty so the legacy P/L panel falls back to per-fill
        # markers instead of one terminal point per market.
        market_pnls={},
        plot_panels=resolved_plot_panels,
    )

    output_abs = Path(output_path).expanduser().resolve()
    chart_title = f"{strategy_name} legacy chart"

    extra_panels: dict[str, Any] = {}
    if PANEL_BRIER_ADVANTAGE in resolved_plot_panels:
        brier_frame = prepare_cumulative_brier_advantage(
            user_probabilities=user_probabilities, market_probabilities=market_probabilities, outcomes=outcomes
        )
        if not brier_frame.empty:
            panel = _build_brier_panel(brier_frame)
            if panel is not None:
                extra_panels[PANEL_BRIER_ADVANTAGE] = panel
        else:
            unavailable_reason = _brier_unavailable_reason(
                user_probabilities=user_probabilities, market_probabilities=market_probabilities, outcomes=outcomes
            )
            if unavailable_reason is not None:
                extra_panels[PANEL_BRIER_ADVANTAGE] = _build_brier_placeholder_panel(unavailable_reason)

    layout = plotting_module.plot(
        result,
        filename=str(output_abs),
        max_markets=max_markets,
        open_browser=open_browser,
        progress=progress,
        plot_panels=resolved_plot_panels,
        extra_panels=extra_panels,
    )
    layout = _apply_layout_overrides(
        layout,
        initial_cash=float(initial_cash),
        max_yes_price_fill_markers=_yes_price_fill_marker_limit(
            fill_count=len(fills), max_points=max_downsample_points
        ),
    )

    return layout, chart_title


def create_legacy_backtest_chart(
    engine: Any,
    output_path: str | Path,
    strategy_name: str,
    platform: str,
    initial_cash: float,
    market_prices: Mapping[str, Sequence[tuple[Any, float]]] | None = None,
    user_probabilities: pd.Series | None = None,
    market_probabilities: pd.Series | None = None,
    outcomes: pd.Series | None = None,
    legacy_repo_path: str | Path | None = None,
    open_browser: bool = False,
    max_markets: int = 30,
    progress: bool = False,
    adaptive_downsampling: bool = True,
    max_downsample_points: int = 5000,
    plot_panels: Sequence[str] | None = None,
) -> str:
    """
    Render a legacy-style interactive chart from a Nautilus backtest engine.
    """
    layout, chart_title = build_legacy_backtest_layout(
        engine=engine,
        output_path=output_path,
        strategy_name=strategy_name,
        platform=platform,
        initial_cash=initial_cash,
        market_prices=market_prices,
        user_probabilities=user_probabilities,
        market_probabilities=market_probabilities,
        outcomes=outcomes,
        legacy_repo_path=legacy_repo_path,
        open_browser=open_browser,
        max_markets=max_markets,
        progress=progress,
        adaptive_downsampling=adaptive_downsampling,
        max_downsample_points=max_downsample_points,
        plot_panels=plot_panels,
    )

    return save_legacy_backtest_layout(layout, output_path, chart_title)
