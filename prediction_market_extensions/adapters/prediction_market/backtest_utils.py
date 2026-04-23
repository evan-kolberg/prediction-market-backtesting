# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software distributed under the
#  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied. See the License for the specific language governing
#  permissions and limitations under the License.
# -------------------------------------------------------------------------------------------------
#  Modified by Evan Kolberg in this repository on 2026-03-11 and 2026-03-15.
#  See the repository NOTICE file for provenance and licensing scope.
#

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
import warnings

import pandas as pd

PricePoint = tuple[object, float]
_DEFAULT_TS_ATTRS = ("ts_event", "ts_init")


def _parse_numeric(value: object, default: float = 0.0) -> float:
    if value is None:
        return default

    if isinstance(value, int | float):
        return float(value)

    text = str(value).replace("_", "").replace("\u2212", "-").strip()
    if not text or text.lower() == "nan":
        return default

    for token in text.split():
        try:
            return float(token)
        except ValueError:
            continue

    return default


def _parse_required_numeric(value: object) -> float | None:
    if value is None:
        return None
    parsed = _parse_numeric(value, default=float("nan"))
    return parsed if pd.notna(parsed) else None


def extract_realized_pnl(pos_report: pd.DataFrame) -> float:
    """
    Parse and sum ``realized_pnl`` values from a positions report DataFrame.
    """
    if pos_report.empty:
        return 0.0
    total = 0.0
    for _, row in pos_report.iterrows():
        total += _parse_numeric(row.get("realized_pnl", 0.0), default=0.0)
    return total


def _timestamp_to_naive_utc_datetime(ts: pd.Timestamp) -> datetime:
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    ts = ts.tz_localize(None)
    if ts.nanosecond:
        ts = ts.floor("us")
    return ts.to_pydatetime()


def to_naive_utc(value: object) -> datetime | None:
    """
    Convert a timestamp-like value to a naive UTC ``datetime``.
    """
    if value is None:
        return None

    if isinstance(value, int | float) and abs(float(value)) > 1e12:
        ts = pd.to_datetime(
            int(value),
            unit="ns",
            utc=True,
            errors="coerce",
        )

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


def extract_price_points(
    records: Sequence[object], *, price_attr: str, ts_attrs: tuple[str, ...] = _DEFAULT_TS_ATTRS
) -> list[PricePoint]:
    """
    Extract ``(timestamp, price)`` pairs from Nautilus records.
    """
    points: list[PricePoint] = []
    for record in records:
        ts_raw = None
        for ts_attr in ts_attrs:
            candidate = getattr(record, ts_attr, None)
            if candidate is not None:
                ts_raw = candidate
                break
        if ts_raw is None:
            continue

        if price_attr == "mid_price":
            bid_price = getattr(record, "bid_price", None)
            ask_price = getattr(record, "ask_price", None)
            if bid_price is None or ask_price is None:
                continue
            try:
                price = (float(bid_price) + float(ask_price)) / 2.0
            except (TypeError, ValueError):
                continue
        else:
            try:
                price = float(getattr(record, price_attr))
            except (AttributeError, TypeError, ValueError):
                continue

        points.append((ts_raw, price))

    return points


def downsample_price_points(points: list[PricePoint], max_points: int = 5000) -> list[PricePoint]:
    """Stride-based downsampling that preserves first, last, and price extrema."""
    n = len(points)
    if n <= max_points:
        return points

    prices = [p for _, p in points]
    must_keep = {0, n - 1}
    # Preserve global min/max price indices
    min_idx = 0
    max_idx = 0
    for i, p in enumerate(prices):
        if p < prices[min_idx]:
            min_idx = i
        if p > prices[max_idx]:
            max_idx = i
    must_keep.add(min_idx)
    must_keep.add(max_idx)

    budget = max(100, max_points - len(must_keep))
    stride = max(1, n // budget)
    selected = sorted(must_keep | set(range(0, n, stride)))
    if len(selected) > max_points:
        strided = set(range(0, n, stride))
        remaining = max_points - len(must_keep)
        stride2 = max(1, len(strided) // remaining) if remaining > 0 else n
        selected = sorted(must_keep | set(sorted(strided)[::stride2]))

    return [points[i] for i in selected]


def _probability_frame(points: Sequence[PricePoint]) -> pd.DataFrame:
    rows: list[tuple[pd.Timestamp, float]] = []
    for ts_raw, price in points:
        if isinstance(ts_raw, int | float) and abs(float(ts_raw)) > 1e12:
            ts = pd.to_datetime(
                int(ts_raw),
                unit="ns",
                utc=True,
                errors="coerce",
            )

        else:
            ts = pd.to_datetime(ts_raw, utc=True, errors="coerce")

        if pd.isna(ts):
            continue

        if isinstance(ts, pd.DatetimeIndex):
            if len(ts) == 0:
                continue
            ts = ts[0]
        assert isinstance(ts, pd.Timestamp)

        rows.append((ts, price))

    frame = (
        pd.DataFrame(rows, columns=["ts", "market_probability"])
        .dropna()
        .sort_values("ts")
        .drop_duplicates(subset=["ts"], keep="last")
        .set_index("ts")
    )
    if frame.empty:
        return frame

    invalid_probabilities = frame[
        (frame["market_probability"] < 0.0) | (frame["market_probability"] > 1.0)
    ]
    if not invalid_probabilities.empty:
        invalid_count = int(len(invalid_probabilities))
        first_invalid_ts = invalid_probabilities.index[0]
        warnings.warn(
            "Probability series contained values outside [0.0, 1.0]; clipping to preserve chart "
            "construction. Inspect upstream loader data for corruption. "
            f"Invalid rows={invalid_count}, first_invalid_ts={first_invalid_ts.isoformat()}",
            RuntimeWarning,
            stacklevel=2,
        )
    frame["market_probability"] = frame["market_probability"].clip(0.0, 1.0)
    return frame


def _resolved_outcome_from_result(info: Mapping[object, object], outcome_name: str) -> float | None:
    result = str(info.get("result", "")).strip().casefold()
    if result not in {"yes", "no"}:
        return None

    if outcome_name == "yes":
        return 1.0 if result == "yes" else 0.0
    if outcome_name == "no":
        return 1.0 if result == "no" else 0.0

    return None


def _resolved_outcome_from_numeric_fields(info: Mapping[object, object]) -> float | None:
    for key in ("settlement_value", "expiration_value"):
        raw_value = info.get(key)
        if raw_value in (None, ""):
            continue

        try:
            numeric_value = float(raw_value)
        except (TypeError, ValueError):
            continue

        if numeric_value in {0.0, 1.0}:
            return numeric_value
        if numeric_value == 100.0:
            return numeric_value / 100.0

    return None


def _resolved_outcome_from_tokens(info: Mapping[object, object], outcome_name: str) -> float | None:
    tokens = info.get("tokens")
    if not isinstance(tokens, Sequence) or not outcome_name:
        return None

    for token in tokens:
        if not isinstance(token, Mapping):
            continue
        token_outcome = str(token.get("outcome", "")).strip().casefold()
        if token_outcome != outcome_name:
            continue
        winner = token.get("winner")
        if isinstance(winner, bool):
            return float(winner)

    return None


def infer_realized_outcome(source: object | None) -> float | None:
    """
    Infer a realized binary outcome from instrument metadata when available.
    """
    if source is None:
        return None

    info = getattr(source, "info", source)
    if not isinstance(info, Mapping):
        return None

    # Ambiguous markets (all prices ~0.5) have no meaningful resolved
    # outcome. Returning 0.5 would systematically deflate Brier scores
    # because (p - 0.5)^2 always favors forecasts near 0.5.
    if info.get("is_50_50_outcome") is True:
        return None

    outcome_name = str(getattr(source, "outcome", "")).strip().casefold()
    resolvers = (
        lambda: _resolved_outcome_from_result(info, outcome_name),
        lambda: _resolved_outcome_from_numeric_fields(info),
        lambda: _resolved_outcome_from_tokens(info, outcome_name),
    )
    for resolver in resolvers:
        resolved = resolver()
        if resolved is not None:
            return resolved

    return None


def compute_binary_settlement_pnl(
    fill_events: Sequence[Mapping[object, object]], resolved_outcome: float | None
) -> float | None:
    """
    Compute binary-market PnL by marking any remaining position to settlement.
    """
    if resolved_outcome is None:
        return None
    if not fill_events:
        return None

    cash = 0.0
    open_qty = 0.0
    commissions = 0.0
    contract_side = "yes"
    saw_fill = False

    for event in fill_events:
        action = str(event.get("action") or "").strip().lower()
        price = _parse_required_numeric(event.get("price"))
        quantity = _parse_numeric(event.get("quantity"), default=0.0)
        commission = _parse_numeric(event.get("commission"), default=0.0)
        if quantity <= 0.0 or price is None:
            continue
        event_side = str(event.get("side") or "").strip().casefold()
        if event_side in {"yes", "no"}:
            contract_side = event_side
        saw_fill = True

        commissions += commission
        if action == "buy":
            cash -= price * quantity
            open_qty += quantity
        elif action == "sell":
            cash += price * quantity
            open_qty -= quantity

    if not saw_fill:
        return None
    settlement_value = (
        float(resolved_outcome) if contract_side == "yes" else 1.0 - float(resolved_outcome)
    )
    return cash + (settlement_value * open_qty) - commissions


def build_brier_inputs(
    points: Sequence[PricePoint],
    window: int,
    realized_outcome: float | None = None,
    warnings_out: list[str] | None = None,
) -> tuple[pd.Series, pd.Series, pd.Series]:
    """
    Build user/market/outcome probability series for cumulative Brier advantage.
    """
    empty = pd.Series(dtype=float)
    if not points:
        return empty, empty, empty
    if window <= 0:
        raise ValueError(f"window must be > 0, got {window}")

    if warnings_out is None:
        frame = _probability_frame(points)
    else:
        with warnings.catch_warnings(record=True) as caught_warnings:
            warnings.simplefilter("always", RuntimeWarning)
            frame = _probability_frame(points)
        for caught_warning in caught_warnings:
            message = str(caught_warning.message)
            if message not in warnings_out:
                warnings_out.append(message)
    if frame.empty:
        return empty, empty, empty

    frame["user_probability"] = (
        frame["market_probability"].rolling(window=window, min_periods=window).mean().clip(0.0, 1.0)
    )
    frame = frame.dropna(subset=["user_probability", "market_probability"])
    if frame.empty:
        return empty, empty, empty

    if realized_outcome is None:
        return (frame["user_probability"].copy(), frame["market_probability"].copy(), empty)

    frame["outcome"] = float(realized_outcome)
    return (
        frame["user_probability"].copy(),
        frame["market_probability"].copy(),
        frame["outcome"].copy(),
    )


def build_market_prices(
    points: Sequence[PricePoint], *, resample_rule: str | None = None
) -> list[tuple[datetime, float]]:
    """
    Convert ``(timestamp, price)`` pairs into sorted chart points.

    Parameters
    ----------
    points : Sequence[PricePoint]
        Raw ``(timestamp, price)`` records.
    resample_rule : str, optional
        Optional pandas offset alias used to resample for chart readability
        (for example ``"5min"``). The last price in each bucket is kept.
    """
    output: list[tuple[datetime, float]] = []
    for ts_raw, price in points:
        ts = to_naive_utc(ts_raw)
        if ts is None:
            continue
        output.append((ts, price))

    if not output:
        return []

    frame = pd.DataFrame(output, columns=["ts", "price"]).sort_values("ts")
    frame = frame.drop_duplicates(subset=["ts"], keep="last")
    if resample_rule:
        frame = frame.set_index("ts").resample(resample_rule).last().dropna().reset_index()
    return [
        (_timestamp_to_naive_utc_datetime(pd.Timestamp(row.ts)), float(row.price))
        for row in frame.itertuples(index=False)
    ]
