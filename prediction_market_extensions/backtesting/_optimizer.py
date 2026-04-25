from __future__ import annotations

import contextlib
import csv
import json
import multiprocessing
import pickle
import tempfile
import traceback
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from itertools import product
from pathlib import Path
from random import Random
from statistics import median
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._market_data_config import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import ReplaySpec
from prediction_market_extensions.backtesting._strategy_configs import StrategyConfigSpec
from prediction_market_extensions.backtesting.data_sources.registry import (
    resolve_market_data_support,
)

if TYPE_CHECKING:
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        PredictionMarketBacktest,
    )


SEARCH_PLACEHOLDER_PREFIX = "__SEARCH__:"
OPTIMIZER_TYPE_PARAMETER_SEARCH = "parameter_search"
DEFAULT_INVALID_SCORE = -1_000_000_000.0
_TOP_CANDIDATE_COUNT = 5
SAMPLER_RANDOM = "random"
SAMPLER_TPE = "tpe"
_SUPPORTED_SAMPLERS = (SAMPLER_RANDOM, SAMPLER_TPE)

type ParameterValues = tuple[tuple[str, Any], ...]
type BacktestEvaluator = Callable[[PredictionMarketBacktest], object]
type ParameterSpec = Mapping[str, Any]


REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class ParameterSearchWindow:
    name: str
    start_time: str
    end_time: str


@dataclass(frozen=True)
class ParameterSearchConfig:
    name: str
    data: MarketDataConfig
    strategy_spec: StrategyConfigSpec
    base_replay: ReplaySpec | None = None
    base_replays: Sequence[ReplaySpec] = ()
    parameter_grid: Mapping[str, Sequence[Any]] = field(default_factory=dict)
    parameter_space: Mapping[str, ParameterSpec] = field(default_factory=dict)
    sampler: Literal["random", "tpe"] = SAMPLER_RANDOM
    train_windows: Sequence[ParameterSearchWindow] = ()
    holdout_windows: Sequence[ParameterSearchWindow] = ()
    max_trials: int = 16
    random_seed: int = 0
    holdout_top_k: int = 5
    initial_cash: float = 100.0
    probability_window: int = 256
    min_book_events: int = 0
    min_price_range: float = 0.0
    min_fills_per_window: int = 1
    execution: ExecutionModelConfig | None = None
    chart_resample_rule: str | None = None
    nautilus_log_level: str = "INFO"
    artifact_root: Path | str = Path("output")
    invalid_score: float = DEFAULT_INVALID_SCORE

    @property
    def optimizer_type(self) -> str:
        return OPTIMIZER_TYPE_PARAMETER_SEARCH

    def __post_init__(self) -> None:
        resolve_market_data_support(
            platform=self.data.platform, data_type=self.data.data_type, vendor=self.data.vendor
        )

        if self.base_replay is None and not self.base_replays:
            raise ValueError("ParameterSearchConfig requires base_replay or base_replays.")
        # base_replays takes precedence; base_replay is a single-market shortcut.
        # Both may be set after dataclasses.replace(), in which case we trust
        # base_replays (which was normalized on the prior instance) and leave
        # base_replay alone for backward-compat repr.
        if self.base_replays:
            normalized_replays = tuple(self.base_replays)
        else:
            normalized_replays = (self.base_replay,)
        for replay in normalized_replays:
            market_slug = getattr(replay, "market_slug", None)
            market_ticker = getattr(replay, "market_ticker", None)
            if market_slug is None and market_ticker is None:
                raise ValueError(
                    "Every ParameterSearchConfig replay must define market_slug or market_ticker."
                )
        object.__setattr__(self, "base_replays", normalized_replays)
        if self.max_trials <= 0:
            raise ValueError("max_trials must be positive.")
        if self.holdout_top_k <= 0:
            raise ValueError("holdout_top_k must be positive.")
        if self.min_fills_per_window < 0:
            raise ValueError("min_fills_per_window must be non-negative.")
        if self.sampler not in _SUPPORTED_SAMPLERS:
            raise ValueError(f"sampler must be one of {_SUPPORTED_SAMPLERS}, got {self.sampler!r}.")

        has_grid = bool(self.parameter_grid)
        has_space = bool(self.parameter_space)
        if not has_grid and not has_space:
            raise ValueError("parameter_grid or parameter_space must be provided.")

        # An already-normalized parameter_space (e.g. via dataclasses.replace on an
        # already-constructed config) has MappingProxy entries with a "type" key.
        # Treat that as authoritative and skip re-validation of the raw space.
        space_already_normalized = has_space and all(
            isinstance(spec, Mapping) and "type" in spec and isinstance(spec, MappingProxyType)
            for spec in self.parameter_space.values()
        )
        if has_grid and has_space and not space_already_normalized:
            raise ValueError("Provide parameter_grid OR parameter_space, not both.")

        normalized_grid: dict[str, tuple[Any, ...]] = {}
        normalized_space: dict[str, ParameterSpec] = {}
        if space_already_normalized:
            for name, spec in self.parameter_space.items():
                normalized_space[str(name)] = spec
                if spec["type"] == "categorical":
                    normalized_grid[str(name)] = tuple(spec["choices"])
        elif has_grid:
            for name, values in self.parameter_grid.items():
                normalized_values = tuple(values)
                if not normalized_values:
                    raise ValueError(f"parameter_grid[{name!r}] must not be empty.")
                normalized_grid[str(name)] = normalized_values
                normalized_space[str(name)] = MappingProxyType(
                    {"type": "categorical", "choices": normalized_values}
                )
        else:
            for name, spec in self.parameter_space.items():
                validated = _validate_parameter_spec(str(name), spec)
                normalized_space[str(name)] = validated
                if validated["type"] == "categorical":
                    normalized_grid[str(name)] = tuple(validated["choices"])

        if (
            self.sampler == SAMPLER_RANDOM
            and not has_grid
            and any(spec["type"] != "categorical" for spec in normalized_space.values())
        ):
            raise ValueError(
                "sampler='random' requires a discrete parameter_grid or an all-categorical "
                "parameter_space. Use sampler='tpe' for continuous spaces."
            )

        placeholders = _collect_search_placeholders(self.strategy_spec)
        if not placeholders:
            raise ValueError(
                "strategy_spec must contain at least one __SEARCH__:<name> placeholder."
            )

        search_keys = set(normalized_space)
        missing_keys = placeholders.difference(search_keys)
        if missing_keys:
            raise ValueError(
                "search space is missing values for placeholders: "
                + ", ".join(sorted(missing_keys))
            )

        unused_keys = search_keys.difference(placeholders)
        if unused_keys:
            raise ValueError("search space includes unused keys: " + ", ".join(sorted(unused_keys)))

        object.__setattr__(self, "parameter_grid", MappingProxyType(normalized_grid))
        object.__setattr__(self, "parameter_space", MappingProxyType(normalized_space))
        object.__setattr__(self, "train_windows", tuple(self.train_windows))
        object.__setattr__(self, "holdout_windows", tuple(self.holdout_windows))
        artifact_root = Path(self.artifact_root).expanduser()
        if not artifact_root.is_absolute():
            artifact_root = REPO_ROOT / artifact_root
        object.__setattr__(self, "artifact_root", artifact_root.resolve())

        if not self.train_windows:
            raise ValueError("train_windows must not be empty.")
        if self.holdout_windows and len(self.base_replays) == 1:
            warnings.warn(
                "Parameter search train/holdout windows are split from a single market replay. "
                "Treat holdout metrics as time-split validation, not cross-market generalization.",
                RuntimeWarning,
                stacklevel=2,
            )


@dataclass(frozen=True)
class ParameterSearchLeaderboardRow:
    trial_id: int
    params: ParameterValues
    train_scores: tuple[float, ...]
    holdout_scores: tuple[float, ...] = ()
    train_median_score: float = 0.0
    holdout_median_score: float | None = None
    train_median_pnl: float = 0.0
    holdout_median_pnl: float | None = None
    train_median_drawdown: float = 0.0
    holdout_median_drawdown: float | None = None
    train_median_fills: float = 0.0
    holdout_median_fills: float | None = None
    train_median_coverage: float = 0.0
    holdout_median_coverage: float | None = None


@dataclass(frozen=True)
class ParameterSearchSummary:
    name: str
    objective_name: str
    candidate_pool_size: int
    evaluated_trials: int
    train_window_names: tuple[str, ...]
    holdout_window_names: tuple[str, ...]
    best_row: ParameterSearchLeaderboardRow
    selected_params: ParameterValues
    leaderboard: tuple[ParameterSearchLeaderboardRow, ...]
    leaderboard_csv_path: str
    summary_json_path: str

    @property
    def optimizer_type(self) -> str:
        return OPTIMIZER_TYPE_PARAMETER_SEARCH


@dataclass(frozen=True)
class _WindowEvaluation:
    window_name: str
    score: float
    pnl: float
    max_drawdown_currency: float
    fills: int
    requested_coverage_ratio: float
    terminated_early: bool
    status: str
    error: str | None = None


def _validate_parameter_spec(name: str, spec: Any) -> ParameterSpec:
    if not isinstance(spec, Mapping):
        raise ValueError(f"parameter_space[{name!r}] must be a mapping, got {type(spec).__name__}.")
    spec_type = spec.get("type")
    if spec_type == "categorical":
        choices = spec.get("choices")
        if not choices:
            raise ValueError(f"parameter_space[{name!r}] categorical needs non-empty 'choices'.")
        return MappingProxyType({"type": "categorical", "choices": tuple(choices)})
    if spec_type in ("int", "float"):
        low = spec.get("low")
        high = spec.get("high")
        if low is None or high is None:
            raise ValueError(f"parameter_space[{name!r}] {spec_type} needs 'low' and 'high'.")
        if low >= high:
            raise ValueError(f"parameter_space[{name!r}]: low must be < high.")
        log = bool(spec.get("log", False))
        step = spec.get("step")
        if step is not None:
            if log:
                raise ValueError(
                    f"parameter_space[{name!r}]: step is not supported with log sampling."
                )
            if step <= 0:
                raise ValueError(f"parameter_space[{name!r}]: step must be positive.")
        payload: dict[str, Any] = {"type": spec_type, "low": low, "high": high, "log": log}
        if step is not None:
            payload["step"] = step
        return MappingProxyType(payload)
    raise ValueError(
        f"parameter_space[{name!r}] has unsupported type {spec_type!r}; "
        "expected 'categorical', 'int', or 'float'."
    )


def _collect_search_placeholders(value: Any) -> set[str]:
    placeholders: set[str] = set()
    if isinstance(value, str) and value.startswith(SEARCH_PLACEHOLDER_PREFIX):
        placeholders.add(value.removeprefix(SEARCH_PLACEHOLDER_PREFIX))
    elif isinstance(value, Mapping):
        for inner in value.values():
            placeholders.update(_collect_search_placeholders(inner))
    elif isinstance(value, list | tuple):
        for inner in value:
            placeholders.update(_collect_search_placeholders(inner))
    return placeholders


def _replace_search_placeholders(value: Any, params: Mapping[str, Any]) -> Any:
    if isinstance(value, str) and value.startswith(SEARCH_PLACEHOLDER_PREFIX):
        key = value.removeprefix(SEARCH_PLACEHOLDER_PREFIX)
        try:
            return params[key]
        except KeyError as exc:
            raise KeyError(f"missing optimization parameter {key!r}") from exc
    if isinstance(value, Mapping):
        return {key: _replace_search_placeholders(inner, params) for key, inner in value.items()}
    if isinstance(value, list):
        return [_replace_search_placeholders(inner, params) for inner in value]
    if isinstance(value, tuple):
        return tuple(_replace_search_placeholders(inner, params) for inner in value)
    return value


def _parameter_candidates(parameter_grid: Mapping[str, Sequence[Any]]) -> list[ParameterValues]:
    keys = tuple(parameter_grid)
    values_product = product(*(parameter_grid[key] for key in keys))
    candidates: list[ParameterValues] = []
    seen: set[str] = set()
    for values in values_product:
        params = tuple(zip(keys, values, strict=True))
        canonical = json.dumps(_json_safe(dict(params)), sort_keys=True)
        if canonical in seen:
            continue
        seen.add(canonical)
        candidates.append(params)
    return candidates


def _sample_parameter_sets(config: ParameterSearchConfig) -> list[ParameterValues]:
    candidates = _parameter_candidates(config.parameter_grid)
    if len(candidates) <= config.max_trials:
        return candidates

    indices = list(range(len(candidates)))
    Random(config.random_seed).shuffle(indices)
    return [candidates[index] for index in indices[: config.max_trials]]


def _windowed_replay(*, base_replay: ReplaySpec, window: ParameterSearchWindow) -> ReplaySpec:
    metadata = dict(getattr(base_replay, "metadata", None) or {})
    metadata["optimization_window"] = window.name

    replacement_kwargs: dict[str, Any] = {
        "start_time": window.start_time,
        "end_time": window.end_time,
        "metadata": metadata,
    }
    if hasattr(base_replay, "lookback_days"):
        replacement_kwargs["lookback_days"] = None
    if hasattr(base_replay, "lookback_hours"):
        replacement_kwargs["lookback_hours"] = None
    return replace(base_replay, **replacement_kwargs)


def _windowed_replays(
    *, base_replays: Sequence[ReplaySpec], window: ParameterSearchWindow
) -> tuple[ReplaySpec, ...]:
    return tuple(_windowed_replay(base_replay=r, window=window) for r in base_replays)


def _build_backtest(
    *,
    config: ParameterSearchConfig,
    trial_id: int,
    window: ParameterSearchWindow,
    params: ParameterValues,
) -> PredictionMarketBacktest:
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        PredictionMarketBacktest,
    )

    return PredictionMarketBacktest(
        **_build_backtest_kwargs(
            config=config,
            trial_id=trial_id,
            window=window,
            params=params,
        )
    )


def _coerce_parameter_values(
    *, config: ParameterSearchConfig, params: ParameterValues | Mapping[str, Any]
) -> ParameterValues:
    if isinstance(params, Mapping):
        return tuple((name, params[name]) for name in config.parameter_space)
    return params


def build_parameter_search_window_backtest(
    *,
    config: ParameterSearchConfig,
    window: ParameterSearchWindow,
    params: ParameterValues | Mapping[str, Any],
    trial_id: int = 1,
    name: str | None = None,
    return_summary_series: bool | None = None,
) -> PredictionMarketBacktest:
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        PredictionMarketBacktest,
    )

    normalized_params = _coerce_parameter_values(config=config, params=params)
    kwargs = _build_backtest_kwargs(
        config=config, trial_id=trial_id, window=window, params=normalized_params
    )
    if name is not None:
        kwargs["name"] = name
    if return_summary_series is not None:
        kwargs["return_summary_series"] = return_summary_series
    return PredictionMarketBacktest(**kwargs)


def _build_backtest_kwargs(
    *,
    config: ParameterSearchConfig,
    trial_id: int,
    window: ParameterSearchWindow,
    params: ParameterValues,
) -> dict[str, Any]:
    params_map = dict(params)
    bound_strategy_spec = _replace_search_placeholders(config.strategy_spec, params_map)
    replays = _windowed_replays(base_replays=config.base_replays, window=window)
    return {
        "name": f"{config.name}:{window.name}:trial-{trial_id:03d}",
        "data": config.data,
        "replays": replays,
        "strategy_configs": [bound_strategy_spec],
        "initial_cash": config.initial_cash,
        "probability_window": config.probability_window,
        "min_book_events": config.min_book_events,
        "min_price_range": config.min_price_range,
        "nautilus_log_level": config.nautilus_log_level,
        "execution": config.execution,
        "chart_resample_rule": config.chart_resample_rule,
        "return_summary_series": True,
    }


def _default_evaluation_worker(
    worker_kwargs: dict[str, Any], result_path: str, send_conn: Any
) -> None:
    try:
        from prediction_market_extensions import install_commission_patch
        from prediction_market_extensions.backtesting._timing_harness import install_timing_harness

        install_commission_patch()
        install_timing_harness()

        from prediction_market_extensions.backtesting._prediction_market_backtest import (
            PredictionMarketBacktest,
        )

        result = PredictionMarketBacktest(**worker_kwargs).run()
        with open(result_path, "wb") as result_file:
            pickle.dump(result, result_file)
        send_conn.send(("ok", result_path))
    except BaseException as exc:  # pragma: no cover - exercised via subprocess
        send_conn.send(("error", {"error": repr(exc), "traceback": traceback.format_exc()}))
    finally:
        send_conn.close()


def _run_default_evaluator_in_subprocess(*, worker_kwargs: dict[str, Any]) -> object:
    ctx = multiprocessing.get_context("spawn")
    recv_conn, send_conn = ctx.Pipe(duplex=False)
    with tempfile.NamedTemporaryFile(
        prefix="optimizer-window-", suffix=".pkl", delete=False
    ) as result_file:
        result_path = result_file.name
    process = ctx.Process(
        target=_default_evaluation_worker,
        args=(worker_kwargs, result_path, send_conn),
        daemon=False,
    )
    process.start()
    send_conn.close()

    payload: tuple[str, Any] | None = None
    try:
        payload = recv_conn.recv()
    except EOFError:
        payload = None
    finally:
        recv_conn.close()
        process.join()

    try:
        if payload is not None:
            status, data = payload
            if status == "ok":
                if process.exitcode not in (0, None):
                    raise RuntimeError(
                        f"Optimizer worker exited with a non-zero code after returning a result: {process.exitcode}"
                    )
                with open(data, "rb") as result_file:
                    return pickle.load(result_file)

            if status == "error":
                message = data.get("error", "Unknown worker error")
                worker_traceback = data.get("traceback", "")
                raise RuntimeError(f"{message}\n\nChild traceback:\n{worker_traceback}".rstrip())

            raise RuntimeError(f"Unexpected optimizer worker payload status {status!r}")

        raise RuntimeError(
            f"Optimizer worker exited without returning a result. Process exit code: {process.exitcode}"
        )
    finally:
        with contextlib.suppress(FileNotFoundError):
            Path(result_path).unlink()


def _coerce_results(value: object) -> list[dict[str, Any]]:
    if isinstance(value, Mapping):
        return [dict(value)]
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        results: list[dict[str, Any]] = []
        for item in value:
            if not isinstance(item, Mapping):
                raise TypeError(
                    "optimizer evaluator must return mappings or a sequence of mappings"
                )
            results.append(dict(item))
        return results
    raise TypeError("optimizer evaluator must return a mapping or a sequence of mappings")


def _series_values(series: object) -> list[float]:
    values: list[float] = []
    if not isinstance(series, Sequence):
        return values
    for point in series:
        value = None
        if isinstance(point, Mapping):
            value = point.get("value")
        elif isinstance(point, Sequence) and not isinstance(point, str) and len(point) >= 2:
            value = point[1]
        if isinstance(value, int | float):
            values.append(float(value))
    return values


def _max_drawdown_currency(equity_series: object) -> float:
    values = _series_values(equity_series)
    if not values:
        return 0.0

    peak = values[0]
    max_drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        max_drawdown = max(max_drawdown, peak - value)
    return max_drawdown


def _joint_portfolio_drawdown(equity_series_list: Sequence[object]) -> float:
    """Compute max drawdown of a synthesized joint-portfolio equity curve.

    Each market's equity series is reindexed onto the union timeline with
    forward-fill (and back-fill for the leading edge), then summed. The
    drawdown is measured on the aggregated curve, so diversification
    across markets can reduce the penalty — which is the point of
    optimizing a joint portfolio.
    """
    if not equity_series_list:
        return 0.0

    import pandas as pd

    frames: list[pd.Series] = []
    for series in equity_series_list:
        if not isinstance(series, Sequence):
            continue
        timestamps: list[Any] = []
        values: list[float] = []
        for point in series:
            ts: Any = None
            value: Any = None
            if isinstance(point, Mapping):
                ts = point.get("timestamp") or point.get("time") or point.get("t")
                value = point.get("value")
            elif (
                isinstance(point, Sequence)
                and not isinstance(point, str | bytes)
                and len(point) >= 2
            ):
                ts = point[0]
                value = point[1]
            if ts is None or not isinstance(value, int | float):
                continue
            timestamps.append(ts)
            values.append(float(value))
        if not timestamps:
            continue
        index = pd.to_datetime(timestamps, utc=True, errors="coerce")
        frame = pd.Series(values, index=index).dropna().sort_index()
        if not frame.empty:
            frames.append(frame)

    if not frames:
        return 0.0

    combined_index = frames[0].index
    for frame in frames[1:]:
        combined_index = combined_index.union(frame.index)
    combined_index = combined_index.sort_values()

    joint = None
    for frame in frames:
        reindexed = frame.reindex(combined_index).ffill()
        if reindexed.empty:
            continue
        first_valid = reindexed.first_valid_index()
        if first_valid is not None:
            reindexed.loc[reindexed.index < first_valid] = 0.0
        reindexed = reindexed.fillna(0.0)
        joint = reindexed if joint is None else joint + reindexed
    if joint is None or joint.empty:
        return 0.0
    running_peak = joint.cummax()
    return float((running_peak - joint).max())


def _as_float(value: object, *, default: float = 0.0) -> float:
    if isinstance(value, int | float):
        return float(value)
    return default


def _as_int(value: object, *, default: int = 0) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return default


def _score_result(
    *,
    pnl: float,
    max_drawdown_currency: float,
    fills: int,
    requested_coverage_ratio: float,
    terminated_early: bool,
    initial_cash: float,
    min_fills_per_window: int,
) -> float:
    terminated_penalty = initial_cash if terminated_early else 0.0
    coverage_penalty = initial_cash * max(0.0, 0.98 - requested_coverage_ratio) * 10.0
    fill_penalty = 2.0 if fills < min_fills_per_window else 0.0
    return (
        pnl - (0.5 * max_drawdown_currency) - terminated_penalty - coverage_penalty - fill_penalty
    )


def _evaluate_window(
    *,
    config: ParameterSearchConfig,
    evaluator: BacktestEvaluator | None,
    trial_id: int,
    params: ParameterValues,
    window: ParameterSearchWindow,
) -> _WindowEvaluation:
    try:
        if evaluator is None:
            raw_results = _run_default_evaluator_in_subprocess(
                worker_kwargs=_build_backtest_kwargs(
                    config=config, trial_id=trial_id, window=window, params=params
                )
            )
        else:
            raw_results = evaluator(
                _build_backtest(
                    config=config,
                    trial_id=trial_id,
                    window=window,
                    params=params,
                )
            )
        results = _coerce_results(raw_results)
    except Exception as exc:
        return _WindowEvaluation(
            window_name=window.name,
            score=config.invalid_score,
            pnl=0.0,
            max_drawdown_currency=0.0,
            fills=0,
            requested_coverage_ratio=0.0,
            terminated_early=True,
            status="error",
            error=str(exc),
        )

    if not results:
        return _WindowEvaluation(
            window_name=window.name,
            score=config.invalid_score,
            pnl=0.0,
            max_drawdown_currency=0.0,
            fills=0,
            requested_coverage_ratio=0.0,
            terminated_early=True,
            status="invalid_result_count",
            error="received 0 results",
        )

    expected_market_count = len(config.base_replays)
    if expected_market_count and len(results) != expected_market_count:
        return _WindowEvaluation(
            window_name=window.name,
            score=config.invalid_score,
            pnl=0.0,
            max_drawdown_currency=0.0,
            fills=0,
            requested_coverage_ratio=0.0,
            terminated_early=True,
            status="invalid_result_count",
            error=f"expected {expected_market_count} results, received {len(results)}",
        )

    pnl = sum(_as_float(r.get("pnl")) for r in results)
    fills = sum(_as_int(r.get("fills")) for r in results)
    coverages = [_as_float(r.get("requested_coverage_ratio"), default=0.0) for r in results]
    requested_coverage_ratio = (sum(coverages) / len(coverages)) if coverages else 0.0
    terminated_early = any(bool(r.get("terminated_early")) for r in results)
    if len(results) == 1:
        max_drawdown_currency = _max_drawdown_currency(results[0].get("equity_series"))
    else:
        max_drawdown_currency = _joint_portfolio_drawdown([r.get("equity_series") for r in results])
    score = _score_result(
        pnl=pnl,
        max_drawdown_currency=max_drawdown_currency,
        fills=fills,
        requested_coverage_ratio=requested_coverage_ratio,
        terminated_early=terminated_early,
        initial_cash=config.initial_cash,
        min_fills_per_window=config.min_fills_per_window,
    )
    return _WindowEvaluation(
        window_name=window.name,
        score=score,
        pnl=pnl,
        max_drawdown_currency=max_drawdown_currency,
        fills=fills,
        requested_coverage_ratio=requested_coverage_ratio,
        terminated_early=terminated_early,
        status="ok",
    )


def _median_metric(values: Sequence[float]) -> float:
    return float(median(values))


def _build_leaderboard_row(
    *,
    trial_id: int,
    params: ParameterValues,
    train_evaluations: Sequence[_WindowEvaluation],
    holdout_evaluations: Sequence[_WindowEvaluation] = (),
) -> ParameterSearchLeaderboardRow:
    train_scores = tuple(evaluation.score for evaluation in train_evaluations)
    holdout_scores = tuple(evaluation.score for evaluation in holdout_evaluations)
    return ParameterSearchLeaderboardRow(
        trial_id=trial_id,
        params=params,
        train_scores=train_scores,
        holdout_scores=holdout_scores,
        train_median_score=_median_metric(train_scores),
        holdout_median_score=(_median_metric(holdout_scores) if holdout_scores else None),
        train_median_pnl=_median_metric([evaluation.pnl for evaluation in train_evaluations]),
        holdout_median_pnl=(
            _median_metric([evaluation.pnl for evaluation in holdout_evaluations])
            if holdout_evaluations
            else None
        ),
        train_median_drawdown=_median_metric(
            [evaluation.max_drawdown_currency for evaluation in train_evaluations]
        ),
        holdout_median_drawdown=(
            _median_metric([evaluation.max_drawdown_currency for evaluation in holdout_evaluations])
            if holdout_evaluations
            else None
        ),
        train_median_fills=_median_metric(
            [float(evaluation.fills) for evaluation in train_evaluations]
        ),
        holdout_median_fills=(
            _median_metric([float(evaluation.fills) for evaluation in holdout_evaluations])
            if holdout_evaluations
            else None
        ),
        train_median_coverage=_median_metric(
            [evaluation.requested_coverage_ratio for evaluation in train_evaluations]
        ),
        holdout_median_coverage=(
            _median_metric(
                [evaluation.requested_coverage_ratio for evaluation in holdout_evaluations]
            )
            if holdout_evaluations
            else None
        ),
    )


def _train_row_sort_key(row: ParameterSearchLeaderboardRow) -> tuple[float, int]:
    return (-row.train_median_score, row.trial_id)


def _final_row_sort_key(row: ParameterSearchLeaderboardRow) -> tuple[int, float, float, int]:
    holdout_rank = (
        row.holdout_median_score if row.holdout_median_score is not None else DEFAULT_INVALID_SCORE
    )
    has_holdout = 0 if row.holdout_median_score is not None else 1
    return (has_holdout, -holdout_rank, -row.train_median_score, row.trial_id)


def _params_dict(params: ParameterValues) -> dict[str, Any]:
    return dict(params)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, tuple | list):
        return [_json_safe(inner) for inner in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, bool | int | float | str) or value is None:
        return value
    return str(value)


def _write_leaderboard_csv(
    *, rows: Sequence[ParameterSearchLeaderboardRow], output_path: Path
) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "trial_id",
        "train_median_score",
        "holdout_median_score",
        "train_median_pnl",
        "holdout_median_pnl",
        "train_median_drawdown",
        "holdout_median_drawdown",
        "train_median_fills",
        "holdout_median_fills",
        "train_median_coverage",
        "holdout_median_coverage",
        "train_scores_json",
        "holdout_scores_json",
        "params_json",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "trial_id": row.trial_id,
                    "train_median_score": f"{row.train_median_score:.6f}",
                    "holdout_median_score": (
                        ""
                        if row.holdout_median_score is None
                        else f"{row.holdout_median_score:.6f}"
                    ),
                    "train_median_pnl": f"{row.train_median_pnl:.6f}",
                    "holdout_median_pnl": (
                        "" if row.holdout_median_pnl is None else f"{row.holdout_median_pnl:.6f}"
                    ),
                    "train_median_drawdown": f"{row.train_median_drawdown:.6f}",
                    "holdout_median_drawdown": (
                        ""
                        if row.holdout_median_drawdown is None
                        else f"{row.holdout_median_drawdown:.6f}"
                    ),
                    "train_median_fills": f"{row.train_median_fills:.3f}",
                    "holdout_median_fills": (
                        ""
                        if row.holdout_median_fills is None
                        else f"{row.holdout_median_fills:.3f}"
                    ),
                    "train_median_coverage": f"{row.train_median_coverage:.6f}",
                    "holdout_median_coverage": (
                        ""
                        if row.holdout_median_coverage is None
                        else f"{row.holdout_median_coverage:.6f}"
                    ),
                    "train_scores_json": json.dumps(list(row.train_scores)),
                    "holdout_scores_json": json.dumps(list(row.holdout_scores)),
                    "params_json": json.dumps(_json_safe(_params_dict(row.params)), sort_keys=True),
                }
            )
    return str(output_path.resolve())


def _summary_payload(
    *, config: ParameterSearchConfig, summary: ParameterSearchSummary
) -> dict[str, Any]:
    best_row = summary.best_row
    return {
        "name": summary.name,
        "optimizer_type": summary.optimizer_type,
        "sampler": config.sampler,
        "market_count": len(config.base_replays),
        "objective_name": summary.objective_name,
        "generated_at": datetime.now(UTC).isoformat(),
        "candidate_pool_size": summary.candidate_pool_size,
        "evaluated_trials": summary.evaluated_trials,
        "max_trials": config.max_trials,
        "random_seed": config.random_seed,
        "holdout_top_k": config.holdout_top_k,
        "min_fills_per_window": config.min_fills_per_window,
        "train_windows": list(summary.train_window_names),
        "holdout_windows": list(summary.holdout_window_names),
        "selected_params": _json_safe(_params_dict(summary.selected_params)),
        "leaderboard_csv_path": summary.leaderboard_csv_path,
        "summary_json_path": summary.summary_json_path,
        "best_candidate": {
            "trial_id": best_row.trial_id,
            "train_median_score": best_row.train_median_score,
            "holdout_median_score": best_row.holdout_median_score,
            "train_median_pnl": best_row.train_median_pnl,
            "holdout_median_pnl": best_row.holdout_median_pnl,
            "train_median_drawdown": best_row.train_median_drawdown,
            "holdout_median_drawdown": best_row.holdout_median_drawdown,
            "train_median_fills": best_row.train_median_fills,
            "holdout_median_fills": best_row.holdout_median_fills,
            "train_median_coverage": best_row.train_median_coverage,
            "holdout_median_coverage": best_row.holdout_median_coverage,
            "params": _json_safe(_params_dict(best_row.params)),
        },
    }


def _write_summary_json(
    *, config: ParameterSearchConfig, summary: ParameterSearchSummary, output_path: Path
) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = _summary_payload(config=config, summary=summary)
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return str(output_path.resolve())


def _format_score(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:10.4f}"


def _print_top_candidates(
    *, rows: Sequence[ParameterSearchLeaderboardRow], holdout_enabled: bool
) -> None:
    print()
    print("Top candidates")
    print(
        "trial  train_score  holdout_score  median_pnl  median_dd  median_fills  median_cov  params"
    )
    for row in rows[:_TOP_CANDIDATE_COUNT]:
        holdout_score = row.holdout_median_score if holdout_enabled else None
        print(
            f"{row.trial_id:>5}  "
            f"{row.train_median_score:11.4f}  "
            f"{_format_score(holdout_score)}  "
            f"{row.train_median_pnl:10.4f}  "
            f"{row.train_median_drawdown:9.4f}  "
            f"{row.train_median_fills:12.1f}  "
            f"{row.train_median_coverage:10.3f}  "
            f"{json.dumps(_json_safe(_params_dict(row.params)), sort_keys=True)}"
        )


def _evaluate_train_windows(
    *,
    config: ParameterSearchConfig,
    evaluator: BacktestEvaluator | None,
    trial_id: int,
    params: ParameterValues,
) -> tuple[_WindowEvaluation, ...]:
    return tuple(
        _evaluate_window(
            config=config, evaluator=evaluator, trial_id=trial_id, params=params, window=window
        )
        for window in config.train_windows
    )


def _run_random_trials(
    config: ParameterSearchConfig,
    *,
    evaluator: BacktestEvaluator | None,
) -> tuple[
    dict[int, tuple[_WindowEvaluation, ...]],
    dict[int, ParameterSearchLeaderboardRow],
    int,
    int,
]:
    candidate_pool = _parameter_candidates(config.parameter_grid)
    sampled_params = _sample_parameter_sets(config)
    train_evaluations_by_trial: dict[int, tuple[_WindowEvaluation, ...]] = {}
    train_rows: dict[int, ParameterSearchLeaderboardRow] = {}
    for trial_id, params in enumerate(sampled_params, start=1):
        train_evaluations = _evaluate_train_windows(
            config=config, evaluator=evaluator, trial_id=trial_id, params=params
        )
        train_evaluations_by_trial[trial_id] = train_evaluations
        train_rows[trial_id] = _build_leaderboard_row(
            trial_id=trial_id, params=params, train_evaluations=train_evaluations
        )
    return train_evaluations_by_trial, train_rows, len(candidate_pool), len(sampled_params)


def _suggest_params_from_trial(
    trial: Any, parameter_space: Mapping[str, ParameterSpec]
) -> ParameterValues:
    values: list[tuple[str, Any]] = []
    for name, spec in parameter_space.items():
        spec_type = spec["type"]
        if spec_type == "categorical":
            value = trial.suggest_categorical(name, list(spec["choices"]))
        elif spec_type == "int":
            step = spec.get("step")
            if step is not None:
                value = trial.suggest_int(name, int(spec["low"]), int(spec["high"]), step=int(step))
            else:
                value = trial.suggest_int(
                    name, int(spec["low"]), int(spec["high"]), log=bool(spec.get("log", False))
                )
        elif spec_type == "float":
            step = spec.get("step")
            if step is not None and not spec.get("log", False):
                value = trial.suggest_float(
                    name, float(spec["low"]), float(spec["high"]), step=float(step)
                )
            else:
                value = trial.suggest_float(
                    name,
                    float(spec["low"]),
                    float(spec["high"]),
                    log=bool(spec.get("log", False)),
                )
        else:  # pragma: no cover - validated in __post_init__
            raise ValueError(f"unsupported spec type {spec_type!r}")
        values.append((name, value))
    return tuple(values)


def _run_tpe_trials(
    config: ParameterSearchConfig,
    *,
    evaluator: BacktestEvaluator | None,
) -> tuple[
    dict[int, tuple[_WindowEvaluation, ...]],
    dict[int, ParameterSearchLeaderboardRow],
    int,
    int,
]:
    try:
        import optuna
    except ImportError as exc:
        raise RuntimeError(
            "sampler='tpe' requires optuna. Install it with `uv pip install optuna`."
        ) from exc

    optuna.logging.set_verbosity(optuna.logging.WARNING)
    sampler = optuna.samplers.TPESampler(seed=config.random_seed)
    study = optuna.create_study(direction="maximize", sampler=sampler)

    train_evaluations_by_trial: dict[int, tuple[_WindowEvaluation, ...]] = {}
    train_rows: dict[int, ParameterSearchLeaderboardRow] = {}

    for trial_id in range(1, config.max_trials + 1):
        trial = study.ask()
        params = _suggest_params_from_trial(trial, config.parameter_space)
        train_evaluations = _evaluate_train_windows(
            config=config, evaluator=evaluator, trial_id=trial_id, params=params
        )
        train_evaluations_by_trial[trial_id] = train_evaluations
        row = _build_leaderboard_row(
            trial_id=trial_id, params=params, train_evaluations=train_evaluations
        )
        train_rows[trial_id] = row
        study.tell(trial, row.train_median_score)

    return train_evaluations_by_trial, train_rows, config.max_trials, config.max_trials


def run_parameter_search(
    config: ParameterSearchConfig, *, evaluator: BacktestEvaluator | None = None
) -> ParameterSearchSummary:
    if config.sampler == SAMPLER_TPE:
        (
            train_evaluations_by_trial,
            train_rows,
            candidate_pool_size,
            evaluated_trials,
        ) = _run_tpe_trials(config, evaluator=evaluator)
    else:
        (
            train_evaluations_by_trial,
            train_rows,
            candidate_pool_size,
            evaluated_trials,
        ) = _run_random_trials(config, evaluator=evaluator)

    rows_by_train = sorted(train_rows.values(), key=_train_row_sort_key)
    holdout_enabled = bool(config.holdout_windows)
    rows_by_trial = dict(train_rows)

    if holdout_enabled:
        top_k = min(config.holdout_top_k, len(rows_by_train))
        for row in rows_by_train[:top_k]:
            holdout_evaluations = tuple(
                _evaluate_window(
                    config=config,
                    evaluator=evaluator,
                    trial_id=row.trial_id,
                    params=row.params,
                    window=window,
                )
                for window in config.holdout_windows
            )
            rows_by_trial[row.trial_id] = _build_leaderboard_row(
                trial_id=row.trial_id,
                params=row.params,
                train_evaluations=train_evaluations_by_trial[row.trial_id],
                holdout_evaluations=holdout_evaluations,
            )

    final_rows = sorted(rows_by_trial.values(), key=_final_row_sort_key)
    best_row = final_rows[0]
    artifact_root = config.artifact_root
    leaderboard_csv_path = artifact_root / f"{config.name}_leaderboard.csv"
    summary_json_path = artifact_root / f"{config.name}_summary.json"
    resolved_leaderboard_csv_path = str(leaderboard_csv_path.resolve())
    resolved_summary_json_path = str(summary_json_path.resolve())
    summary = ParameterSearchSummary(
        name=config.name,
        objective_name="risk_adjusted_score",
        candidate_pool_size=candidate_pool_size,
        evaluated_trials=evaluated_trials,
        train_window_names=tuple(window.name for window in config.train_windows),
        holdout_window_names=tuple(window.name for window in config.holdout_windows),
        best_row=best_row,
        selected_params=best_row.params,
        leaderboard=tuple(final_rows),
        leaderboard_csv_path=resolved_leaderboard_csv_path,
        summary_json_path=resolved_summary_json_path,
    )
    _write_leaderboard_csv(rows=summary.leaderboard, output_path=leaderboard_csv_path)
    _write_summary_json(
        config=config,
        summary=summary,
        output_path=summary_json_path,
    )

    print()
    if config.sampler == SAMPLER_TPE:
        print(
            f"Parameter search complete for {config.name} "
            f"(sampler=tpe): evaluated {summary.evaluated_trials} trials."
        )
    else:
        print(
            f"Parameter search complete for {config.name} "
            f"(sampler=random): evaluated {summary.evaluated_trials} of "
            f"{summary.candidate_pool_size} parameter combinations."
        )
    print(
        "Selected params: "
        + json.dumps(_json_safe(_params_dict(summary.selected_params)), sort_keys=True)
    )
    print(f"Leaderboard CSV: {summary.leaderboard_csv_path}")
    print(f"Summary JSON: {summary.summary_json_path}")
    _print_top_candidates(rows=summary.leaderboard, holdout_enabled=holdout_enabled)
    return summary


__all__ = [
    "OPTIMIZER_TYPE_PARAMETER_SEARCH",
    "SEARCH_PLACEHOLDER_PREFIX",
    "OptimizationConfig",
    "OptimizationLeaderboardRow",
    "OptimizationSummary",
    "OptimizationWindow",
    "ParameterSearchConfig",
    "ParameterSearchLeaderboardRow",
    "ParameterSearchSummary",
    "ParameterSearchWindow",
    "build_optimization_window_backtest",
    "build_parameter_search_window_backtest",
    "run_parameter_optimization",
    "run_parameter_search",
]


OptimizationWindow = ParameterSearchWindow
OptimizationConfig = ParameterSearchConfig
OptimizationLeaderboardRow = ParameterSearchLeaderboardRow
OptimizationSummary = ParameterSearchSummary
build_optimization_window_backtest = build_parameter_search_window_backtest
run_parameter_optimization = run_parameter_search
