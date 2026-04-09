from __future__ import annotations

import contextlib
import csv
import json
import multiprocessing
import pickle
import tempfile
import traceback
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import UTC
from datetime import datetime
from itertools import product
from pathlib import Path
from random import Random
from statistics import median
from types import MappingProxyType
from typing import Any
from typing import TYPE_CHECKING

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._market_data_config import MarketDataConfig
from backtests._shared._replay_specs import ReplaySpec
from backtests._shared._strategy_configs import StrategyConfigSpec
from backtests._shared.data_sources.registry import resolve_market_data_support

if TYPE_CHECKING:
    from backtests._shared._prediction_market_backtest import PredictionMarketBacktest


SEARCH_PLACEHOLDER_PREFIX = "__SEARCH__:"
DEFAULT_INVALID_SCORE = -1_000_000_000.0
_TOP_CANDIDATE_COUNT = 5

type ParameterValues = tuple[tuple[str, Any], ...]
type BacktestEvaluator = Callable[[PredictionMarketBacktest], object]


REPO_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class OptimizationWindow:
    name: str
    start_time: str
    end_time: str


@dataclass(frozen=True)
class OptimizationConfig:
    name: str
    data: MarketDataConfig
    base_replay: ReplaySpec
    strategy_spec: StrategyConfigSpec
    parameter_grid: Mapping[str, Sequence[Any]]
    train_windows: Sequence[OptimizationWindow]
    holdout_windows: Sequence[OptimizationWindow] = ()
    max_trials: int = 16
    random_seed: int = 0
    holdout_top_k: int = 5
    initial_cash: float = 100.0
    probability_window: int = 256
    min_trades: int = 0
    min_quotes: int = 0
    min_price_range: float = 0.0
    min_fills_per_window: int = 1
    execution: ExecutionModelConfig | None = None
    chart_resample_rule: str | None = None
    emit_html: bool = False
    chart_output_path: Path | str | None = None
    nautilus_log_level: str = "INFO"
    artifact_root: Path | str = Path("output")
    invalid_score: float = DEFAULT_INVALID_SCORE

    def __post_init__(self) -> None:
        resolve_market_data_support(
            platform=self.data.platform,
            data_type=self.data.data_type,
            vendor=self.data.vendor,
        )

        market_slug = getattr(self.base_replay, "market_slug", None)
        market_ticker = getattr(self.base_replay, "market_ticker", None)
        if market_slug is None and market_ticker is None:
            raise ValueError(
                "OptimizationConfig.base_replay must define market_slug or market_ticker."
            )
        if self.max_trials <= 0:
            raise ValueError("max_trials must be positive.")
        if self.holdout_top_k <= 0:
            raise ValueError("holdout_top_k must be positive.")
        if self.min_fills_per_window < 0:
            raise ValueError("min_fills_per_window must be non-negative.")

        normalized_grid: dict[str, tuple[Any, ...]] = {}
        for name, values in self.parameter_grid.items():
            normalized_values = tuple(values)
            if not normalized_values:
                raise ValueError(f"parameter_grid[{name!r}] must not be empty.")
            normalized_grid[str(name)] = normalized_values
        if not normalized_grid:
            raise ValueError("parameter_grid must not be empty.")

        placeholders = _collect_search_placeholders(self.strategy_spec)
        if not placeholders:
            raise ValueError(
                "strategy_spec must contain at least one __SEARCH__:<name> placeholder."
            )

        missing_keys = placeholders.difference(normalized_grid)
        if missing_keys:
            raise ValueError(
                "parameter_grid is missing values for placeholders: "
                + ", ".join(sorted(missing_keys))
            )

        unused_keys = set(normalized_grid).difference(placeholders)
        if unused_keys:
            raise ValueError(
                "parameter_grid includes unused keys: " + ", ".join(sorted(unused_keys))
            )

        object.__setattr__(self, "parameter_grid", MappingProxyType(normalized_grid))
        object.__setattr__(self, "train_windows", tuple(self.train_windows))
        object.__setattr__(self, "holdout_windows", tuple(self.holdout_windows))
        artifact_root = Path(self.artifact_root).expanduser()
        if not artifact_root.is_absolute():
            artifact_root = REPO_ROOT / artifact_root
        object.__setattr__(self, "artifact_root", artifact_root.resolve())

        if not self.train_windows:
            raise ValueError("train_windows must not be empty.")


@dataclass(frozen=True)
class OptimizationLeaderboardRow:
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
class OptimizationSummary:
    name: str
    objective_name: str
    candidate_pool_size: int
    evaluated_trials: int
    train_window_names: tuple[str, ...]
    holdout_window_names: tuple[str, ...]
    best_row: OptimizationLeaderboardRow
    selected_params: ParameterValues
    leaderboard: tuple[OptimizationLeaderboardRow, ...]
    leaderboard_csv_path: str
    summary_json_path: str


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
        return {
            key: _replace_search_placeholders(inner, params)
            for key, inner in value.items()
        }
    if isinstance(value, list):
        return [_replace_search_placeholders(inner, params) for inner in value]
    if isinstance(value, tuple):
        return tuple(_replace_search_placeholders(inner, params) for inner in value)
    return value


def _parameter_candidates(
    parameter_grid: Mapping[str, Sequence[Any]],
) -> list[ParameterValues]:
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


def _sample_parameter_sets(config: OptimizationConfig) -> list[ParameterValues]:
    candidates = _parameter_candidates(config.parameter_grid)
    if len(candidates) <= config.max_trials:
        return candidates

    indices = list(range(len(candidates)))
    Random(config.random_seed).shuffle(indices)
    return [candidates[index] for index in indices[: config.max_trials]]


def _windowed_replay(
    *,
    base_replay: ReplaySpec,
    window: OptimizationWindow,
) -> ReplaySpec:
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
    return replace(
        base_replay,
        **replacement_kwargs,
    )


def _build_backtest(
    *,
    config: OptimizationConfig,
    trial_id: int,
    window: OptimizationWindow,
    params: ParameterValues,
) -> PredictionMarketBacktest:
    from backtests._shared._prediction_market_backtest import PredictionMarketBacktest

    return PredictionMarketBacktest(
        **_build_backtest_kwargs(
            config=config,
            trial_id=trial_id,
            window=window,
            params=params,
        )
    )


def _build_backtest_kwargs(
    *,
    config: OptimizationConfig,
    trial_id: int,
    window: OptimizationWindow,
    params: ParameterValues,
) -> dict[str, Any]:
    params_map = dict(params)
    bound_strategy_spec = _replace_search_placeholders(config.strategy_spec, params_map)
    replay = _windowed_replay(base_replay=config.base_replay, window=window)
    return {
        "name": f"{config.name}:{window.name}:trial-{trial_id:03d}",
        "data": config.data,
        "replays": (replay,),
        "strategy_configs": [bound_strategy_spec],
        "initial_cash": config.initial_cash,
        "probability_window": config.probability_window,
        "min_trades": config.min_trades,
        "min_quotes": config.min_quotes,
        "min_price_range": config.min_price_range,
        "nautilus_log_level": config.nautilus_log_level,
        "execution": config.execution,
        "chart_resample_rule": config.chart_resample_rule,
        "emit_html": config.emit_html,
        "chart_output_path": config.chart_output_path,
        "return_summary_series": True,
    }


def _default_evaluation_worker(
    worker_kwargs: dict[str, Any],
    result_path: str,
    send_conn: Any,
) -> None:
    try:
        from _nautilus_bootstrap import install_local_nautilus_overrides

        install_local_nautilus_overrides()

        from backtests._shared._prediction_market_backtest import (
            PredictionMarketBacktest,
        )

        result = PredictionMarketBacktest(**worker_kwargs).run()
        with open(result_path, "wb") as result_file:
            pickle.dump(result, result_file)
        send_conn.send(("ok", result_path))
    except BaseException as exc:  # pragma: no cover - exercised via subprocess
        send_conn.send(
            (
                "error",
                {
                    "error": repr(exc),
                    "traceback": traceback.format_exc(),
                },
            )
        )
    finally:
        send_conn.close()


def _run_default_evaluator_in_subprocess(
    *,
    worker_kwargs: dict[str, Any],
) -> object:
    ctx = multiprocessing.get_context("spawn")
    recv_conn, send_conn = ctx.Pipe(duplex=False)
    with tempfile.NamedTemporaryFile(
        prefix="optimizer-window-",
        suffix=".pkl",
        delete=False,
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
                        "Optimizer worker exited with a non-zero code after returning a result: "
                        f"{process.exitcode}"
                    )
                with open(data, "rb") as result_file:
                    return pickle.load(result_file)

            if status == "error":
                message = data.get("error", "Unknown worker error")
                worker_traceback = data.get("traceback", "")
                raise RuntimeError(
                    f"{message}\n\nChild traceback:\n{worker_traceback}".rstrip()
                )

            raise RuntimeError(f"Unexpected optimizer worker payload status {status!r}")

        raise RuntimeError(
            "Optimizer worker exited without returning a result. "
            f"Process exit code: {process.exitcode}"
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
    raise TypeError(
        "optimizer evaluator must return a mapping or a sequence of mappings"
    )


def _series_values(series: object) -> list[float]:
    values: list[float] = []
    if not isinstance(series, Sequence):
        return values
    for point in series:
        value = None
        if isinstance(point, Mapping):
            value = point.get("value")
        elif (
            isinstance(point, Sequence)
            and not isinstance(point, str)
            and len(point) >= 2
        ):
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
        pnl
        - (0.5 * max_drawdown_currency)
        - terminated_penalty
        - coverage_penalty
        - fill_penalty
    )


def _evaluate_window(
    *,
    config: OptimizationConfig,
    evaluator: BacktestEvaluator | None,
    trial_id: int,
    params: ParameterValues,
    window: OptimizationWindow,
) -> _WindowEvaluation:
    try:
        if evaluator is None:
            raw_results = _run_default_evaluator_in_subprocess(
                worker_kwargs=_build_backtest_kwargs(
                    config=config,
                    trial_id=trial_id,
                    window=window,
                    params=params,
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
    except Exception as exc:  # noqa: BLE001
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

    if len(results) != 1:
        return _WindowEvaluation(
            window_name=window.name,
            score=config.invalid_score,
            pnl=0.0,
            max_drawdown_currency=0.0,
            fills=0,
            requested_coverage_ratio=0.0,
            terminated_early=True,
            status="invalid_result_count",
            error=f"expected 1 result, received {len(results)}",
        )

    result = results[0]
    pnl = _as_float(result.get("pnl"))
    fills = _as_int(result.get("fills"))
    requested_coverage_ratio = _as_float(
        result.get("requested_coverage_ratio"),
        default=0.0,
    )
    terminated_early = bool(result.get("terminated_early"))
    max_drawdown_currency = _max_drawdown_currency(result.get("equity_series"))
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
) -> OptimizationLeaderboardRow:
    train_scores = tuple(evaluation.score for evaluation in train_evaluations)
    holdout_scores = tuple(evaluation.score for evaluation in holdout_evaluations)
    return OptimizationLeaderboardRow(
        trial_id=trial_id,
        params=params,
        train_scores=train_scores,
        holdout_scores=holdout_scores,
        train_median_score=_median_metric(train_scores),
        holdout_median_score=(
            _median_metric(holdout_scores) if holdout_scores else None
        ),
        train_median_pnl=_median_metric(
            [evaluation.pnl for evaluation in train_evaluations]
        ),
        holdout_median_pnl=(
            _median_metric([evaluation.pnl for evaluation in holdout_evaluations])
            if holdout_evaluations
            else None
        ),
        train_median_drawdown=_median_metric(
            [evaluation.max_drawdown_currency for evaluation in train_evaluations]
        ),
        holdout_median_drawdown=(
            _median_metric(
                [evaluation.max_drawdown_currency for evaluation in holdout_evaluations]
            )
            if holdout_evaluations
            else None
        ),
        train_median_fills=_median_metric(
            [float(evaluation.fills) for evaluation in train_evaluations]
        ),
        holdout_median_fills=(
            _median_metric(
                [float(evaluation.fills) for evaluation in holdout_evaluations]
            )
            if holdout_evaluations
            else None
        ),
        train_median_coverage=_median_metric(
            [evaluation.requested_coverage_ratio for evaluation in train_evaluations]
        ),
        holdout_median_coverage=(
            _median_metric(
                [
                    evaluation.requested_coverage_ratio
                    for evaluation in holdout_evaluations
                ]
            )
            if holdout_evaluations
            else None
        ),
    )


def _train_row_sort_key(row: OptimizationLeaderboardRow) -> tuple[float, int]:
    return (-row.train_median_score, row.trial_id)


def _final_row_sort_key(
    row: OptimizationLeaderboardRow,
) -> tuple[int, float, float, int]:
    holdout_rank = (
        row.holdout_median_score
        if row.holdout_median_score is not None
        else DEFAULT_INVALID_SCORE
    )
    has_holdout = 0 if row.holdout_median_score is not None else 1
    return (has_holdout, -holdout_rank, -row.train_median_score, row.trial_id)


def _params_dict(params: ParameterValues) -> dict[str, Any]:
    return {name: value for name, value in params}


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
    *,
    rows: Sequence[OptimizationLeaderboardRow],
    output_path: Path,
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
                        ""
                        if row.holdout_median_pnl is None
                        else f"{row.holdout_median_pnl:.6f}"
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
                    "params_json": json.dumps(
                        _json_safe(_params_dict(row.params)),
                        sort_keys=True,
                    ),
                }
            )
    return str(output_path.resolve())


def _summary_payload(
    *,
    config: OptimizationConfig,
    summary: OptimizationSummary,
) -> dict[str, Any]:
    best_row = summary.best_row
    return {
        "name": summary.name,
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
    *,
    config: OptimizationConfig,
    summary: OptimizationSummary,
    output_path: Path,
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
    *,
    rows: Sequence[OptimizationLeaderboardRow],
    holdout_enabled: bool,
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


def run_parameter_optimization(
    config: OptimizationConfig,
    *,
    evaluator: BacktestEvaluator | None = None,
) -> OptimizationSummary:
    candidate_pool = _parameter_candidates(config.parameter_grid)
    sampled_params = _sample_parameter_sets(config)

    train_evaluations_by_trial: dict[int, tuple[_WindowEvaluation, ...]] = {}
    train_rows: dict[int, OptimizationLeaderboardRow] = {}
    for trial_id, params in enumerate(sampled_params, start=1):
        train_evaluations = tuple(
            _evaluate_window(
                config=config,
                evaluator=evaluator,
                trial_id=trial_id,
                params=params,
                window=window,
            )
            for window in config.train_windows
        )
        train_evaluations_by_trial[trial_id] = train_evaluations
        train_rows[trial_id] = _build_leaderboard_row(
            trial_id=trial_id,
            params=params,
            train_evaluations=train_evaluations,
        )

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
    summary = OptimizationSummary(
        name=config.name,
        objective_name="risk_adjusted_score",
        candidate_pool_size=len(candidate_pool),
        evaluated_trials=len(sampled_params),
        train_window_names=tuple(window.name for window in config.train_windows),
        holdout_window_names=tuple(window.name for window in config.holdout_windows),
        best_row=best_row,
        selected_params=best_row.params,
        leaderboard=tuple(final_rows),
        leaderboard_csv_path=resolved_leaderboard_csv_path,
        summary_json_path=resolved_summary_json_path,
    )
    _write_leaderboard_csv(
        rows=summary.leaderboard,
        output_path=leaderboard_csv_path,
    )
    _write_summary_json(
        config=config,
        summary=summary,
        output_path=summary_json_path,
    )

    print()
    print(
        f"Optimization complete for {config.name}: "
        f"evaluated {summary.evaluated_trials} of {summary.candidate_pool_size} parameter combinations."
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
    "OptimizationConfig",
    "OptimizationLeaderboardRow",
    "OptimizationSummary",
    "OptimizationWindow",
    "SEARCH_PLACEHOLDER_PREFIX",
    "run_parameter_optimization",
]
