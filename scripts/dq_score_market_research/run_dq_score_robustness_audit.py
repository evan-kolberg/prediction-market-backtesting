from __future__ import annotations

import importlib.util
import json
import math
import sys
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
RESEARCH_PATH = ROOT / "research_dq_score_market.py"
SEED = 20260817
BOOTSTRAP_REPEATS = 5_000


def load_research_module() -> Any:
    spec = importlib.util.spec_from_file_location("dq_market_research", RESEARCH_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {RESEARCH_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if not np.isfinite(value) else float(value)
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def add_tweak_signals(frame: pd.DataFrame, research: Any) -> tuple[pd.DataFrame, dict[str, Any]]:
    output = frame.copy()
    specs: dict[str, Any] = {}
    base = {
        "process": "dq2_process_hgb_l15_r10_surprise_ewm5",
        "outcome": "dq2_outcome_hgb_l15_r10_surprise_ewm10",
        "epa": "baseline_cumulative_epa",
        "market": "dq2_market_hgb_15m_l7_r5",
    }
    build = output["epoch"].isin(research.BUILD_EPOCHS)
    z_columns: dict[str, str] = {}
    for label, column in base.items():
        values = pd.to_numeric(output[column], errors="coerce")
        build_values = values.loc[build].dropna()
        center = float(build_values.median())
        scale = float((build_values - center).abs().median() * 1.4826)
        if not np.isfinite(scale) or scale <= 1e-9:
            scale = float(build_values.std())
        z_column = f"audit_z_{label}"
        output[z_column] = (values - center) / scale
        z_columns[label] = z_column

    combinations = {
        "tweak_process_outcome_ensemble": ("process", "outcome"),
        "tweak_process_epa_ensemble": ("process", "epa"),
        "tweak_outcome_epa_ensemble": ("outcome", "epa"),
        "tweak_process_outcome_epa_ensemble": ("process", "outcome", "epa"),
    }
    for signal_id, labels in combinations.items():
        columns = [z_columns[label] for label in labels]
        output[signal_id] = output[columns].mean(axis=1, skipna=False)
        specs[signal_id] = research.SignalSpec(
            signal_id=signal_id,
            family="DQ2_TWEAK_ENSEMBLE",
            description=f"Build-only robust-z ensemble of {', '.join(labels)}",
            inputs=",".join(base[label] for label in labels),
            information_horizon="retrospective_cv_plus_60s_market",
        )

    for left, right in (("process", "outcome"), ("outcome", "epa")):
        signal_id = f"tweak_{left}_{right}_agreement"
        left_values = output[z_columns[left]]
        right_values = output[z_columns[right]]
        agreement = np.sign(left_values).eq(np.sign(right_values))
        output[signal_id] = ((left_values + right_values) / 2.0).where(agreement)
        specs[signal_id] = research.SignalSpec(
            signal_id=signal_id,
            family="DQ2_TWEAK_AGREEMENT",
            description=f"{left}/{right} ensemble active only when signs agree",
            inputs=f"{base[left]},{base[right]}",
            information_horizon="retrospective_cv_plus_60s_market",
        )

    home_fair = pd.to_numeric(output["home_entry_fair"], errors="coerce").clip(0, 1)
    uncertainty = 4.0 * home_fair * (1.0 - home_fair)
    for label in ("market", "outcome", "process"):
        signal_id = f"tweak_{label}_uncertainty_scaled"
        output[signal_id] = output[z_columns[label]] * uncertainty
        specs[signal_id] = research.SignalSpec(
            signal_id=signal_id,
            family="DQ2_TWEAK_PRICE_AWARE",
            description=f"Build-only robust-z {label} score scaled toward liquid 50/50 states",
            inputs=f"{base[label]},home_entry_fair",
            information_horizon="retrospective_cv_plus_60s_market",
        )

    remaining_scale = np.sqrt(
        pd.to_numeric(output["remaining_minutes"], errors="coerce").clip(0, 60) / 60.0
    )
    signal_id = "tweak_outcome_remaining_time_scaled"
    output[signal_id] = output[z_columns["outcome"]] * remaining_scale
    specs[signal_id] = research.SignalSpec(
        signal_id=signal_id,
        family="DQ2_TWEAK_TIME_AWARE",
        description="Outcome-aware DQ surprise shrunk as the game approaches expiration",
        inputs=f"{base['outcome']},remaining_minutes",
        information_horizon="retrospective_cv_plus_60s_market",
    )
    return output, specs


def select_tweak_finalists(search: pd.DataFrame) -> pd.DataFrame:
    eligible = search.loc[search["eligible_for_ranking"]].sort_values(
        ["median_weekly_score", "mean_weekly_score"], ascending=False
    )
    selected = [eligible.drop_duplicates("signal_id").head(12)]
    selected.append(eligible.groupby("horizon", sort=False).head(3))
    selected.append(eligible.groupby(["family", "horizon"], sort=False).head(1))
    return (
        pd.concat(selected, ignore_index=True)
        .drop_duplicates(["signal_id", "threshold_quantile", "orientation", "horizon"])
        .reset_index(drop=True)
    )


def candidate_configs(
    base_finalists: pd.DataFrame,
    base_results: pd.DataFrame,
    tweak_finalists: pd.DataFrame,
    tweak_results: pd.DataFrame,
) -> pd.DataFrame:
    confirmation_name = "REG_WEEKS_14_18_CONFIRMATION"
    base_confirmation = base_results.loc[base_results["period"].eq(confirmation_name)]
    keep = base_confirmation.loc[
        base_confirmation["trades"].ge(20)
        & base_confirmation["games"].ge(10)
        & base_confirmation["mean_return"].gt(0)
    ]
    required_ids = {
        "p11_v2_exact",
        "baseline_cumulative_epa",
        "baseline_current_dq_cumulative",
        "baseline_net_yards_007",
    }
    keep = pd.concat(
        [keep, base_confirmation.loc[base_confirmation["signal_id"].isin(required_ids)]],
        ignore_index=True,
    )
    tweak_confirmation = tweak_results.loc[
        tweak_results["period"].eq(confirmation_name)
        & tweak_results["trades"].ge(20)
        & tweak_results["games"].ge(10)
        & tweak_results["mean_return"].gt(0)
    ]
    keep = pd.concat([keep, tweak_confirmation], ignore_index=True)
    keys = ["signal_id", "threshold_quantile", "orientation", "horizon"]
    keep = keep.drop_duplicates(keys)
    source = pd.concat([base_finalists, tweak_finalists], ignore_index=True)
    columns = ["signal_id", "family", "threshold_quantile", "threshold", "orientation", "horizon"]
    return keep[keys].merge(source[columns].drop_duplicates(keys), on=keys, how="left")


def evaluate_scenario(
    research: Any,
    frame: pd.DataFrame,
    configs: pd.DataFrame,
    scenario: str,
    latency_seconds: int,
    friction_bps: float,
    min_price: float,
    max_price: float,
    max_spread: float,
) -> pd.DataFrame:
    original = (
        research.MIN_ENTRY_PRICE,
        research.MAX_ENTRY_PRICE,
        research.MAX_SPREAD,
    )
    research.MIN_ENTRY_PRICE = min_price
    research.MAX_ENTRY_PRICE = max_price
    research.MAX_SPREAD = max_spread
    rows: list[dict[str, Any]] = []
    try:
        for period, epochs in (
            ("REG_WEEKS_14_18_CONFIRMATION", research.CONFIRMATION_EPOCHS),
            ("POSTSEASON_FINAL_AUDIT", research.FINAL_AUDIT_EPOCHS),
        ):
            period_frame = frame.loc[frame["epoch"].isin(epochs)]
            for config in configs.itertuples(index=False):
                trades = research.build_trades(
                    period_frame,
                    config.signal_id,
                    float(config.threshold),
                    config.horizon,
                    int(config.orientation),
                    friction_bps=friction_bps,
                    exact_non_overlap=True,
                )
                rows.append(
                    {
                        "scenario": scenario,
                        "latency_seconds": latency_seconds,
                        "friction_bps": friction_bps,
                        "min_entry_price": min_price,
                        "max_entry_price": max_price,
                        "max_spread": max_spread,
                        "period": period,
                        "signal_id": config.signal_id,
                        "family": config.family,
                        "threshold_quantile": config.threshold_quantile,
                        "threshold": config.threshold,
                        "orientation": int(config.orientation),
                        "horizon": config.horizon,
                        **research.summarize_trades(trades),
                        **research.weekly_objective(trades, epochs),
                    }
                )
    finally:
        (
            research.MIN_ENTRY_PRICE,
            research.MAX_ENTRY_PRICE,
            research.MAX_SPREAD,
        ) = original
    return pd.DataFrame(rows)


def refresh_quotes(research: Any, frame: pd.DataFrame, latency_seconds: int) -> pd.DataFrame:
    with duckdb.connect(str(research.DB_PATH), read_only=True) as connection:
        quotes = research.attach_market_quotes(connection, frame, latency_seconds)
    overlapping = [
        column for column in quotes.columns if column != "core_play_id" and column in frame.columns
    ]
    return frame.drop(columns=overlapping).merge(
        quotes, on="core_play_id", how="inner", validate="one_to_one"
    )


def cluster_bootstrap(trades: pd.DataFrame, rng: np.random.Generator) -> dict[str, Any]:
    if trades.empty:
        return {
            "bootstrap_mean_return_p025": np.nan,
            "bootstrap_mean_return_p50": np.nan,
            "bootstrap_mean_return_p975": np.nan,
            "bootstrap_probability_positive": np.nan,
            "top_game_abs_pnl_share": np.nan,
            "leave_one_game_out_min_pnl": np.nan,
            "leave_one_game_out_max_pnl": np.nan,
        }
    game = trades.groupby("core_game_id", sort=False).agg(
        pnl=("pnl", "sum"), trades=("pnl", "size")
    )
    pnl = game["pnl"].to_numpy(dtype=float)
    count = game["trades"].to_numpy(dtype=float)
    indices = rng.integers(0, len(game), size=(BOOTSTRAP_REPEATS, len(game)))
    sampled_pnl = pnl[indices].sum(axis=1)
    sampled_count = count[indices].sum(axis=1)
    sampled_return = sampled_pnl / (sampled_count * 10.0)
    total_pnl = float(pnl.sum())
    leave_one_out = total_pnl - pnl
    abs_sum = float(np.abs(pnl).sum())
    return {
        "bootstrap_mean_return_p025": float(np.quantile(sampled_return, 0.025)),
        "bootstrap_mean_return_p50": float(np.quantile(sampled_return, 0.5)),
        "bootstrap_mean_return_p975": float(np.quantile(sampled_return, 0.975)),
        "bootstrap_probability_positive": float((sampled_return > 0).mean()),
        "top_game_abs_pnl_share": float(np.abs(pnl).max() / abs_sum) if abs_sum > 0 else np.nan,
        "leave_one_game_out_min_pnl": float(leave_one_out.min()),
        "leave_one_game_out_max_pnl": float(leave_one_out.max()),
    }


def bootstrap_candidates(research: Any, frame: pd.DataFrame, configs: pd.DataFrame) -> pd.DataFrame:
    rng = np.random.default_rng(SEED)
    rows: list[dict[str, Any]] = []
    periods = (
        ("REG_WEEKS_14_18_CONFIRMATION", research.CONFIRMATION_EPOCHS),
        ("POSTSEASON_FINAL_AUDIT", research.FINAL_AUDIT_EPOCHS),
        (
            "CONFIRMATION_PLUS_FINAL",
            research.CONFIRMATION_EPOCHS | research.FINAL_AUDIT_EPOCHS,
        ),
    )
    for config in configs.itertuples(index=False):
        for period, epochs in periods:
            trades = research.build_trades(
                frame.loc[frame["epoch"].isin(epochs)],
                config.signal_id,
                float(config.threshold),
                config.horizon,
                int(config.orientation),
                exact_non_overlap=True,
            )
            price_bucket = pd.cut(
                trades["entry_price"],
                bins=[0.0, 0.15, 0.35, 0.65, 0.85, 1.0],
                labels=["0-.15", ".15-.35", ".35-.65", ".65-.85", ".85-1"],
                include_lowest=True,
            )
            bucket_pnl = (
                trades.assign(price_bucket=price_bucket)
                .groupby("price_bucket", observed=True)["pnl"]
                .sum()
            )
            rows.append(
                {
                    "period": period,
                    "signal_id": config.signal_id,
                    "family": config.family,
                    "threshold_quantile": config.threshold_quantile,
                    "orientation": int(config.orientation),
                    "horizon": config.horizon,
                    **research.summarize_trades(trades),
                    **cluster_bootstrap(trades, rng),
                    "pnl_by_entry_price_bucket": json.dumps(
                        {str(key): float(value) for key, value in bucket_pnl.items()},
                        sort_keys=True,
                    ),
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    research = load_research_module()
    panel = pd.read_parquet(ROOT / "dq_market_panel_stage1.parquet")
    # Build only the new specifications here; existing inventory is retained in its
    # original artifact and does not need to be reconstructed for the tweak screen.
    panel, tweak_specs = add_tweak_signals(panel, research)
    tweak_search = research.broad_vectorized_search(panel, tweak_specs)
    tweak_search.to_csv(ROOT / "tweak_vectorized_search_results.csv", index=False)
    tweak_finalists = select_tweak_finalists(tweak_search)
    tweak_finalists.to_csv(ROOT / "tweak_selected_configs.csv", index=False)
    tweak_confirmation, tweak_confirmation_trades = research.evaluate_finalists(
        panel,
        tweak_finalists,
        "REG_WEEKS_14_18_CONFIRMATION",
        research.CONFIRMATION_EPOCHS,
    )
    tweak_final, tweak_final_trades = research.evaluate_finalists(
        panel,
        tweak_finalists,
        "POSTSEASON_FINAL_AUDIT",
        research.FINAL_AUDIT_EPOCHS,
    )
    tweak_results = pd.concat([tweak_confirmation, tweak_final], ignore_index=True)
    tweak_trades = pd.concat([tweak_confirmation_trades, tweak_final_trades], ignore_index=True)
    tweak_results.to_csv(ROOT / "tweak_oos_results.csv", index=False)
    tweak_trades.to_parquet(ROOT / "tweak_oos_trades.parquet", index=False)

    base_finalists = pd.read_csv(ROOT / "selected_finalist_configs.csv")
    base_results = pd.read_csv(ROOT / "finalist_oos_results.csv")
    configs = candidate_configs(base_finalists, base_results, tweak_finalists, tweak_results)
    configs.to_csv(ROOT / "robustness_candidate_configs.csv", index=False)

    scenario_frames: list[pd.DataFrame] = []
    scenarios = (
        ("BASE_60S_25BPS", 25.0, 0.05, 0.95, 0.10),
        ("ZERO_FEE", 0.0, 0.05, 0.95, 0.10),
        ("FEE_50BPS", 50.0, 0.05, 0.95, 0.10),
        ("FEE_100BPS", 100.0, 0.05, 0.95, 0.10),
        ("PRICE_10_90", 25.0, 0.10, 0.90, 0.10),
        ("PRICE_15_85", 25.0, 0.15, 0.85, 0.10),
        ("SPREAD_MAX_05", 25.0, 0.05, 0.95, 0.05),
    )
    for name, friction, min_price, max_price, spread in scenarios:
        scenario_frames.append(
            evaluate_scenario(
                research,
                panel,
                configs,
                name,
                60,
                friction,
                min_price,
                max_price,
                spread,
            )
        )
    for latency in (120, 180):
        latency_panel = refresh_quotes(research, panel, latency)
        scenario_frames.append(
            evaluate_scenario(
                research,
                latency_panel,
                configs,
                f"LATENCY_{latency}S",
                latency,
                25.0,
                0.05,
                0.95,
                0.10,
            )
        )
    sensitivity = pd.concat(scenario_frames, ignore_index=True)
    sensitivity.to_csv(ROOT / "robustness_sensitivity_results.csv", index=False)

    bootstrap = bootstrap_candidates(research, panel, configs)
    bootstrap.to_csv(ROOT / "robustness_cluster_bootstrap.csv", index=False)
    manifest = {
        "bootstrap_repeats": BOOTSTRAP_REPEATS,
        "candidate_configs": len(configs),
        "database_path": str(research.DB_PATH),
        "database_read_only": True,
        "scenarios": [name for name, *_ in scenarios] + ["LATENCY_120S", "LATENCY_180S"],
        "tweak_signals": len(tweak_specs),
        "tweak_configurations_screened": len(tweak_search),
        "tweak_finalists": len(tweak_finalists),
        "warning": "CV annotation availability timestamps are absent; all CV-derived results remain retrospective/non-causal.",
    }
    (ROOT / "robustness_manifest.json").write_text(
        json.dumps(json_safe(manifest), indent=2, sort_keys=True), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
