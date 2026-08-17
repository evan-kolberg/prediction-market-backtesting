from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
RESEARCH_PATH = ROOT / "research_dq_score_market.py"
TWEAK_PATH = ROOT / "run_dq_score_robustness_audit.py"
SEED = 20260817
FIRST_TEST_EPOCH = 12
LAST_TEST_EPOCH = 22
VALIDATION_WEEKS = 3
TOP_APPROXIMATE_PER_CLASS = 8
BOOTSTRAP_REPEATS = 5_000
NESTED_THRESHOLD_QUANTILES = (0.70, 0.80, 0.90)


def load_module(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def model_class(family: str) -> str:
    mapping = {
        "P11_V2_EXACT": "P11_EXACT",
        "P11_V2_GRID": "P11_GRID",
        "BASELINE": "CANONICAL_BASELINES",
        "RECENCY_BASELINE": "RECENCY_BASELINES",
        "PRIOR_SQ_EPA": "PRIOR_SQ_EPA",
        "DQ2_EPA_RIDGE": "DQ2_EPA_LINEAR",
        "DQ2_EPA_ELASTIC": "DQ2_EPA_LINEAR",
        "DQ2_EPA_HGB": "DQ2_EPA_HGB",
        "DQ2_MARKET_RIDGE": "DQ2_MARKET_LINEAR",
        "DQ2_MARKET_ELASTIC": "DQ2_MARKET_LINEAR",
        "DQ2_MARKET_HGB": "DQ2_MARKET_HGB",
        "DQ2_WINPROB_RESIDUAL": "DQ2_WINPROB",
        "DQ2_TWEAK_ENSEMBLE": "DQ2_TWEAKS",
        "DQ2_TWEAK_AGREEMENT": "DQ2_TWEAKS",
        "DQ2_TWEAK_PRICE_AWARE": "DQ2_TWEAKS",
        "DQ2_TWEAK_TIME_AWARE": "DQ2_TWEAKS",
    }
    return mapping.get(family, family)


def load_specs(research: Any) -> dict[str, Any]:
    inventory = pd.read_csv(ROOT / "signal_inventory_stage1.csv").fillna("")
    all_specs = {
        row.signal_id: research.SignalSpec(
            signal_id=row.signal_id,
            family=row.family,
            description=row.description,
            inputs=row.inputs,
            information_horizon=row.information_horizon,
        )
        for row in inventory.itertuples(index=False)
    }
    fixed = {
        "p11_v2_exact",
        "p11_cvw0_k0_c0.07_tp11_cosine",
        "p11_cvw0.5_k0_c0.07_tp11_cosine",
        "p11_cvw1_k0_c0.07_tp11_cosine",
        "p11_cvw0.5_k3_c0.1_tp11_cosine",
        "p11_cvw0.5_k3_c0.1_telapsed_sqrt",
        "baseline_net_yards_007",
        "baseline_current_dq_cumulative",
        "baseline_cumulative_epa",
        "recent_net_yards_ewm20",
        "recent_epa_ewm20",
        "recent_dq_play_ewm20",
        "prior_sq_process_adjustment_ewm5",
        "dq2_winprob_market_only_c0.1",
        "dq2_winprob_market_plus_dq_c0.1",
    }
    learned_hgb = {
        f"dq2_{feature_set}_hgb_{size}_surprise_ewm{span}"
        for feature_set in ("process", "outcome")
        for size in ("l7_r5", "l15_r10")
        for span in (5, 10)
    }
    direct_hgb = {
        f"dq2_market_hgb_{horizon}_{size}"
        for horizon in ("15m", "30m")
        for size in ("l7_r5", "l15_r10", "l31_r20")
    }
    keep = fixed | learned_hgb | direct_hgb
    missing = sorted(keep - set(all_specs))
    if missing:
        raise RuntimeError(f"Nested shortlist signals missing from inventory: {missing}")
    return {signal_id: all_specs[signal_id] for signal_id in sorted(keep)}


def exact_validation_selection(
    research: Any,
    panel: pd.DataFrame,
    search: pd.DataFrame,
    validation_epochs: set[int],
) -> pd.DataFrame:
    eligible = search.loc[search["eligible_for_ranking"]].copy()
    eligible["model_class"] = eligible["family"].map(model_class)
    approximate = (
        eligible.sort_values(["median_weekly_score", "mean_weekly_score"], ascending=False)
        .groupby("model_class", sort=False)
        .head(TOP_APPROXIMATE_PER_CLASS)
    )
    validation = panel.loc[panel["epoch"].isin(validation_epochs)]
    rows: list[dict[str, Any]] = []
    for row in approximate.itertuples(index=False):
        trades = research.build_trades(
            validation,
            row.signal_id,
            float(row.threshold),
            row.horizon,
            int(row.orientation),
            exact_non_overlap=True,
        )
        summary = research.summarize_trades(trades)
        weekly = research.weekly_objective(trades, validation_epochs)
        rows.append(
            {
                "model_class": row.model_class,
                "signal_id": row.signal_id,
                "family": row.family,
                "threshold_quantile": row.threshold_quantile,
                "threshold": row.threshold,
                "orientation": int(row.orientation),
                "horizon": row.horizon,
                **{f"validation_{key}": value for key, value in summary.items()},
                **{f"validation_{key}": value for key, value in weekly.items()},
            }
        )
    result = pd.DataFrame(rows)
    result["exact_eligible"] = result["validation_trades"].ge(12) & result["validation_games"].ge(7)
    return (
        result.loc[result["exact_eligible"]]
        .sort_values(
            ["validation_median_weekly_score", "validation_mean_weekly_score"],
            ascending=False,
        )
        .groupby("model_class", sort=False)
        .head(1)
        .reset_index(drop=True)
    )


def bootstrap_summary(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {}
    game = trades.groupby("core_game_id", sort=False).agg(
        pnl=("pnl", "sum"), trades=("pnl", "size")
    )
    pnl = game["pnl"].to_numpy(dtype=float)
    count = game["trades"].to_numpy(dtype=float)
    rng = np.random.default_rng(SEED)
    indices = rng.integers(0, len(game), size=(BOOTSTRAP_REPEATS, len(game)))
    returns = pnl[indices].sum(axis=1) / (10.0 * count[indices].sum(axis=1))
    return {
        "bootstrap_mean_return_p025": float(np.quantile(returns, 0.025)),
        "bootstrap_mean_return_p50": float(np.quantile(returns, 0.5)),
        "bootstrap_mean_return_p975": float(np.quantile(returns, 0.975)),
        "bootstrap_probability_positive": float((returns > 0).mean()),
    }


def main() -> None:
    research = load_module("dq_market_research_nested", RESEARCH_PATH)
    tweaks = load_module("dq_market_robustness_nested", TWEAK_PATH)
    panel = pd.read_parquet(ROOT / "dq_market_panel_stage1.parquet")
    specs = load_specs(research)
    panel, tweak_specs = tweaks.add_tweak_signals(panel, research)
    specs.update(tweak_specs)
    original_build = research.BUILD_EPOCHS
    original_selection = research.SELECTION_EPOCHS
    original_quantiles = research.THRESHOLD_QUANTILES
    selection_rows: list[pd.DataFrame] = []
    trade_rows: list[pd.DataFrame] = []
    try:
        for test_epoch in range(FIRST_TEST_EPOCH, LAST_TEST_EPOCH + 1):
            validation_epochs = set(range(test_epoch - VALIDATION_WEEKS, test_epoch))
            calibration_epochs = set(range(6, test_epoch - VALIDATION_WEEKS))
            research.BUILD_EPOCHS = calibration_epochs
            research.SELECTION_EPOCHS = validation_epochs
            research.THRESHOLD_QUANTILES = NESTED_THRESHOLD_QUANTILES
            print(
                f"nested test epoch {test_epoch}: screening {len(specs)} signals",
                flush=True,
            )
            search = research.broad_vectorized_search(panel, specs)
            selected = exact_validation_selection(research, panel, search, validation_epochs)
            selected.insert(0, "test_epoch", test_epoch)
            selected.insert(1, "calibration_epochs", ",".join(map(str, sorted(calibration_epochs))))
            selected.insert(2, "validation_epochs", ",".join(map(str, sorted(validation_epochs))))
            test = panel.loc[panel["epoch"].eq(test_epoch)]
            test_summaries: list[dict[str, Any]] = []
            for config in selected.itertuples(index=False):
                trades = research.build_trades(
                    test,
                    config.signal_id,
                    float(config.threshold),
                    config.horizon,
                    int(config.orientation),
                    exact_non_overlap=True,
                )
                summary = research.summarize_trades(trades)
                test_summaries.append(
                    {
                        "model_class": config.model_class,
                        **{f"test_{key}": value for key, value in summary.items()},
                    }
                )
                if not trades.empty:
                    trade_rows.append(
                        trades.assign(
                            test_epoch=test_epoch,
                            model_class=config.model_class,
                            signal_id=config.signal_id,
                            family=config.family,
                            threshold_quantile=config.threshold_quantile,
                            threshold=config.threshold,
                            orientation=int(config.orientation),
                            horizon=config.horizon,
                        )
                    )
            if test_summaries:
                selected = selected.merge(
                    pd.DataFrame(test_summaries), on="model_class", how="left"
                )
            selection_rows.append(selected)
            print(
                f"nested test epoch {test_epoch}: selected {len(selected)} model classes",
                flush=True,
            )
    finally:
        research.BUILD_EPOCHS = original_build
        research.SELECTION_EPOCHS = original_selection
        research.THRESHOLD_QUANTILES = original_quantiles
    selections = pd.concat(selection_rows, ignore_index=True)
    trades = pd.concat(trade_rows, ignore_index=True) if trade_rows else pd.DataFrame()
    selections.to_csv(ROOT / "nested_weekly_selections.csv", index=False)
    trades.to_parquet(ROOT / "nested_weekly_trades.parquet", index=False)
    summaries: list[dict[str, Any]] = []
    for model_name, model_trades in trades.groupby("model_class", sort=False):
        model_trades = model_trades.sort_values("entry_us")
        summaries.append(
            {
                "model_class": model_name,
                "test_epochs": model_trades["test_epoch"].nunique(),
                **research.summarize_trades(model_trades),
                **bootstrap_summary(model_trades),
                "positive_test_weeks": int(
                    model_trades.groupby("test_epoch")["pnl"].sum().gt(0).sum()
                ),
                "total_test_weeks": LAST_TEST_EPOCH - FIRST_TEST_EPOCH + 1,
            }
        )
    pd.DataFrame(summaries).sort_values("mean_return", ascending=False).to_csv(
        ROOT / "nested_weekly_summary.csv", index=False
    )
    manifest = {
        "bootstrap_repeats": BOOTSTRAP_REPEATS,
        "first_test_epoch": FIRST_TEST_EPOCH,
        "last_test_epoch": LAST_TEST_EPOCH,
        "signals_screened_per_epoch": len(specs),
        "threshold_quantiles": list(NESTED_THRESHOLD_QUANTILES),
        "selection_policy": (
            "thresholds from epochs before the trailing 3-week validation; "
            "top approximate candidates reranked with exact non-overlap; "
            "one configuration per model class executed on the next week"
        ),
        "warning": (
            "The formulas were researched on this season and CV publication timestamps "
            "are missing. This reduces but does not eliminate researcher/look-ahead bias."
        ),
    }
    (ROOT / "nested_weekly_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
