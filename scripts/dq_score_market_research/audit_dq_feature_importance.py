from __future__ import annotations

import importlib.util
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.pipeline import Pipeline


ROOT = Path(__file__).resolve().parent
TRAIN_PATH = ROOT / "train_dq_score_learned_models.py"
PANEL_PATH = ROOT / "dq_market_panel_stage1.parquet"
SEED = 20260817
BUILD_EPOCHS = set(range(6, 11))
SELECTION_EPOCHS = set(range(11, 14))


def load_training_module() -> Any:
    spec = importlib.util.spec_from_file_location("dq_learned_training", TRAIN_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to import {TRAIN_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def make_model(leaves: int = 15, l2: float = 10.0) -> Pipeline:
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
            (
                "model",
                HistGradientBoostingRegressor(
                    learning_rate=0.05,
                    max_iter=150,
                    max_leaf_nodes=leaves,
                    l2_regularization=l2,
                    random_state=SEED,
                ),
            ),
        ]
    )


def dedupe_market(frame: pd.DataFrame) -> pd.DataFrame:
    return (
        frame.sort_values(["core_game_id", "decision_proxy_us"])
        .groupby(["core_game_id", "five_minute_bucket"], sort=False)
        .tail(1)
    )


def metric_row(
    model_id: str,
    target: str,
    feature_set: str,
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: list[str],
    model: Pipeline,
) -> dict[str, Any]:
    prediction = model.predict(test[features])
    rmse = math.sqrt(mean_squared_error(test[target], prediction))
    return {
        "model_id": model_id,
        "target": target,
        "feature_set": feature_set,
        "train_rows": len(train),
        "test_rows": len(test),
        "features": len(features),
        "mae": float(mean_absolute_error(test[target], prediction)),
        "rmse": float(rmse),
        "r2": float(r2_score(test[target], prediction)),
        "target_mean": float(test[target].mean()),
        "target_std": float(test[target].std()),
    }


def individual_importance(
    model_id: str,
    model: Pipeline,
    test: pd.DataFrame,
    target: str,
    features: list[str],
) -> list[dict[str, Any]]:
    result = permutation_importance(
        model,
        test[features],
        test[target],
        scoring="neg_mean_squared_error",
        n_repeats=12,
        random_state=SEED,
        n_jobs=1,
    )
    return [
        {
            "model_id": model_id,
            "feature": feature,
            "mse_increase_mean": float(result.importances_mean[index]),
            "mse_increase_std": float(result.importances_std[index]),
        }
        for index, feature in enumerate(features)
    ]


def grouped_importance(
    model_id: str,
    model: Pipeline,
    test: pd.DataFrame,
    target: str,
    features: list[str],
    groups: dict[str, list[str]],
) -> list[dict[str, Any]]:
    rng = np.random.default_rng(SEED)
    base_prediction = model.predict(test[features])
    base_mse = mean_squared_error(test[target], base_prediction)
    rows: list[dict[str, Any]] = []
    for group_name, group_features in groups.items():
        selected = [feature for feature in group_features if feature in features]
        increases: list[float] = []
        for _ in range(20):
            shuffled = test[features].copy()
            order = rng.permutation(len(shuffled))
            shuffled.loc[:, selected] = shuffled[selected].to_numpy()[order]
            prediction = model.predict(shuffled)
            increases.append(float(mean_squared_error(test[target], prediction) - base_mse))
        rows.append(
            {
                "model_id": model_id,
                "feature_group": group_name,
                "features": ",".join(selected),
                "mse_increase_mean": float(np.mean(increases)),
                "mse_increase_std": float(np.std(increases, ddof=1)),
            }
        )
    return rows


def fit_and_audit(
    panel: pd.DataFrame,
    model_id: str,
    target: str,
    feature_set: str,
    features: list[str],
    groups: dict[str, list[str]],
    market_dedupe: bool = False,
    leaves: int = 15,
    l2: float = 10.0,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    train = panel.loc[panel["epoch"].isin(BUILD_EPOCHS)].dropna(subset=[target]).copy()
    test = panel.loc[panel["epoch"].isin(SELECTION_EPOCHS)].dropna(subset=[target]).copy()
    if market_dedupe:
        train = dedupe_market(train)
        test = dedupe_market(test)
    train = train.loc[train[features].notna().any(axis=1)]
    test = test.loc[test[features].notna().any(axis=1)]
    model = make_model(leaves=leaves, l2=l2)
    model.fit(train[features], train[target])
    metrics = metric_row(model_id, target, feature_set, train, test, features, model)
    individual = individual_importance(model_id, model, test, target, features)
    grouped = grouped_importance(model_id, model, test, target, features, groups)
    return metrics, individual, grouped


def main() -> None:
    training = load_training_module()
    panel = training.add_model_features(pd.read_parquet(PANEL_PATH))
    epa_groups = {
        "pre_play_state": training.PRESTATE,
        "computer_vision_process": training.PROCESS_CV,
        "observed_play_outcome": training.OUTCOME,
    }
    p11_features = [feature for feature in training.DQ_MARKET if feature.startswith("p11_")]
    baseline_features = [
        feature
        for feature in training.DQ_MARKET
        if feature.startswith("baseline_") or feature.startswith("cum_")
    ]
    recency_features = [feature for feature in training.DQ_MARKET if feature.startswith("recent_")]
    prior_sq_features = [feature for feature in training.DQ_MARKET if feature.startswith("prior_")]
    market_groups = {
        "market_and_game_state": training.MARKET_STATE,
        "p11_dq": p11_features,
        "cumulative_outcome_baselines": baseline_features,
        "recent_form": recency_features,
        "prior_sq_models": prior_sq_features,
    }
    audits = (
        (
            "epa_prestate_hgb",
            "epa",
            "prestate",
            training.PRESTATE,
            epa_groups,
            False,
            15,
            10.0,
        ),
        (
            "epa_process_hgb",
            "epa",
            "process",
            training.PRESTATE + training.PROCESS_CV,
            epa_groups,
            False,
            15,
            10.0,
        ),
        (
            "epa_outcome_hgb",
            "epa",
            "outcome",
            training.PRESTATE + training.PROCESS_CV + training.OUTCOME,
            epa_groups,
            False,
            15,
            10.0,
        ),
        (
            "market_15m_state_only_hgb",
            "home_15m_fair_markout",
            "market_state",
            training.MARKET_STATE,
            market_groups,
            True,
            7,
            5.0,
        ),
        (
            "market_15m_plus_dq_hgb",
            "home_15m_fair_markout",
            "market_plus_dq",
            training.MARKET_STATE + training.DQ_MARKET,
            market_groups,
            True,
            7,
            5.0,
        ),
    )
    metrics: list[dict[str, Any]] = []
    individual: list[dict[str, Any]] = []
    grouped: list[dict[str, Any]] = []
    for args in audits:
        metric, feature_rows, group_rows = fit_and_audit(panel, *args)
        metrics.append(metric)
        individual.extend(feature_rows)
        grouped.extend(group_rows)
        print(f"audited {metric['model_id']}", flush=True)
    metric_frame = pd.DataFrame(metrics)
    metric_frame.to_csv(ROOT / "feature_ablation_metrics.csv", index=False)
    pd.DataFrame(individual).sort_values(
        ["model_id", "mse_increase_mean"], ascending=[True, False]
    ).to_csv(ROOT / "feature_permutation_importance.csv", index=False)
    pd.DataFrame(grouped).sort_values(
        ["model_id", "mse_increase_mean"], ascending=[True, False]
    ).to_csv(ROOT / "feature_group_importance.csv", index=False)
    summary = {
        "build_epochs": sorted(BUILD_EPOCHS),
        "selection_epochs": sorted(SELECTION_EPOCHS),
        "cv_availability": "UNKNOWN_RETROSPECTIVE_ONLY",
        "method": "fit on build weeks; permutation and ablation metrics on untouched selection weeks",
        "models": len(metrics),
    }
    (ROOT / "feature_importance_manifest.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
