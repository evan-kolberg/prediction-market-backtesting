from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNet, LogisticRegression, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler, StandardScaler


ROOT = Path(__file__).resolve().parent
PANEL_PATH = ROOT / "dq_market_panel_stage1.parquet"
OUTPUT_PATH = ROOT / "learned_signals.parquet"
SEED = 20260817
MIN_TRAIN_EPOCHS = 2


@dataclass(frozen=True)
class ModelRecipe:
    model_id: str
    family: str
    model: Any
    feature_set: str
    target: str


def log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {message}", flush=True)


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


def dump_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(json_safe(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def numeric_bool(frame: pd.DataFrame, column: str) -> pd.Series:
    return pd.to_numeric(frame[column], errors="coerce").astype(float)


def add_model_features(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["action_pass"] = df["observed_action"].eq("pass").astype(float)
    df["home_possession"] = df["home_offense"].astype(float)
    for column in [
        "is_under_pressure",
        "is_pressure_2yards",
        "is_pressure_3yards",
        "is_completion",
        "is_successful",
        "is_explosive",
        "is_turnover",
        "is_turnover_on_downs",
        "is_first_down",
        "is_tight_coverage",
        "is_open_target",
        "is_stuffed",
        "is_breakaway",
        "is_light_box",
        "is_stacked_box",
    ]:
        df[column] = numeric_bool(df, column)
    df["pressure_x_dropback_depth"] = df["is_under_pressure"] * df["qb_dropback_depth"]
    df["pocket_x_time_to_throw"] = df["pocket_quality"] * df["time_to_throw"]
    df["separation_x_depth"] = df["receiver_separation"] * df["depth_of_target"]
    df["log_ydstogo"] = np.log1p(pd.to_numeric(df["ydstogo"], errors="coerce").clip(0))
    df["score_margin_x_late"] = df["score_margin_home"] * df["elapsed_fraction"]
    home_fair = pd.to_numeric(df["home_entry_fair"], errors="coerce").clip(0.01, 0.99)
    df["market_logit"] = np.log(home_fair / (1.0 - home_fair))
    df["market_home_fair"] = home_fair
    df["home_15m_fair_markout"] = df["home_exit_15m_fair"] - home_fair
    df["home_30m_fair_markout"] = df["home_exit_30m_fair"] - home_fair
    elapsed_seconds = (df["decision_proxy_us"] - df["actual_start_ts_utc"] * 1_000_000) / 1_000_000
    df["five_minute_bucket"] = np.floor(elapsed_seconds.clip(lower=0) / 300).astype(int)
    return df


PRESTATE = [
    "action_pass",
    "down",
    "ydstogo",
    "log_ydstogo",
    "yardline_100",
    "remaining_minutes",
    "elapsed_fraction",
    "ep",
]
PROCESS_CV = [
    "time_to_throw",
    "qb_dropback_depth",
    "defender_distance_at_release",
    "is_under_pressure",
    "is_pressure_2yards",
    "is_pressure_3yards",
    "men_in_box",
    "pocket_quality",
    "pressure_x_dropback_depth",
    "pocket_x_time_to_throw",
    "depth_of_target",
    "receiver_separation",
    "defender_distance_at_catch",
    "is_tight_coverage",
    "is_open_target",
    "catch_quality",
    "separation_x_depth",
    "time_to_first_contact",
    "yards_before_contact",
    "defenders_in_radius_at_tackle",
    "rush_quality",
]
OUTCOME = [
    "play_yards",
    "is_completion",
    "is_successful",
    "is_explosive",
    "is_turnover",
    "is_turnover_on_downs",
    "is_first_down",
    "yards_after_catch",
    "yards_after_contact",
    "is_stuffed",
    "is_breakaway",
]
MARKET_STATE = [
    "market_logit",
    "market_home_fair",
    "home_entry_spread",
    "home_entry_imbalance",
    "away_entry_imbalance",
    "score_margin_home",
    "score_margin_x_late",
    "remaining_minutes",
    "elapsed_fraction",
    "home_possession",
    "down",
    "ydstogo",
    "yardline_100",
]
DQ_MARKET = [
    "p11_v2_exact",
    "p11_cvw0.5_k3_c0.1_tp11_cosine",
    "p11_cvw1_k0_c0.1_tp11_cosine",
    "baseline_net_yards_007",
    "baseline_current_dq_cumulative",
    "baseline_cumulative_epa",
    "cum_net_yards_margin",
    "cum_epa_margin",
    "cum_dq_play_margin",
    "recent_dq_play_ewm5",
    "recent_dq_play_ewm10",
    "recent_dq_play_ewm20",
    "recent_epa_ewm5",
    "recent_epa_ewm10",
    "recent_net_yards_ewm5",
    "recent_net_yards_ewm10",
    "prior_sq_adjustment_ewm5",
    "prior_process_surprise_ewm5",
    "prior_sq_process_adjustment_ewm5",
]


def linear_pipeline(model: Any, robust: bool = False) -> Pipeline:
    scaler = RobustScaler() if robust else StandardScaler()
    return Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
            ("scaler", scaler),
            ("model", model),
        ]
    )


def process_recipes() -> list[ModelRecipe]:
    recipes: list[ModelRecipe] = []
    for feature_set in ("process", "outcome"):
        for alpha in (0.1, 1.0, 10.0, 100.0):
            recipes.append(
                ModelRecipe(
                    f"dq2_{feature_set}_ridge_a{alpha:g}",
                    "DQ2_EPA_RIDGE",
                    linear_pipeline(Ridge(alpha=alpha)),
                    feature_set,
                    "epa",
                )
            )
        for alpha, l1_ratio in ((0.001, 0.1), (0.01, 0.1), (0.01, 0.5)):
            recipes.append(
                ModelRecipe(
                    f"dq2_{feature_set}_elastic_a{alpha:g}_l{l1_ratio:g}",
                    "DQ2_EPA_ELASTIC",
                    linear_pipeline(
                        ElasticNet(
                            alpha=alpha,
                            l1_ratio=l1_ratio,
                            max_iter=10_000,
                            random_state=SEED,
                        )
                    ),
                    feature_set,
                    "epa",
                )
            )
        for leaves, l2 in ((7, 5.0), (15, 10.0)):
            recipes.append(
                ModelRecipe(
                    f"dq2_{feature_set}_hgb_l{leaves}_r{l2:g}",
                    "DQ2_EPA_HGB",
                    Pipeline(
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
                    ),
                    feature_set,
                    "epa",
                )
            )
    return recipes


def market_recipes() -> list[ModelRecipe]:
    recipes: list[ModelRecipe] = []
    for target in ("home_15m_fair_markout", "home_30m_fair_markout"):
        suffix = "15m" if "15m" in target else "30m"
        for alpha in (0.1, 1.0, 10.0, 100.0):
            recipes.append(
                ModelRecipe(
                    f"dq2_market_ridge_{suffix}_a{alpha:g}",
                    "DQ2_MARKET_RIDGE",
                    linear_pipeline(Ridge(alpha=alpha)),
                    "market",
                    target,
                )
            )
        for alpha, l1_ratio in ((0.0001, 0.1), (0.001, 0.1), (0.001, 0.5)):
            recipes.append(
                ModelRecipe(
                    f"dq2_market_elastic_{suffix}_a{alpha:g}_l{l1_ratio:g}",
                    "DQ2_MARKET_ELASTIC",
                    linear_pipeline(
                        ElasticNet(
                            alpha=alpha,
                            l1_ratio=l1_ratio,
                            max_iter=10_000,
                            random_state=SEED,
                        )
                    ),
                    "market",
                    target,
                )
            )
        for leaves, l2 in ((7, 5.0), (15, 10.0), (31, 20.0)):
            recipes.append(
                ModelRecipe(
                    f"dq2_market_hgb_{suffix}_l{leaves}_r{l2:g}",
                    "DQ2_MARKET_HGB",
                    Pipeline(
                        [
                            ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
                            (
                                "model",
                                HistGradientBoostingRegressor(
                                    learning_rate=0.04,
                                    max_iter=150,
                                    max_leaf_nodes=leaves,
                                    l2_regularization=l2,
                                    random_state=SEED,
                                ),
                            ),
                        ]
                    ),
                    "market",
                    target,
                )
            )
    return recipes


def feature_columns(recipe: ModelRecipe) -> list[str]:
    if recipe.feature_set == "process":
        return PRESTATE + PROCESS_CV
    if recipe.feature_set == "outcome":
        return PRESTATE + PROCESS_CV + OUTCOME
    if recipe.feature_set == "market":
        return MARKET_STATE + DQ_MARKET
    if recipe.feature_set == "market_only":
        return MARKET_STATE
    raise ValueError(recipe.feature_set)


def expanding_predictions(
    df: pd.DataFrame,
    recipe: ModelRecipe,
    *,
    dedupe_market_training: bool = False,
) -> tuple[pd.Series, list[dict[str, Any]], Any]:
    predictions = pd.Series(np.nan, index=df.index, dtype=float)
    logs: list[dict[str, Any]] = []
    features = feature_columns(recipe)
    fitted = None
    epochs = sorted(int(value) for value in df["epoch"].unique())
    for epoch in epochs:
        earlier = [value for value in epochs if value < epoch]
        if len(earlier) < MIN_TRAIN_EPOCHS:
            continue
        train = df.loc[df["epoch"].isin(earlier)].copy()
        test = df.loc[df["epoch"].eq(epoch)].copy()
        train = train.dropna(subset=[recipe.target])
        test_valid = test[features].notna().any(axis=1)
        if dedupe_market_training:
            train = train.sort_values(["core_game_id", "decision_proxy_us"])
            train = train.groupby(["core_game_id", "five_minute_bucket"], sort=False).tail(1)
        if len(train) < 500 or not test_valid.any():
            continue
        model = recipe.model
        model.fit(train[features], train[recipe.target])
        predictions.loc[test.index[test_valid]] = model.predict(test.loc[test_valid, features])
        logs.append(
            {
                "model_id": recipe.model_id,
                "test_epoch": epoch,
                "train_rows": len(train),
                "test_rows": int(test_valid.sum()),
            }
        )
        fitted = model
    return predictions, logs, fitted


def add_process_signals(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    signal_data: dict[str, pd.Series] = {}
    inventory: list[dict[str, Any]] = []
    logs: list[dict[str, Any]] = []
    for index, recipe in enumerate(process_recipes(), start=1):
        prediction, model_logs, _ = expanding_predictions(df, recipe)
        logs.extend(model_logs)
        signed_prediction = pd.Series(
            np.where(df["home_offense"], prediction, -prediction), index=df.index
        )
        signed_surprise = pd.Series(
            np.where(df["home_offense"], prediction - df["epa"], -(prediction - df["epa"])),
            index=df.index,
        )
        grouped_keys = df["core_game_id"]
        cumulative_id = f"{recipe.model_id}_cum_margin"
        signal_data[cumulative_id] = signed_prediction.groupby(grouped_keys, sort=False).cumsum()
        inventory.append(
            {
                "signal_id": cumulative_id,
                "family": recipe.family,
                "description": f"Cumulative home-signed OOS EPA prediction from {recipe.model_id}",
                "inputs": ",".join(feature_columns(recipe)),
                "information_horizon": "POST_PLAY_RETROSPECTIVE_CV",
            }
        )
        for span in (5, 10, 20):
            process_id = f"{recipe.model_id}_process_ewm{span}"
            surprise_id = f"{recipe.model_id}_surprise_ewm{span}"
            signal_data[process_id] = signed_prediction.groupby(grouped_keys, sort=False).transform(
                lambda values: values.ewm(span=span, adjust=False, min_periods=3).mean()
            )
            signal_data[surprise_id] = signed_surprise.groupby(grouped_keys, sort=False).transform(
                lambda values: values.ewm(span=span, adjust=False, min_periods=3).mean()
            )
            inventory.extend(
                [
                    {
                        "signal_id": process_id,
                        "family": recipe.family,
                        "description": f"Recent home-signed OOS process EPA, span {span}",
                        "inputs": ",".join(feature_columns(recipe)),
                        "information_horizon": "POST_PLAY_RETROSPECTIVE_CV",
                    },
                    {
                        "signal_id": surprise_id,
                        "family": recipe.family,
                        "description": f"Recent process-minus-realized EPA, span {span}",
                        "inputs": ",".join(feature_columns(recipe)),
                        "information_horizon": "POST_PLAY_RETROSPECTIVE_CV",
                    },
                ]
            )
        if index % 5 == 0:
            log(f"EPA models: {index}/{len(process_recipes())}")
    return pd.concat([df, pd.DataFrame(signal_data, index=df.index)], axis=1), inventory, logs


def add_market_signals(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    signal_data: dict[str, pd.Series] = {}
    inventory: list[dict[str, Any]] = []
    logs: list[dict[str, Any]] = []
    fitted_models: dict[str, Any] = {}
    recipes = market_recipes()
    for index, recipe in enumerate(recipes, start=1):
        prediction, model_logs, fitted = expanding_predictions(
            df, recipe, dedupe_market_training=True
        )
        signal_data[recipe.model_id] = prediction
        logs.extend(model_logs)
        if fitted is not None:
            fitted_models[recipe.model_id] = fitted
        inventory.append(
            {
                "signal_id": recipe.model_id,
                "family": recipe.family,
                "description": f"Walk-forward direct prediction of {recipe.target}",
                "inputs": ",".join(feature_columns(recipe)),
                "information_horizon": "POST_PLAY_RETROSPECTIVE_CV_PLUS_MARKET",
            }
        )
        if index % 5 == 0:
            log(f"Market models: {index}/{len(recipes)}")
    return (
        pd.concat([df, pd.DataFrame(signal_data, index=df.index)], axis=1),
        inventory,
        logs,
        fitted_models,
    )


def add_outcome_residual_signals(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, list[dict[str, Any]], list[dict[str, Any]]]:
    signal_data: dict[str, pd.Series] = {}
    inventory: list[dict[str, Any]] = []
    logs: list[dict[str, Any]] = []
    epochs = sorted(int(value) for value in df["epoch"].unique())
    feature_sets = {
        "market_only": MARKET_STATE,
        "market_plus_dq": MARKET_STATE + DQ_MARKET,
    }
    for feature_name, features in feature_sets.items():
        for c_value in (0.01, 0.1, 1.0, 10.0):
            signal_id = f"dq2_winprob_{feature_name}_c{c_value:g}"
            prediction = pd.Series(np.nan, index=df.index, dtype=float)
            for epoch in epochs:
                earlier = [value for value in epochs if value < epoch]
                if len(earlier) < MIN_TRAIN_EPOCHS:
                    continue
                train = df.loc[df["epoch"].isin(earlier)].copy()
                test = df.loc[df["epoch"].eq(epoch)].copy()
                train = train.dropna(subset=["home_won"])
                if len(train) < 500 or train["home_won"].nunique() < 2:
                    continue
                per_game_count = train.groupby("core_game_id")["core_play_id"].transform("size")
                weights = 1.0 / per_game_count
                model = Pipeline(
                    [
                        (
                            "imputer",
                            SimpleImputer(strategy="median", add_indicator=True),
                        ),
                        ("scaler", StandardScaler()),
                        (
                            "model",
                            LogisticRegression(
                                C=c_value,
                                max_iter=2_000,
                                random_state=SEED,
                            ),
                        ),
                    ]
                )
                model.fit(train[features], train["home_won"], model__sample_weight=weights)
                prediction.loc[test.index] = model.predict_proba(test[features])[:, 1]
                logs.append(
                    {
                        "model_id": signal_id,
                        "test_epoch": epoch,
                        "train_rows": len(train),
                        "test_rows": len(test),
                    }
                )
            signal_data[signal_id] = prediction - df["market_home_fair"]
            inventory.append(
                {
                    "signal_id": signal_id,
                    "family": "DQ2_WINPROB_RESIDUAL",
                    "description": "Walk-forward model probability minus moneyline fair probability",
                    "inputs": ",".join(features),
                    "information_horizon": "POST_PLAY_RETROSPECTIVE_CV_PLUS_MARKET",
                }
            )
    return pd.concat([df, pd.DataFrame(signal_data, index=df.index)], axis=1), inventory, logs


def save_coefficients(
    df: pd.DataFrame, recipes: list[ModelRecipe], fitted_models: dict[str, Any]
) -> None:
    rows: list[dict[str, Any]] = []
    for recipe in recipes:
        model = fitted_models.get(recipe.model_id)
        if model is None or "model" not in model.named_steps:
            continue
        estimator = model.named_steps["model"]
        if not hasattr(estimator, "coef_"):
            continue
        imputer = model.named_steps["imputer"]
        base_features = feature_columns(recipe)
        indicator_features = [
            f"missing:{base_features[index]}" for index in imputer.indicator_.features_
        ]
        names = base_features + indicator_features
        for feature, coefficient in zip(names, estimator.coef_, strict=False):
            rows.append(
                {
                    "model_id": recipe.model_id,
                    "feature": feature,
                    "standardized_coefficient": float(coefficient),
                    "abs_coefficient": abs(float(coefficient)),
                }
            )
    pd.DataFrame(rows).sort_values(["model_id", "abs_coefficient"], ascending=[True, False]).to_csv(
        ROOT / "learned_model_coefficients.csv", index=False
    )


def main() -> None:
    if not PANEL_PATH.exists():
        raise FileNotFoundError(PANEL_PATH)
    log("Loading stage-1 market panel")
    panel = add_model_features(pd.read_parquet(PANEL_PATH))
    log(f"Loaded {len(panel):,} rows")
    panel, process_inventory, process_logs = add_process_signals(panel)
    panel, market_inventory, market_logs, fitted_models = add_market_signals(panel)
    panel, outcome_inventory, outcome_logs = add_outcome_residual_signals(panel)
    inventory = process_inventory + market_inventory + outcome_inventory
    signal_columns = [item["signal_id"] for item in inventory]
    output = panel[["core_play_id", *signal_columns]].copy()
    output.to_parquet(OUTPUT_PATH, index=False)
    pd.DataFrame(inventory).to_csv(ROOT / "learned_signal_inventory.csv", index=False)
    pd.DataFrame(process_logs + market_logs + outcome_logs).to_csv(
        ROOT / "learned_model_walkforward_log.csv", index=False
    )
    save_coefficients(panel, market_recipes(), fitted_models)
    joblib.dump(fitted_models, ROOT / "learned_market_models_latest.joblib")
    dump_json(
        ROOT / "learned_model_manifest.json",
        {
            "rows": len(output),
            "signals": len(signal_columns),
            "process_models": len(process_recipes()),
            "market_models": len(market_recipes()),
            "outcome_models": 8,
            "split_rule": "expanding by epoch; test epoch strictly after all training epochs",
            "market_training_deduplication": "last row per game/five-minute bucket",
            "cv_availability": "UNKNOWN_RETROSPECTIVE_ONLY",
        },
    )
    log(f"Wrote {len(signal_columns):,} learned OOS signals")


if __name__ == "__main__":
    main()
