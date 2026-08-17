from __future__ import annotations

import hashlib
import json
import math
import os
import time
import warnings
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)


ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("KCH123_DB_PATH", r"D:\kch123_analysis\kch123_football.duckdb"))
PRIOR_OOS_PATH = Path(
    os.environ.get(
        "KCH123_PRIOR_SQ_OOS_PATH",
        r"D:\kch123_analysis\sq_pass_expected_epa_research\oos_predictions.parquet",
    )
)
LEARNED_SIGNALS_PATH = ROOT / "learned_signals.parquet"
LEARNED_INVENTORY_PATH = ROOT / "learned_signal_inventory.csv"
SEED = 20260817

BUILD_EPOCHS = set(range(6, 11))
SELECTION_EPOCHS = set(range(11, 14))
CONFIRMATION_EPOCHS = set(range(14, 19))
FINAL_AUDIT_EPOCHS = set(range(19, 23))

POINT_COEFFICIENTS = (0.04, 0.055, 0.07, 0.085, 0.10)
CV_WEIGHTS = (0.0, 0.25, 0.50, 0.75, 1.0)
SHRINK_K = (0.0, 3.0, 8.0, 15.0)
TIME_WEIGHTS = ("none", "p11_cosine", "remaining_linear", "elapsed_sqrt")
HORIZONS = ("5m", "15m", "30m", "settle")
THRESHOLD_QUANTILES = (0.60, 0.70, 0.80, 0.85, 0.90, 0.95)
LATENCY_SECONDS = 60
MAX_QUOTE_DELAY_SECONDS = 120
MAX_SPREAD = 0.10
MIN_ENTRY_PRICE = 0.05
MAX_ENTRY_PRICE = 0.95
STAKE_DOLLARS = 10.0
ROUNDTRIP_FRICTION_BPS = 25.0

CV_PRIORS = {
    "ttt": 2.5,
    "qb_space": 4.5,
    "separation": 3.5,
    "depth": 10.0,
    "tffc": 3.0,
}


@dataclass(frozen=True)
class SignalSpec:
    signal_id: str
    family: str
    description: str
    inputs: str
    information_horizon: str = "POST_PLAY_RETROSPECTIVE_CV"


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
    if isinstance(value, (np.bool_,)):
        return bool(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def dump_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(json_safe(payload), indent=2, sort_keys=True),
        encoding="utf-8",
    )


def load_plays(connection: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = """
    WITH play_rows AS (
        SELECT
            p.play_id,
            p.home_score,
            p.away_score,
            p.narrative_available_ts_utc,
            p.feed_created_ts_utc
        FROM core.plays AS p
        WHERE p.league='NFL'
    )
    SELECT
        a.core_play_id,
        a.core_game_id,
        a.source_order,
        a.effective_ts_utc,
        a.eligible_fill_ts_utc,
        a.epa_available_ts_utc,
        a.season,
        a.season_type_label,
        a.week,
        a.home_team_code,
        a.away_team_code,
        a.posteam_code,
        a.defteam_code,
        a.quarter,
        a.time_remaining_seconds,
        a.play_type,
        a.description,
        a.down,
        a.ydstogo,
        a.yardline_100,
        a.yards_gained,
        a.eligible_for_ep,
        a.ep,
        a.epa,
        a.observed_action,
        a.fd70_row_id,
        a.net_yards,
        a.was_flag,
        a.is_successful,
        a.is_explosive,
        a.is_turnover,
        a.is_turnover_on_downs,
        a.is_first_down,
        a.time_to_throw,
        a.qb_dropback_depth,
        a.is_under_pressure,
        a.is_pressure_2yards,
        a.is_pressure_3yards,
        a.defender_distance_at_release,
        a.depth_of_target,
        a.is_completion,
        a.receiver_separation,
        a.is_tight_coverage,
        a.is_open_target,
        a.yards_after_catch,
        a.defender_distance_at_catch,
        a.time_to_first_contact,
        a.yards_before_contact,
        a.yards_after_contact,
        a.is_stuffed,
        a.is_breakaway,
        a.is_light_box,
        a.is_stacked_box,
        a.defenders_in_radius_at_tackle,
        a.men_in_box,
        a.pocket_quality,
        a.catch_quality,
        a.rush_quality,
        a.dq_points_recomputed,
        a.cv_annotation_available_ts_utc,
        a.cv_availability_status,
        a.retrospective_only,
        p.home_score,
        p.away_score,
        p.narrative_available_ts_utc,
        p.feed_created_ts_utc,
        g.actual_start_ts_utc,
        g.actual_end_ts_utc,
        g.final_home_score,
        g.final_away_score
    FROM marts.nfl_play_advanced_with_shotsense AS a
    JOIN play_rows AS p ON p.play_id=a.core_play_id
    JOIN core.games AS g ON g.game_id=a.core_game_id
    WHERE a.season=2025
      AND a.season_type_label IN ('Regular Season','Postseason')
      AND a.observed_action IN ('pass','run')
      AND a.posteam_code IS NOT NULL
      AND COALESCE(a.was_flag, FALSE)=FALSE
      AND COALESCE(a.play_type, '') NOT IN ('Sack','No Play','Kneel')
    ORDER BY g.actual_start_ts_utc, a.source_order, a.core_play_id
    """
    frame = connection.execute(query).fetchdf()
    if frame.empty:
        raise RuntimeError("No eligible NFL pass/run rows found")
    return frame


def add_epoch_and_base_fields(frame: pd.DataFrame) -> pd.DataFrame:
    df = frame.copy()
    df["epoch"] = np.where(
        df["season_type_label"].eq("Regular Season"),
        df["week"],
        18 + df["week"],
    ).astype(int)
    df["play_yards"] = pd.to_numeric(df["net_yards"], errors="coerce").fillna(
        pd.to_numeric(df["yards_gained"], errors="coerce")
    )
    df["home_offense"] = df["posteam_code"].eq(df["home_team_code"])
    df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce").fillna(0)
    df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce").fillna(0)
    df["score_margin_home"] = df["home_score"] - df["away_score"]
    df["home_signed_play_yards"] = np.where(df["home_offense"], df["play_yards"], -df["play_yards"])
    df["home_signed_epa"] = np.where(df["home_offense"], df["epa"], -df["epa"])
    df["home_signed_dq_play"] = np.where(
        df["home_offense"],
        df["dq_points_recomputed"],
        -df["dq_points_recomputed"],
    )
    # CV publication is unknown. EPA availability is used only as a conservative
    # retrospective decision-time proxy, never represented as the true CV timestamp.
    proxy_seconds = pd.to_numeric(df["epa_available_ts_utc"], errors="coerce")
    proxy_seconds = proxy_seconds.fillna(
        pd.to_numeric(df["narrative_available_ts_utc"], errors="coerce")
    ).fillna(pd.to_numeric(df["eligible_fill_ts_utc"], errors="coerce"))
    df["decision_proxy_ts_utc"] = proxy_seconds
    df["decision_proxy_us"] = proxy_seconds * 1_000_000
    df["remaining_minutes"] = (
        pd.to_numeric(df["time_remaining_seconds"], errors="coerce") / 60.0
    ).clip(0, 60)
    df["elapsed_fraction"] = (1.0 - df["remaining_minutes"] / 60.0).clip(0, 1)
    df["home_won"] = (df["final_home_score"] > df["final_away_score"]).astype(int)
    return df


def _cum_side_state(df: pd.DataFrame, home_side: bool) -> dict[str, pd.Series]:
    side = df["home_offense"] if home_side else ~df["home_offense"]
    game = df["core_game_id"]
    is_pass = side & df["observed_action"].eq("pass")
    is_run = side & df["observed_action"].eq("run")

    def cumulative(values: pd.Series) -> pd.Series:
        return values.groupby(game, sort=False).cumsum()

    def expanding_mean(values: pd.Series, mask: pd.Series) -> tuple[pd.Series, pd.Series]:
        numeric = pd.to_numeric(values, errors="coerce").where(mask)
        count = numeric.notna().astype(float).groupby(game, sort=False).cumsum()
        total = numeric.fillna(0).groupby(game, sort=False).cumsum()
        mean = total / count.replace(0, np.nan)
        return mean, count

    pass_plays = cumulative(is_pass.astype(float))
    rush_plays = cumulative(is_run.astype(float))
    pass_yards = cumulative(df["play_yards"].where(is_pass, 0).fillna(0))
    rush_yards = cumulative(df["play_yards"].where(is_run, 0).fillna(0))

    ttt, ttt_n = expanding_mean(df["time_to_throw"].clip(0.8, 7.0), is_pass)
    qb_space, qb_space_n = expanding_mean(
        df["defender_distance_at_release"].clip(1.0, 13.0), is_pass
    )
    separation, separation_n = expanding_mean(df["receiver_separation"].clip(0.8, 15.0), is_pass)
    depth, depth_n = expanding_mean(df["depth_of_target"].clip(0.5, 50.0), is_pass)
    tffc, tffc_n = expanding_mean(df["time_to_first_contact"].clip(0.5, 7.5), is_run)
    return {
        "pass_plays": pass_plays,
        "rush_plays": rush_plays,
        "pass_yards": pass_yards,
        "rush_yards": rush_yards,
        "ttt": ttt.clip(1.5, 4.0),
        "ttt_n": ttt_n,
        "qb_space": qb_space.clip(2.0, 7.0),
        "qb_space_n": qb_space_n,
        "separation": separation.clip(1.8, 8.0),
        "separation_n": separation_n,
        "depth": depth.clip(4.0, 30.0),
        "depth_n": depth_n,
        "tffc": tffc.clip(2.0, 4.5),
        "tffc_n": tffc_n,
    }


def _shrunk(values: pd.Series, count: pd.Series, prior: float, k: float) -> pd.Series:
    if k == 0:
        return values
    weight = count / (count + k)
    return weight * values.fillna(prior) + (1.0 - weight) * prior


def _p11_dq_side(
    state: dict[str, pd.Series],
    cv_weight: float,
    coefficient: float,
    shrink_k: float,
) -> pd.Series:
    ttt = _shrunk(state["ttt"], state["ttt_n"], CV_PRIORS["ttt"], shrink_k)
    qb_space = _shrunk(state["qb_space"], state["qb_space_n"], CV_PRIORS["qb_space"], shrink_k)
    separation = _shrunk(
        state["separation"],
        state["separation_n"],
        CV_PRIORS["separation"],
        shrink_k,
    )
    depth = _shrunk(state["depth"], state["depth_n"], CV_PRIORS["depth"], shrink_k)
    tffc = _shrunk(state["tffc"], state["tffc_n"], CV_PRIORS["tffc"], shrink_k)
    pocket_quality = -0.5 + 0.2 * ttt + qb_space
    receiver_space = cv_weight * pocket_quality + (1.0 - cv_weight) * separation
    catch_quality = 3.6 + 0.15 * receiver_space + 0.10 * depth + 0.02 * receiver_space * depth
    effective_pass = (1.0 - cv_weight) * state["pass_yards"] + cv_weight * state[
        "pass_plays"
    ] * catch_quality
    production_fallback = catch_quality.isna()
    effective_pass = effective_pass.where(~production_fallback, state["pass_yards"])

    rush_quality = 1.5 + tffc
    effective_rush = (1.0 - cv_weight) * state["rush_yards"] + cv_weight * state[
        "rush_plays"
    ] * rush_quality
    effective_rush = effective_rush.where(~rush_quality.isna(), state["rush_yards"])
    return coefficient * (effective_pass + effective_rush)


def _time_weight(df: pd.DataFrame, name: str) -> pd.Series:
    remaining = df["remaining_minutes"]
    if name == "none":
        return pd.Series(1.0, index=df.index)
    if name == "p11_cosine":
        return -0.45 * np.cos((np.pi / 30.0) * remaining) + 0.55
    if name == "remaining_linear":
        return (remaining / 60.0).clip(0.05, 1.0)
    if name == "elapsed_sqrt":
        return np.sqrt(df["elapsed_fraction"].clip(lower=0.01))
    raise ValueError(f"Unknown time weight {name}")


def add_static_signals(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, SignalSpec]]:
    df = frame.copy()
    specs: dict[str, SignalSpec] = {}
    home = _cum_side_state(df, True)
    away = _cum_side_state(df, False)
    score_margin = df["score_margin_home"]

    cache: dict[tuple[float, float, float], pd.Series] = {}
    for cv_weight in CV_WEIGHTS:
        for shrink_k in SHRINK_K:
            if cv_weight == 0 and shrink_k != 0:
                continue
            for coefficient in POINT_COEFFICIENTS:
                home_dq = _p11_dq_side(home, cv_weight, coefficient, shrink_k)
                away_dq = _p11_dq_side(away, cv_weight, coefficient, shrink_k)
                base = 0.5 * ((home_dq - away_dq) - score_margin)
                cache[(cv_weight, shrink_k, coefficient)] = base
                for time_name in TIME_WEIGHTS:
                    signal_id = f"p11_cvw{cv_weight:g}_k{shrink_k:g}_c{coefficient:g}_t{time_name}"
                    df[signal_id] = base * _time_weight(df, time_name)
                    specs[signal_id] = SignalSpec(
                        signal_id=signal_id,
                        family="P11_V2_GRID",
                        description=(
                            "P11 cumulative DQ-minus-score margin with "
                            f"cv_weight={cv_weight:g}, shrink_k={shrink_k:g}, "
                            f"coefficient={coefficient:g}, time={time_name}"
                        ),
                        inputs="yards, attempts, cumulative CV means, score, clock",
                    )

    grouped = df.groupby("core_game_id", sort=False)
    df["cum_net_yards_margin"] = grouped["home_signed_play_yards"].cumsum()
    df["cum_epa_margin"] = grouped["home_signed_epa"].cumsum()
    df["cum_dq_play_margin"] = grouped["home_signed_dq_play"].cumsum()
    simple = {
        "baseline_net_yards_007": (
            0.5 * (0.07 * df["cum_net_yards_margin"] - score_margin),
            "Simple 0.07 × cumulative net-yards margin minus scoreboard margin",
        ),
        "baseline_current_dq_cumulative": (
            0.5 * (df["cum_dq_play_margin"] - score_margin),
            "Cumulative KCH per-play DQ contribution minus scoreboard margin",
        ),
        "baseline_cumulative_epa": (
            df["cum_epa_margin"],
            "Cumulative conventional EPA margin",
        ),
    }
    for signal_id, (values, description) in simple.items():
        df[signal_id] = values
        specs[signal_id] = SignalSpec(
            signal_id=signal_id,
            family="BASELINE",
            description=description,
            inputs="canonical post-play production",
            information_horizon="POST_PLAY_CANONICAL",
        )

    for source, label in [
        ("home_signed_play_yards", "net_yards"),
        ("home_signed_epa", "epa"),
        ("home_signed_dq_play", "dq_play"),
    ]:
        for span in (5, 10, 20):
            signal_id = f"recent_{label}_ewm{span}"
            df[signal_id] = grouped[source].transform(
                lambda series: series.ewm(span=span, adjust=False, min_periods=3).mean()
            )
            specs[signal_id] = SignalSpec(
                signal_id=signal_id,
                family="RECENCY_BASELINE",
                description=f"Home-signed {label} exponentially weighted mean, span {span}",
                inputs=label,
                information_horizon="POST_PLAY_CANONICAL",
            )

    exact = cache[(0.5, 0.0, 0.07)]
    df["p11_v2_exact"] = exact * _time_weight(df, "p11_cosine")
    specs["p11_v2_exact"] = SignalSpec(
        signal_id="p11_v2_exact",
        family="P11_V2_EXACT",
        description="Exact reviewed P11 V2 defaults with its cosine time weight",
        inputs="yards, attempts, cumulative CV means, score, P11 time transform",
    )
    return df, specs


def add_prior_sq_signals(
    frame: pd.DataFrame, specs: dict[str, SignalSpec]
) -> tuple[pd.DataFrame, dict[str, SignalSpec]]:
    if not PRIOR_OOS_PATH.exists():
        return frame, specs
    prior = pd.read_parquet(PRIOR_OOS_PATH)
    wanted = [
        column
        for column in [
            "core_play_id",
            "sq_adjustment",
            "process_surprise",
            "sq_process_adjustment",
        ]
        if column in prior.columns
    ]
    if "core_play_id" not in wanted:
        return frame, specs
    prior = prior[wanted].drop_duplicates("core_play_id")
    df = frame.merge(prior, on="core_play_id", how="left", validate="one_to_one")
    for source in ["sq_adjustment", "process_surprise", "sq_process_adjustment"]:
        if source not in df.columns:
            continue
        signed = np.where(df["home_offense"], df[source], -df[source])
        signed_col = f"home_signed_{source}"
        df[signed_col] = signed
        for span in (5, 10, 20):
            signal_id = f"prior_{source}_ewm{span}"
            df[signal_id] = df.groupby("core_game_id", sort=False)[signed_col].transform(
                lambda series: series.ewm(span=span, adjust=False, min_periods=3).mean()
            )
            specs[signal_id] = SignalSpec(
                signal_id=signal_id,
                family="PRIOR_SQ_EPA",
                description=f"Prior OOS {source}, home-signed EWM span {span}",
                inputs="previous SQ pass-EPA research OOS artifact",
            )
    return df, specs


def attach_market_quotes(
    connection: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
    latency_seconds: int,
) -> pd.DataFrame:
    columns = [
        "core_play_id",
        "core_game_id",
        "decision_proxy_us",
        "actual_start_ts_utc",
        "actual_end_ts_utc",
        "epoch",
    ]
    market_input = frame[columns].dropna(subset=["decision_proxy_us"]).copy()
    market_input["entry_target_us"] = (
        market_input["decision_proxy_us"] + latency_seconds * 1_000_000
    ).astype("int64")
    for label, seconds in [("5m", 300), ("15m", 900), ("30m", 1800)]:
        market_input[f"exit_{label}_target_us"] = (
            market_input["entry_target_us"] + seconds * 1_000_000
        ).astype("int64")
    connection.register("dq_market_input", market_input)
    query = """
    WITH token_events AS (
        SELECT
            e.*,
            ht.asset_id AS home_asset_id,
            CAST(ht.settled_price AS DOUBLE) AS home_settled,
            away_token.asset_id AS away_asset_id,
            CAST(away_token.settled_price AS DOUBLE) AS away_settled
        FROM dq_market_input AS e
        JOIN core.nfl_telonex_market_tokens AS ht
          ON ht.game_id=e.core_game_id
         AND ht.market_type='moneyline'
         AND ht.contract_role='home_team'
        JOIN core.nfl_telonex_market_tokens AS away_token
          ON away_token.game_id=e.core_game_id
         AND away_token.market_type='moneyline'
         AND away_token.contract_role='away_team'
    ), sane_odds AS (
        SELECT
            asset_id,
            minute_us,
            last_observed_timestamp_us AS quote_available_us,
            best_bid,
            best_bid_size,
            best_ask,
            best_ask_size,
            mid,
            microprice_l1,
            spread,
            imbalance_5
        FROM core.nfl_telonex_odds_1m
        WHERE best_bid BETWEEN 0 AND 1
          AND best_ask BETWEEN 0 AND 1
          AND best_bid <= best_ask
          AND best_ask-best_bid <= 0.20
          AND close_event_timestamp_us BETWEEN minute_us AND minute_us + 60000000
          AND last_observed_timestamp_us >= close_event_timestamp_us
          AND last_observed_timestamp_us-close_event_timestamp_us <= 2000000
    )
    SELECT
        e.*,
        he.quote_available_us AS home_entry_us,
        he.best_bid AS home_entry_bid,
        he.best_bid_size AS home_entry_bid_size,
        he.best_ask AS home_entry_ask,
        he.best_ask_size AS home_entry_ask_size,
        COALESCE(he.microprice_l1, he.mid) AS home_entry_fair,
        he.spread AS home_entry_spread,
        he.imbalance_5 AS home_entry_imbalance,
        ae.quote_available_us AS away_entry_us,
        ae.best_bid AS away_entry_bid,
        ae.best_bid_size AS away_entry_bid_size,
        ae.best_ask AS away_entry_ask,
        ae.best_ask_size AS away_entry_ask_size,
        COALESCE(ae.microprice_l1, ae.mid) AS away_entry_fair,
        ae.spread AS away_entry_spread,
        ae.imbalance_5 AS away_entry_imbalance,
        h5.quote_available_us AS home_exit_5m_us,
        h5.best_bid AS home_exit_5m_bid,
        h5.best_bid_size AS home_exit_5m_bid_size,
        COALESCE(h5.microprice_l1,h5.mid) AS home_exit_5m_fair,
        a5.quote_available_us AS away_exit_5m_us,
        a5.best_bid AS away_exit_5m_bid,
        a5.best_bid_size AS away_exit_5m_bid_size,
        COALESCE(a5.microprice_l1,a5.mid) AS away_exit_5m_fair,
        h15.quote_available_us AS home_exit_15m_us,
        h15.best_bid AS home_exit_15m_bid,
        h15.best_bid_size AS home_exit_15m_bid_size,
        COALESCE(h15.microprice_l1,h15.mid) AS home_exit_15m_fair,
        a15.quote_available_us AS away_exit_15m_us,
        a15.best_bid AS away_exit_15m_bid,
        a15.best_bid_size AS away_exit_15m_bid_size,
        COALESCE(a15.microprice_l1,a15.mid) AS away_exit_15m_fair,
        h30.quote_available_us AS home_exit_30m_us,
        h30.best_bid AS home_exit_30m_bid,
        h30.best_bid_size AS home_exit_30m_bid_size,
        COALESCE(h30.microprice_l1,h30.mid) AS home_exit_30m_fair,
        a30.quote_available_us AS away_exit_30m_us,
        a30.best_bid AS away_exit_30m_bid,
        a30.best_bid_size AS away_exit_30m_bid_size,
        COALESCE(a30.microprice_l1,a30.mid) AS away_exit_30m_fair
    FROM token_events AS e
    ASOF LEFT JOIN sane_odds AS he
      ON e.home_asset_id=he.asset_id AND e.entry_target_us <= he.quote_available_us
    ASOF LEFT JOIN sane_odds AS ae
      ON e.away_asset_id=ae.asset_id AND e.entry_target_us <= ae.quote_available_us
    ASOF LEFT JOIN sane_odds AS h5
      ON e.home_asset_id=h5.asset_id AND e.exit_5m_target_us <= h5.quote_available_us
    ASOF LEFT JOIN sane_odds AS a5
      ON e.away_asset_id=a5.asset_id AND e.exit_5m_target_us <= a5.quote_available_us
    ASOF LEFT JOIN sane_odds AS h15
      ON e.home_asset_id=h15.asset_id AND e.exit_15m_target_us <= h15.quote_available_us
    ASOF LEFT JOIN sane_odds AS a15
      ON e.away_asset_id=a15.asset_id AND e.exit_15m_target_us <= a15.quote_available_us
    ASOF LEFT JOIN sane_odds AS h30
      ON e.home_asset_id=h30.asset_id AND e.exit_30m_target_us <= h30.quote_available_us
    ASOF LEFT JOIN sane_odds AS a30
      ON e.away_asset_id=a30.asset_id AND e.exit_30m_target_us <= a30.quote_available_us
    """
    try:
        quotes = connection.execute(query).fetchdf()
    finally:
        connection.unregister("dq_market_input")
    return quotes


def merge_quotes(frame: pd.DataFrame, quotes: pd.DataFrame) -> pd.DataFrame:
    quote_columns = [column for column in quotes.columns if column != "core_play_id"]
    duplicate = set(frame.columns).intersection(quote_columns)
    quote_columns = [column for column in quote_columns if column not in duplicate]
    return frame.merge(
        quotes[["core_play_id", *quote_columns]],
        on="core_play_id",
        how="inner",
        validate="one_to_one",
    )


def merge_learned_signals(
    frame: pd.DataFrame, specs: dict[str, SignalSpec]
) -> tuple[pd.DataFrame, dict[str, SignalSpec]]:
    if os.environ.get("DQ_SKIP_LEARNED") == "1":
        return frame, specs
    if not LEARNED_SIGNALS_PATH.exists() or not LEARNED_INVENTORY_PATH.exists():
        return frame, specs
    learned = pd.read_parquet(LEARNED_SIGNALS_PATH)
    if learned["core_play_id"].duplicated().any():
        raise RuntimeError("learned_signals contains duplicate core_play_id rows")
    inventory = pd.read_csv(LEARNED_INVENTORY_PATH).fillna("")
    output = frame.merge(learned, on="core_play_id", how="left", validate="one_to_one")
    for row in inventory.itertuples(index=False):
        specs[row.signal_id] = SignalSpec(
            signal_id=row.signal_id,
            family=row.family,
            description=row.description,
            inputs=row.inputs,
            information_horizon=row.information_horizon,
        )
    return output, specs


def market_audit(frame: pd.DataFrame) -> dict[str, Any]:
    result: dict[str, Any] = {
        "rows": len(frame),
        "games": frame["core_game_id"].nunique(),
        "epochs": sorted(int(value) for value in frame["epoch"].unique()),
        "cv_timestamp_nonnull": int(frame["cv_annotation_available_ts_utc"].notna().sum()),
        "availability_status_counts": frame["cv_availability_status"]
        .value_counts(dropna=False)
        .to_dict(),
    }
    for side in ("home", "away"):
        entry_delay = (frame[f"{side}_entry_us"] - frame["entry_target_us"]) / 1_000_000
        result[f"{side}_entry_delay_seconds_quantiles"] = entry_delay.quantile(
            [0, 0.25, 0.5, 0.75, 0.95, 1]
        ).to_dict()
        result[f"{side}_valid_entry_rows"] = int(
            (
                entry_delay.between(0, MAX_QUOTE_DELAY_SECONDS)
                & frame[f"{side}_entry_ask"].between(MIN_ENTRY_PRICE, MAX_ENTRY_PRICE)
                & frame[f"{side}_entry_spread"].between(0, MAX_SPREAD)
            ).sum()
        )
    result["epoch_rows"] = (
        frame.groupby("epoch", observed=True)
        .agg(rows=("core_play_id", "size"), games=("core_game_id", "nunique"))
        .reset_index()
        .to_dict(orient="records")
    )
    return result


def _trade_arrays(
    frame: pd.DataFrame,
    horizon: str,
    direction_home: pd.Series,
    friction_bps: float,
) -> pd.DataFrame:
    home = direction_home.to_numpy(dtype=bool)

    def choose(home_column: str, away_column: str) -> np.ndarray:
        return np.where(home, frame[home_column], frame[away_column])

    entry_us = choose("home_entry_us", "away_entry_us")
    entry_ask = choose("home_entry_ask", "away_entry_ask")
    entry_ask_size = choose("home_entry_ask_size", "away_entry_ask_size")
    entry_spread = choose("home_entry_spread", "away_entry_spread")
    entry_fair = choose("home_entry_fair", "away_entry_fair")
    entry_delay = (entry_us - frame["entry_target_us"].to_numpy()) / 1_000_000
    shares = STAKE_DOLLARS / entry_ask
    entry_depth_ok = entry_ask_size >= shares

    if horizon == "settle":
        exit_us = frame["actual_end_ts_utc"].to_numpy(dtype=float) * 1_000_000
        exit_price = choose("home_settled", "away_settled")
        exit_fair = exit_price
        exit_delay = np.zeros(len(frame), dtype=float)
        exit_depth_ok = np.ones(len(frame), dtype=bool)
    else:
        exit_us = choose(f"home_exit_{horizon}_us", f"away_exit_{horizon}_us")
        exit_price = choose(f"home_exit_{horizon}_bid", f"away_exit_{horizon}_bid")
        exit_bid_size = choose(f"home_exit_{horizon}_bid_size", f"away_exit_{horizon}_bid_size")
        exit_fair = choose(f"home_exit_{horizon}_fair", f"away_exit_{horizon}_fair")
        target = frame[f"exit_{horizon}_target_us"].to_numpy()
        exit_delay = (exit_us - target) / 1_000_000
        exit_depth_ok = exit_bid_size >= shares

    valid = (
        np.isfinite(entry_us)
        & np.isfinite(entry_ask)
        & np.isfinite(exit_us)
        & np.isfinite(exit_price)
        & (exit_us > entry_us)
        & (entry_delay >= 0)
        & (entry_delay <= MAX_QUOTE_DELAY_SECONDS)
        & (entry_ask >= MIN_ENTRY_PRICE)
        & (entry_ask <= MAX_ENTRY_PRICE)
        & (entry_spread >= 0)
        & (entry_spread <= MAX_SPREAD)
        & entry_depth_ok
        & exit_depth_ok
    )
    if horizon != "settle":
        valid &= (exit_delay >= 0) & (exit_delay <= MAX_QUOTE_DELAY_SECONDS)
        valid &= exit_price >= 0
    friction = friction_bps / 10_000.0
    trade_return = exit_price / entry_ask - 1.0 - friction
    fair_markout = exit_fair - entry_fair
    return pd.DataFrame(
        {
            "valid_quote": valid,
            "side": np.where(home, "home", "away"),
            "entry_us": entry_us,
            "exit_us": exit_us,
            "entry_price": entry_ask,
            "exit_price": exit_price,
            "entry_spread": entry_spread,
            "entry_delay_seconds": entry_delay,
            "exit_delay_seconds": exit_delay,
            "shares": shares,
            "trade_return": trade_return,
            "pnl": STAKE_DOLLARS * trade_return,
            "fair_markout": fair_markout,
        },
        index=frame.index,
    )


def build_trades(
    frame: pd.DataFrame,
    signal_id: str,
    threshold: float,
    horizon: str,
    orientation: int,
    friction_bps: float = ROUNDTRIP_FRICTION_BPS,
    exact_non_overlap: bool = False,
) -> pd.DataFrame:
    signal = pd.to_numeric(frame[signal_id], errors="coerce") * orientation
    active = signal.abs() >= threshold
    direction_home = signal > 0
    trade_fields = _trade_arrays(frame, horizon, direction_home, friction_bps)
    base_columns = [
        "core_play_id",
        "core_game_id",
        "epoch",
        "source_order",
        "decision_proxy_us",
        "actual_start_ts_utc",
        "home_won",
    ]
    trades = pd.concat(
        [frame[base_columns], signal.rename("oriented_signal"), trade_fields], axis=1
    )
    trades = trades.loc[active & trades["valid_quote"]].copy()
    if trades.empty:
        return trades
    trades = trades.sort_values(["entry_us", "core_game_id", "source_order", "core_play_id"])
    if exact_non_overlap:
        accepted: list[int] = []
        for _, game in trades.groupby("core_game_id", sort=False):
            next_free = -np.inf
            for row in game.sort_values("entry_us").itertuples():
                if row.entry_us < next_free:
                    continue
                accepted.append(row.Index)
                next_free = row.exit_us
        trades = trades.loc[accepted].sort_values("entry_us")
    else:
        if horizon == "settle":
            trades = trades.groupby("core_game_id", sort=False).head(1)
        else:
            hold_seconds = {"5m": 300, "15m": 900, "30m": 1800}[horizon]
            elapsed = (
                trades["decision_proxy_us"] - trades["actual_start_ts_utc"] * 1_000_000
            ) / 1_000_000
            trades["screen_bucket"] = np.floor(elapsed.clip(lower=0) / hold_seconds).astype(int)
            trades = trades.groupby(["core_game_id", "screen_bucket"], sort=False).head(1)
    return trades.sort_values("entry_us").reset_index(drop=True)


def max_drawdown(pnl: pd.Series) -> float:
    if pnl.empty:
        return 0.0
    equity = pnl.cumsum()
    drawdown = equity.cummax() - equity
    return float(drawdown.max())


def summarize_trades(trades: pd.DataFrame) -> dict[str, Any]:
    if trades.empty:
        return {
            "trades": 0,
            "games": 0,
            "pnl": 0.0,
            "mean_return": np.nan,
            "median_return": np.nan,
            "win_rate": np.nan,
            "max_drawdown": 0.0,
            "profit_factor": np.nan,
            "mean_fair_markout": np.nan,
        }
    gains = trades.loc[trades["pnl"] > 0, "pnl"].sum()
    losses = -trades.loc[trades["pnl"] < 0, "pnl"].sum()
    return {
        "trades": len(trades),
        "games": trades["core_game_id"].nunique(),
        "pnl": float(trades["pnl"].sum()),
        "mean_return": float(trades["trade_return"].mean()),
        "median_return": float(trades["trade_return"].median()),
        "win_rate": float((trades["pnl"] > 0).mean()),
        "max_drawdown": max_drawdown(trades["pnl"]),
        "profit_factor": float(gains / losses) if losses > 0 else np.nan,
        "mean_fair_markout": float(trades["fair_markout"].mean()),
    }


def weekly_objective(trades: pd.DataFrame, epochs: set[int]) -> dict[str, Any]:
    scores: list[float] = []
    pnls: list[float] = []
    positive = 0
    for epoch in sorted(epochs):
        week = trades.loc[trades["epoch"].eq(epoch)].sort_values("entry_us")
        pnl = float(week["pnl"].sum())
        score = pnl - 0.5 * max_drawdown(week["pnl"])
        scores.append(score)
        pnls.append(pnl)
        positive += int(pnl > 0)
    return {
        "median_weekly_score": float(np.median(scores)),
        "mean_weekly_score": float(np.mean(scores)),
        "mean_weekly_pnl": float(np.mean(pnls)),
        "positive_weeks": positive,
        "weeks": len(epochs),
    }


def broad_vectorized_search(frame: pd.DataFrame, specs: dict[str, SignalSpec]) -> pd.DataFrame:
    build = frame.loc[frame["epoch"].isin(BUILD_EPOCHS)]
    selection = frame.loc[frame["epoch"].isin(SELECTION_EPOCHS)]
    rows: list[dict[str, Any]] = []
    signal_items = list(specs.items())
    for index, (signal_id, spec) in enumerate(signal_items, start=1):
        build_values = pd.to_numeric(build[signal_id], errors="coerce").abs().dropna()
        if len(build_values) < 100 or build_values.nunique() < 5:
            continue
        thresholds = {
            quantile: float(build_values.quantile(quantile)) for quantile in THRESHOLD_QUANTILES
        }
        for quantile, threshold in thresholds.items():
            if not np.isfinite(threshold) or threshold <= 0:
                continue
            for orientation in (1, -1):
                for horizon in HORIZONS:
                    trades = build_trades(
                        selection,
                        signal_id,
                        threshold,
                        horizon,
                        orientation,
                        exact_non_overlap=False,
                    )
                    summary = summarize_trades(trades)
                    weekly = weekly_objective(trades, SELECTION_EPOCHS)
                    rows.append(
                        {
                            "signal_id": signal_id,
                            "family": spec.family,
                            "threshold_quantile": quantile,
                            "threshold": threshold,
                            "orientation": orientation,
                            "horizon": horizon,
                            **summary,
                            **weekly,
                        }
                    )
        if index % 50 == 0:
            log(f"Broad screen: {index}/{len(signal_items)} signals")
    result = pd.DataFrame(rows)
    if result.empty:
        raise RuntimeError("Broad market search produced no configurations")
    result["eligible_for_ranking"] = result["trades"].ge(15) & result["games"].ge(8)
    result["selection_rank"] = (
        result["median_weekly_score"]
        .where(result["eligible_for_ranking"])
        .rank(ascending=False, method="min")
    )
    return result.sort_values(
        ["eligible_for_ranking", "median_weekly_score", "mean_weekly_score"],
        ascending=[False, False, False],
    ).reset_index(drop=True)


def finalist_configs(search: pd.DataFrame) -> pd.DataFrame:
    eligible = search.loc[search["eligible_for_ranking"]].copy()
    chosen: list[pd.Series] = []
    # Keep only one optimized configuration per underlying signal, then ensure
    # all benchmark families remain represented in the confirmation audit.
    for _, row in eligible.drop_duplicates("signal_id").head(20).iterrows():
        chosen.append(row)
    for _, row in (
        eligible.sort_values("median_weekly_score", ascending=False)
        .groupby("horizon", sort=False)
        .head(5)
        .iterrows()
    ):
        chosen.append(row)
    for _, row in (
        eligible.sort_values("median_weekly_score", ascending=False)
        .groupby(["family", "horizon"], sort=False)
        .head(1)
        .iterrows()
    ):
        chosen.append(row)
    required = [
        "p11_v2_exact",
        "baseline_net_yards_007",
        "baseline_current_dq_cumulative",
        "baseline_cumulative_epa",
    ]
    for signal_id in required:
        subset = eligible.loc[eligible["signal_id"].eq(signal_id)]
        if not subset.empty:
            chosen.append(subset.iloc[0])
    finalists = pd.DataFrame(chosen).drop_duplicates(
        ["signal_id", "threshold_quantile", "orientation", "horizon"]
    )
    return finalists.reset_index(drop=True)


def evaluate_finalists(
    frame: pd.DataFrame, finalists: pd.DataFrame, period_name: str, epochs: set[int]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    period = frame.loc[frame["epoch"].isin(epochs)]
    result_rows: list[dict[str, Any]] = []
    all_trades: list[pd.DataFrame] = []
    for row in finalists.itertuples():
        trades = build_trades(
            period,
            row.signal_id,
            row.threshold,
            row.horizon,
            int(row.orientation),
            exact_non_overlap=True,
        )
        summary = summarize_trades(trades)
        weekly = weekly_objective(trades, epochs)
        result_rows.append(
            {
                "period": period_name,
                "signal_id": row.signal_id,
                "family": row.family,
                "threshold_quantile": row.threshold_quantile,
                "threshold": row.threshold,
                "orientation": int(row.orientation),
                "horizon": row.horizon,
                **summary,
                **weekly,
            }
        )
        if not trades.empty:
            trades = trades.assign(
                period=period_name,
                signal_id=row.signal_id,
                family=row.family,
                threshold_quantile=row.threshold_quantile,
                orientation=int(row.orientation),
                horizon=row.horizon,
            )
            all_trades.append(trades)
    result = pd.DataFrame(result_rows)
    detail = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
    return result, detail


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    log("Opening DuckDB read-only")
    with duckdb.connect(str(DB_PATH), read_only=True) as connection:
        plays = add_epoch_and_base_fields(load_plays(connection))
        log(f"Loaded {len(plays):,} eligible pass/run plays")
        plays, specs = add_static_signals(plays)
        plays, specs = add_prior_sq_signals(plays, specs)
        log(f"Constructed {len(specs):,} static and prior-study signal variants")
        quotes = attach_market_quotes(connection, plays, LATENCY_SECONDS)
    panel = merge_quotes(plays, quotes)
    log(f"Attached forward executable quotes to {len(panel):,} play rows")
    panel, specs = merge_learned_signals(panel, specs)
    log(f"Research panel contains {len(specs):,} total signal variants")
    panel.to_parquet(ROOT / "dq_market_panel_stage1.parquet", index=False)
    pd.DataFrame([asdict(spec) for spec in specs.values()]).to_csv(
        ROOT / "signal_inventory_stage1.csv", index=False
    )
    dump_json(ROOT / "market_data_audit_stage1.json", market_audit(panel))
    if os.environ.get("DQ_STAGE1_ONLY") == "1":
        log("Stage-1-only mode complete")
        return
    log("Running broad vectorized configuration search")
    search = broad_vectorized_search(panel, specs)
    search.to_csv(ROOT / "vectorized_search_results.csv", index=False)
    finalists = finalist_configs(search)
    finalists.to_csv(ROOT / "selected_finalist_configs.csv", index=False)
    log(
        f"Screened {len(search):,} configurations; "
        f"retained {len(finalists):,} for exact non-overlap replay"
    )
    confirmation, confirmation_trades = evaluate_finalists(
        panel,
        finalists,
        "REG_WEEKS_14_18_CONFIRMATION",
        CONFIRMATION_EPOCHS,
    )
    final_audit, final_audit_trades = evaluate_finalists(
        panel,
        finalists,
        "POSTSEASON_FINAL_AUDIT",
        FINAL_AUDIT_EPOCHS,
    )
    evaluations = pd.concat([confirmation, final_audit], ignore_index=True)
    trade_detail = pd.concat([confirmation_trades, final_audit_trades], ignore_index=True)
    evaluations.to_csv(ROOT / "finalist_oos_results.csv", index=False)
    trade_detail.to_parquet(ROOT / "finalist_oos_trades.parquet", index=False)
    source_hash = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    dump_json(
        ROOT / "stage1_manifest.json",
        {
            "database_path": str(DB_PATH),
            "database_read_only": True,
            "script_sha256": source_hash,
            "rows": len(panel),
            "games": panel["core_game_id"].nunique(),
            "signals": len(specs),
            "configurations_screened": len(search),
            "finalists": len(finalists),
            "latency_seconds": LATENCY_SECONDS,
            "availability_status": "NON_CAUSAL_RETROSPECTIVE_CV_TIMESTAMP_UNKNOWN",
        },
    )
    log("Static-model research artifacts written")


if __name__ == "__main__":
    main()
