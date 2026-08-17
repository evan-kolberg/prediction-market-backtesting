from __future__ import annotations

import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("KCH123_DB_PATH", r"D:\kch123_analysis\kch123_football.duckdb"))
TRADE_ARTIFACTS = (
    "finalist_oos_trades.parquet",
    "tweak_oos_trades.parquet",
    "nested_weekly_trades.parquet",
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


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


def add_check(checks: list[dict[str, Any]], name: str, passed: bool, detail: Any) -> None:
    checks.append(
        {
            "check": name,
            "status": "PASS" if passed else "FAIL",
            "detail": detail,
        }
    )


def overlap_violations(trades: pd.DataFrame, artifact: str) -> int:
    if artifact == "nested_weekly_trades.parquet":
        group_columns = ["test_epoch", "model_class", "core_game_id"]
    else:
        group_columns = [
            "period",
            "signal_id",
            "threshold_quantile",
            "orientation",
            "horizon",
            "core_game_id",
        ]
    violations = 0
    for _, group in trades.groupby(group_columns, sort=False, dropna=False):
        ordered = group.sort_values("entry_us")
        previous_exit = ordered["exit_us"].shift()
        violations += int((ordered["entry_us"] < previous_exit).sum())
    return violations


def trade_checks(checks: list[dict[str, Any]], panel: pd.DataFrame) -> dict[str, Any]:
    target = panel[["core_play_id", "entry_target_us"]]
    summary: dict[str, Any] = {}
    for artifact in TRADE_ARTIFACTS:
        trades = pd.read_parquet(ROOT / artifact)
        audited = trades.merge(target, on="core_play_id", how="left", validate="many_to_one")
        entry_before_target = int((audited["entry_us"] < audited["entry_target_us"]).sum())
        entry_after_exit = int((audited["entry_us"] >= audited["exit_us"]).sum())
        delay_invalid = int((~audited["entry_delay_seconds"].between(0, 120)).sum())
        short = audited.loc[~audited["horizon"].eq("settle")]
        exit_delay_invalid = int((~short["exit_delay_seconds"].between(0, 120)).sum())
        price_invalid = int(
            (
                ~audited["entry_price"].between(0.05, 0.95) | ~audited["exit_price"].between(0, 1)
            ).sum()
        )
        overlaps = overlap_violations(audited, artifact)
        result = {
            "rows": len(audited),
            "games": audited["core_game_id"].nunique(),
            "entry_before_target": entry_before_target,
            "entry_not_before_exit": entry_after_exit,
            "entry_delay_invalid": delay_invalid,
            "short_exit_delay_invalid": exit_delay_invalid,
            "price_invalid": price_invalid,
            "non_overlap_violations": overlaps,
        }
        summary[artifact] = result
        add_check(
            checks,
            f"trade_invariants:{artifact}",
            all(
                value == 0
                for key, value in result.items()
                if key
                not in {
                    "rows",
                    "games",
                }
            ),
            result,
        )
    return summary


def nested_temporal_checks(checks: list[dict[str, Any]]) -> None:
    selections = pd.read_csv(ROOT / "nested_weekly_selections.csv")
    violations = 0
    for row in selections.itertuples(index=False):
        calibration = [int(item) for item in str(row.calibration_epochs).split(",")]
        validation = [int(item) for item in str(row.validation_epochs).split(",")]
        if max(calibration) >= min(validation) or max(validation) >= int(row.test_epoch):
            violations += 1
    add_check(
        checks,
        "nested_temporal_order",
        violations == 0,
        {"rows": len(selections), "violations": violations},
    )


def artifact_checksums() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    patterns = ("*.py", "*.csv", "*.json", "*.parquet", "*.md", "*.html")
    paths = sorted(
        {
            path
            for pattern in patterns
            for path in ROOT.glob(pattern)
            if path.name != "artifact_checksums.csv"
        }
    )
    for path in paths:
        rows.append(
            {
                "file": path.name,
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
            }
        )
    return pd.DataFrame(rows)


def main() -> None:
    checks: list[dict[str, Any]] = []
    db_before = DB_PATH.stat()
    db_hash = sha256_file(DB_PATH)
    db_after = DB_PATH.stat()
    add_check(
        checks,
        "database_unchanged_during_audit",
        db_before.st_size == db_after.st_size and db_before.st_mtime_ns == db_after.st_mtime_ns,
        {
            "bytes": db_after.st_size,
            "mtime_ns": db_after.st_mtime_ns,
            "sha256": db_hash,
        },
    )

    panel = pd.read_parquet(ROOT / "dq_market_panel_stage1.parquet")
    add_check(
        checks,
        "panel_primary_key_unique",
        not panel["core_play_id"].duplicated().any(),
        {"rows": len(panel), "games": panel["core_game_id"].nunique()},
    )
    add_check(
        checks,
        "cv_publication_timestamps_present",
        bool(panel["cv_annotation_available_ts_utc"].notna().all()),
        {
            "nonnull": int(panel["cv_annotation_available_ts_utc"].notna().sum()),
            "rows": len(panel),
            "interpretation": "Expected limitation: CV-derived scores are retrospective only.",
        },
    )
    trade_summary = trade_checks(checks, panel)
    nested_temporal_checks(checks)

    walkforward = pd.read_csv(ROOT / "learned_model_walkforward_log.csv")
    add_check(
        checks,
        "learned_walkforward_epochs_valid",
        bool(walkforward["test_epoch"].between(6, 22).all()),
        {
            "rows": len(walkforward),
            "min_test_epoch": int(walkforward["test_epoch"].min()),
            "max_test_epoch": int(walkforward["test_epoch"].max()),
            "source_rule": "training epochs are strictly less than each test epoch",
        },
    )
    stage_manifest = json.loads((ROOT / "stage1_manifest.json").read_text("utf-8"))
    source_hash = sha256_file(ROOT / "research_dq_score_market.py")
    add_check(
        checks,
        "stage_manifest_source_hash",
        stage_manifest["script_sha256"] == source_hash,
        {
            "manifest": stage_manifest["script_sha256"],
            "current": source_hash,
        },
    )

    bootstrap = pd.read_csv(ROOT / "robustness_cluster_bootstrap.csv")
    nested = pd.read_csv(ROOT / "nested_weekly_summary.csv")
    add_check(
        checks,
        "uncertainty_intervals_complete",
        bool(
            bootstrap.loc[bootstrap["trades"].gt(0), "bootstrap_mean_return_p025"].notna().all()
            and nested["bootstrap_mean_return_p025"].notna().all()
        ),
        {
            "fixed_rows": len(bootstrap),
            "fixed_nonempty_rows": int(bootstrap["trades"].gt(0).sum()),
            "nested_rows": len(nested),
        },
    )

    failed = [check for check in checks if check["status"] == "FAIL"]
    expected_limitation = {
        "check": "cv_publication_timestamps_present",
        "status": "FAIL",
    }
    unexpected = [
        check
        for check in failed
        if not (
            check["check"] == expected_limitation["check"]
            and check["status"] == expected_limitation["status"]
        )
    ]
    audit_status = "PASS_WITH_CRITICAL_LIMITATIONS" if not unexpected else "FAIL"
    result = {
        "audit_status": audit_status,
        "checks": checks,
        "database": {
            "path": str(DB_PATH),
            "bytes": db_after.st_size,
            "mtime_ns": db_after.st_mtime_ns,
            "sha256": db_hash,
            "opened_by_research_scripts": "read_only=True",
        },
        "trade_artifacts": trade_summary,
        "critical_limitations": [
            "No CV annotation publication timestamp exists for any panel row.",
            "All CV-derived results are retrospective and cannot establish live tradability.",
            "All game-clustered 95% intervals for leading strategies include zero.",
            "The 2025 season was used for research; post-hoc formula search creates researcher degrees of freedom.",
        ],
    }
    (ROOT / "self_audit_results.json").write_text(
        json.dumps(json_safe(result), indent=2, sort_keys=True), encoding="utf-8"
    )
    artifact_checksums().to_csv(ROOT / "artifact_checksums.csv", index=False)
    print(audit_status)


if __name__ == "__main__":
    main()
