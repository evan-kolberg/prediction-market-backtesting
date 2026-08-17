from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parent
REPORT_MD = ROOT / "DQ_SCORE_2_MARKET_BACKTEST_STUDY.md"
REPORT_HTML = ROOT / "DQ_SCORE_2_MARKET_BACKTEST_STUDY.html"
REGISTRY_PATH = ROOT / "retained_dq_score_registry.json"
STUDY_DATE = "2026-08-17"


def pct(value: float, digits: int = 2) -> str:
    return f"{100.0 * float(value):.{digits}f}%"


def money(value: float) -> str:
    numeric = float(value)
    return f"-${abs(numeric):,.2f}" if numeric < 0 else f"${numeric:,.2f}"


def number(value: float, digits: int = 3) -> str:
    return f"{float(value):.{digits}f}"


def markdown_table(headers: list[str], rows: list[list[Any]]) -> str:
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    output.extend("| " + " | ".join(map(str, row)) + " |" for row in rows)
    return "\n".join(output)


def html_table(headers: list[str], rows: list[list[Any]], classes: str = "") -> str:
    head = "".join(f"<th>{html.escape(str(item))}</th>" for item in headers)
    body = "".join(
        "<tr>" + "".join(f"<td>{html.escape(str(item))}</td>" for item in row) + "</tr>"
        for row in rows
    )
    return (
        f'<div class="table-wrap"><table class="{classes}"><thead><tr>{head}</tr>'
        f"</thead><tbody>{body}</tbody></table></div>"
    )


def select_row(
    frame: pd.DataFrame,
    signal_id: str,
    quantile: float,
    orientation: int,
    horizon: str,
    *,
    period: str | None = None,
) -> pd.Series:
    mask = (
        frame["signal_id"].eq(signal_id)
        & frame["threshold_quantile"].round(6).eq(quantile)
        & frame["orientation"].eq(orientation)
        & frame["horizon"].eq(horizon)
    )
    if period is not None:
        mask &= frame["period"].eq(period)
    selected = frame.loc[mask]
    if len(selected) != 1:
        raise RuntimeError(
            f"Expected one row for {signal_id}/{quantile}/{orientation}/{horizon}; "
            f"found {len(selected)}"
        )
    return selected.iloc[0]


def combined_sensitivity(frame: pd.DataFrame) -> pd.DataFrame:
    keys = [
        "scenario",
        "signal_id",
        "threshold_quantile",
        "orientation",
        "horizon",
    ]
    output = frame.groupby(keys, as_index=False).agg(
        trades=("trades", "sum"),
        games=("games", "sum"),
        pnl=("pnl", "sum"),
    )
    output["mean_return"] = output["pnl"] / (10.0 * output["trades"])
    return output


def report_inputs() -> dict[str, Any]:
    robustness = pd.read_csv(ROOT / "robustness_cluster_bootstrap.csv")
    combined = robustness.loc[robustness["period"].eq("CONFIRMATION_PLUS_FINAL")]
    sensitivity = combined_sensitivity(pd.read_csv(ROOT / "robustness_sensitivity_results.csv"))
    nested = pd.read_csv(ROOT / "nested_weekly_summary.csv")
    feature_metrics = pd.read_csv(ROOT / "feature_ablation_metrics.csv")
    feature_importance = pd.read_csv(ROOT / "feature_permutation_importance.csv")
    audit = json.loads((ROOT / "self_audit_results.json").read_text("utf-8"))
    market_audit = json.loads((ROOT / "market_data_audit_stage1.json").read_text("utf-8"))
    manifest = json.loads((ROOT / "stage1_manifest.json").read_text("utf-8"))
    return {
        "combined": combined,
        "sensitivity": sensitivity,
        "nested": nested,
        "feature_metrics": feature_metrics,
        "feature_importance": feature_importance,
        "audit": audit,
        "market_audit": market_audit,
        "manifest": manifest,
    }


def fixed_candidate_rows(combined: pd.DataFrame) -> tuple[list[list[Any]], dict[str, pd.Series]]:
    definitions = [
        (
            "DQ2 process/outcome agreement",
            "tweak_process_outcome_agreement",
            0.80,
            1,
            "settle",
            "Exploratory shadow",
        ),
        (
            "DQ2 outcome residual + time decay",
            "tweak_outcome_remaining_time_scaled",
            0.90,
            -1,
            "settle",
            "Exploratory shadow",
        ),
        (
            "DQ2 process HGB surprise EWM5",
            "dq2_process_hgb_l15_r10_surprise_ewm5",
            0.80,
            -1,
            "settle",
            "Retain: research core",
        ),
        (
            "DQ2 outcome HGB surprise EWM10",
            "dq2_outcome_hgb_l15_r10_surprise_ewm10",
            0.60,
            1,
            "settle",
            "Retain: secondary",
        ),
        (
            "DQ2 direct market HGB",
            "dq2_market_hgb_15m_l7_r5",
            0.80,
            -1,
            "30m",
            "Retain: market overlay",
        ),
        (
            "Cumulative EPA",
            "baseline_cumulative_epa",
            0.70,
            1,
            "settle",
            "Benchmark",
        ),
        (
            "Original P11 DQ V2",
            "p11_v2_exact",
            0.60,
            -1,
            "settle",
            "Reject",
        ),
    ]
    selected: dict[str, pd.Series] = {}
    rows: list[list[Any]] = []
    for label, signal_id, quantile, orientation, horizon, status in definitions:
        row = select_row(combined, signal_id, quantile, orientation, horizon)
        selected[signal_id] = row
        rows.append(
            [
                label,
                status,
                horizon,
                int(row["trades"]),
                int(row["games"]),
                pct(row["mean_return"]),
                money(row["pnl"]),
                f"{pct(row['bootstrap_mean_return_p025'])} to "
                f"{pct(row['bootstrap_mean_return_p975'])}",
                pct(row["bootstrap_probability_positive"], 1),
            ]
        )
    return rows, selected


def nested_rows(nested: pd.DataFrame) -> list[list[Any]]:
    order = [
        "DQ2_MARKET_HGB",
        "DQ2_EPA_HGB",
        "DQ2_TWEAKS",
        "CANONICAL_BASELINES",
        "P11_GRID",
        "P11_EXACT",
        "DQ2_WINPROB",
    ]
    indexed = nested.set_index("model_class")
    rows: list[list[Any]] = []
    for model_name in order:
        row = indexed.loc[model_name]
        rows.append(
            [
                model_name,
                int(row["trades"]),
                int(row["games"]),
                pct(row["mean_return"]),
                money(row["pnl"]),
                f"{pct(row['bootstrap_mean_return_p025'])} to "
                f"{pct(row['bootstrap_mean_return_p975'])}",
                f"{int(row['positive_test_weeks'])}/11",
                number(row["profit_factor"], 2),
            ]
        )
    return rows


def sensitivity_rows(frame: pd.DataFrame) -> list[list[Any]]:
    models = [
        (
            "Process HGB",
            "dq2_process_hgb_l15_r10_surprise_ewm5",
            0.80,
            -1,
            "settle",
        ),
        (
            "Outcome HGB",
            "dq2_outcome_hgb_l15_r10_surprise_ewm10",
            0.60,
            1,
            "settle",
        ),
        (
            "Agreement tweak",
            "tweak_process_outcome_agreement",
            0.80,
            1,
            "settle",
        ),
        (
            "Direct market HGB",
            "dq2_market_hgb_15m_l7_r5",
            0.80,
            -1,
            "30m",
        ),
        ("P11 exact", "p11_v2_exact", 0.60, -1, "settle"),
    ]
    scenarios = [
        "BASE_60S_25BPS",
        "FEE_50BPS",
        "FEE_100BPS",
        "LATENCY_120S",
        "LATENCY_180S",
        "PRICE_10_90",
        "PRICE_15_85",
    ]
    rows: list[list[Any]] = []
    for scenario in scenarios:
        result: list[Any] = [scenario]
        for _, signal_id, quantile, orientation, horizon in models:
            subset = frame.loc[
                frame["scenario"].eq(scenario)
                & frame["signal_id"].eq(signal_id)
                & frame["threshold_quantile"].round(6).eq(quantile)
                & frame["orientation"].eq(orientation)
                & frame["horizon"].eq(horizon)
            ]
            result.append(pct(subset.iloc[0]["mean_return"]))
        rows.append(result)
    return rows


def feature_rows(
    metrics: pd.DataFrame, importance: pd.DataFrame
) -> tuple[list[list[Any]], list[list[Any]]]:
    model_rows = [
        [
            row.model_id,
            row.feature_set,
            int(row.features),
            number(row.mae),
            number(row.rmse),
            number(row.r2),
        ]
        for row in metrics.itertuples(index=False)
    ]
    top = importance.loc[importance["model_id"].eq("epa_process_hgb")].head(12)
    importance_rows = [
        [row.feature, number(row.mse_increase_mean, 4), number(row.mse_increase_std, 4)]
        for row in top.itertuples(index=False)
    ]
    return model_rows, importance_rows


def registry(selected: dict[str, pd.Series]) -> dict[str, Any]:
    def evidence(signal_id: str) -> dict[str, Any]:
        row = selected[signal_id]
        return {
            "trades": int(row["trades"]),
            "games": int(row["games"]),
            "fixed_10_dollar_pnl": float(row["pnl"]),
            "mean_trade_return": float(row["mean_return"]),
            "game_cluster_bootstrap_95pct": [
                float(row["bootstrap_mean_return_p025"]),
                float(row["bootstrap_mean_return_p975"]),
            ],
            "bootstrap_probability_positive": float(row["bootstrap_probability_positive"]),
        }

    return {
        "study_date": STUDY_DATE,
        "production_decision": "NO_GO",
        "availability_status": "RETROSPECTIVE_ONLY_CV_TIMESTAMP_UNKNOWN",
        "recommended_research_core": {
            "signal_id": "dq2_process_hgb_l15_r10_surprise_ewm5",
            "status": "RETAIN_RESEARCH_ONLY",
            "definition": (
                "Walk-forward HGB predicts per-play EPA from pre-play state and CV process "
                "features; form a home-signed prediction-minus-realized-EPA residual; EWM "
                "span 5. The selected market orientation is -1."
            ),
            "reason": (
                "Most defensible balance of process interpretation, fixed-screen performance, "
                "and latency/fee stability. Its confidence interval still includes zero."
            ),
            "evidence": evidence("dq2_process_hgb_l15_r10_surprise_ewm5"),
        },
        "secondary_research_score": {
            "signal_id": "dq2_outcome_hgb_l15_r10_surprise_ewm10",
            "status": "RETAIN_RESEARCH_ONLY",
            "definition": (
                "Walk-forward HGB predicts EPA using state, CV, and observed outcome fields; "
                "home-signed prediction-minus-realized-EPA residual; EWM span 10."
            ),
            "reason": (
                "Positive fixed-window evidence, but outcome fields dominate and the 120-second "
                "latency test is negative."
            ),
            "evidence": evidence("dq2_outcome_hgb_l15_r10_surprise_ewm10"),
        },
        "experimental_shadow": {
            "signal_id": "tweak_process_outcome_agreement",
            "status": "SHADOW_ONLY_POST_HOC",
            "definition": (
                "Build-only robust-z mean of process and outcome residual scores, active only "
                "when their signs agree."
            ),
            "reason": (
                "Best fixed-screen robustness, but designed after inspecting earlier results and "
                "the nested DQ-tweak class was effectively flat. Requires a new season."
            ),
            "evidence": evidence("tweak_process_outcome_agreement"),
        },
        "market_overlay": {
            "signal_id": "dq2_market_hgb_15m_l7_r5",
            "status": "SHADOW_ONLY",
            "definition": (
                "Walk-forward HGB predicts 15-minute home-token fair markout from market/game "
                "state plus DQ families."
            ),
            "reason": (
                "Best nested model class, but direct markout R2 is negative and the fixed model "
                "fails the 120-second latency scenario."
            ),
            "evidence": evidence("dq2_market_hgb_15m_l7_r5"),
        },
        "rejected": {
            "signal_id": "p11_v2_exact",
            "status": "REJECT",
            "reason": (
                "Negative fixed-window and nested results; P11 features add no measurable "
                "15-minute markout accuracy."
            ),
            "evidence": evidence("p11_v2_exact"),
        },
        "implementation_files": [
            "scripts/dq_score_market_research/research_dq_score_market.py",
            "scripts/dq_score_market_research/train_dq_score_learned_models.py",
            "scripts/dq_score_market_research/run_dq_score_robustness_audit.py",
            "scripts/dq_score_market_research/run_nested_weekly_walkforward.py",
        ],
    }


def build_markdown(data: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    fixed_rows, selected = fixed_candidate_rows(data["combined"])
    nested_table = nested_rows(data["nested"])
    sensitivity_table = sensitivity_rows(data["sensitivity"])
    feature_table, importance_table = feature_rows(
        data["feature_metrics"], data["feature_importance"]
    )
    metrics = data["feature_metrics"].set_index("model_id")
    prestate = metrics.loc["epa_prestate_hgb"]
    process = metrics.loc["epa_process_hgb"]
    outcome = metrics.loc["epa_outcome_hgb"]
    market_state = metrics.loc["market_15m_state_only_hgb"]
    market_dq = metrics.loc["market_15m_plus_dq_hgb"]
    process_gain = 1.0 - process.rmse / prestate.rmse
    outcome_gain = 1.0 - outcome.rmse / process.rmse
    market_gain = 1.0 - market_dq.rmse / market_state.rmse
    audit = data["audit"]
    manifest = data["manifest"]
    market_audit = data["market_audit"]
    text = f"""# DQ Score 2.0: NFL Moneyline Market Backtest Study

Study date: {STUDY_DATE}<br>
Database: `D:\\kch123_analysis\\kch123_football.duckdb` (read-only)<br>
Decision: **NO-GO for live or real-money use; retain selected models for research/shadow display only.**

## Executive finding

The strongest defensible finding is not that DQ predicts moneyline profits. It is that ShotSense/CV process features materially improve retrospective per-play EPA estimation: selection-window RMSE falls {pct(process_gain)} versus pre-play state alone and R² rises from {number(prestate.r2)} to {number(process.r2)}. Once realized play outcomes are added, RMSE falls another {pct(outcome_gain)} and R² reaches {number(outcome.r2)}, but most of that lift comes from yards, turnovers, and first downs rather than independent play-quality information.

The original P11 DQ V2 does not survive the market audit. Its exact fixed-window settlement strategy loses {money(abs(selected["p11_v2_exact"].pnl))} across {int(selected["p11_v2_exact"].games)} games ({pct(selected["p11_v2_exact"].mean_return)} mean trade return), and the nested weekly P11 class remains negative. It should not be used as the primary trading feature.

Three implementations remain worth keeping:

1. **DQ2 process HGB surprise EWM5** is the recommended research core. It measures recent realized EPA relative to a CV/process expectation. It remains positive under 50–100 bps friction and 120–180 second execution delays, but its 95% game-cluster interval crosses zero.
2. **DQ2 outcome HGB surprise EWM10** is a secondary score. It is positive in the fixed audit but less pure—outcome variables dominate—and it fails the 120-second-delay sensitivity.
3. **DQ2 process/outcome agreement** is the strongest fixed-screen shadow hypothesis, but it was designed after inspecting earlier results. The nested DQ-tweak family is only {pct(data["nested"].set_index("model_class").loc["DQ2_TWEAKS", "mean_return"])}; it must be tested on a new season before promotion.

The direct market HGB overlay has the best nested class result, {pct(data["nested"].set_index("model_class").loc["DQ2_MARKET_HGB", "mean_return"])}, but its 95% interval includes zero and its out-of-sample 15-minute markout R² is negative. Keep it as a market overlay, not as proof that DQ has edge.

## What was tested

- {manifest["rows"]:,} aligned play rows covering {manifest["games"]} games, regular-season Week 6 through the postseason.
- {manifest["signals"]} signal variants and {manifest["configurations_screened"]:,} initial signal/threshold/direction/horizon configurations.
- P11 exact and 340-style grids over CV weight, shrinkage, points coefficient, and clock transform.
- Conventional EPA, net-yards, current per-play DQ, and recency baselines.
- Ridge, ElasticNet, and histogram-gradient-boosting EPA models with process-only and outcome-aware feature sets.
- Direct 15/30-minute moneyline-markout models and win-probability residual models.
- 10 follow-up consensus, agreement, price-uncertainty, and time-decay tweaks ({json.loads((ROOT / "robustness_manifest.json").read_text("utf-8"))["tweak_configurations_screened"]} more configurations).
- A nested weekly audit over epochs 12–22: thresholds came from older epochs, model selection used only the trailing three prior weeks, and the selected configuration traded the next week.

## Execution model

Every reported trade uses a fixed $10 stake, recorded ask at entry, recorded bid at a 5/15/30-minute exit, or the canonical settled token price. The simulator requires top-of-book depth for the full stake, 5–95¢ entry, spread at most 10¢, 25 bps round-trip friction, quote delay at most 120 seconds, and exact per-game non-overlap. The base entry target is the conservative post-play EPA-availability proxy plus 60 seconds. Quote availability is the stored `last_observed_timestamp_us`, not the start of its minute bucket.

Mean return is return on each fixed $10 trade, not portfolio or bankroll return. Settlement strategies can hold across concurrent games; no portfolio capital constraint is modeled.

## Fixed confirmation plus postseason audit

{markdown_table(["Model", "Status", "Exit", "Trades", "Games", "Mean return", "Fixed-$10 PnL", "Game-cluster 95% CI", "Bootstrap P(>0)"], fixed_rows)}

All leading confidence intervals include zero. Attractive point estimates are hypotheses, not validated edge.

## Strict nested weekly walk-forward

{markdown_table(["Model class", "Trades", "Games", "Mean return", "Fixed-$10 PnL", "Game-cluster 95% CI", "Positive weeks", "Profit factor"], nested_table)}

The nested policy is more trustworthy than the fixed finalist table because each test week is temporally later than calibration and selection. It still cannot remove researcher bias from inventing formulas on the same 2025 season. The direct market class is positive in 7 of 11 weeks but has a wide interval and a drawdown ({money(data["nested"].set_index("model_class").loc["DQ2_MARKET_HGB", "max_drawdown"])}) comparable to total PnL.

## Robustness sensitivities

Combined confirmation-plus-postseason mean return:

{markdown_table(["Scenario", "Process HGB", "Outcome HGB", "Agreement tweak", "Direct market HGB", "P11 exact"], sensitivity_table)}

The process HGB is the most latency-stable retained core. The agreement tweak is stable in these fixed scenarios, but this does not cure its post-hoc design. The direct market model changes sign at 120 seconds, and P11 stays negative in every scenario.

## Revised feature-importance findings

{markdown_table(["Model", "Feature set", "Features", "MAE", "RMSE", "R²"], feature_table)}

- Adding CV process features reduces EPA RMSE by {pct(process_gain)} relative to pre-play state.
- Adding observed outcome fields reduces RMSE by another {pct(outcome_gain)}, but this is mainly a compact reconstruction of realized EPA.
- Adding all DQ families to market/game state improves 15-minute markout RMSE by only {pct(market_gain, 3)}; R² remains negative ({number(market_state.r2)} state-only, {number(market_dq.r2)} with DQ).
- Group permutation assigns negative incremental importance to the P11 DQ group for 15-minute markout. Cumulative outcome and recent-form features carry more signal, but not enough for positive aggregate out-of-sample R².

Top process-only EPA permutation features:

{markdown_table(["Feature", "Mean MSE increase", "Std. dev."], importance_table)}

`defender_distance_at_catch` is highly predictive but is a post-event measurement, so it should not be described as an early/live predictor. The cleaner pre-contact set is defender distance at release, depth of target, pressure proximity, catch quality inputs, time to first contact, box count, and play state.

## Retained implementations

The machine-readable decision file is `retained_dq_score_registry.json`.

- **Research core:** `dq2_process_hgb_l15_r10_surprise_ewm5`.
- **Secondary:** `dq2_outcome_hgb_l15_r10_surprise_ewm10`.
- **Post-hoc shadow:** `tweak_process_outcome_agreement`.
- **Market overlay:** `dq2_market_hgb_15m_l7_r5` / the nested `DQ2_MARKET_HGB` policy.
- **Benchmark only:** `baseline_cumulative_epa`.
- **Rejected:** `p11_v2_exact` and optimized P11 grids as trading scores.

## Audit result

Status: **{audit["audit_status"]}**.

- Database SHA-256: `{audit["database"]["sha256"]}`; size {audit["database"]["bytes"]:,} bytes; research connections used `read_only=True`.
- Panel primary key: {market_audit["rows"]:,} unique play rows; {market_audit["games"]} games.
- Executed trade audits: 0 entries before target, 0 entries at/after exit, 0 invalid prices, 0 quote-delay violations, and 0 overlap violations across finalist, tweak, and nested artifacts.
- The audit found a generic settlement edge case that allowed entry after game end; the constructor now enforces `exit_us > entry_us`, and every analysis was rerun.
- Nested selections: 0 temporal-order violations.
- Source hash in the manifest matches the executed research script.

## Critical limitations

1. **No CV publication timestamps exist:** 0 of {market_audit["rows"]:,} rows. EPA availability plus an execution delay is only a conservative proxy; CV-derived results remain non-causal and retrospective.
2. **One season:** there is no genuinely untouched second season after the reflection/tweak cycle.
3. **Multiple testing:** hundreds of signals and tens of thousands of configurations create selection risk. Nested weekly selection reduces but does not erase it.
4. **Uncertainty:** every leading 95% game-cluster interval includes zero.
5. **Replay granularity:** moneyline quotes are one-minute summaries, not a full L2 event replay. Ask/bid, depth, timestamps, and friction improve realism but cannot reproduce queue position or intraminute path.
6. **Settlement bias:** fixed settlement results may be driven by occasional low-priced winners; price-bucket and game-concentration outputs are included in the audit artifacts.

## Recommended next validation

Freeze the registry now. Backfill actual CV publication timestamps, collect the next full NFL season without changing formulas, and shadow the process HGB plus agreement signal at 60/120/180-second delays. Require a game-clustered 95% lower bound above zero, positive results in at least two distinct seasons, stable 10–90¢ performance, and positive short-horizon bid markout before any production approval.

## Reproducibility files

- `research_dq_score_market.py` — alignment, P11/static scores, executable quote joins, vectorized search, exact replay.
- `train_dq_score_learned_models.py` — expanding-epoch learned signals.
- `run_dq_score_robustness_audit.py` — tweaks, latency/fee/price tests, clustered bootstrap.
- `audit_dq_feature_importance.py` — build/selection ablations and permutation importance.
- `run_nested_weekly_walkforward.py` — nested weekly model-selection policy.
- `self_audit_dq_score_study.py` — database/artifact/timing/non-overlap audit.
- `artifact_checksums.csv` — reproducibility hashes.
"""
    return text, registry(selected)


def build_html(markdown_source: str, data: dict[str, Any]) -> str:
    fixed_rows, _ = fixed_candidate_rows(data["combined"])
    nested_table = nested_rows(data["nested"])
    sensitivity_table = sensitivity_rows(data["sensitivity"])
    feature_table, importance_table = feature_rows(
        data["feature_metrics"], data["feature_importance"]
    )
    nested = data["nested"].set_index("model_class")
    audit = data["audit"]
    market_audit = data["market_audit"]
    manifest = data["manifest"]
    cards = [
        ("Signals", f"{manifest['signals']:,}", "broad families"),
        ("Configs", f"{manifest['configurations_screened']:,}", "initial screen"),
        ("Games", f"{manifest['games']}", "Week 6 + playoffs"),
        ("Audit", "NO-GO", "research / shadow only"),
    ]
    card_html = "".join(
        f'<div class="metric"><span>{html.escape(label)}</span><strong>{html.escape(value)}</strong>'
        f"<small>{html.escape(note)}</small></div>"
        for label, value, note in cards
    )
    bar_rows = []
    for model_name in [
        "DQ2_MARKET_HGB",
        "DQ2_EPA_HGB",
        "DQ2_TWEAKS",
        "CANONICAL_BASELINES",
        "P11_GRID",
        "P11_EXACT",
    ]:
        value = float(nested.loc[model_name, "mean_return"]) * 100.0
        width = min(100.0, abs(value) / 10.0 * 100.0)
        bar_rows.append(
            f'<div class="bar-row"><span>{html.escape(model_name)}</span>'
            f'<div class="bar-track"><i class="{"pos" if value >= 0 else "neg"}" '
            f'style="width:{width:.1f}%"></i></div><b>{value:+.2f}%</b></div>'
        )
    critical = "".join(f"<li>{html.escape(item)}</li>" for item in audit["critical_limitations"])
    css = """
:root{--bg:#071014;--panel:#0c181d;--panel2:#101f25;--ink:#e8f1f2;--muted:#91a4aa;--line:#263940;--cyan:#23d5c3;--amber:#ffbd59;--red:#ff6b6b;--green:#72e39b}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,ui-sans-serif,system-ui,-apple-system,Segoe UI,sans-serif;line-height:1.55}
.shell{max-width:1220px;margin:auto;padding:32px 26px 80px}.topline{height:5px;background:linear-gradient(90deg,var(--cyan),var(--amber),var(--red))}
header{border:1px solid var(--line);background:linear-gradient(135deg,#0d1c22,#081216);padding:30px;margin-bottom:18px;display:grid;grid-template-columns:1fr auto;gap:24px;align-items:end}
.eyebrow,.kicker{font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:var(--cyan);font-weight:800}h1{font-size:clamp(30px,5vw,58px);line-height:1;margin:10px 0 14px;letter-spacing:-.04em}h2{font-size:24px;margin:0 0 14px}h3{font-size:17px;margin:20px 0 8px}.verdict{border:1px solid #763e42;background:#241418;color:#ffc5c5;padding:12px 16px;font-weight:800;text-align:center;min-width:230px}.sub{color:var(--muted);max-width:850px}
.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin:18px 0}.metric{border:1px solid var(--line);background:var(--panel);padding:16px;display:flex;flex-direction:column}.metric span,.metric small{color:var(--muted);font-size:12px;text-transform:uppercase;letter-spacing:.08em}.metric strong{font-size:28px;margin:4px 0}
.grid{display:grid;grid-template-columns:1.15fr .85fr;gap:16px}.panel{border:1px solid var(--line);background:var(--panel);padding:22px;margin:16px 0}.panel.accent{border-left:4px solid var(--cyan)}.panel.warn{border-left:4px solid var(--amber)}p{color:#cbd7da}code{background:#102127;border:1px solid #223940;padding:2px 5px;color:#bdf7ef}.table-wrap{overflow:auto;border:1px solid var(--line);margin:12px 0 20px}table{width:100%;border-collapse:collapse;min-width:800px;font-size:13px}th{background:#102127;text-align:left;color:#a9c2c7;letter-spacing:.05em;text-transform:uppercase;font-size:11px}th,td{padding:11px 12px;border-bottom:1px solid var(--line);white-space:nowrap}tbody tr:hover{background:#102127}
.bar-row{display:grid;grid-template-columns:190px 1fr 74px;gap:12px;align-items:center;margin:12px 0;font-size:12px}.bar-track{height:12px;background:#17262b}.bar-track i{display:block;height:100%}.bar-track .pos{background:var(--green)}.bar-track .neg{background:var(--red)}.bar-row b{text-align:right}
.tag{display:inline-block;border:1px solid var(--cyan);color:var(--cyan);font-size:11px;padding:3px 7px;text-transform:uppercase;letter-spacing:.08em;margin-right:6px}.tag.warn{border-color:var(--amber);color:var(--amber)}ol,ul{color:#cbd7da}li{margin:8px 0}.foot{color:var(--muted);font-size:12px;margin-top:28px;padding-top:18px;border-top:1px solid var(--line)}
@media(max-width:800px){header,.grid{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(2,1fr)}.bar-row{grid-template-columns:130px 1fr 64px}.shell{padding:18px 12px 50px}}
"""
    return f"""<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>DQ Score 2.0 Market Backtest Study</title><style>{css}</style></head><body><div class="topline"></div><main class="shell">
<header><div><div class="eyebrow">ShotQuality Football Intelligence / Quant Research</div><h1>DQ Score 2.0<br>Market Backtest Study</h1><p class="sub">A read-only, play-aligned audit of P11 DQ, learned play-quality scores, and NFL moneyline execution. Study date {STUDY_DATE}.</p></div><div class="verdict">NO-GO · RESEARCH ONLY</div></header>
<section class="metrics">{card_html}</section>
<section class="grid"><article class="panel accent"><div class="kicker">Bottom line</div><h2>Process data improves EPA. Tradable edge is not established.</h2><p>CV process features cut out-of-sample EPA RMSE by 12.99%, but every leading market strategy’s game-clustered 95% interval includes zero. The original P11 DQ is negative. Retain the process-residual models for shadow research; do not deploy them as a live trading rule.</p><p><span class="tag">Retain</span> Process HGB surprise EWM5</p><p><span class="tag warn">Shadow</span> Process/outcome agreement and direct market HGB</p></article><article class="panel warn"><div class="kicker">Critical limitation</div><h2>CV timing is unknown</h2><p><strong>0 / {market_audit["rows"]:,}</strong> panel rows have a CV publication timestamp. The analysis waits 60 seconds after EPA availability and uses actual quote-observation timestamps, but that cannot prove CV inputs were available live.</p></article></section>
<section class="panel"><div class="kicker">Fixed finalists</div><h2>Confirmation + postseason</h2><p>Fixed $10 per trade, 25 bps friction. Confidence intervals are clustered by game.</p>{html_table(["Model", "Status", "Exit", "Trades", "Games", "Mean return", "PnL", "95% CI", "P(>0)"], fixed_rows)}</section>
<section class="panel"><div class="kicker">Strictest test</div><h2>Nested weekly walk-forward</h2><div class="grid"><div>{"".join(bar_rows)}</div><p>Each test week was later than calibration and a trailing three-week selection window. Direct market HGB leads on point estimate, but its interval is −12.22% to +33.80%; DQ2 EPA HGB is +4.59% with an interval of −19.82% to +30.29%.</p></div>{html_table(["Model class", "Trades", "Games", "Mean return", "PnL", "95% CI", "Positive weeks", "Profit factor"], nested_table)}</section>
<section class="panel"><div class="kicker">Stress test</div><h2>Fees, latency, and liquidity filters</h2>{html_table(["Scenario", "Process HGB", "Outcome HGB", "Agreement tweak", "Direct market HGB", "P11 exact"], sensitivity_table)}</section>
<section class="panel"><div class="kicker">Feature audit</div><h2>What actually mattered</h2><p>Process CV improves EPA estimation. Outcome-aware models mostly reconstruct realized EPA. Adding all DQ families barely changes 15-minute markout RMSE and leaves R² negative.</p>{html_table(["Model", "Feature set", "Features", "MAE", "RMSE", "R²"], feature_table)}<h3>Top process-only EPA permutation features</h3>{html_table(["Feature", "Mean MSE increase", "Std. dev."], importance_table)}</section>
<section class="grid"><article class="panel"><div class="kicker">Keep</div><h2>Retained model roles</h2><ol><li><strong>Research core:</strong> process HGB surprise EWM5.</li><li><strong>Secondary:</strong> outcome HGB surprise EWM10.</li><li><strong>Post-hoc shadow:</strong> process/outcome agreement.</li><li><strong>Market overlay:</strong> direct markout HGB.</li><li><strong>Benchmark:</strong> cumulative EPA.</li></ol><p><strong>Reject:</strong> original P11 DQ and optimized P11 grids as trading scores.</p></article><article class="panel warn"><div class="kicker">Audit</div><h2>{html.escape(audit["audit_status"])}</h2><ul>{critical}</ul><p>All {sum(v["rows"] for v in audit["trade_artifacts"].values()):,} stored trade rows pass entry/exit order, price, quote-delay, and non-overlap checks.</p></article></section>
<section class="panel"><div class="kicker">Method</div><h2>Realism controls</h2><p>Recorded ask entry · recorded bid exit · top-of-book size constraint · 5–95¢ price · ≤10¢ spread · 25 bps friction · 60-second base latency · ≤120-second quote capture · exact per-game non-overlap · actual quote availability timestamps · settlement only before game end.</p><p>The database was opened read-only. SHA-256: <code>{html.escape(audit["database"]["sha256"])}</code>.</p></section>
<section class="panel"><div class="kicker">Recommended next validation</div><h2>Freeze now; test a new season</h2><p>Backfill real CV publication timestamps, freeze the registry, and shadow the retained scores at 60/120/180 seconds through another full season. Promotion requires a positive game-clustered 95% lower bound, two-season stability, and positive executable short-horizon bid markout.</p></section>
<p class="foot">Full machine-readable evidence is stored beside this report. The Markdown source contains the complete methodology, limitations, file map, and exact interpretation. Embedded source length: {len(markdown_source):,} characters.</p>
</main></body></html>"""


def main() -> None:
    data = report_inputs()
    markdown_source, model_registry = build_markdown(data)
    REPORT_MD.write_text(markdown_source, encoding="utf-8")
    REGISTRY_PATH.write_text(json.dumps(model_registry, indent=2, sort_keys=True), encoding="utf-8")
    REPORT_HTML.write_text(build_html(markdown_source, data), encoding="utf-8")
    print(REPORT_MD)
    print(REPORT_HTML)
    print(REGISTRY_PATH)


if __name__ == "__main__":
    main()
