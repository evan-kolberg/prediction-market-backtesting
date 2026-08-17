# DQ Score 2.0: NFL Moneyline Market Backtest Study

> [Open the self-contained HTML report](assets/dq-score-market-study/report.html) ·
> [Retained-model registry](assets/dq-score-market-study/retained-models.json) ·
> [Self-audit evidence](assets/dq-score-market-study/self_audit_results.json)
>
> The repository contains the research implementation and curated audit
> summaries. It intentionally excludes the private DuckDB database, raw
> play/quote panel, fitted model binaries, and trade-level Parquet files.

Study date: 2026-08-17<br>
Database: `D:\kch123_analysis\kch123_football.duckdb` (read-only)<br>
Decision: **NO-GO for live or real-money use; retain selected models for research/shadow display only.**

## Executive finding

The strongest defensible finding is not that DQ predicts moneyline profits. It is that ShotSense/CV process features materially improve retrospective per-play EPA estimation: selection-window RMSE falls 12.99% versus pre-play state alone and R² rises from -0.012 to 0.233. Once realized play outcomes are added, RMSE falls another 52.85% and R² reaches 0.830, but most of that lift comes from yards, turnovers, and first downs rather than independent play-quality information.

The original P11 DQ V2 does not survive the market audit. Its exact fixed-window settlement strategy loses $54.74 across 76 games (-7.20% mean trade return), and the nested weekly P11 class remains negative. It should not be used as the primary trading feature.

Three implementations remain worth keeping:

1. **DQ2 process HGB surprise EWM5** is the recommended research core. It measures recent realized EPA relative to a CV/process expectation. It remains positive under 50–100 bps friction and 120–180 second execution delays, but its 95% game-cluster interval crosses zero.
2. **DQ2 outcome HGB surprise EWM10** is a secondary score. It is positive in the fixed audit but less pure—outcome variables dominate—and it fails the 120-second-delay sensitivity.
3. **DQ2 process/outcome agreement** is the strongest fixed-screen shadow hypothesis, but it was designed after inspecting earlier results. The nested DQ-tweak family is only 0.36%; it must be tested on a new season before promotion.

The direct market HGB overlay has the best nested class result, 9.84%, but its 95% interval includes zero and its out-of-sample 15-minute markout R² is negative. Keep it as a market overlay, not as proof that DQ has edge.

## What was tested

- 23,788 aligned play rows covering 206 games, regular-season Week 6 through the postseason.
- 516 signal variants and 24,768 initial signal/threshold/direction/horizon configurations.
- P11 exact and 340-style grids over CV weight, shrinkage, points coefficient, and clock transform.
- Conventional EPA, net-yards, current per-play DQ, and recency baselines.
- Ridge, ElasticNet, and histogram-gradient-boosting EPA models with process-only and outcome-aware feature sets.
- Direct 15/30-minute moneyline-markout models and win-probability residual models.
- 10 follow-up consensus, agreement, price-uncertainty, and time-decay tweaks (480 more configurations).
- A nested weekly audit over epochs 12–22: thresholds came from older epochs, model selection used only the trailing three prior weeks, and the selected configuration traded the next week.

## Execution model

Every reported trade uses a fixed $10 stake, recorded ask at entry, recorded bid at a 5/15/30-minute exit, or the canonical settled token price. The simulator requires top-of-book depth for the full stake, 5–95¢ entry, spread at most 10¢, 25 bps round-trip friction, quote delay at most 120 seconds, and exact per-game non-overlap. The base entry target is the conservative post-play EPA-availability proxy plus 60 seconds. Quote availability is the stored `last_observed_timestamp_us`, not the start of its minute bucket.

Mean return is return on each fixed $10 trade, not portfolio or bankroll return. Settlement strategies can hold across concurrent games; no portfolio capital constraint is modeled.

## Fixed confirmation plus postseason audit

| Model | Status | Exit | Trades | Games | Mean return | Fixed-$10 PnL | Game-cluster 95% CI | Bootstrap P(>0) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| DQ2 process/outcome agreement | Exploratory shadow | settle | 86 | 86 | 26.15% | $224.92 | -8.78% to 66.85% | 91.9% |
| DQ2 outcome residual + time decay | Exploratory shadow | settle | 78 | 78 | 27.83% | $217.04 | -2.27% to 61.20% | 96.2% |
| DQ2 process HGB surprise EWM5 | Retain: research core | settle | 89 | 89 | 5.15% | $45.88 | -14.39% to 25.72% | 69.3% |
| DQ2 outcome HGB surprise EWM10 | Retain: secondary | settle | 90 | 90 | 6.35% | $57.16 | -18.88% to 31.99% | 68.0% |
| DQ2 direct market HGB | Retain: market overlay | 30m | 83 | 53 | 5.13% | $42.60 | -3.24% to 13.97% | 87.7% |
| Cumulative EPA | Benchmark | settle | 62 | 62 | 7.61% | $47.18 | -14.08% to 32.54% | 71.8% |
| Original P11 DQ V2 | Reject | settle | 76 | 76 | -7.20% | -$54.74 | -24.23% to 11.08% | 20.4% |

All leading confidence intervals include zero. Attractive point estimates are hypotheses, not validated edge.

## Strict nested weekly walk-forward

| Model class | Trades | Games | Mean return | Fixed-$10 PnL | Game-cluster 95% CI | Positive weeks | Profit factor |
| --- | --- | --- | --- | --- | --- | --- | --- |
| DQ2_MARKET_HGB | 110 | 106 | 9.84% | $108.19 | -12.22% to 33.80% | 7/11 | 1.23 |
| DQ2_EPA_HGB | 122 | 113 | 4.59% | $55.99 | -19.82% to 30.29% | 5/11 | 1.09 |
| DQ2_TWEAKS | 110 | 104 | 0.36% | $3.99 | -23.25% to 27.82% | 6/11 | 1.01 |
| CANONICAL_BASELINES | 90 | 70 | -1.83% | -$16.49 | -25.52% to 24.39% | 4/11 | 0.95 |
| P11_GRID | 80 | 63 | -3.28% | -$26.27 | -22.80% to 16.92% | 5/11 | 0.90 |
| P11_EXACT | 84 | 58 | -4.58% | -$38.46 | -18.98% to 11.95% | 4/11 | 0.83 |
| DQ2_WINPROB | 74 | 71 | -8.82% | -$65.28 | -30.03% to 12.26% | 2/11 | 0.81 |

The nested policy is more trustworthy than the fixed finalist table because each test week is temporally later than calibration and selection. It still cannot remove researcher bias from inventing formulas on the same 2025 season. The direct market class is positive in 7 of 11 weeks but has a wide interval and a drawdown ($108.80) comparable to total PnL.

## Robustness sensitivities

Combined confirmation-plus-postseason mean return:

| Scenario | Process HGB | Outcome HGB | Agreement tweak | Direct market HGB | P11 exact |
| --- | --- | --- | --- | --- | --- |
| BASE_60S_25BPS | 5.15% | 6.35% | 26.15% | 5.13% | -7.20% |
| FEE_50BPS | 4.90% | 6.10% | 25.90% | 4.88% | -7.45% |
| FEE_100BPS | 4.40% | 5.60% | 25.40% | 4.38% | -7.95% |
| LATENCY_120S | 10.87% | -0.29% | 30.13% | -2.59% | -5.34% |
| LATENCY_180S | 10.73% | 6.81% | 16.62% | 0.34% | -5.91% |
| PRICE_10_90 | 2.60% | 10.16% | 34.05% | 5.30% | -9.17% |
| PRICE_15_85 | 4.81% | 14.38% | 16.92% | 5.59% | -9.81% |

The process HGB is the most latency-stable retained core. The agreement tweak is stable in these fixed scenarios, but this does not cure its post-hoc design. The direct market model changes sign at 120 seconds, and P11 stays negative in every scenario.

## Revised feature-importance findings

| Model | Feature set | Features | MAE | RMSE | R² |
| --- | --- | --- | --- | --- | --- |
| epa_prestate_hgb | prestate | 8 | 0.910 | 1.345 | -0.012 |
| epa_process_hgb | process | 29 | 0.726 | 1.170 | 0.233 |
| epa_outcome_hgb | outcome | 40 | 0.246 | 0.552 | 0.830 |
| market_15m_state_only_hgb | market_state | 13 | 0.081 | 0.120 | -0.021 |
| market_15m_plus_dq_hgb | market_plus_dq | 32 | 0.081 | 0.120 | -0.020 |

- Adding CV process features reduces EPA RMSE by 12.99% relative to pre-play state.
- Adding observed outcome fields reduces RMSE by another 52.85%, but this is mainly a compact reconstruction of realized EPA.
- Adding all DQ families to market/game state improves 15-minute markout RMSE by only 0.042%; R² remains negative (-0.021 state-only, -0.020 with DQ).
- Group permutation assigns negative incremental importance to the P11 DQ group for 15-minute markout. Cumulative outcome and recent-form features carry more signal, but not enough for positive aggregate out-of-sample R².

Top process-only EPA permutation features:

| Feature | Mean MSE increase | Std. dev. |
| --- | --- | --- |
| defender_distance_at_catch | 0.9317 | 0.0331 |
| defender_distance_at_release | 0.3110 | 0.0165 |
| down | 0.0653 | 0.0037 |
| yards_before_contact | 0.0323 | 0.0036 |
| ydstogo | 0.0275 | 0.0038 |
| yardline_100 | 0.0248 | 0.0048 |
| depth_of_target | 0.0221 | 0.0033 |
| ep | 0.0116 | 0.0031 |
| is_pressure_2yards | 0.0080 | 0.0013 |
| catch_quality | 0.0064 | 0.0019 |
| men_in_box | 0.0043 | 0.0019 |
| time_to_first_contact | 0.0039 | 0.0011 |

`defender_distance_at_catch` is highly predictive but is a post-event measurement, so it should not be described as an early/live predictor. The cleaner pre-contact set is defender distance at release, depth of target, pressure proximity, catch quality inputs, time to first contact, box count, and play state.

## Retained implementations

The machine-readable decision file is
[`retained-models.json`](assets/dq-score-market-study/retained-models.json).

- **Research core:** `dq2_process_hgb_l15_r10_surprise_ewm5`.
- **Secondary:** `dq2_outcome_hgb_l15_r10_surprise_ewm10`.
- **Post-hoc shadow:** `tweak_process_outcome_agreement`.
- **Market overlay:** `dq2_market_hgb_15m_l7_r5` / the nested `DQ2_MARKET_HGB` policy.
- **Benchmark only:** `baseline_cumulative_epa`.
- **Rejected:** `p11_v2_exact` and optimized P11 grids as trading scores.

## Audit result

Status: **PASS_WITH_CRITICAL_LIMITATIONS**.

- Database SHA-256: `334179c1d740f03bbe884e34b8fef7f95d9ec0d09309dfa51965b9e2682ef9e5`; size 1,357,393,920 bytes; research connections used `read_only=True`.
- Panel primary key: 23,788 unique play rows; 206 games.
- Executed trade audits: 0 entries before target, 0 entries at/after exit, 0 invalid prices, 0 quote-delay violations, and 0 overlap violations across finalist, tweak, and nested artifacts.
- The audit found a generic settlement edge case that allowed entry after game end; the constructor now enforces `exit_us > entry_us`, and every analysis was rerun.
- Nested selections: 0 temporal-order violations.
- Source hash in the manifest matches the executed research script.

## Critical limitations

1. **No CV publication timestamps exist:** 0 of 23,788 rows. EPA availability plus an execution delay is only a conservative proxy; CV-derived results remain non-causal and retrospective.
2. **One season:** there is no genuinely untouched second season after the reflection/tweak cycle.
3. **Multiple testing:** hundreds of signals and tens of thousands of configurations create selection risk. Nested weekly selection reduces but does not erase it.
4. **Uncertainty:** every leading 95% game-cluster interval includes zero.
5. **Replay granularity:** moneyline quotes are one-minute summaries, not a full L2 event replay. Ask/bid, depth, timestamps, and friction improve realism but cannot reproduce queue position or intraminute path.
6. **Settlement bias:** fixed settlement results may be driven by occasional low-priced winners; price-bucket and game-concentration outputs are included in the audit artifacts.

## Recommended next validation

Freeze the registry now. Backfill actual CV publication timestamps, collect the next full NFL season without changing formulas, and shadow the process HGB plus agreement signal at 60/120/180-second delays. Require a game-clustered 95% lower bound above zero, positive results in at least two distinct seasons, stable 10–90¢ performance, and positive short-horizon bid markout before any production approval.

## Reproducibility files

- `scripts/dq_score_market_research/research_dq_score_market.py` — alignment,
  P11/static scores, executable quote joins, vectorized search, and exact replay.
- `scripts/dq_score_market_research/train_dq_score_learned_models.py` —
  expanding-epoch learned signals.
- `scripts/dq_score_market_research/run_dq_score_robustness_audit.py` — tweaks,
  latency/fee/price tests, and clustered bootstrap.
- `scripts/dq_score_market_research/audit_dq_feature_importance.py` —
  build/selection ablations and permutation importance.
- `scripts/dq_score_market_research/run_nested_weekly_walkforward.py` — nested
  weekly model-selection policy.
- `scripts/dq_score_market_research/self_audit_dq_score_study.py` —
  database/artifact/timing/non-overlap audit.
- `docs/assets/dq-score-market-study/` — curated result tables, manifests,
  retained-model registry, self-audit, and HTML report.
- `docs/assets/dq-score-market-study/published-checksums.csv` — SHA-256 hashes
  for the committed implementation, report, and evidence bundle.

## Reproducing the study

These scripts are a standalone research workflow, not a Nautilus backtest
runner. Point them at a local KCH123 DuckDB copy; every database connection is
opened with `read_only=True`.

```powershell
$env:KCH123_DB_PATH = 'D:\kch123_analysis\kch123_football.duckdb'
$env:KCH123_PRIOR_SQ_OOS_PATH = 'D:\kch123_analysis\sq_pass_expected_epa_research\oos_predictions.parquet'
$env:DQ_SKIP_LEARNED = '1'
$env:DQ_STAGE1_ONLY = '1'

uv run --with duckdb --with pandas --with pyarrow python scripts/dq_score_market_research/research_dq_score_market.py

Remove-Item Env:DQ_SKIP_LEARNED
Remove-Item Env:DQ_STAGE1_ONLY

uv run --with pandas --with pyarrow --with scikit-learn --with joblib python scripts/dq_score_market_research/train_dq_score_learned_models.py
uv run --with duckdb --with pandas --with pyarrow python scripts/dq_score_market_research/research_dq_score_market.py
uv run --with duckdb --with pandas --with pyarrow python scripts/dq_score_market_research/run_dq_score_robustness_audit.py
uv run --with pandas --with pyarrow --with scikit-learn python scripts/dq_score_market_research/audit_dq_feature_importance.py
uv run --with duckdb --with pandas --with pyarrow python scripts/dq_score_market_research/run_nested_weekly_walkforward.py
uv run --with pandas --with pyarrow python scripts/dq_score_market_research/self_audit_dq_score_study.py
uv run --with pandas --with pyarrow python scripts/dq_score_market_research/build_dq_score_study_report.py
```

If the optional prior-study Parquet is unavailable, omit
`KCH123_PRIOR_SQ_OOS_PATH`; the workflow will skip those signal families.
