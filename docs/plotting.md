# Plotting

Single-market plotting is built into the shared runner flow used by the public
prediction-market backtests.

Optimizer runners write search artifacts to `output/` as CSV and JSON. They do
not emit one HTML chart per trial by default.

Every public runner now exposes two explicit plotting controls at top level:

- `EMIT_HTML` keeps per-run HTML generation on or off in the file itself
- `CHART_OUTPUT_PATH` keeps the destination explicit instead of hiding it in
  shared defaults

Good examples:

- [`backtests/kalshi_trade_tick_breakout.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_breakout.py)
- [`backtests/kalshi_trade_tick_panic_fade.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_panic_fade.py)
- [`backtests/polymarket_quote_tick_pmxt_panic_fade.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_panic_fade.py)
- [`backtests/polymarket_quote_tick_pmxt_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_vwap_reversion.py)

## Output Types

There are three distinct HTML/report modes in the repo layer:

- per-sim legacy chart:
  enabled by `EMIT_HTML = True` and written under `CHART_OUTPUT_PATH`
- combined legacy report:
  enabled by `REPORT.combined_report=True` plus `COMBINED_REPORT_PATH`; this is
  a concatenation of already-generated per-sim HTML pages
- aggregate multi-market report:
  enabled by `REPORT.summary_report=True` plus `SUMMARY_REPORT_PATH`; this is a
  true aggregate report built from summary series, not a pasted-together page

Typical public-runner combinations:

- single-market runner:
  only `EMIT_HTML` and `CHART_OUTPUT_PATH`
- fixed-basket multi-market runner:
  per-market legacy charts plus `SUMMARY_REPORT_PATH`
- PMXT multi-sim runner:
  per-sim legacy charts plus both `COMBINED_REPORT_PATH` and
  `SUMMARY_REPORT_PATH`

Important runtime detail:

- `COMBINED_REPORT_PATH` depends on the individual per-sim HTML files already
  existing, so public runners that use it also keep `EMIT_HTML = True`
- `SUMMARY_REPORT_PATH` depends on summary-series data being returned from the
  backtest, so runners that use it also set `return_summary_series=True` in the
  experiment config

## Output Paths

Public runners now spell the default destination out as
`CHART_OUTPUT_PATH="output"`. The shared runner layer resolves that relative
path from the repo root, so it consistently lands in this repo's
`output/` directory instead of depending on the shell's current working
directory.

That means direct script execution still writes to the repo-local `output/`
directory:

```bash
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

The generated HTML lands under `prediction-market-backtesting/output/`, not
under whichever directory you launched the command from.

The shared runner layer still accepts `CHART_OUTPUT_PATH=None` as a legacy
shorthand for the same repo-root `output/` destination, but public runner files
should be explicit.

If you want to override that, set:

- `CHART_OUTPUT_PATH="output/custom.html"` for one explicit file path
- `CHART_OUTPUT_PATH="output/charts"` for one explicit directory
- `CHART_OUTPUT_PATH="output/{name}_{market_id}.html"` for an explicit template
- `CHART_OUTPUT_PATH="/absolute/path/to/charts"` for a true absolute path

Only `{name}` and `{market_id}` are valid template placeholders.

When a shared runner points at a single file path, it appends the market id
before the suffix. The PMXT multi-sim helper also preserves unique per-sim
names when multiple labeled sims reuse the same underlying market slug.

Charts are written to `output/`, typically with names like:

- `output/<backtest>_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_ema_crossover_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_breakout_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_rsi_reversion_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_spread_capture_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_multi_sim_runner_combined_legacy.html`
- `output/polymarket_quote_tick_pmxt_multi_sim_runner_multi_market.html`
- `output/polymarket_quote_tick_pmxt_ema_optimizer_leaderboard.csv`
- `output/polymarket_quote_tick_pmxt_ema_optimizer_summary.json`

The default naming rules are:

- `CHART_OUTPUT_PATH="output"`:
  `output/<runner_name>_<market_or_sim_label>_legacy.html`
- `COMBINED_REPORT_PATH="output/<runner_name>_combined_legacy.html"`:
  one concatenated page for all per-sim HTML reports
- `SUMMARY_REPORT_PATH="output/<runner_name>_multi_market.html"`:
  one aggregate report spanning all markets or all labeled sims

## Multi-Market References

The clearest multi-market plotting references are the flat Polymarket trade-tick
runner files:

- [`backtests/polymarket_trade_tick_sports_final_period_momentum.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_sports_final_period_momentum.py)
- [`backtests/polymarket_trade_tick_sports_late_favorite_limit_hold.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_sports_late_favorite_limit_hold.py)
- [`backtests/polymarket_trade_tick_sports_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_sports_vwap_reversion.py)

Those runners now write one per-market legacy chart per replay and one
aggregate summary chart under `output/`, typically with names like:

- `output/polymarket_trade_tick_sports_final_period_momentum_<market>_legacy.html`
- `output/polymarket_trade_tick_sports_late_favorite_limit_hold_<market>_legacy.html`
- `output/polymarket_trade_tick_sports_vwap_reversion_<market>_legacy.html`

- `output/polymarket_trade_tick_sports_final_period_momentum_multi_market.html`
- `output/polymarket_trade_tick_sports_late_favorite_limit_hold_multi_market.html`
- `output/polymarket_trade_tick_sports_vwap_reversion_multi_market.html`

The PMXT multi-sim example runner also writes an optional combined
concatenation page in addition to the aggregate chart:

- `output/polymarket_quote_tick_pmxt_multi_sim_runner_combined_legacy.html`
- `output/polymarket_quote_tick_pmxt_multi_sim_runner_multi_market.html`

`COMBINED_REPORT_PATH` is a concatenation of already-generated individual chart
pages. `SUMMARY_REPORT_PATH` is the true aggregate report with shared panels
across runs.

In the fixed sports runners, "multi-market" is literal: one report spans
multiple different market slugs. In the PMXT multi-sim runner, "multi-market"
really means one aggregate report spanning multiple labeled sims, even though
all four replays use the same underlying market slug.
