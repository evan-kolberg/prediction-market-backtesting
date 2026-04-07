# Plotting

Single-market plotting is built into the shared runner flow used by the public
prediction-market backtests.

The repo-layer plotting contract is intentionally split into two surfaces:

- one detailed HTML file per loaded replay or labeled sim
- one aggregate summary HTML file for the whole basket when the runner asks for it

Inside that aggregate summary surface, there is another important split:

- portfolio-wide panels collapse the whole basket into one combined series, so
  they stay one line even if you run hundreds or effectively unbounded sims
- comparison panels keep one line per market or per labeled sim, so the point
  is cross-sim comparison rather than one aggregate portfolio path

That separation is what lets charting stay useful across very different run
sizes. One market can still show execution markers, PnL ticks, and the rest of
the dense legacy detail. A basket of 30, 400, or more sims can still open
quickly because the shared summary report is built from aggregated summary
series instead of trying to inline every raw tick, fill, and panel from every
run into one browser page.

Optimizer runners write search artifacts to `output/` as CSV and JSON. They do
not emit one HTML chart per trial by default.

Every public runner now exposes explicit plotting controls at top level:

- `EMIT_HTML` keeps per-run HTML generation on or off in the file itself
- `CHART_OUTPUT_PATH` keeps the destination explicit instead of hiding it in
  shared defaults
- `DETAIL_PLOT_PANELS` chooses which per-sim panels render and in what order
- `SUMMARY_PLOT_PANELS` chooses which aggregate multi-market panels render and
  in what order when a runner emits a summary report

Good examples:

- [`backtests/kalshi_trade_tick_breakout.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_breakout.py)
- [`backtests/kalshi_trade_tick_panic_fade.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_panic_fade.py)
- [`backtests/polymarket_quote_tick_pmxt_panic_fade.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_panic_fade.py)
- [`backtests/polymarket_quote_tick_pmxt_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_vwap_reversion.py)

## Scaling Model

Think about plotting in terms of overview versus drilldown:

- single-market run:
  one detail HTML file is the whole story, so showing fills, execution, price,
  and market-level PnL on that one run is reasonable
- midsize basket, such as 10 to 30 sims:
  one detail HTML per sim still works well, and one aggregate summary chart is
  still a useful shared overview
- large basket, such as 400+ sims:
  the same contract still holds, but the summary report becomes the primary
  overview surface while detailed inspection happens by opening the individual
  per-sim HTML files on demand

The important constraint is that the repo no longer promises one mega-page with
every chart inlined. Trying to concatenate hundreds of Bokeh documents or plot
every fill across every sim in one page does not scale honestly. The current
approach keeps the dense information where it belongs, inside the individual
run that produced it, and keeps the aggregate report focused on the panels that
summarize across runs cleanly.

On dense single-sim YES-price panels, fill markers are still preserved, but the
adapter may sample them down to a readable marker budget instead of drawing
every single fill point.

That distinction also applies inside the summary HTML itself:

- `total_equity`, `periodic_pnl`, and `monthly_returns` are portfolio-wide summary
  panels built from the combined basket, so they stay one aggregate line or one
  aggregate bar series
- `equity`, `allocation`, `drawdown`, `rolling_sharpe`, `cash_equity`, and `brier_advantage`
  are comparison panels, so they can draw one line per market or per sim inside the same
  summary report
- `brier_advantage` works on market slugs, not just individual sims


## Output Types

There are two distinct HTML/report modes in the repo layer:

- per-sim legacy chart:
  enabled by `EMIT_HTML = True` and written under `CHART_OUTPUT_PATH`
- aggregate multi-market report:
  enabled by `REPORT.summary_report=True` plus `SUMMARY_REPORT_PATH`; this is a
  true aggregate report built from summary series, not a pasted-together page

Typical public-runner combinations:

- single-market runner:
  `EMIT_HTML`, `CHART_OUTPUT_PATH`, and `DETAIL_PLOT_PANELS`
- fixed-basket multi-market runner:
  per-market legacy charts plus `SUMMARY_REPORT_PATH`
- PMXT multi-sim runner:
  per-sim legacy charts plus `SUMMARY_REPORT_PATH`

This gives users the best of both worlds:

- detail charts can stay rich and execution-focused for one market or one sim
- the basket summary can mix true portfolio-wide panels such as
  `total_equity`, `periodic_pnl`, and `monthly_returns` with comparison panels
  such as `equity`, `drawdown`, and `cash_equity`
- large baskets do not have to give up drilldown, because each run still keeps
  its own full-detail HTML artifact

The default summary panel set intentionally excludes panels such as
`yes_price` and `market_pnl`, because those are most useful at the individual
run level and do not scale cleanly once they would need one line or one marker
stream per sim.

Important runtime detail:

- `SUMMARY_REPORT_PATH` depends on summary-series data being returned from the
  backtest, so runners that use it also set `return_summary_series=True` in the
  experiment config
- `DETAIL_PLOT_PANELS` and `SUMMARY_PLOT_PANELS` are ordered tuples of stable
  panel ids, so the runner chooses both inclusion and vertical stacking order

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
- `output/polymarket_quote_tick_pmxt_multi_sim_runner_multi_market.html`
- `output/polymarket_quote_tick_pmxt_ema_optimizer_leaderboard.csv`
- `output/polymarket_quote_tick_pmxt_ema_optimizer_summary.json`

The default naming rules are:

- `CHART_OUTPUT_PATH="output"`:
  `output/<runner_name>_<market_or_sim_label>_legacy.html`
- `SUMMARY_REPORT_PATH="output/<runner_name>_multi_market.html"`:
  one aggregate report spanning all markets or all labeled sims

The supported panel ids are:

- `total_equity`
- `equity`
- `market_pnl`
- `periodic_pnl`
- `yes_price`
- `allocation`
- `drawdown`
- `rolling_sharpe`
- `cash_equity`
- `monthly_returns`
- `brier_advantage`

## Example Summary Output

The PMXT multi-sim runner output below is a good example of the intended large
basket workflow: the terminal prints the per-sim summary table, each sim can
still emit its own detail chart, and the aggregate summary report is written as
one separate HTML artifact.

![PMXT multi-sim summary output](assets/pmxt-multi-sim-summary-example.png)

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

The PMXT multi-sim example runner writes per-sim detail charts plus one
aggregate summary chart:

- `output/polymarket_quote_tick_pmxt_multi_sim_runner_multi_market.html`

`SUMMARY_REPORT_PATH` is the true aggregate report with shared panels across
runs. Large baskets should rely on that summary surface plus on-demand per-sim
detail charts instead of one concatenated mega-page.

That means the scaling story is stable across run sizes:

- if you run one market, the detail chart is the main artifact
- if you run a few dozen markets or labeled sims, the summary report remains a
  convenient shared overview
- if you run hundreds of sims, the summary report still scales because it is
  built from summary series, while the detailed execution view stays available
  one sim at a time

In the fixed sports runners, "multi-market" is literal: one report spans
multiple different market slugs. In the PMXT multi-sim runner, "multi-market"
really means one aggregate report spanning multiple labeled sims, even though
all four replays use the same underlying market slug.
