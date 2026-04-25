# Plotting

The repo now emits summary HTML reports only. Individual per-market HTML report
generation has been removed. Summary reports can still include per-market rows,
per-market lines, fill markers, and comparison panels when the selected panel
set asks for them.

## Scaling Model

Think about plotting as one shared report for a run:

- Single-market runners can still emit a summary report with one market.
- Basket runners emit one joint-portfolio summary report.
- Per-market drilldown should happen through report panels and tables, not
  separate generated HTML files.

Summary reports are built from summary-series artifacts returned by the
backtest. Runner controls:

- `REPORT.summary_report=True`
- `REPORT.summary_report_path=SUMMARY_REPORT_PATH`
- `REPORT.summary_plot_panels=SUMMARY_PLOT_PANELS`
- `return_summary_series=True` on `build_replay_experiment(...)`

The full public basket panel set is:

```python
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "total_drawdown",
    "drawdown",
    "total_rolling_sharpe",
    "rolling_sharpe",
    "total_cash_equity",
    "cash_equity",
    "monthly_returns",
    "total_brier_advantage",
    "brier_advantage",
)
```

For large baskets, prefer portfolio-wide panels first:

- `total_equity`
- `total_drawdown`
- `total_rolling_sharpe`
- `total_cash_equity`
- `total_brier_advantage`
- `periodic_pnl`
- `monthly_returns`

Add per-market panels like `equity`, `market_pnl`, `yes_price`, and
`allocation` only when the basket is small enough that the lines remain
readable.

## Downsampling

The plotting layer downsamples large time series before Bokeh serialization.
This keeps report size bounded when replay windows contain hundreds of thousands
of book events.

The downsampler preserves:

- first and last points
- fill bars
- equity peak and max-drawdown points
- evenly spaced remaining points up to the target budget

Summary artifact construction also downsamples price points before building
dense equity curves. This avoids constructing full-resolution portfolio
timelines only to downsample them again in the chart layer.

## Output Types

The active output type is the aggregate summary report. It is a real report
built from run artifacts, not a concatenation of individual market pages.

Typical basket setup:

```python
SUMMARY_REPORT_PATH = "output/polymarket_book_joint_portfolio_runner_joint_portfolio.html"

REPORT = MarketReportConfig(
    count_key="book_events",
    count_label="Book Events",
    pnl_label="PnL (USDC)",
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
)

EXPERIMENT = build_replay_experiment(
    ...,
    report=REPORT,
    return_summary_series=True,
)
```

Known-but-unavailable panels are skipped. Unknown panel ids raise immediately so
runner typos do not silently produce incomplete reports.

## Output Paths

Public runners use explicit report paths under `output/`:

- `output/polymarket_book_joint_portfolio_runner_joint_portfolio.html`
- `output/polymarket_telonex_book_joint_portfolio_runner_joint_portfolio.html`
- `output/polymarket_book_ema_optimizer_leaderboard.csv`
- `output/polymarket_book_ema_optimizer_summary.json`

The shared runner layer resolves relative paths from the repo root, so direct
script execution writes into this checkout's `output/` directory regardless of
the shell's current working directory.

Supported panel ids:

- `total_equity`
- `total_drawdown`
- `total_rolling_sharpe`
- `total_cash_equity`
- `total_brier_advantage`
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

Run a public basket runner:

```bash
uv run python backtests/polymarket_book_joint_portfolio_runner.py
uv run python backtests/polymarket_telonex_book_joint_portfolio_runner.py
```

Expected artifacts:

- one terminal summary table
- one summary HTML report when `REPORT.summary_report=True`
- no per-market HTML files

## Multi-Market References

Current multi-market examples:

- `backtests/polymarket_book_joint_portfolio_runner.py`
- `backtests/polymarket_telonex_book_joint_portfolio_runner.py`
- `backtests/pmxt_book_joint_portfolio_runner.ipynb`
- `backtests/telonex_book_joint_portfolio_runner.ipynb`

These runners use one shared Nautilus account and one shared portfolio path for
the whole basket. Optimizer drawdown uses the summed equity curve, not the sum
of per-market drawdowns.
