# Backtests And Runners

## Repo Layout

- `strategies/` contains reusable strategy classes and configs
- `strategies/private/` is for git-ignored local strategy modules
- `backtests/` contains flat public runner entrypoints
- `backtests/_shared/` contains shared runner plumbing, data-source adapters,
  strategy-config binding, timing, reporting, and replay helpers
- `backtests/private/` is for git-ignored local runners

Only `backtests/*.py` and `backtests/private/*.py` are discoverable runner
entrypoints. Any other subdirectory under `backtests/` should be support code
only.

Good public examples:

- reusable EMA logic:
  [`strategies/ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/ema_crossover.py)
- reusable late-favorite limit-hold logic:
  [`strategies/late_favorite_limit_hold.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/late_favorite_limit_hold.py)
- Kalshi native trade-tick runner:
  [`backtests/kalshi_trade_tick_ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_ema_crossover.py)
- Polymarket native trade-tick runner:
  [`backtests/polymarket_trade_tick_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_vwap_reversion.py)
- Polymarket quote-tick runner with PMXT vendor data:
  [`backtests/polymarket_quote_tick_pmxt_ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_ema_crossover.py)
- PMXT labeled multi-sim runner:
  [`backtests/polymarket_quote_tick_pmxt_multi_sim_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_multi_sim_runner.py)
- fixed-basket multi-market runner pattern:
  [`backtests/polymarket_trade_tick_sports_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_sports_vwap_reversion.py)

Those public runners are intended as readable research demos, not profitability
claims. Result payloads now separate the requested replay window from the data
window that actually loaded, including `planned_start`, `planned_end`,
`loaded_start`, `loaded_end`, `coverage_ratio` for loaded-data coverage, and
`requested_coverage_ratio` for requested-window coverage.

Public Kalshi trade-tick runners use the same flat manifest pattern, but pin
`end_time` to a known-good close window so the direct script path stays
deterministic. If you adapt one for fresh research and remove that pin, the
replay falls back to rolling-lookback behavior again.

## Runner Contract

Public runners should read like flat experiment specs.
The public contract is manifest-first: typed replay specs plus one
`EXPERIMENT` object. `PredictionMarketBacktest` is now an internal executor
used by the shared experiment layer. The canonical shape is:

```python
from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._experiments import build_replay_experiment
from backtests._shared._experiments import run_experiment
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._replay_specs import PolymarketPMXTQuoteReplay
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick

NAME = "polymarket_quote_tick_pmxt_ema_crossover"
DESCRIPTION = "EMA crossover momentum on one Polymarket market"
EMIT_HTML = True
CHART_OUTPUT_PATH = "output"

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=(
        "local:/Volumes/LaCie/pmxt_raws",
        "archive:r2.pmxt.dev",
        "relay:209-209-10-83.sslip.io",
    ),
)

REPLAYS = (
    PolymarketPMXTQuoteReplay(
        market_slug="market-slug",
        token_index=0,
        start_time="2026-03-19T07:35:57.277659Z",
        end_time="2026-03-24T07:35:57.277659Z",
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickEMACrossoverStrategy",
        "config_path": "strategies:QuoteTickEMACrossoverConfig",
        "config": {
            "trade_size": Decimal("100"),
            "fast_period": 64,
            "slow_period": 256,
            "entry_buffer": 0.0005,
            "take_profit": 0.010,
            "stop_loss": 0.010,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
)

EXECUTION = ExecutionModelConfig(
    queue_position=True,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)

EXPERIMENT = build_replay_experiment(
    name=NAME,
    description=DESCRIPTION,
    data=DATA,
    replays=REPLAYS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=256,
    min_quotes=500,
    min_price_range=0.005,
    execution=EXECUTION,
    report=REPORT,
    empty_message="No sims met the quote-tick requirements.",
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
)

@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)
```

Every public runner should expose:

- `NAME`
- `DESCRIPTION`
- `EMIT_HTML`
- `CHART_OUTPUT_PATH`
- `DETAIL_PLOT_PANELS` when the runner emits per-sim legacy HTML charts
- `SUMMARY_REPORT_PATH` when the runner emits one aggregate multi-market HTML page
- `SUMMARY_PLOT_PANELS` when the runner emits an aggregate multi-market HTML page
- `DATA`
- `REPLAYS`
- `STRATEGY_CONFIGS`
- `REPORT` when the runner prints a summary table or writes aggregate reports
- `EXECUTION` when the runner models non-default queue position or exchange latency
- `EXPERIMENT`
- `run()`

Use `CHART_OUTPUT_PATH="output"` for the normal public-runner default. The
shared runner layer resolves that relative path from the repo root so it lands
under this repo's `output/` directory consistently.

## HTML And Report Modes

The repo-layer runner contract distinguishes two different output shapes:

- per-sim legacy chart:
  controlled by `EMIT_HTML`, `CHART_OUTPUT_PATH`, and `DETAIL_PLOT_PANELS`
- aggregate multi-market report:
  controlled by `REPORT.summary_report=True`, `SUMMARY_REPORT_PATH`, and
  `SUMMARY_PLOT_PANELS`

Those are not interchangeable:

- per-sim legacy charts are one HTML file per loaded replay or labeled sim
- an aggregate multi-market report is built from summary series and shows all
  markets or labeled sims in one shared report

The corresponding runner patterns are:

- single-market runner:
  `EMIT_HTML = True`, `CHART_OUTPUT_PATH = "output"`, and
  `DETAIL_PLOT_PANELS = (...)`
- multi-market runner:
  the single-market settings plus `SUMMARY_REPORT_PATH`,
  `SUMMARY_PLOT_PANELS = (...)`,
  `REPORT.summary_report=True`, and `return_summary_series=True`
- PMXT multi-sim runner:
  the single-market settings plus `SUMMARY_REPORT_PATH`,
  `REPORT.summary_report=True`, and `return_summary_series=True`

This split is deliberate. It lets one run keep the dense detail that matters
for inspection, such as execution markers and per-market PnL structure, without
forcing a basket run to inline every raw chart into one browser page.

In practice:

- single-market runs treat the detail HTML as the primary artifact
- midsize baskets can rely on both the detail HTML files and the shared summary
  report
- large baskets, including 400+ sim sweeps, rely on the summary report for the
  overview while the per-sim HTML files remain the drilldown surface

That is why `SUMMARY_PLOT_PANELS` should focus on panels that summarize across
runs cleanly, while `DETAIL_PLOT_PANELS` can stay richer and more execution
oriented.

Minimal shapes:

```python
EMIT_HTML = True
CHART_OUTPUT_PATH = "output"
DETAIL_PLOT_PANELS = (
    "equity",
    "market_pnl",
    "periodic_pnl",
    "yes_price",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)
```

```python
SUMMARY_REPORT_PATH = f"output/{NAME}_multi_market.html"
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "equity",
    "periodic_pnl",
    "allocation",
    "drawdown",
    "rolling_sharpe",
    "cash_equity",
    "monthly_returns",
    "brier_advantage",
)

REPORT = MarketReportConfig(
    ...,
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
)

EXPERIMENT = build_replay_experiment(
    ...,
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
    detail_plot_panels=DETAIL_PLOT_PANELS,
    return_summary_series=True,
)
```

Practical constraints:

- `SUMMARY_REPORT_PATH` depends on summary-series data, so the experiment must
  opt into `return_summary_series=True`
- `CHART_OUTPUT_PATH` templates may reference only `{name}` and `{market_id}`
- panel lists are ordered tuples of stable ids, so inclusion and layout order
  are explicit in the runner file
- known-but-unavailable panels are skipped; unknown panel ids raise immediately
- the shared summary report scales because it is built from summary-series data,
  not by concatenating hundreds of per-sim legacy HTML files

## Optimization Runners

Parameter-search runners are a separate repo-layer orchestration surface. They
should stay above the replay executor, not inside it.

The canonical optimization shape is:

- `DATA` for the venue, modality, vendor, and source priority
- `BASE_REPLAY` for the shared market identity
- `TRAIN_WINDOWS` and optional `HOLDOUT_WINDOWS` for explicit replay windows
- `STRATEGY_SPEC` with `__SEARCH__:<name>` placeholders inside one tunable
  strategy config
- `PARAMETER_GRID` with finite candidate values for each placeholder
- `EXECUTION` for queue-position and latency assumptions
- `EMIT_HTML` to explicitly keep chart HTML on or off
- `CHART_OUTPUT_PATH` to keep output location explicit in the file
- `OPTIMIZATION` for the shared search config
- `run()` to launch the search and write optimizer artifacts under `output/`

Good reference:

- [`backtests/polymarket_quote_tick_pmxt_ema_optimizer.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_ema_optimizer.py)

These files are research tooling, not profitability claims. The selected config
is only the best config under the declared windows, execution assumptions, and
scoring function in that runner file. They are also not quick smoke checks: the
optimizer can take minutes, sweep multiple train and holdout windows, and
intentionally explore configurations that stop early under
`AccountBalanceNegative`.

## Designing Good Runner Files

A runner file should answer the experiment questions directly:

- which venue or platform is being replayed
- which data modality is being used
- which vendor supplies that modality
- which source priority should be used
- which market or basket of markets is being replayed
- what the capital and execution assumptions are
- which strategy config or configs should be bound into the run

Keep the top-level file declarative. Keep shared mechanics in `backtests/_shared/`.

That division is deliberate:

- `DATA` selects the platform, modality, vendor, and source priority
- `REPLAYS` is the instrument basket, whether that basket contains one market or many
- `STRATEGY_CONFIGS` is the stable strategy payload passed into the experiment
- `EXECUTION` holds optional queue-position and latency assumptions
- `EXPERIMENT` owns the replay manifest, reporting, and execution settings

## Multi-Market Strategy Configs

The replay executor supports either one strategy instance per replay or one
batch-level strategy config that references the full basket.

Useful config sentinels:

- `__SIM_INSTRUMENT_ID__` binds to the current sim instrument
- `__ALL_SIM_INSTRUMENT_IDS__` binds to every loaded sim instrument in the basket
- `__SIM_METADATA__:<key>` binds metadata from replay `metadata`

That lets a runner expose `REPLAYS` explicitly and still pass one clean
`STRATEGY_CONFIGS` payload into the experiment.

## Running Backtests

Interactive menu:

```bash
make backtest
```

The menu uses `Textual`, so you get a scrollable runner list on the left and a
details/preview pane on the right. Single-letter shortcuts still launch
runners directly, `/` focuses the filter box, `Esc` clears the filter, and the
highlighted runner's full file contents stay visible while you browse.

![Unified backtest runner menu](assets/backtests-menu-textual.png)

Use the menu when you want to browse what is runnable in the current worktree.
The left pane is the discoverable runner list, while the right pane shows the
exact file path, runner metadata, and source preview for the highlighted entry.

Equivalent direct command:

```bash
uv run python main.py
```

Direct script execution is usually better once you know the runner you want:

```bash
uv run python backtests/kalshi_trade_tick_ema_crossover.py
uv run python backtests/polymarket_trade_tick_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
uv run python backtests/polymarket_quote_tick_pmxt_multi_sim_runner.py
```

When a runner keeps `CHART_OUTPUT_PATH="output"`, those direct commands still
write into this repo's `output/` directory. The shared runner layer resolves
that relative path from the repo root rather than from your shell's current
working directory.

Public runners keep their experiment inputs in code. PMXT quote-tick runners
pin absolute sample windows; public Kalshi trade-tick runners also pin
`end_time` so the bundled market stays directly runnable. Native trade-tick
runners without that pin still use rolling lookbacks. If you want a different
market, window, cash value, vendor source priority, or chart behavior, edit
`DATA`, `REPLAYS`, `STRATEGY_CONFIGS`, `EMIT_HTML`, or `CHART_OUTPUT_PATH` in
the runner file, or copy the file into
`backtests/private/` and customize it there.

That distinction matters for examples: runners with explicit `start_time` or
`end_time` are the most durable direct-script demos, while rolling native
runners can drift with venue activity and may need a refreshed market or window.

Optimizer runners follow the same rule: the file itself should carry the train
windows, holdout windows, parameter grid, chart-emission toggle, chart output
path, and search scoring assumptions.

## Editing Runner Inputs

The public runner layer no longer depends on shell env vars for experiment
definition. The file itself should carry the actual values.

Use these top-level objects as the edit surface:

- `EMIT_HTML` to skip per-run HTML output when you are sweeping many runners
- `CHART_OUTPUT_PATH` for an explicit file, directory, or `{name}` /
  `{market_id}` template
- `DETAIL_PLOT_PANELS` for explicit per-sim panel inclusion and ordering
- `SUMMARY_REPORT_PATH` for one aggregate HTML report when the runner should
  show all markets or labeled sims in one shared report
- `SUMMARY_PLOT_PANELS` for explicit aggregate panel inclusion and ordering
- `DATA` for platform, modality, vendor, and source priority
- `REPLAYS` for one market or a basket of markets
- `STRATEGY_CONFIGS` for strategy paths and parameter payloads
- `EXECUTION` for optional queue-position and latency heuristics
- `EXPERIMENT` for shared execution requirements like cash, quote/trade minimums,
  probability window, report policy, and Nautilus log level

Low-level loader env vars still exist for custom integrations and private
workflows:

- `KALSHI_REST_BASE_URL`
- `POLYMARKET_GAMMA_BASE_URL`, `POLYMARKET_TRADE_API_BASE_URL`,
  `POLYMARKET_CLOB_BASE_URL`
- `PMXT_RAW_ROOT`, `PMXT_REMOTE_BASE_URL`, `PMXT_RELAY_BASE_URL`,
  `PMXT_CACHE_DIR`, `PMXT_DISABLE_CACHE`
- `BACKTEST_ENABLE_TIMING=0`

## Data Vendor Notes

### Native Vendors

- `native` means the loader is using venue-native APIs or venue-native historical
  adapters
- public runners pin native source selection in `DATA.sources`
- Kalshi native runners use explicit `rest:` source entries in `DATA.sources`
- Polymarket native runners use explicit `gamma:`, `trades:`, and `clob:`
  source entries in `DATA.sources`
- low-level native loader URLs can still be overridden outside the public runner
  layer if you are building a custom workflow

### PMXT

- PMXT is the first documented quote-tick vendor adapter in this repo
- the preferred sustained workflow is raw-first: point runners at a local raw
  mirror when you have one, otherwise let them pull from archive and relay
- use `archive:archive.example.com` when you want the runner to fetch raw
  archive hours explicitly
- use `local:/path/to/raw-hours` when you want the runner to fetch from a
  local PMXT raw mirror explicitly
- use `relay:relay.example.com` when you want the runner to fetch raw hours
  from a relay explicitly
- after the cache layer, PMXT quote-tick runners try the explicit raw sources
  in the exact order you list them
- PMXT source parsing is strict on purpose; only `local:`, `archive:`,
  and `relay:` are accepted in `DATA.sources`
- the local PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`
- the shared public relay is now treated as a raw mirror service; filtered
  relay behavior is legacy or self-hosted
- direct script execution keeps normal Nautilus output visible, and runners that
  opt into `@timing_harness` keep timing output too

For vendor-specific data-source behavior and timings, use:

- [Data Vendors, Local Mirrors, And Raw PMXT](pmxt-byod.md)
- [Vendor Fetch Sources And Timing](pmxt-fetch-sources.md)
- [Mirror And Relay Ops](pmxt-relay.md)
