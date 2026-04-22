# Backtests And Runners

## Repo Layout

- `strategies/` contains reusable strategy classes and configs
- `strategies/private/` is for git-ignored local strategy modules
- `backtests/` contains flat public runner entrypoints
- `prediction_market_extensions/backtesting/` contains shared runner plumbing, data-source adapters,
  strategy-config binding, timing, reporting, and replay helpers
- `prediction_market_extensions/backtesting/optimizers/` is the extension-side
  namespace for optimizer families such as parameter search
- `backtests/private/` is for git-ignored local runners

Only flat `backtests/*.py`, `backtests/*.ipynb`, `backtests/private/*.py`, and
`backtests/private/*.ipynb` files are discoverable runner entrypoints. Any
other subdirectory under `backtests/` should be support code only.

Good public examples:

- reusable EMA logic:
  [`strategies/ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/strategies/ema_crossover.py)
- reusable late-favorite limit-hold logic:
  [`strategies/late_favorite_limit_hold.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/strategies/late_favorite_limit_hold.py)
- Kalshi native trade-tick runner:
  [`backtests/kalshi_trade_tick_breakout.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/kalshi_trade_tick_breakout.py)
- Kalshi native trade-tick joint-portfolio basket runner:
  [`backtests/kalshi_trade_tick_joint_portfolio_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/kalshi_trade_tick_joint_portfolio_runner.py)
- Kalshi native trade-tick independent basket runner:
  [`backtests/kalshi_trade_tick_independent_multi_replay_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/kalshi_trade_tick_independent_multi_replay_runner.py)
- Polymarket native trade-tick runner:
  [`backtests/polymarket_trade_tick_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_trade_tick_vwap_reversion.py)
- Polymarket native trade-tick joint-portfolio basket runner:
  [`backtests/polymarket_trade_tick_joint_portfolio_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_trade_tick_joint_portfolio_runner.py)
- Polymarket native trade-tick independent basket runner:
  [`backtests/polymarket_trade_tick_independent_multi_replay_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_trade_tick_independent_multi_replay_runner.py)
- Polymarket quote-tick runner with PMXT vendor data:
  [`backtests/polymarket_quote_tick_ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_quote_tick_ema_crossover.py)
- PMXT joint-portfolio basket runner:
  [`backtests/polymarket_quote_tick_joint_portfolio_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_quote_tick_joint_portfolio_runner.py)
- Telonex joint-portfolio basket runner:
  [`backtests/polymarket_telonex_quote_tick_joint_portfolio_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v3/backtests/polymarket_telonex_quote_tick_joint_portfolio_runner.py)
- PMXT independent basket runner:
  [`backtests/polymarket_quote_tick_independent_multi_replay_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_quote_tick_independent_multi_replay_runner.py)
- PMXT independent 25-replay basket runner:
  [`backtests/polymarket_quote_tick_independent_25_replay_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_quote_tick_independent_25_replay_runner.py)
- PMXT optimizer runner:
  [`backtests/polymarket_quote_tick_ema_optimizer.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_quote_tick_ema_optimizer.py)
- generic optimizer notebook research runner:
  [`backtests/generic_optimizer_research.ipynb`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/generic_optimizer_research.ipynb)

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
used by the shared experiment layer.

`MarketDataConfig` owns platform, vendor, and data type selection.
`TradeReplay` and `QuoteReplay` only describe which replay window or market to
load. Generic executor surfaces such as
`_prediction_market_runner.run_single_market_backtest(...)` and
`_independent_multi_replay_runner.run_independent_multi_replay_backtest_async(...)` dispatch from that
config instead of encoding platform or vendor names into the runner API.

Multi-replay execution mode is explicit now:

- `multi_replay_mode="joint_portfolio"` means one shared Nautilus engine run,
  one shared account, and one true portfolio path across the whole basket
- `multi_replay_mode="independent"` means one isolated run per replay and one
  stitched aggregate report after the fact

That distinction matters. Joint-portfolio charts answer "what happened to one
bankroll trading this basket?" Independent charts answer "how did these replays
perform if each one got its own bankroll?"

Concrete files under `backtests/` can still stay explicit in their filenames
when that helps humans scan examples quickly. The canonical shape is:

```python
from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._execution_config import StaticLatencyConfig
from prediction_market_extensions.backtesting._experiments import build_replay_experiment
from prediction_market_extensions.backtesting._experiments import run_experiment
from prediction_market_extensions.backtesting._prediction_market_backtest import MarketReportConfig
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import PMXT, Polymarket, QuoteTick

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=(
        "local:/Volumes/LaCie/pmxt_raws",
        "archive:r2v2.pmxt.dev",
        "archive:r2.pmxt.dev",
    ),
)

REPLAYS = (
    QuoteReplay(
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
    name="polymarket_quote_tick_ema_crossover",
    description="EMA crossover momentum on one Polymarket market",
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
    emit_html=True,
    chart_output_path="output",
)

@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)
```

Every public runner should expose:

- `DETAIL_PLOT_PANELS` when the runner emits per-sim legacy HTML charts
- `SUMMARY_REPORT_PATH` when the runner emits one aggregate multi-market HTML page
- `SUMMARY_PLOT_PANELS` when the runner emits an aggregate multi-market HTML page
- `DATA`
- `REPLAYS`
- `STRATEGY_CONFIGS`
- `multi_replay_mode` inside `EXPERIMENT` when `len(REPLAYS) > 1`
- `REPORT` when the runner prints a summary table or writes aggregate reports
- `EXECUTION` when the runner models non-default queue position or exchange latency
- `EXPERIMENT`
- `run()`

The runner's `name`, `description`, `emit_html`, and `chart_output_path` are
passed as literal kwargs directly into `build_replay_experiment(...)` rather
than being exposed as top-level module constants. Use
`chart_output_path="output"` for the normal public-runner default. The shared
runner layer resolves that relative path from the repo root so it lands under
this repo's `output/` directory consistently.

Optimizer runners are the one deliberate variant in that contract. Parameter
search runners expose `BASE_REPLAY`, `TRAIN_WINDOWS`, `HOLDOUT_WINDOWS`,
`STRATEGY_SPEC`, `PARAMETER_GRID`, and `PARAMETER_SEARCH` instead of `REPLAYS`
plus chart-summary report fields. They still keep `DATA`, `EXECUTION`,
`EXPERIMENT`, and `run()` at top level so the menu, tests, and direct runner
path stay uniform.

## HTML And Report Modes

The repo-layer runner contract distinguishes two different output shapes:

- per-sim legacy chart:
  controlled by `emit_html`, `chart_output_path`, and `DETAIL_PLOT_PANELS`
- aggregate multi-market report:
  controlled by `REPORT.summary_report=True`, `SUMMARY_REPORT_PATH`, and
  `SUMMARY_PLOT_PANELS`

Those are not interchangeable:

- per-sim legacy charts are one HTML file per loaded replay or labeled sim
- an aggregate multi-market report is built from summary series and shows all
  markets or labeled sims in one shared report

The corresponding runner patterns are:

- single-market runner:
  `emit_html=True`, `chart_output_path="output"`, and
  `DETAIL_PLOT_PANELS = (...)`
- joint-portfolio runner:
  the single-market settings plus `SUMMARY_REPORT_PATH`,
  `SUMMARY_PLOT_PANELS = (...)`,
  `REPORT.summary_report=True`, `return_summary_series=True`, and
  `multi_replay_mode="joint_portfolio"`
- independent multi-replay runner:
  the single-market settings plus `SUMMARY_REPORT_PATH`,
  `REPORT.summary_report=True`, `return_summary_series=True`, and
  `multi_replay_mode="independent"`

Keep the roles separate:

- `DETAIL_PLOT_PANELS` is the drilldown surface for one replay, so it can stay
  execution-heavy
- `SUMMARY_PLOT_PANELS` is the basket overview surface, so it should stay
  readable when more than one replay is present

In practice:

- single-market runs treat the detail HTML as the primary artifact
- midsize baskets often want both the per-sim HTML files and the shared summary
  report
- large baskets rely on the summary report for the overview while the per-sim
  HTML files remain the drilldown surface

Panel selection guidance lives in [`plotting.md`](plotting.md#scaling-model).

Minimal shapes:

```python
DETAIL_PLOT_PANELS = (
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

```python
SUMMARY_REPORT_PATH = "output/polymarket_quote_tick_joint_portfolio_runner_joint_portfolio.html"
SUMMARY_PLOT_PANELS = (
    "total_equity",
    "total_drawdown",
    "total_rolling_sharpe",
    "total_cash_equity",
    "total_brier_advantage",
    "periodic_pnl",
    "monthly_returns",
)

REPORT = MarketReportConfig(
    ...,
    summary_report=True,
    summary_report_path=SUMMARY_REPORT_PATH,
    summary_plot_panels=SUMMARY_PLOT_PANELS,
)

EXPERIMENT = build_replay_experiment(
    ...,
    emit_html=False,
    chart_output_path="output",
    detail_plot_panels=DETAIL_PLOT_PANELS,
    return_summary_series=True,
    multi_replay_mode="joint_portfolio",
)
```

Practical constraints:

- `SUMMARY_REPORT_PATH` depends on summary-series data, so the experiment must
  opt into `return_summary_series=True`
- `chart_output_path` templates may reference `{name}` and `{market_id}`; independent
  multi-replay detail charts may also reference `{sim_label}` for stable per-replay
  names
- panel lists are ordered tuples of stable ids, so inclusion and layout order
  are explicit in the runner file
- known-but-unavailable panels are skipped; unknown panel ids raise immediately
- the shared summary report scales because it is built from summary-series data,
  not by concatenating hundreds of per-sim legacy HTML files

## Optimization Runners

Optimizer runners keep the same flat top-level manifest style, but they swap
replay lists for an explicit search contract:

- `BASE_REPLAY` defines the market and token the optimizer mutates across
  windows
- `TRAIN_WINDOWS` and `HOLDOUT_WINDOWS` pin the exact historical windows used
  for scoring
- `STRATEGY_SPEC` defines the strategy config payload with
  `__SEARCH__:<name>` placeholders
- `PARAMETER_GRID` provides the candidate values for those placeholders
- `OPTIMIZER` is the generic optimizer handle consumed by notebooks and future
  optimizer tooling
- `PARAMETER_SEARCH` carries the explicit parameter-search config consumed by
  `ParameterSearchExperiment`

Current parameter-search helpers live under
`prediction_market_extensions.backtesting.optimizers`.

The public optimizer example is:

- [`backtests/polymarket_quote_tick_ema_optimizer.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/polymarket_quote_tick_ema_optimizer.py)

That runner is intentionally research-oriented. It writes leaderboard and
summary artifacts under `output/` by default and keeps `emit_html=False` so
one parameter sweep does not emit one legacy chart per trial.

## Designing Good Runner Files

A runner file should answer the experiment questions directly:

- which venue or platform is being replayed
- which data modality is being used
- which vendor supplies that modality
- which source priority should be used
- which market or basket of markets is being replayed
- what the capital and execution assumptions are
- which strategy config or configs should be bound into the run

Keep the top-level file declarative. Keep shared mechanics in `prediction_market_extensions/backtesting/`.

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
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_trade_tick_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_ema_crossover.py
uv run python backtests/polymarket_quote_tick_joint_portfolio_runner.py
uv run python backtests/polymarket_quote_tick_independent_multi_replay_runner.py
```

When a runner keeps `chart_output_path="output"`, those direct commands still
write into this repo's `output/` directory. The shared runner layer resolves
that relative path from the repo root rather than from your shell's current
working directory.

Public runners keep their experiment inputs in code. PMXT quote-tick runners
pin absolute sample windows; public Kalshi trade-tick runners also pin
`end_time` so the bundled market stays directly runnable. Native trade-tick
runners without that pin still use rolling lookbacks. If you want a different
market, window, cash value, vendor source priority, or chart behavior, edit
`DATA`, `REPLAYS`, `STRATEGY_CONFIGS`, or the `emit_html` / `chart_output_path` kwargs in
the runner file, or copy the file into
`backtests/private/` and customize it there.

That distinction matters for examples: runners with explicit `start_time` or
`end_time` are the most durable direct-script demos, while rolling native
runners can drift with venue activity and may need a refreshed market or window.

Optimizer runners follow the same rule: the file itself should carry the train
windows, holdout windows, parameter grid, chart-emission toggle, chart output
path, and search scoring assumptions.

## Notebook Runners

Flat notebook files under `backtests/` and `backtests/private/` are also valid
runner entrypoints.

The notebook contract is intentionally thin:

- the notebook must live at a flat discoverable path, just like a `.py` runner
- optional notebook metadata under `metadata.prediction_market_backtest` can set
  `name` and `description` for the menu
- if that metadata is absent, the menu falls back to the notebook filename and
  first markdown heading
- execution happens in place from the repo root through `nbclient`

The public research example is:

- [`backtests/generic_optimizer_research.ipynb`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/v2/backtests/generic_optimizer_research.ipynb)

That notebook is intentionally optimizer-contract driven rather than
strategy-specific. Change the `OPTIMIZER_MODULE` cell to any runner module that
exposes `OPTIMIZER`; today's public optimizer family is
`optimizer_type="parameter_search"` and still also publishes `PARAMETER_SEARCH`
plus the older `OPTIMIZATION` alias. The notebook runs a compact sweep, selects
the winning params, and replays one train or holdout window with HTML enabled.

After a notebook-backed run finishes, the runner scans `output/` for HTML files
updated during that run and rewrites one autogenerated markdown cell at the end
of the notebook. That cell embeds the primary HTML artifact, usually the
joint-portfolio or independent summary report when one exists, and links any additional
HTML artifacts from the same run.

## Editing Runner Inputs

The public runner layer no longer depends on shell env vars for experiment
definition. The file itself should carry the actual values.

Use these top-level objects as the edit surface:

- the `emit_html` kwarg on `build_replay_experiment(...)` to skip per-run HTML
  output when you are sweeping many runners
- the `chart_output_path` kwarg for an explicit file, directory, or `{name}` /
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
- `PMXT_RAW_ROOT`, `PMXT_REMOTE_BASE_URL`, `PMXT_CACHE_DIR`,
  `PMXT_DISABLE_CACHE`
- `TELONEX_LOCAL_DIR`, `TELONEX_API_BASE_URL`, `TELONEX_API_KEY`,
  `TELONEX_CHANNEL`, `TELONEX_CACHE_ROOT`
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
  mirror when you have one, otherwise let them pull from archive
- use `archive:archive.example.com` when you want the runner to fetch raw
  archive hours explicitly
- use `local:/path/to/raw-hours` when you want the runner to fetch from a
  local PMXT raw mirror explicitly
- after the cache layer, PMXT quote-tick runners try the explicit raw sources
  in the exact order you list them
- PMXT source parsing is strict on purpose; only `local:` and `archive:` are
  accepted in `DATA.sources`
- the local PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`
- direct script execution keeps normal Nautilus output visible, and runners that
  opt into `@timing_harness` keep timing output too

### Telonex

- `telonex` is a Polymarket quote-tick vendor backed by Telonex Parquet files
- Telonex source parsing accepts `local:` and `api:` only
- `api:` reads `TELONEX_API_KEY` from the environment and constructs Telonex
  download URLs; never commit keys or put them in `DATA.sources`
- API-day payloads are cached by default at
  `~/.cache/nautilus_trader/telonex`; set `TELONEX_CACHE_ROOT=0` to disable or
  `TELONEX_CACHE_ROOT=/path/to/cache` to move it
- when the cache is enabled, the `Telonex source:` line includes `cache` before
  the configured `local:` and `api:` entries
- prefer `local:/Volumes/LaCie/telonex_data` for repeatable research once files
  have been downloaded with `scripts/telonex_download_data.py` or
  `make download-telonex-data`
- the local downloader consolidates by default to
  `polymarket/<market_slug>/<outcome>/<channel>.parquet`
- Telonex timing output is daily-file based: the `@timing_harness` progress bar
  reports active `telonex local`, `telonex cache`, or `telonex api` loads,
  byte progress when the API response exposes a size, scan rows, and completed
  per-day lines

For vendor-specific data-source behavior and timings, use:

- [Data Vendors And Local Mirrors](data-vendors.md)
- [Vendor Fetch Sources And Timing](vendor-fetch-sources.md)
