# Backtests And Runners

## Repo Layout

- `strategies/` contains reusable strategy classes and configs.
- `strategies/private/` is for git-ignored local strategy modules.
- `backtests/` contains flat public runner entrypoints.
- `backtests/private/` is for git-ignored local runner entrypoints.
- `prediction_market_extensions/backtesting/` contains shared runner plumbing,
  data-source adapters, timing, reporting, artifacts, and optimizer helpers.

Only flat `backtests/*.py`, `backtests/*.ipynb`, `backtests/private/*.py`, and
`backtests/private/*.ipynb` files are discoverable by the menu. Subdirectories
under `backtests/` should be support code only.

Current public Python runners:

- `backtests/polymarket_book_ema_crossover.py`
- `backtests/polymarket_book_ema_optimizer.py`
- `backtests/polymarket_book_joint_portfolio_runner.py`
- `backtests/polymarket_beffer45_trade_replay_telonex.py`
- `backtests/polymarket_telonex_book_joint_portfolio_runner.py`

Current public notebook runners:

- `backtests/generic_optimizer_research.ipynb`
- `backtests/generic_tpe_research.ipynb`
- `backtests/polymarket_beffer45_trade_replay_telonex.ipynb`
- `backtests/pmxt_book_joint_portfolio_runner.ipynb`
- `backtests/telonex_book_joint_portfolio_runner.ipynb`

These are research demos, not profitability claims. They are pinned to concrete
markets, windows, sources, strategy configs, and execution assumptions so direct
script runs stay reproducible.

## Runner Contract

Public Python runners are flat script entrypoints. Each runner exposes `run()`
and builds its concrete experiment inline inside that function. This keeps the
script import side effect small while preserving explicit inputs: platform,
vendor, source priority, replay windows, strategy config, execution model, and
report config are still visible in one file.

The menu discovers runner metadata by AST-scanning literal `name=` and
`description=` fields in `build_replay_experiment(...)` or
`ParameterSearchExperiment(...)`. Do not hide those behind variables.

The canonical book-runner shape is:

```python
from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from prediction_market_extensions.backtesting._execution_config import (
    ExecutionModelConfig,
    StaticLatencyConfig,
)
from prediction_market_extensions.backtesting._experiments import (
    build_replay_experiment,
    run_experiment,
)
from prediction_market_extensions.backtesting._prediction_market_backtest import (
    MarketReportConfig,
)
from prediction_market_extensions.backtesting._prediction_market_runner import (
    MarketDataConfig,
)
from prediction_market_extensions.backtesting._replay_specs import BookReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import Book, PMXT, Polymarket

@timing_harness
def run() -> None:
    run_experiment(
        build_replay_experiment(
            name="polymarket_book_ema_crossover",
            description="EMA crossover momentum on one Polymarket market using L2 book data",
            data=MarketDataConfig(
                platform=Polymarket,
                data_type=Book,
                vendor=PMXT,
                sources=(
                    "local:/Volumes/storage/pmxt_data",
                    "archive:r2v2.pmxt.dev",
                    "archive:r2.pmxt.dev",
                ),
            ),
            replays=(
                BookReplay(
                    market_slug="market-slug",
                    token_index=0,
                    start_time="2026-03-19T07:35:57.277659Z",
                    end_time="2026-03-24T07:35:57.277659Z",
                ),
            ),
            strategy_configs=[
                {
                    "strategy_path": "strategies:BookEMACrossoverStrategy",
                    "config_path": "strategies:BookEMACrossoverConfig",
                    "config": {
                        "trade_size": Decimal("100"),
                        "fast_period": 64,
                        "slow_period": 256,
                        "entry_buffer": 0.0005,
                        "take_profit": 0.010,
                        "stop_loss": 0.010,
                    },
                },
            ],
            initial_cash=100.0,
            probability_window=256,
            min_book_events=500,
            min_price_range=0.005,
            execution=ExecutionModelConfig(
                queue_position=True,
                latency_model=StaticLatencyConfig(
                    base_latency_ms=75.0,
                    insert_latency_ms=10.0,
                    update_latency_ms=5.0,
                    cancel_latency_ms=5.0,
                ),
            ),
            report=MarketReportConfig(
                count_key="book_events",
                count_label="Book Events",
                pnl_label="PnL (USDC)",
            ),
            empty_message="No replays met the book requirements.",
        )
    )
```

Important contract details:

- Use `BookReplay`, not `QuoteReplay` or `TradeReplay`.
- Use `data_type=Book`, whose string value is `"book"`.
- Gate replay coverage with `min_book_events`.
- PMXT and Telonex adapters emit Nautilus `OrderBookDeltas` for L2 MBP book
  state.
- `TradeTick` records are execution-only and are loaded by the replay adapter,
  not by runner strategy configs.
- Public runners should preserve normal Nautilus output and timing output by
  default.

## HTML And Report Modes

Per-market HTML report generation has been removed. Public runners now use the
summary-report path when HTML output is needed.

The active HTML/report surface is inline `MarketReportConfig` with:

- `summary_report=True`
- `summary_report_path="output/<name>.html"`
- `summary_plot_panels=(...)`
- `return_summary_series=True` on `build_replay_experiment(...)`

The summary report can still contain per-market rows and comparison panels. The
removed behavior is separate one-file-per-market detail HTML generation.

Typical basket config:

```python
build_replay_experiment(
    ...,
    report=MarketReportConfig(
        count_key="book_events",
        count_label="Book Events",
        pnl_label="PnL (USDC)",
        summary_report=True,
        summary_report_path="output/polymarket_book_joint_portfolio_runner_joint_portfolio.html",
        summary_plot_panels=(
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
        ),
    ),
)
```

Known panel ids live in [Plotting](plotting.md).

## Optimization Runners

Optimizer runners keep the same inline explicit-source style but build a
`ParameterSearchExperiment`. The inline `ParameterSearchConfig` pins:

- the base replay market and token
- train and holdout windows
- strategy payload with `__SEARCH__:<name>` placeholders
- `parameter_grid` for discrete random-grid values
- `parameter_space` for TPE ranges when Optuna is used
- scoring, sampler, coverage, and holdout settings

The scoring objective is documented in [Research](research.md).

## Designing Good Runner Files

A runner should answer these questions directly in code:

- Which platform is being replayed?
- Which data type is being used?
- Which vendor supplies that data?
- Which source priority should be used?
- Which market or basket is being replayed?
- Which strategy config is bound into the run?
- Which capital, latency, and queue-position assumptions apply?
- Which report, if any, should be emitted?

Keep shared mechanics out of runner files. The inline inputs should still be
obvious:

- `MarketDataConfig` selects platform, data type, vendor, and source priority.
- `BookReplay` selects the instrument basket and windows.
- `strategy_configs` binds strategy classes and parameters.
- `ExecutionModelConfig` holds queue-position and latency assumptions.
- `MarketReportConfig` controls terminal and HTML reporting.
- `build_replay_experiment(...)` owns cash, probability window,
  `min_book_events`, price-range filters, and log level.

## Multi-Market Strategy Configs

The executor supports one strategy instance per replay or one batch-level
strategy config that references the full basket.

Useful sentinels:

- `__SIM_INSTRUMENT_ID__` binds to the current replay instrument.
- `__ALL_SIM_INSTRUMENT_IDS__` binds to every loaded replay instrument.
- `__SIM_METADATA__:<key>` binds metadata from `BookReplay.metadata`.

This lets a runner keep the replay basket and strategy payload explicit inside
the `build_replay_experiment(...)` call without reintroducing module-level
configuration constants.

## Running Backtests

Interactive menu:

```bash
make backtest
```

Equivalent direct menu command:

```bash
uv run python main.py
```

Direct runner execution is usually better once you know what you want:

```bash
uv run python backtests/polymarket_book_ema_crossover.py
uv run python backtests/polymarket_book_ema_optimizer.py
uv run python backtests/polymarket_book_joint_portfolio_runner.py
uv run python backtests/polymarket_telonex_book_joint_portfolio_runner.py
```

To run every public Python backtest entrypoint:

```bash
uv run python scripts/run_all_backtests.py
```

The menu discovers flat Python and notebook runners. It uses `Textual`, supports
filtering with `/`, and shows the current runner file contents so you can
inspect the exact experiment before launching it.

## Notebook Runners

Flat notebook files under `backtests/` and `backtests/private/` are valid menu
entrypoints.

Notebook contract:

- The notebook must live at a flat discoverable path.
- Optional metadata under `metadata.prediction_market_backtest` can set `name`
  and `description`.
- If metadata is absent, the menu falls back to the filename and first markdown
  heading.
- Execution happens from the repo root through `nbclient`.

Public notebooks:

- `backtests/generic_optimizer_research.ipynb`
- `backtests/generic_tpe_research.ipynb`
- `backtests/pmxt_book_joint_portfolio_runner.ipynb`
- `backtests/telonex_book_joint_portfolio_runner.ipynb`

## Editing Runner Inputs

The public runner layer does not depend on shell env vars for experiment
definition. Edit the runner file, or copy it into `backtests/private/`, when
you want a different market, source priority, strategy, cash value, latency
model, or report path.

Primary edit surface:

- the `MarketDataConfig(...)` passed as `data`
- the `BookReplay(...)` entries passed as `replays`
- the `strategy_configs=(...)` tuple
- the `ExecutionModelConfig(...)` passed as `execution`
- the `MarketReportConfig(...)` passed as `report`
- the optimizer or experiment windows passed to the runner factory

Low-level env vars still exist for custom workflows:

- `POLYMARKET_GAMMA_BASE_URL`
- `POLYMARKET_TRADE_API_BASE_URL`
- `POLYMARKET_CLOB_BASE_URL`
- `PMXT_RAW_ROOT`
- `PMXT_REMOTE_BASE_URL`
- `PMXT_CACHE_DIR`
- `PMXT_DISABLE_CACHE`
- `TELONEX_LOCAL_DIR`
- `TELONEX_API_BASE_URL`
- `TELONEX_API_KEY`
- `TELONEX_CHANNEL`
- `TELONEX_CACHE_ROOT`
- `BACKTEST_ENABLE_TIMING=0`

## Data Vendor Notes

### Native Vendors

The public v3 runner surface is focused on Polymarket book replay through PMXT
and Telonex. Native source env vars remain available for lower-level extension
work, but public direct-runner examples should not reintroduce standalone
trade-tick replay.

### PMXT

- PMXT is the raw-hourly Polymarket L2 vendor path.
- Public runners usually list `local:/Volumes/storage/pmxt_data` first, then
  `archive:r2v2.pmxt.dev`, then `archive:r2.pmxt.dev`.
- PMXT source parsing is strict: only `local:` and `archive:` entries are
  supported in `MarketDataConfig.sources`.
- The local filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`.
- Timing output is enabled by default unless `BACKTEST_ENABLE_TIMING=0` is set.

### Telonex

- Telonex is the full-snapshot Polymarket L2 vendor path.
- Public Telonex runners use `data_type=Book`, `vendor=Telonex`, and
  `book_snapshot_full`.
- Telonex source parsing accepts `local:` and `api:` only.
- `api:` reads `TELONEX_API_KEY` from the environment or from
  `api:<key>` in runner source config. Do not commit private keys.
- API-day payloads are cached by default at
  `~/.cache/nautilus_trader/telonex`.
- Prefer `local:/Volumes/storage/telonex_data` for repeated research once the
  downloader has warmed the mirror.

For vendor-specific behavior and timings, use:

- [Data Vendors And Local Mirrors](data-vendors.md)
- [Vendor Fetch Sources And Timing](vendor-fetch-sources.md)
