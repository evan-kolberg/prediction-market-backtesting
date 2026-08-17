# Backtests And Runners

## Repo Layout

- `strategies/` contains reusable strategy classes and configs.
- `strategies/private/` contains historical strategy modules that were once
  local-only. They remain in that namespace for import compatibility.
- `backtests/` contains flat public runner entrypoints.
- `backtests/private/` contains historical Telonex research runners that were
  once local-only. They are tracked as an archive, not as the recommended
  baseline smoke-test set.
- `prediction_market_extensions/backtesting/` contains shared runner plumbing,
  data-source adapters, timing, reporting, artifacts, and optimizer helpers.

The menu only considers flat `backtests/*.py`, `backtests/*.ipynb`,
`backtests/private/*.py`, and `backtests/private/*.ipynb` files. Python files
still need a `run()` function or `EXPERIMENT` object to appear. Subdirectories
under `backtests/` should be support code only.

Current public Python runners:

- `backtests/polymarket_book_ema_crossover.py`
- `backtests/polymarket_book_ema_optimizer.py`
- `backtests/polymarket_book_joint_portfolio_runner.py`
- `backtests/polymarket_beffer45_trade_replay_telonex.py`
- `backtests/polymarket_btc_5m_late_favorite_taker_hold.py`
- `backtests/polymarket_btc_5m_pair_arbitrage.py`
- `backtests/polymarket_marketlens_book_btc_5m_pair_arbitrage.py`
- `backtests/polymarket_pmxt_book_100_replay_runner.py`
- `backtests/polymarket_telonex_book_100_replay_runner.py`
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

## Archived Private Research

The `private/` namespace is now a historical research archive. The files remain
under `backtests/private/` and `strategies/private/` so old import paths and
saved model profiles keep working, but they are no longer hidden from git.
Treat them as study material and reproducibility aids, not as maintained trading
recommendations or guaranteed public smoke tests.

Tracked private strategies:

- `strategies/private/btc_snapshot_model.py`: the BTC 5m snapshot model
  strategy used by the archived Nautilus replay validator and live sandbox
  runners.
- `strategies/private/passive_pair_accumulation.py`: a maker-first
  complementary-token strategy. It watches paired YES/NO books, posts post-only
  buy limits on both legs when the fee-adjusted combined passive cost is below
  settlement value, holds matched shares to resolution, and market-sells any
  unmatched surplus after a bounded completion window.

Tracked private cache helpers:

- `backtests/private/telonex_btc_5m_polymarket_download.py`
- `backtests/private/telonex_binance_btc_download.py`

The Polymarket helper downloads BTC 5m `book_snapshot_full` API-day files into
the Telonex cache layout. The Binance helper downloads spot `trades`,
`book_snapshot_25`, and optional `quotes` parquet days into the spot cache
layout used by the snapshot research runner. Both are direct CLI helpers with
`main()` entrypoints, and both require `TELONEX_API_KEY` in the environment or
`.env`.

Tracked BTC 5m snapshot model runners:

```text
backtests/private/telonex_btc_5m_snapshot_model_research.py
backtests/private/telonex_btc_5m_snapshot_model_walkforward.py
backtests/private/telonex_btc_5m_snapshot_model_runner_validate.py
```

The research runner builds BTC 5m snapshot rows, fits the logistic model used by
the published `live/models/btc_snapshot_model_*.json` profiles, evaluates fixed
policy grids, and writes research artifacts under
`output/telonex_churn/model_research/`. The walk-forward runner consumes a
dataset CSV emitted by that research runner, refits the logistic model across
rolling train/validation/holdout folds, and writes fold-level CSV and JSON
summaries. The runner validator replays a selected model through Nautilus/Telonex
L2 chunks with the archived strategy, then writes chunk CSVs, fills/skips CSVs,
a summary JSON, and an HTML report.

Tracked BTC 5m strategy-family validators:

- `backtests/private/telonex_btc_5m_passive_pair_accumulation_search.py`
- `backtests/private/telonex_btc_5m_passive_pair_forward_validate.py`
- `backtests/private/telonex_btc_5m_passive_pair_chunked_forward_validate.py`
- `backtests/private/telonex_btc_5m_late_favorite_chunked_forward_validate.py`
- `backtests/private/telonex_btc_5m_microprice_imbalance_chunked_forward_validate.py`
- `backtests/private/telonex_btc_5m_pair_arbitrage_chunked_forward_validate.py`

The passive-pair search uses the archived
`BookPassivePairAccumulationStrategy`. The other validators reuse public
strategy classes such as late-favorite taker-hold, microprice imbalance, and
binary pair arbitrage with archived champion parameters and later BTC 5m
forward windows. Several of these modules share evaluation helpers from the
passive-pair search runner; that coupling is historical and intentionally kept
visible.

Tracked broader Telonex research runners:

- `backtests/private/telonex_general_market_value_rebound_search.py`
- `backtests/private/telonex_resolved_sports_research.py`

The general-market runner compares deep-value, panic-fade, and VWAP-reversion
candidates across a configurable subset of the Telonex 100-market replay sample
and defaults to 80 selected slugs split between train and holdout. The
resolved-sports runner discovers recently resolved Polymarket sports markets
and compares late-favorite or final-period-momentum candidates over their
closing windows.

These archived runners usually require Telonex API access, local Telonex cache
coverage, and enough memory for multi-market L2 replay. The expected local cache
layouts for the BTC snapshot workflow are:

```text
~/.cache/nautilus_trader/telonex/api-days/*/polymarket/book_snapshot_full/{slug}/outcome={Up,Down}/{date}.parquet
~/.cache/nautilus_trader/telonex-binance/raw/binance/{symbol}/trades/{date}.parquet
~/.cache/nautilus_trader/telonex-binance/raw/binance/{symbol}/book_snapshot_25/{date}.parquet
~/.cache/nautilus_trader/telonex-binance/raw/binance/{symbol}/quotes/{date}.parquet
```

The quote cache is only required for profiles trained with quote features.
Cross-asset profiles additionally require matching `ethusdt`, `solusdt`, or
`xrpusdt` spot caches.

All tracked private runners define `run()` and can appear in the backtest menu.
They are intentionally omitted from `scripts/run_all_backtests.py`, which only
targets first-class public runners under `backtests/*.py`.

A Telonex subscriber can use these files to approximate the original workflow:
hydrate the Polymarket and Binance caches for the dates and spot symbols in the
target model summary, run the research script with matching `TELONEX_CHURN_*`
environment settings, then run the walk-forward or runner-validation scripts
against the emitted dataset/model. Exact bit-for-bit reproduction still depends
on Telonex retaining the same historical files and on matching the original
environment choices recorded in the model JSON summaries and runner constants.

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
                pnl_label="PnL (pUSD)",
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
        pnl_label="PnL (pUSD)",
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
- `backtests/polymarket_beffer45_trade_replay_telonex.ipynb`
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

The public runner surface is focused on Polymarket book replay through PMXT,
Telonex, and Marketlens. Native source env vars remain available for
lower-level extension work, but public direct-runner examples should not
reintroduce standalone trade-tick replay.

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
- Public Telonex runners list `api:${TELONEX_API_KEY}` first, then
  `local:/Volumes/storage/telonex_data` as the standard local mirror fallback.
- `api:` reads `TELONEX_API_KEY` from the environment or from
  `api:<key>` in runner source config. Do not commit private keys.
- API-day payloads are cached by default at
  `~/.cache/nautilus_trader/telonex`.

### Marketlens

- Marketlens is the hosted snapshot-plus-delta Polymarket L2 vendor path.
- Public Marketlens runners use `data_type=Book` and `vendor=Marketlens`.
- Marketlens source parsing accepts `api:` only; there is no local mirror mode.
- Public Marketlens runners list `sources=("api:${MARKETLENS_API_KEY}",)`.
- `api:` reads `MARKETLENS_API_KEY` from the environment or from `api:<key>` in
  runner source config. Do not commit private keys.
- Book deltas and execution trade ticks come from one history stream per
  market, cached by default at `~/.cache/nautilus_trader/marketlens`.
- Only resolved, delta-collected markets are served; anything else is skipped
  with a warning rather than replayed at lower fidelity.

For vendor-specific behavior and timings, use:

- [Data Vendors And Local Mirrors](data-vendors.md)
- [Vendor Fetch Sources And Timing](vendor-fetch-sources.md)
- [Data Loading](data-loading.md)
