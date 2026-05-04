# Setup

## Prerequisites

- Python 3.12+ (`3.13` recommended)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Install

```bash
git clone https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting

# conda's linker flags can conflict with the venv
unset CONDA_PREFIX

uv venv --python 3.13
uv pip install "nautilus_trader[polymarket,visualization]==1.225.0" bokeh plotly numpy py-clob-client duckdb textual nbformat nbclient ipykernel optuna python-dotenv
```

If you want to build docs locally:

```bash
uv pip install mkdocs-shadcn
```

You can also use:

```bash
make install
```

After setup, run commands with `uv run ...`. You do not need to manually
activate the virtualenv.

## First Run

Interactive menu:

```bash
make backtest
```

Equivalent direct menu command:

```bash
uv run python main.py
```

The menu shows flat runner entrypoints under `backtests/` and
`backtests/private/`. It supports filtering with `/`, arrow-key navigation, and
direct launch with `Enter`.

Direct Python runners:

```bash
uv run python backtests/polymarket_book_ema_crossover.py
uv run python backtests/polymarket_book_ema_optimizer.py
uv run python backtests/polymarket_book_joint_portfolio_runner.py
uv run python backtests/polymarket_telonex_book_joint_portfolio_runner.py
```

All public Python runners:

```bash
uv run python scripts/run_all_backtests.py
```

Public runner files carry their market, source, and execution assumptions in
code. To use a different market, source priority, or strategy config, edit the
runner directly or copy it into `backtests/private/`.

Repo-layer source syntax is explicit:

- PMXT book runners use `local:` and `archive:`.
- Telonex book runners use `local:` and `api:`.
- Public runners should use `data_type=Book` and `BookReplay`.
- Public Polymarket book runners replay L2 `OrderBookDeltas` and interleave
  real Polymarket `TradeTick` records for execution only. Strategies consume
  book state; trade ticks drive matching, queue-position updates, and
  `trade_execution=True`.

Mirror PMXT raw archive hours locally:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The PMXT downloader is incremental. Existing local files are skipped unless you
explicitly request overwrite behavior, so rerunning the command fills missing
hours without replacing completed hours.

PMXT replay loads can read multiple raw hours ahead. For local mirrors, the
repo wrapper defaults to `PMXT_PREFETCH_WORKERS=8`; adjust it only after
checking local disk throughput:

```bash
PMXT_PREFETCH_WORKERS=8 uv run python backtests/polymarket_book_joint_portfolio_runner.py
```

Mirror a small Telonex window:

```bash
TELONEX_API_KEY=... make download-telonex-data TELONEX_DOWNLOAD_FLAGS='\
  --market-slug us-recession-by-end-of-2026 \
  --outcome-id 0 \
  --channels book_snapshot_full onchain_fills trades \
  --start-date 2026-01-19 \
  --end-date 2026-02-01'
```

Mirror Telonex full-book data for all markets:

```bash
uv run python scripts/telonex_download_data.py \
  --destination /Volumes/storage/telonex_data \
  --all-markets \
  --channels book_snapshot_full onchain_fills trades
```

Add `--max-days 100` to run a bounded post-resume smoke test before starting a
full mirror.

`book_snapshot_full` is the canonical Telonex book channel. `onchain_fills` is
the preferred execution-tick source, and `trades` covers days where the
onchain-fill parquet is absent or empty before falling back to Polymarket's
public trade API. Telonex runner sources should list the intended local mirror
first, then include `api:${TELONEX_API_KEY}` as a runtime-expanded API fallback.

The Telonex downloader writes Hive-partitioned parquet files under
`<destination>/data/` and a DuckDB manifest at `<destination>/telonex.duckdb`.

Telonex replay loading has separate concurrency controls for different
resources. `BACKTEST_REPLAY_LOAD_WORKERS` defaults to `32` for replay-level
source staging, `BACKTEST_REPLAY_MATERIALIZE_WORKERS` defaults to `4` for the
memory-heavy replay object materialization stage, `TELONEX_API_WORKERS`
defaults to `128` for API fetches, and `TELONEX_FILE_WORKERS` defaults to `28`
for local parquet/DuckDB/cache file work.
It is crash-safe and resumable: completed days and empty days are recorded in
the manifest, and reruns skip already-recorded work. The writer queue is bounded
and periodically flushed so long `--all-markets` runs do not accumulate pending
Arrow tables indefinitely.

Throughput and memory controls:

- `--workers` controls concurrent HTTP downloads.
- `--max-days` caps post-resume day jobs for smoke tests.
- Telonex runner API day loading uses `TELONEX_PREFETCH_WORKERS`, default
  `128`.
- `--parse-workers` or `TELONEX_PARSE_WORKERS` controls concurrent Arrow
  parquet decoders.
- `--writer-queue-items` or `TELONEX_WRITER_QUEUE_ITEMS` bounds parsed day
  results waiting for the writer. Default: `128`.
- `--pending-commit-items` or `TELONEX_PENDING_COMMIT_ITEMS` bounds completed
  day results held before manifest commit. Default: `128`.
- The downloader still inserts an hourly forced writer drain that closes open
  Parquet part writers, commits their manifest rows, and prints RSS/open-part
  diagnostics. Higher queue limits improve throughput while staying finite.

## Timing And Cache Defaults

- Timing output is on by default for `make backtest`, `uv run python main.py`,
  and direct script runners that use `@timing_harness`.
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out.
- Normal Nautilus logs are still printed; timing output is additive.
- PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`.
- Public PMXT runners usually pin `local:/Volumes/storage/pmxt_data` first,
  `archive:r2v2.pmxt.dev` second, and `archive:r2.pmxt.dev` third.
- Telonex API-day cache is enabled by default at
  `~/.cache/nautilus_trader/telonex`.
- Telonex warm cache reads prefer `.fast.parquet` sidecars to avoid slow nested
  list-of-struct decoding.
- Telonex replay also materializes converted `OrderBookDeltas` under
  `book-deltas-v1` and non-empty converted execution `TradeTick`s under
  `trade-ticks-v1`; repeated backtests can skip local/API decoding and report
  `telonex deltas cache`, `telonex onchain_fills cache`, or
  `telonex trades cache` in timing output. Trade-tick source labels include the
  exact Telonex channel, such as `telonex local onchain_fills` or
  `telonex local trades`. Empty Telonex onchain-fill days continue to Telonex
  `trades`, then the Polymarket trade fallback.
- `make clear-telonex-cache` clears Telonex API-day and materialized replay
  caches, and refuses configured local data stores.
- `make clear-polymarket-cache` clears the Polymarket public trade-tick cache
  under `~/.cache/nautilus_trader/polymarket_trades`; Telonex cache clearing
  does not remove those fallback trade files.

## Extension Architecture

This repo does not vendor NautilusTrader in-tree. Runtime code comes from
upstream `nautilus_trader==1.225.0`, and local extensions live under
`prediction_market_extensions/`.

Extensions import from upstream Nautilus and add prediction-market-specific
adapters, fee models, fill models, replay adapters, and runner utilities. The
startup hook `install_commission_patch()` installs the corrected fee formula
used by this repository.

Do not install a local Nautilus fork from this repo. Normal setup is the
upstream PyPI package plus this checkout.
