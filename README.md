# prediction-market-backtesting

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/evan-kolberg/prediction-market-backtesting?style=social)

[![Licensing: Mixed](https://img.shields.io/badge/licensing-MIT%20%2B%20LGPL--3.0--or--later-blue.svg)](NOTICE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
![Python](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white)
![Rust](https://img.shields.io/badge/rust-1.93.1-CE422B?logo=rust&logoColor=white)
![Rust Edition](https://img.shields.io/badge/edition-2024-CE422B?logo=rust&logoColor=white)
![NautilusTrader](https://img.shields.io/badge/NautilusTrader-1.224.0-1E3A5F)
![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/evan-kolberg/prediction-market-backtesting)
![GitHub code size](https://img.shields.io/github/languages/code-size/evan-kolberg/prediction-market-backtesting)
![GitHub top language](https://img.shields.io/github/languages/top/evan-kolberg/prediction-market-backtesting)
![GitHub open issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)

> UNDER CONSTRUCTION: the public PMXT relay/VPS is being reworked. Expect
> processed/prebuilt coverage and VPS stats to be unstable while the old
> processed layer is purged and a ClickHouse-backed replacement is planned. In
> the meantime, the PMXT L2 runners support BYOD/local sources. See
> [`docs/pmxt-byod.md`](docs/pmxt-byod.md).

Relay VPS statistics (may be stale while the relay is under construction):

[![PMXT relay](https://209-209-10-83.sslip.io/v1/badge/status.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![Relay CPU](https://209-209-10-83.sslip.io/v1/badge/cpu.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay mem](https://209-209-10-83.sslip.io/v1/badge/mem.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay disk](https://209-209-10-83.sslip.io/v1/badge/disk.svg)](https://209-209-10-83.sslip.io/v1/system)

[![PMXT mirrored](https://209-209-10-83.sslip.io/v1/badge/mirrored.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT processed](https://209-209-10-83.sslip.io/v1/badge/processed.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT latest](https://209-209-10-83.sslip.io/v1/badge/latest.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![PMXT lag](https://209-209-10-83.sslip.io/v1/badge/lag.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![PMXT rate](https://209-209-10-83.sslip.io/v1/badge/rate.svg?v=1)](https://209-209-10-83.sslip.io/v1/stats)

[![PMXT file](https://209-209-10-83.sslip.io/v1/badge/prebuild-file.svg?v=1)](https://209-209-10-83.sslip.io/v1/events?limit=50)
[![PMXT rows](https://209-209-10-83.sslip.io/v1/badge/prebuild-progress.svg?v=1)](https://209-209-10-83.sslip.io/v1/events?limit=50)

If the processing rate fulls under 1 hr/hr, then that means the VPS will never catch up. Since this project became a lot more popular that I had expected, I may have to switch to something more sustainable like AWS. Still figuring this out.


## Note: This is still in development

Backtesting framework for prediction market trading strategies on [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built off of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with custom exchange adapters. More focus on Polymarket because of the free availability of L2 data.

Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Charting preview](https://raw.githubusercontent.com/evan-kolberg/prediction-market-backtesting/main/docs/assets/charting-preview.jpeg)

> IMPORTANT: Kalshi public backtests here are trade-tick replay only. Polymarket public backtests now default to the PMXT relay-backed historical L2 path, which replays hourly PMXT order-book events through NautilusTrader's `L2_MBP` matching engine. That is much better than the old one-tick slippage proxy for taker-style execution, but it still does **not** fully solve passive-order realism because public L2 MBP data does not reveal true queue position. Also, cold PMXT archive ingestion can still take a long time, especially for larger windows. That is why I set up a mirror/relay on a VPS that pre-crunches book data for each market. More on that below.

## Table of Contents

- [Setup](#setup)
- [Writing Strategies and Backtests](#writing-strategies-and-backtests)
- [Running Backtests](#running-backtests)
- [Execution Modeling](#execution-modeling)
- [PMXT Relay](#pmxt-relay)
- [Plotting](#plotting)
- [Testing](#testing)
- [Updating the Subtree](#updating-the-subtree)
- [Roadmap](#roadmap)
- [Known Issues](#known-issues)
- [License](#license)
  - [What is LGPL-3.0-or-later](#what-is-lgpl-30-or-later)
  - [NautilusTrader attribution](#nautilustrader-attribution)
  - [What this means for you](#what-this-means-for-you)


## Setup

### Prerequisites

- Python 3.12+ (3.13 recommended)
- [Rust toolchain](https://rustup.rs/) ≥ 1.93.1 — required to build NautilusTrader's Cython/Rust extensions
- [uv](https://docs.astral.sh/uv/getting-started/installation/) — for virtual environment and package management

### Install

```bash
git clone https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting

# conda's linker flags conflict with the rust build
unset CONDA_PREFIX

# create a venv and install everything
# compiling the rust & cython extensions will take a hot minute
uv venv --python 3.13
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb
```

You can also use:

```bash
make install
```

After setup, use `uv run python <script>` to run anything. No manual `source .venv/bin/activate` is needed.

## Writing Strategies and Backtests

This repo now has a hard split:

- `strategies/` contains reusable strategy classes and configs.
- `strategies/private/` is for git-ignored local strategy modules.
- `backtests/` contains runnable backtest entrypoints and orchestration helpers.
- `backtests/private/` is for git-ignored local backtest runners.

Good public examples:

- Reusable EMA strategy logic: [`strategies/ema_crossover.py`](strategies/ema_crossover.py)
- Reusable final-period momentum logic: [`strategies/final_period_momentum.py`](strategies/final_period_momentum.py)
- Reusable late-favorite limit-hold logic: [`strategies/late_favorite_limit_hold.py`](strategies/late_favorite_limit_hold.py)
- Kalshi trade-tick runner using a root strategy module: [`backtests/kalshi_trade_tick/kalshi_breakout.py`](backtests/kalshi_trade_tick/kalshi_breakout.py)
- PMXT Polymarket quote-tick runners live under `backtests/polymarket_quote_tick/`.
  Current public runners cover EMA crossover, breakout, RSI reversion, spread
  capture, panic fade, VWAP reversion, threshold momentum, final-period
  momentum, deep value, and late-favorite limit hold.
- Archived legacy Polymarket trade-tick runners live under [`backtests/polymarket_trade_tick/`](backtests/polymarket_trade_tick).

Backtest entrypoints should expose three things at module level:

```python
NAME = "my_strategy"           # shown in the menu
DESCRIPTION = "one-liner"      # shown in the menu

async def run() -> None:       # called when selected
    ...
```

Use the root `strategies` package for signal logic, then import that logic into a thin backtest runner. Export new reusable configs and classes from [`strategies/__init__.py`](strategies/__init__.py) so runners can import them cleanly.

Three common runner patterns already exist:

- Kalshi trade-tick backtests via [`backtests/kalshi_trade_tick/_kalshi_single_market_trade_runner.py`](backtests/kalshi_trade_tick/_kalshi_single_market_trade_runner.py)
- Polymarket trade-tick backtests via [`backtests/polymarket_trade_tick/_polymarket_single_market_runner.py`](backtests/polymarket_trade_tick/_polymarket_single_market_runner.py)
- Polymarket PMXT L2 backtests via [`backtests/polymarket_quote_tick/_polymarket_single_market_pmxt_runner.py`](backtests/polymarket_quote_tick/_polymarket_single_market_pmxt_runner.py)

## Running Backtests

Interactive menu:

```bash
make backtest
```

Any module in `backtests/` or `backtests/private/` with `NAME`, `DESCRIPTION`, and `async def run()` shows up here.

Equivalent direct command:

```bash
uv run python main.py
```

Direct script execution is usually better once you know which runner you want:

```bash
MARKET_TICKER=<kalshi-market-ticker> uv run python backtests/kalshi_trade_tick/kalshi_breakout.py
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
MARKET_SLUG=<polymarket-market-slug> uv run python backtests/polymarket_trade_tick/polymarket_vwap_reversion.py
```

If you omit the market env vars, the public runners fall back to the defaults
bundled in each module. The PMXT single-market L2 runners are intentionally
pinned to one known-good historical slice in code, and the PMXT sports
multi-market example is pinned to a small fixed set of recent March 2026 sports
futures with explicit multi-day windows, so those examples still replay cleanly
without the latest upstream PMXT hours.

Most runners are configured through environment variables. Common ones:

- `MARKET_TICKER` for Kalshi single-market runners
- `MARKET_SLUG` for Polymarket trade-tick single-market runners
- `TOKEN_INDEX` to choose which Polymarket outcome token to backtest
- `LOOKBACK_DAYS` for data window size
- `PMXT_DATA_SOURCE` for PMXT L2 runner source selection:
  `auto`, `relay`, `raw-remote`, `raw-local`, or `filtered-local`
- `PMXT_LOCAL_MIRROR_DIR` for `PMXT_DATA_SOURCE=raw-local`
- `PMXT_LOCAL_FILTERED_DIR` for `PMXT_DATA_SOURCE=filtered-local`
- `PMXT_RELAY_BASE_URL` to override the default public relay or disable it
  with `PMXT_RELAY_BASE_URL=0`
- `PMXT_CACHE_DIR` / `PMXT_DISABLE_CACHE` to control the local PMXT filtered
  parquet cache
- `TRADE_SIZE` and `INITIAL_CASH` for sizing
- `TARGET_RESULTS` for multi-market runners

For exact PMXT BYOD layouts and the expected L2 payload shape, see
[`docs/pmxt-byod.md`](docs/pmxt-byod.md).

The interactive menu always prints total wall time. PMXT fetch timing
instrumentation is enabled by default in `make backtest` / `uv run python
main.py`. If you want a quieter run, turn it off explicitly:

```bash
BACKTEST_ENABLE_TIMING=0 make backtest
```

## Execution Modeling

Backtests here replay venue data from Kalshi and Polymarket into NautilusTrader.
The main things which affect realized backtest performance beyond the raw API
data are:

- exchange fee models
- slippage for taker-style orders
- existing engine behavior such as IOC handling, price rounding, cash-account limits, and `AccountBalanceNegative` stops

### Fees

- Kalshi uses a nonlinear expected-earnings fee model.
- Polymarket uses the venue fee model plus CLOB `fee-rate` enrichment when the
  market payload itself reports zero fees.
- If a venue reports zero fees for a market, the backtest also applies zero fees.

### Slippage

- Shared prediction-market backtests default to a custom taker fill model.
- Non-limit orders get a deterministic one-tick adverse fill.
- Polymarket uses the market's own tick size (trade tick config).
- Kalshi uses one cent as the effective order tick for taker slippage.
- Limit orders keep the default Nautilus matching behavior and do not get the
  forced one-tick adverse move.
- PMXT-backed Polymarket L2 backtests do **not** use the synthetic one-tick
  taker fill model. They replay historical `OrderBookDeltas` with
  `book_type=L2_MBP` and `liquidity_consumption=True`.

### Limits

- Kalshi backtests are trade-tick replay only (conservative taker-execution
  proxy). Polymarket PMXT-backed backtests are full L2 order-book replay.
- Neither path models queue position for passive/maker orders — public L2 MBP
  data does not reveal where a resting order sits in the queue.
- Taker-heavy strategies that try to harvest very small price changes can look
  much worse once fees and one-tick slippage are turned on.
- PMXT L2 data is useful for modeling taker slippage (book depth tells you how
  far a market order walks through levels), but maker fill probability requires
  L3 data we don't have.

### PMXT Polymarket L2

- The public relay is currently under construction. If it is slow, unavailable,
  or still catching up, use BYOD/local PMXT sources instead of waiting on the
  VPS.
- PMXT quote-tick runners now support explicit source selection:
  - `PMXT_DATA_SOURCE=auto` keeps the current cache -> relay -> raw remote chain
  - `PMXT_DATA_SOURCE=relay` forces relay-first mode
  - `PMXT_DATA_SOURCE=raw-remote` skips the relay
  - `PMXT_DATA_SOURCE=raw-local PMXT_LOCAL_MIRROR_DIR=/path/to/raw` replays a local raw PMXT mirror
  - `PMXT_DATA_SOURCE=filtered-local PMXT_LOCAL_FILTERED_DIR=/path/to/filtered` replays local prefiltered parquet only
- The exact BYOD directory layout and required L2 payload shape are documented
  in [`docs/pmxt-byod.md`](docs/pmxt-byod.md).
- Public Polymarket PMXT runners now default to the public relay at
  `https://209-209-10-83.sslip.io`.
- For each required hour, the loader tries the relay first:

```text
https://209-209-10-83.sslip.io/v1/filtered/<condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

  and only falls back to the raw PMXT archive on `r2.pmxt.dev` if that relay
  hour is missing or unavailable.
- The relay returns the same two-column parquet shape the loader already
  consumes:
  - `update_type`
  - `data`
- That means the backtests stay plug-and-play. There is no separate Polymarket
  relay data model at the strategy or runner layer.
- The loader still scans one extra hour before the requested start so it can
  find a valid snapshot and rebuild the book. File count is therefore roughly
  `requested_window_hours + 2`.
- Example file counts:
  - a 2-hour window needs about 4 hourly files
  - a 4-hour window needs about 6 hourly files
  - a 24-hour window needs about 26 hourly files
  - a 48-hour window needs about 50 hourly files
- When the loader has to fall back to raw PMXT, it is still expensive:
  - it opens each remote parquet file over HTTPS and uses range requests
  - parquet pushdown can prune by `market_id` and `update_type`
  - `token_id` still cannot be pruned at the parquet level because it lives
    inside the JSON `data` payload
  - the loader therefore still has to pull the surviving `data` strings for
    the market-level scan back to this machine, regex-filter them for one
    `token_id`, decode the JSON, replay the book, and build a fully ordered
    in-memory event list before the backtest can start
- The raw fallback path is faster than it used to be:
  - Arrow record batches are streamed incrementally instead of materializing
    whole hourly tables before decode
  - remote HTTPS reads use in-memory readahead by default with `32 MiB` blocks
  - multiple hours are prefetched in parallel while still yielding them back in
    chronological order
- Relay-backed PMXT is much faster once the server has already precrunched the
  requested hours:
  - in one same-hour benchmark, a relay fetch took about `0.478s` versus
    `45.309s` for the equivalent raw PMXT remote scan, or roughly `95x` faster
  - in one end-to-end EMA crossover run on the same market/window, the default
    relay-backed path took about `14.02s` versus `189.18s` with
    `PMXT_RELAY_BASE_URL=0`, while producing the same fills and PnL
- Relay controls:
  - default relay: `https://209-209-10-83.sslip.io`
  - `PMXT_RELAY_BASE_URL=https://your-relay-host` overrides the relay host
  - `PMXT_RELAY_BASE_URL=0` disables relay usage and forces raw archive scans
- Raw fallback tuning:
  - `PMXT_PREFETCH_WORKERS=8` changes hourly prefetch parallelism
  - `PMXT_HTTP_BLOCK_SIZE_MB=64` changes the in-memory HTTP readahead block size
  - `PMXT_HTTP_CACHE_TYPE=bytes` switches the HTTP file cache strategy
- Local PMXT disk cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`. The loader writes the filtered hourly parquet
  table for one market/token/hour to:

```text
~/.cache/nautilus_trader/pmxt/<condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

- Local cache reuse rules:
  - same market, same token, same hour: cache hit
  - same market, same token, overlapping window: overlapping cached hours are reused
  - same hour, different market: no reuse, because the cache key includes `condition_id`
  - same market, different token/outcome: no reuse, because the cache key includes `token_id`
- This means repeated runs of the same PMXT market can get much faster after
  the first pass, but multi-market runs still do not
  share a universal raw-hour cache.
- Local cache controls:
  - unset `PMXT_CACHE_DIR` to use the default cache root at `~/.cache/nautilus_trader/pmxt`
  - `PMXT_CACHE_DIR=/custom/path` uses a custom cache root
  - `PMXT_CACHE_DIR=0` disables local PMXT disk cache
  - `PMXT_DISABLE_CACHE=1` disables local PMXT disk cache entirely
- Cache size is currently unbounded. There is no eviction policy or size cap.
  If disk cache stays enabled, it grows with the number of unique
  `(condition_id, token_id, hour)` tuples you backtest. Very active markets and
  long lookbacks will produce larger cached parquet files than quiet markets
  and short windows. Check current size with:

```bash
du -sh ~/.cache/nautilus_trader/pmxt
```

Or clear it with:

```bash
make clear-pmxt-cache
```

- Example: run a relay-backed PMXT backtest with the runner's bundled sample
  market/window and the default local cache:

```bash
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

- Example: run the same PMXT backtest against a local raw mirror:

```bash
PMXT_DATA_SOURCE=raw-local \
PMXT_LOCAL_MIRROR_DIR=/data/pmxt/raw \
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

- Example: run it against local prefiltered parquet only:

```bash
PMXT_DATA_SOURCE=filtered-local \
PMXT_LOCAL_FILTERED_DIR=/data/pmxt/filtered \
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

- If the relay does not have a requested hour yet, the loader still falls back
  cleanly to raw PMXT archive scans, so first-run relay misses can remain slow
  until the relay backfill reaches that part of history.
- Short windows can still fail if the selected range never includes usable L2
  book state for that instrument.

## PMXT Relay

The repo now includes a self-hosted relay implementation under
[`pmxt_relay/`](pmxt_relay/).

> PLEASE do NOT try to break into the VPS. There is nothing valuable on it; it is a mirror to speed things up. With this, please do not misuse the API. It's here for you to use, not abuse.

> The public relay has 4 TB of storage, 6 GB of RAM, and 4 shared vCPUs.

The relay is built to solve the PMXT cold-start problem without changing the
backtest schema. Three pipeline stages run continuously:

1. **Mirror** — poll the PMXT archive index and download every raw hourly
   parquet file
2. **Shard** — extract `market_id` and `token_id` columns from the raw JSON
   into one canonical processed parquet per hour
3. **Prebuild** — split each processed hour into ~37K tiny filtered
   `(condition_id, token_id, hour)` parquet files so backtests can pull only
   the markets they need. Newest hours are prebuilt first so recent data is
   available quickly while the backlog fills in.

The filtered output is served over HTTP and the PMXT loader fetches it
directly via `PMXT_RELAY_BASE_URL`.

The filtered output keeps the same two columns the loader already consumes:

- `update_type`
- `data`

That means the backtests do not need a new data model. They just get a much
cheaper first hop than scanning raw global PMXT hours on every cold run.

Relay layout:

```text
.pmxt-relay/              # default local dev root
  raw/YYYY/MM/DD/...
  processed/YYYY/MM/DD/...
  filtered/<condition_id>/<token_id>/...
  state/relay.sqlite3
  tmp/
```

On the VPS, point `PMXT_RELAY_DATA_DIR` somewhere larger such as
`/srv/pmxt-relay`.

Three long-running processes are expected:

- API server: `uv run python -m pmxt_relay api`
- mirror/precompute worker: `uv run python -m pmxt_relay worker`
- filtered prebuild worker: `uv run python -m pmxt_relay prebuild-filtered`

Systemd examples live in [`pmxt_relay/systemd/`](pmxt_relay/systemd/) so all
three services can come back automatically after reboot.

For a public-facing relay, the shipped API now also:

- rate-limits requests per IP
- validates market, token, and filename path shapes before touching disk
- keeps raw passthrough off by default
- serves hardened headers on both JSON and parquet routes

The full deployment and hardening notes live in
[`pmxt_relay/README.md`](pmxt_relay/README.md).

Relay progress can be checked over HTTP:

- `/v1/stats` for high-level counts
  This now includes `processed_hours_last_24h` and
  `processed_hours_per_hour_24h` so we can see whether the relay is actually
  finishing hours fast enough instead of just looking busy.
- `/v1/system` for live CPU, memory, and relay-disk usage
- `/v1/queue` for `pending/processing/error` queue state
- `/v1/events?limit=100` for recent discover/download/process/error events
- `/v1/inflight` for active temp-tree progress while a large hour is still
  being crunched
- `/v1/badge/status`, `/v1/badge/backfill`, `/v1/badge/mirrored`,
  `/v1/badge/processed`, `/v1/badge/rate`, `/v1/badge/latest`, `/v1/badge/lag`,
  `/v1/badge/prebuild-file`, `/v1/badge/prebuild-progress`, `/v1/badge/cpu`,
  `/v1/badge/mem`, and `/v1/badge/disk` for the live README status tags

The relay mirrors the full PMXT archive and stores a single processed shard per
hour alongside a background-prebuilt filtered cache. Any single PMXT backtest only
downloads the tiny per-`(condition_id, token_id, hour)` slices it needs. The
point is that one backtest no longer has to scan or download the global hourly
archive file just to recover one market.
The first full backfill is still limited by preprocessing throughput, not by
serving speed, but once a given hour has been processed the relay path is far
cheaper than raw archive scanning.

## Plotting

Single-market plotting is built into the shared runner flow used by the public prediction-market backtests. Good examples:

- [`backtests/kalshi_trade_tick/kalshi_breakout.py`](backtests/kalshi_trade_tick/kalshi_breakout.py)
- [`backtests/kalshi_trade_tick/kalshi_panic_fade.py`](backtests/kalshi_trade_tick/kalshi_panic_fade.py)
- [`backtests/polymarket_quote_tick/polymarket_pmxt_relay_panic_fade.py`](backtests/polymarket_quote_tick/polymarket_pmxt_relay_panic_fade.py)
- [`backtests/polymarket_quote_tick/polymarket_pmxt_relay_vwap_reversion.py`](backtests/polymarket_quote_tick/polymarket_pmxt_relay_vwap_reversion.py)

These write HTML charts to `output/`, typically with names like `output/<backtest>_<market>_legacy.html`.

PMXT single-market runners do the same. Example outputs:

- `output/polymarket_pmxt_relay_ema_crossover_<market>_legacy.html`
- `output/polymarket_pmxt_relay_breakout_<market>_legacy.html`
- `output/polymarket_pmxt_relay_rsi_reversion_<market>_legacy.html`
- `output/polymarket_pmxt_relay_spread_capture_<market>_legacy.html`
- `output/polymarket_pmxt_relay_sports_vwap_reversion_combined_legacy.html`
- `output/polymarket_pmxt_relay_sports_vwap_reversion_multi_market.html`

Multi-market plotting examples are still available under the archived
[`backtests/polymarket_trade_tick/`](backtests/polymarket_trade_tick) set:

- [`backtests/polymarket_trade_tick/polymarket_sports_final_period_momentum.py`](backtests/polymarket_trade_tick/polymarket_sports_final_period_momentum.py)
- [`backtests/polymarket_trade_tick/polymarket_sports_vwap_reversion.py`](backtests/polymarket_trade_tick/polymarket_sports_vwap_reversion.py)

Those runners write one legacy multi-market summary chart to `output/`, typically
with names like:

- `output/polymarket_sports_final_period_momentum_multi_market.html`
- `output/polymarket_sports_vwap_reversion_multi_market.html`

The Polymarket trade-tick multi-market examples are archived and no longer
shown in the default menu, but they are still the clearest references for the
combined multi-market plotting flow.

## Testing

```bash
make test
```

Runs the repo pytest suite. Coverage is mixed: fast unit tests for strategy,
loader, cache, and relay logic; relay processor/API integration tests against
temp dirs; and a small number of smoke tests that exercise real backtest flows.
Generated legacy-chart output is redirected to isolated pytest temp directories
so nothing in the working tree is mutated.

## Updating the Subtree

```bash
make update
```

Unlike git submodules, subtrees copy upstream code directly into this repo — there's no live link. `make update` currently pulls the upstream `nautilus_pm` `charting` branch.

## Roadmap

- [ ] live paper trading mode *** lowest priority currently
- [x] multi-market support within strategies
- [x] better position sizing capabilities
- [x] fee modeling [PR#4](https://github.com/ben-gramling/nautilus_pm/pull/4)
- [ ] total slippage modeling *** PMXT L2 data is good for taker modeling, but we can't model maker w/o L3 [PR#6](https://github.com/ben-gramling/nautilus_pm/pull/6), [PR#9](https://github.com/evan-kolberg/prediction-market-backtesting/pull/9)
- [x] polymarket L2 order book backtests [PR#10](https://github.com/evan-kolberg/prediction-market-backtesting/pull/10)
- [x] public relay on a VPS for pre-crunching book data from PMXT -- massively speeds up backtests [PR#17](https://github.com/evan-kolberg/prediction-market-backtesting/pull/17), [PR#18](https://github.com/evan-kolberg/prediction-market-backtesting/pull/18), [PR#19](https://github.com/evan-kolberg/prediction-market-backtesting/pull/19), [PR#20](https://github.com/evan-kolberg/prediction-market-backtesting/pull/20), [PR#21](https://github.com/evan-kolberg/prediction-market-backtesting/pull/21), [PR#22](https://github.com/evan-kolberg/prediction-market-backtesting/pull/22), [PR#24](https://github.com/evan-kolberg/prediction-market-backtesting/pull/24), [PR#25](https://github.com/evan-kolberg/prediction-market-backtesting/pull/25), [PR#34](https://github.com/evan-kolberg/prediction-market-backtesting/pull/34), [PR#35](https://github.com/evan-kolberg/prediction-market-backtesting/pull/35), [PR#36](https://github.com/evan-kolberg/prediction-market-backtesting/pull/36)
- [ ] kalshi L2 order book backtests **** we don't have this data yet
- [x] much better & informative charting [PR#5](https://github.com/ben-gramling/nautilus_pm/pull/5)


## Known Issues

No outstanding repo-level issues are currently tracked here.

Recently fixed:

- [x] PMXT relay misses or raw-fallback PMXT L2 loads can still take a long time -- relay now returns 404 for non-prebuilt hours (no more server-side scanning), client falls back to r2.pmxt.dev [PR#22](https://github.com/evan-kolberg/prediction-market-backtesting/pull/22)
- [x] Public relay rate-limiting previously collapsed proxied clients into one shared bucket behind Caddy; relay now trusts forwarded client IPs only from configured local proxies [PR#25](https://github.com/evan-kolberg/prediction-market-backtesting/pull/25)
- [x] Relay request-rate buckets could accumulate stale one-off clients forever; expired buckets are now pruned instead of lingering in memory [PR#25](https://github.com/evan-kolberg/prediction-market-backtesting/pull/25)
- [x] Relay throughput was too opaque to judge from backlog counts alone; observability now includes a rolling completed-hours rate plus live prebuild file and row-progress badges [PR#34](https://github.com/evan-kolberg/prediction-market-backtesting/pull/34), [PR#35](https://github.com/evan-kolberg/prediction-market-backtesting/pull/35)
- [x] Relay worker and prebuild services could crash and reset inflight work on transient SQLite lock contention; relay DB writes now retry instead of killing the process, and read-only startup paths avoid unnecessary maintenance writes [PR#36](https://github.com/evan-kolberg/prediction-market-backtesting/pull/36)
- [x] PMXT L2 backtests could fail with `No order book data found` on longer windows because quote ticks could be replayed ahead of their first book snapshot; PMXT events are now ordered by event time with book updates before quotes [PR#26](https://github.com/evan-kolberg/prediction-market-backtesting/pull/26)
- [x] `polymarket_simple_quoter.py` could fail when run directly because the repo-root bootstrap happened too late for `_defaults`; direct-script imports now initialize the repo root first [PR#26](https://github.com/evan-kolberg/prediction-market-backtesting/pull/26)

## License

This repository uses **mixed licensing** because it vendors and extends
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader), which is
licensed under the
[GNU Lesser General Public License v3.0 or later (LGPL-3.0-or-later)](https://www.gnu.org/licenses/lgpl-3.0.en.html).

### What is LGPL-3.0-or-later

| Scope | License | File |
|---|---|---|
| `nautilus_pm/` (vendored NautilusTrader subtree) | LGPL-3.0-or-later | [`nautilus_pm/LICENSE`](nautilus_pm/LICENSE) |
| Root files with a "Derived from NautilusTrader" or "Modified by Evan Kolberg" notice (most of `backtests/`, `strategies/`, and select `tests/` files) | LGPL-3.0-or-later | [`COPYING.LESSER`](COPYING.LESSER), [`COPYING`](COPYING) |
| Everything else (`main.py`, `Makefile`, docs, repo metadata, etc.) | MIT | [`LICENSE-MIT`](LICENSE-MIT) |

The full LGPL and GPL license texts are in [`COPYING.LESSER`](COPYING.LESSER) and
[`COPYING`](COPYING). The [`NOTICE`](NOTICE) file lists every LGPL-covered file
outside the subtree, along with modification dates and upstream lineage.

### NautilusTrader attribution

This project includes a vendored copy of
[NautilusTrader](https://github.com/nautechsystems/nautilus_trader)
(Copyright 2015-2026 Nautech Systems Pty Ltd) under `nautilus_pm/`. The upstream
LGPL-3.0-or-later license and copyright notices are preserved in
[`nautilus_pm/LICENSE`](nautilus_pm/LICENSE). All files modified or added within
the vendored subtree carry dated file-level notices identifying the changes. The
most recent subtree sync references upstream split commit `f51c805c9f`.

### What this means for you

- **Using this repo as-is**: no action needed. The LGPL obligations are satisfied
  by the notices already present.
- **Forking or redistributing**: you must keep the LGPL license files, the
  [`NOTICE`](NOTICE), and the per-file modification headers intact. Any further
  modifications to LGPL-covered files must also be noted.
- **Linking as a library**: if you import LGPL-covered modules into your own
  proprietary project, the LGPL requires that users can re-link against modified
  versions of the LGPL code. See the
  [LGPL FAQ](https://www.gnu.org/licenses/gpl-faq.html#LGPLStaticVsDynamic) for
  details.

See [`LICENSE`](LICENSE) for the top-level guide, or [`NOTICE`](NOTICE) for the
full file-by-file breakdown.

---

## Star History

<a href="https://www.star-history.com/#evan-kolberg/prediction-market-backtesting&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
 </picture>
</a>
