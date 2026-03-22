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

Relay VPS:
[![PMXT relay](https://209-209-10-83.sslip.io/v1/badge/status.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT mirrored](https://209-209-10-83.sslip.io/v1/badge/mirrored.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT processed](https://209-209-10-83.sslip.io/v1/badge/processed.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT latest](https://209-209-10-83.sslip.io/v1/badge/latest.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![PMXT lag](https://209-209-10-83.sslip.io/v1/badge/lag.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![Relay CPU](https://209-209-10-83.sslip.io/v1/badge/cpu.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay mem](https://209-209-10-83.sslip.io/v1/badge/mem.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay disk](https://209-209-10-83.sslip.io/v1/badge/disk.svg)](https://209-209-10-83.sslip.io/v1/system)

# This is still in development. Relay backtests may not work until I get it all sorted out.

Backtesting framework for prediction market trading strategies on [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built off of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with custom exchange adapters. More focus on Polymarket because of the free availability of L2 data.

Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Image](https://github.com/user-attachments/assets/e9b00915-9413-42d8-aeff-c2bde627c3d8)

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
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client
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
- PMXT Polymarket quote-tick runners live under `backtests/polymarket_pmxt_relay_*.py`.
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

- Kalshi trade-tick backtests via [`backtests/_kalshi_single_market_trade_runner.py`](backtests/_kalshi_single_market_trade_runner.py)
- Polymarket trade-tick backtests via [`backtests/_polymarket_single_market_runner.py`](backtests/_polymarket_single_market_runner.py)
- Polymarket PMXT L2 backtests via [`backtests/_polymarket_single_market_pmxt_runner.py`](backtests/_polymarket_single_market_pmxt_runner.py)

## Running Backtests

Interactive menu:

```bash
make backtest
```

Any module in `backtests/` or `backtests/private/` with `NAME`, `DESCRIPTION`, and `async def run()` shows up here, except the archived `backtests/polymarket_trade_tick/` set.

Equivalent direct command:

```bash
uv run python main.py
```

Direct script execution is usually better once you know which runner you want:

```bash
MARKET_TICKER=KXNEXTIRANLEADER-45JAN01-MKHA uv run python backtests/kalshi_trade_tick/kalshi_breakout.py
MARKET_SLUG=will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 END_TIME=2026-03-15T18:00:00Z LOOKBACK_HOURS=4 uv run python -m backtests.polymarket_pmxt_relay_ema_crossover
MARKET_SLUG=will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 uv run python backtests/polymarket_trade_tick/polymarket_vwap_reversion.py
```

Most runners are configured through environment variables. Common ones:

- `MARKET_TICKER` for Kalshi single-market runners
- `MARKET_SLUG` for Polymarket single-market runners
- `TOKEN_INDEX` to choose which Polymarket outcome token to backtest
- `LOOKBACK_DAYS` for data window size
- `LOOKBACK_HOURS` for PMXT L2 runners
- `PMXT_RELAY_BASE_URL` to override the default public relay or disable it
  with `PMXT_RELAY_BASE_URL=0`
- `TRADE_SIZE` and `INITIAL_CASH` for sizing
- `TARGET_RESULTS` for multi-market runners

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

- This is a conservative taker-execution proxy, not full order-book replay.
- Historical backtests here do not model queue position, full L2 depth, or
  exact partial-sweep behavior.
- Taker-heavy strategies that try to harvest very small price changes can look
  much worse once fees and one-tick slippage are turned on.
- PMXT improves Polymarket taker fills materially, but passive-order fills are
  still approximate because public L2 MBP data cannot tell you your exact place
  in queue.

### PMXT Polymarket L2

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
  `LOOKBACK_HOURS + 2`.
- Example file counts:
  - `LOOKBACK_HOURS=2` needs about 4 hourly files
  - `LOOKBACK_HOURS=4` needs about 6 hourly files
  - `LOOKBACK_HOURS=24` needs about 26 hourly files
  - `LOOKBACK_HOURS=48` needs about 50 hourly files
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
- Local PMXT disk cache is optional and off by default. If `PMXT_CACHE_DIR` is
  set, the loader writes the filtered hourly parquet table for one
  market/token/hour to:

```text
~/.cache/nautilus_trader/pmxt/<condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

- Local cache reuse rules:
  - same market, same token, same hour: cache hit
  - same market, same token, overlapping window: overlapping cached hours are reused
  - same hour, different market: no reuse, because the cache key includes `condition_id`
  - same market, different token/outcome: no reuse, because the cache key includes `token_id`
- This means repeated runs of the same PMXT market can get much faster after
  the first pass if disk cache is enabled, but multi-market runs still do not
  share a universal raw-hour cache.
- Local cache controls:
  - `PMXT_CACHE_DIR=1` enables disk cache at `~/.cache/nautilus_trader/pmxt`
  - `PMXT_CACHE_DIR=/custom/path` enables disk cache at a custom root
  - `PMXT_DISABLE_CACHE=1` disables local PMXT disk cache entirely
- Cache size is currently unbounded. There is no eviction policy or size cap.
  If disk cache is enabled, it grows with the number of unique
  `(condition_id, token_id, hour)` tuples you backtest. Very active markets and
  long lookbacks will produce larger cached parquet files than quiet markets
  and short windows. Check current size with:

```bash
du -sh ~/.cache/nautilus_trader/pmxt
```

- Example: force local PMXT disk cache on for a relay-backed backtest:

```bash
PMXT_CACHE_DIR=1 END_TIME=2026-03-16T13:00:00Z LOOKBACK_HOURS=2 MIN_PRICE_RANGE=0 TRADE_SIZE=10 \
  uv run python backtests/polymarket_pmxt_relay_ema_crossover.py
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

The relay is built to solve the PMXT cold-start problem without changing the
backtest schema:

- mirror every raw hourly PMXT parquet file
- keep polling the PMXT archive index for new hours
- precompute one canonical processed parquet per hour with extracted
  `market_id` and `token_id` columns
- eagerly prebuild the tiny filtered `(condition_id, token_id, hour)` parquet
  files on the server so backtests can pull them directly
- serve those filtered files over HTTP
- let the PMXT loader fetch those filtered files directly with
  `PMXT_RELAY_BASE_URL`

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
- `/v1/system` for live CPU, memory, and relay-disk usage
- `/v1/queue` for `pending/processing/error` queue state
- `/v1/events?limit=100` for recent discover/download/process/error events
- `/v1/inflight` for active temp-tree progress while a large hour is still
  being crunched
- `/v1/badge/status`, `/v1/badge/backfill`, `/v1/badge/mirrored`,
  `/v1/badge/processed`, `/v1/badge/latest`, `/v1/badge/lag`,
  `/v1/badge/cpu`, `/v1/badge/mem`, and
  `/v1/badge/disk` for the live README status tags

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
- [`backtests/polymarket_pmxt_relay_panic_fade.py`](backtests/polymarket_pmxt_relay_panic_fade.py)
- [`backtests/polymarket_pmxt_relay_vwap_reversion.py`](backtests/polymarket_pmxt_relay_vwap_reversion.py)

These write HTML charts to `output/`, typically with names like `output/<backtest>_<market>_legacy.html`.

PMXT single-market runners do the same. Example outputs:

- `output/polymarket_pmxt_relay_ema_crossover_<market>_legacy.html`
- `output/polymarket_pmxt_relay_breakout_<market>_legacy.html`
- `output/polymarket_pmxt_relay_rsi_reversion_<market>_legacy.html`
- `output/polymarket_pmxt_relay_spread_capture_<market>_legacy.html`

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

Runs the end-to-end test suite against the live APIs. Each test redirects generated legacy-chart output to an isolated pytest temp directory so nothing in the working tree is mutated.

## Updating the Subtree

```bash
make update
```

Unlike git submodules, subtrees copy upstream code directly into this repo — there's no live link. `make update` currently pulls the upstream `nautilus_pm` `charting` branch.

## Roadmap

- [ ] live paper trading mode
- [x] multi-market support within strategies
- [x] better position sizing capabilities
- [x] fee modeling [PR#4](https://github.com/ben-gramling/nautilus_pm/pull/4)
- [ ] total slippage modeling *** [PR#6](https://github.com/ben-gramling/nautilus_pm/pull/6), [PR#9](https://github.com/evan-kolberg/prediction-market-backtesting/pull/9)
- [x] polymarket L2 order book backtests [PR#10](https://github.com/evan-kolberg/prediction-market-backtesting/pull/10)
- [x] public relay on a VPS for pre-crunching book data from PMXT -- massively speeds up backtests [PR#17](https://github.com/evan-kolberg/prediction-market-backtesting/pull/17)
- [ ] kalshi L2 order book backtests
- [x] much better & informative charting [PR#5](https://github.com/ben-gramling/nautilus_pm/pull/5)


## Known Issues

- [ ] Poly/Kalshi APIs rate-limit a lot. Kalshi seems worse. (for trade tick config)
- [ ] PMXT relay misses or raw-fallback PMXT L2 loads can still take a long time
- [ ] multi-market runs do not yet share a universal raw per-hour cache, and optional local filtered-disk-cache growth is currently unbounded

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
