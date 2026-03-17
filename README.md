# prediction-market-backtesting

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/evan-kolberg/prediction-market-backtesting?style=social)

[![Licensing: Mixed](https://img.shields.io/badge/licensing-MIT%20%2B%20LGPL--3.0--or--later-blue.svg)](NOTICE) [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) [![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv) ![Python](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white) ![Rust](https://img.shields.io/badge/rust-1.93.1-CE422B?logo=rust&logoColor=white) ![Rust Edition](https://img.shields.io/badge/edition-2024-CE422B?logo=rust&logoColor=white) ![NautilusTrader](https://img.shields.io/badge/NautilusTrader-1.224.0-1E3A5F) ![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting) ![GitHub commit activity](https://img.shields.io/github/commit-activity/m/evan-kolberg/prediction-market-backtesting) ![GitHub code size](https://img.shields.io/github/languages/code-size/evan-kolberg/prediction-market-backtesting) ![GitHub top language](https://img.shields.io/github/languages/top/evan-kolberg/prediction-market-backtesting) ![GitHub open issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)

Backtesting framework for prediction market trading strategies on [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built off of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with custom exchange adapters.

> Miss the old engine? See the [`legacy`](https://github.com/evan-kolberg/prediction-market-backtesting/tree/legacy) branch. Though, I don't recommend you continue using that one. 

Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Image](https://github.com/user-attachments/assets/e9b00915-9413-42d8-aeff-c2bde627c3d8)

> IMPORTANT: Kalshi public backtests here are still bar/trade replay only. Polymarket now also has a PMXT-backed historical L2 path which replays hourly PMXT order-book events through NautilusTrader's `L2_MBP` matching engine. That is much better than the old one-tick slippage proxy for taker-style execution, but it still does **not** fully solve passive-order realism because public L2 MBP data does not reveal true queue position. Also, pulling those historical order book archives will cause the backtests to take a long time, especially for multiple markets. 

## Table of Contents

- [Setup](#setup)
- [Writing Strategies and Backtests](#writing-strategies-and-backtests)
- [Running Backtests](#running-backtests)
- [Execution Modeling](#execution-modeling)
- [Plotting](#plotting)
- [Testing](#testing)
- [Updating the Subtree](#updating-the-subtree)
- [Roadmap](#roadmap)
- [Known Issues](#known-issues)
- [License](#license)


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
- Kalshi runner using a root strategy module: [`backtests/kalshi_breakout.py`](backtests/kalshi_breakout.py)
- Polymarket runner using a root strategy module: [`backtests/polymarket_vwap_reversion.py`](backtests/polymarket_vwap_reversion.py)
- PMXT Polymarket quote-tick runners live under `backtests/polymarket_pmxt_*.py`.
  Current public runners cover EMA crossover, breakout, RSI reversion, spread
  capture, panic fade, VWAP reversion, threshold momentum, final-period
  momentum, deep value, and late-favorite limit hold.
- Public multi-market runner: [`backtests/polymarket_sports_final_period_momentum.py`](backtests/polymarket_sports_final_period_momentum.py)
- Public resolved multi-market runner with settlement-adjusted PnL: [`backtests/polymarket_sports_late_favorite_limit_hold.py`](backtests/polymarket_sports_late_favorite_limit_hold.py)
- [`backtests/polymarket_sports_vwap_reversion.py`](backtests/polymarket_sports_vwap_reversion.py)

Backtest entrypoints should expose three things at module level:

```python
NAME = "my_strategy"           # shown in the menu
DESCRIPTION = "one-liner"      # shown in the menu

async def run() -> None:       # called when selected
    ...
```

Use the root `strategies` package for signal logic, then import that logic into a thin backtest runner. Export new reusable configs and classes from [`strategies/__init__.py`](strategies/__init__.py) so runners can import them cleanly.

Two common runner patterns already exist:

- Kalshi bar backtests via [`backtests/_kalshi_single_market_runner.py`](backtests/_kalshi_single_market_runner.py)
- Polymarket trade-tick backtests via [`backtests/_polymarket_single_market_runner.py`](backtests/_polymarket_single_market_runner.py)
- Polymarket PMXT L2 backtests via [`backtests/_polymarket_single_market_pmxt_runner.py`](backtests/_polymarket_single_market_pmxt_runner.py)

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
MARKET_TICKER=KXNEXTIRANLEADER-45JAN01-MKHA uv run python backtests/kalshi_breakout.py
MARKET_SLUG=will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 uv run python backtests/polymarket_vwap_reversion.py
MARKET_SLUG=will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 END_TIME=2026-03-15T18:00:00Z LOOKBACK_HOURS=4 uv run python -m backtests.polymarket_pmxt_ema_crossover
MARKET_SLUGS=nfl-was-gb-2025-09-11,nfl-nyj-cin-2025-10-26 TARGET_RESULTS=2 uv run python backtests/polymarket_sports_final_period_momentum.py
TARGET_RESULTS=50 uv run python -m backtests.polymarket_sports_late_favorite_limit_hold
```

These hit live APIs. Expect latency and rate limits.

Most runners are configured through environment variables. Common ones:

- `MARKET_TICKER` for Kalshi single-market runners
- `MARKET_SLUG` for Polymarket single-market runners
- `LOOKBACK_DAYS` for data window size
- `LOOKBACK_HOURS` for PMXT L2 runners
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
- Polymarket uses the market's own tick size.
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

- PMXT data comes from hourly parquet files on `r2.pmxt.dev`.
- The loader scans one extra hour before the requested start so it can find a
  valid snapshot and rebuild the book. File count is therefore roughly
  `LOOKBACK_HOURS + 2`.
- Example file counts:
  - `LOOKBACK_HOURS=2` scans 4 hourly files
  - `LOOKBACK_HOURS=4` scans 6 hourly files
  - `LOOKBACK_HOURS=24` scans 26 hourly files
  - `LOOKBACK_HOURS=48` scans 50 hourly files
- The loader filters to one market and one token, then keeps the filtered
  `OrderBookDeltas` and `QuoteTick` records in memory for the current run.
- There is no local PMXT cache in this repo yet. Separate PMXT runs re-download
  and re-decode the same window.
- Recent 2-hour sample on
  `will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026`
  (`END_TIME=2026-03-16T13:00:00Z`, `TRADE_SIZE=10`, HTML enabled):
  - loader-only scan: `1286` quotes + `1286` delta batches in about `86s`
  - end-to-end runs:
    `ema_crossover` `176.214s`,
    `rsi_reversion` `183.718s`,
    `spread_capture` `173.466s`
- Fixed quote-breakout sanity sample on the same market over 4 hours
  (`2026-03-16T09:00:00Z` to `2026-03-16T13:00:00Z`, `TRADE_SIZE=10`,
  `MIN_PRICE_RANGE=0`): `2484` quotes, `2` fills, `PnL -0.0350`
- 48-hour sample on the same market
  (`2026-03-14T13:00:00Z` to `2026-03-16T13:00:00Z`, `TRADE_SIZE=1`, HTML enabled):
  - loader-only ingest: `1954.49s` (`32.57m`)
  - files read: `50` unique hourly parquet files, `100` opens total
  - ingress measured at the file-read layer: `18.277 GiB`
  - filtered dataset size: `32819` quotes + `32819` delta batches
  - strategy runtime after the data is already in memory:
    `ema_crossover` `3.360s`,
    `rsi_reversion` `3.325s`,
    `spread_capture` `3.394s`
  - if you run those PMXT modules separately today, each run still pays the
    ~`32.6m` loader cost because the window is not cached locally
- Short windows can still fail if the selected range never includes usable L2
  book state for that instrument.

  <img width="751" height="434" alt="Image" src="https://github.com/user-attachments/assets/4dcfa115-4785-4a7e-ac10-41a4bfb3f8eb" />

## Plotting

Single-market plotting is built into the shared runner flow used by the public prediction-market backtests. Good examples:

- [`backtests/kalshi_breakout.py`](backtests/kalshi_breakout.py)
- [`backtests/kalshi_panic_fade.py`](backtests/kalshi_panic_fade.py)
- [`backtests/polymarket_panic_fade.py`](backtests/polymarket_panic_fade.py)
- [`backtests/polymarket_vwap_reversion.py`](backtests/polymarket_vwap_reversion.py)

These write HTML charts to `output/`, typically with names like `output/<backtest>_<market>_legacy.html`.

PMXT single-market runners do the same. Example outputs:

- `output/polymarket_pmxt_ema_crossover_<market>_legacy.html`
- `output/polymarket_pmxt_breakout_<market>_legacy.html`
- `output/polymarket_pmxt_rsi_reversion_<market>_legacy.html`
- `output/polymarket_pmxt_spread_capture_<market>_legacy.html`

Multi-market plotting example:

- [`backtests/polymarket_sports_final_period_momentum.py`](backtests/polymarket_sports_final_period_momentum.py)
- [`backtests/polymarket_sports_vwap_reversion.py`](backtests/polymarket_sports_vwap_reversion.py)

By default that script:

- runs repeated single-market backtests,
- writes per-market legacy charts to `output/`, and
- writes an aggregate multi-market chart to `output/polymarket_sports_final_period_momentum_multi_market.html`.

Optional combined-report output is available with:

```bash
COMBINED_REPORT=true uv run python backtests/polymarket_sports_final_period_momentum.py
```

That writes `output/polymarket_sports_final_period_momentum_combined_legacy.html`.

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
- [ ] kalshi L2 order book backtests
- [x] much better & informative charting [PR#5](https://github.com/ben-gramling/nautilus_pm/pull/5)


## Known Issues

- [ ] APIs rate-limit a lot. Kalshi seems worse. (for trade tick config)
- [ ] Pulling L2 data from PMXT archives takes an insane amount of time (and data)

## License

This repository uses mixed licensing.

- `nautilus_pm/` stays under LGPL-3.0-or-later, with upstream notices preserved.
- Root files called out in [NOTICE](NOTICE) as NautilusTrader-derived are also
  LGPL-3.0-or-later.
- Root files not called out there remain MIT.

See [LICENSE](LICENSE), [LICENSE-MIT](LICENSE-MIT), [COPYING.LESSER](COPYING.LESSER),
[COPYING](COPYING), and [NOTICE](NOTICE) for the repo-level breakdown.

---

## Star History

<a href="https://www.star-history.com/#evan-kolberg/prediction-market-backtesting&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
 </picture>
</a>
