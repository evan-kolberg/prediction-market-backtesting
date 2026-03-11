# prediction-market-backtesting

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/evan-kolberg/prediction-market-backtesting?style=social)

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE) [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) [![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv) ![Python](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white) ![Rust](https://img.shields.io/badge/rust-1.93.1-CE422B?logo=rust&logoColor=white) ![Rust Edition](https://img.shields.io/badge/edition-2024-CE422B?logo=rust&logoColor=white) ![NautilusTrader](https://img.shields.io/badge/NautilusTrader-1.224.0-1E3A5F) ![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting) ![GitHub commit activity](https://img.shields.io/github/commit-activity/m/evan-kolberg/prediction-market-backtesting) ![GitHub code size](https://img.shields.io/github/languages/code-size/evan-kolberg/prediction-market-backtesting) ![GitHub top language](https://img.shields.io/github/languages/top/evan-kolberg/prediction-market-backtesting) ![GitHub open issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)

Backtesting framework for prediction market trading strategies on [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), powered by [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with custom exchange adapters.

> Miss the old engine? See the [`legacy`](https://github.com/evan-kolberg/prediction-market-backtesting/tree/legacy) branch. Though, I don't recommend you continue using that one. 


## NEW

Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Image](https://github.com/user-attachments/assets/e9b00915-9413-42d8-aeff-c2bde627c3d8)


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
- Public multi-market runner: [`backtests/polymarket_sports_final_period_momentum.py`](backtests/polymarket_sports_final_period_momentum.py)
- Public resolved multi-market runner with settlement-adjusted PnL: [`backtests/polymarket_sports_late_favorite_limit_hold.py`](backtests/polymarket_sports_late_favorite_limit_hold.py)

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
MARKET_SLUGS=nfl-was-gb-2025-09-11,nfl-nyj-cin-2025-10-26 TARGET_RESULTS=2 uv run python backtests/polymarket_sports_final_period_momentum.py
TARGET_RESULTS=50 uv run python -m backtests.polymarket_sports_late_favorite_limit_hold
```

These hit live APIs. Expect latency and rate limits.

Most runners are configured through environment variables. Common ones:

- `MARKET_TICKER` for Kalshi single-market runners
- `MARKET_SLUG` for Polymarket single-market runners
- `LOOKBACK_DAYS` for data window size
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

### Limits

- This is a conservative taker-execution proxy, not full order-book replay.
- Historical backtests here do not model queue position, full L2 depth, or
  exact partial-sweep behavior.
- Taker-heavy strategies that try to harvest very small price changes can look
  much worse once fees and one-tick slippage are turned on.

## Plotting

Single-market plotting is built into the shared runner flow used by the public prediction-market backtests. Good examples:

- [`backtests/kalshi_breakout.py`](backtests/kalshi_breakout.py)
- [`backtests/kalshi_panic_fade.py`](backtests/kalshi_panic_fade.py)
- [`backtests/polymarket_panic_fade.py`](backtests/polymarket_panic_fade.py)
- [`backtests/polymarket_vwap_reversion.py`](backtests/polymarket_vwap_reversion.py)

These write HTML charts to `output/`, typically with names like `output/<backtest>_<market>_legacy.html`.

Multi-market plotting example:

- [`backtests/polymarket_sports_final_period_momentum.py`](backtests/polymarket_sports_final_period_momentum.py)

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
- [ ] live trading (thinking of [pmxt](https://github.com/pmxt-dev/pmxt))
- [x] multi-market support within strategies
- [x] better position sizing capabilities
- [x] fee modeling, slippage modeling *** exchange fees, maker/taker fees, etc [PR#4](https://github.com/ben-gramling/nautilus_pm/pull/4), [PR#6](https://github.com/ben-gramling/nautilus_pm/pull/6)
- [x] much better & informative charting [PR#5](https://github.com/ben-gramling/nautilus_pm/pull/5)

> Note: i'm still not entirely positive that slippage was implemented correctly. i aimed for a conservative approach, and supposedly there are slippage limits on these platforms, but still remains a challenge to model properly. TLDR; pain in the ass.

## Known Issues

- [ ] APIs rate-limit a lot. Kalshi seems worse.
- [ ] just found this: ```[ERROR] BACKTESTER-001.BacktestEngine: Stopping backtest from AccountBalanceNegative(balance=-4.223222, currency=USDC.e)``` will investigate soon

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Star History

<a href="https://www.star-history.com/#evan-kolberg/prediction-market-backtesting&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
 </picture>
</a>
