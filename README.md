# prediction-market-backtesting

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub watchers](https://img.shields.io/github/watchers/evan-kolberg/prediction-market-backtesting?style=social)

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE) [![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff) [![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv) ![Python](https://img.shields.io/badge/python-3.12%2B-3776AB?logo=python&logoColor=white) ![Rust](https://img.shields.io/badge/rust-1.93.1-CE422B?logo=rust&logoColor=white) ![Rust Edition](https://img.shields.io/badge/edition-2024-CE422B?logo=rust&logoColor=white) ![NautilusTrader](https://img.shields.io/badge/NautilusTrader-1.224.0-1E3A5F) ![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting) ![GitHub commit activity](https://img.shields.io/github/commit-activity/m/evan-kolberg/prediction-market-backtesting) ![GitHub code size](https://img.shields.io/github/languages/code-size/evan-kolberg/prediction-market-backtesting) ![GitHub top language](https://img.shields.io/github/languages/top/evan-kolberg/prediction-market-backtesting) ![GitHub open issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)

Backtesting framework for prediction market trading strategies on [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), powered by [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with custom exchange adapters.

> Miss the old engine? See the [`legacy`](https://github.com/evan-kolberg/prediction-market-backtesting/tree/legacy) branch. Though, I don't recommend you continue using that one. 

---

## Table of Contents

- [Architecture](#architecture)
- [Setup](#setup)
- [Writing a Strategy](#writing-a-strategy)
- [Running a Backtest](#running-a-backtest)
- [Testing](#testing)
- [Roadmap](#roadmap)
- [Known Issues](#known-issues)
- [License](#license)

---

## Architecture

This repo uses [nautilus_pm](https://github.com/ben-gramling/nautilus_pm) as a git subtree — a fork of NautilusTrader with custom Kalshi and Polymarket adapters. Data is fetched via REST APIs (no more 50 GB downloads like the [`legacy`](https://github.com/evan-kolberg/prediction-market-backtesting/tree/legacy) branch).


### Strategy Approaches

| Strategy | Exchange | Data | Engine |
|---|---|---|---|
| `kalshi_ema_cross` | Kalshi | Minute OHLCV bars via REST → Parquet catalog | `BacktestNode` |
| `polymarket_ema_cross` | Polymarket | Trade ticks via REST → in-memory | `BacktestEngine` |

> These are examples that you can take a look at. For obvious reasons, winning strategies won't be pushed to the repo to be shared publically.

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
uv pip install -e nautilus_pm/ bokeh numpy py-clob-client
```

After this, use `uv run python <script>` to run anything — no manual `source .venv/bin/activate` needed.

## Writing a Strategy

Create a `.py` file in the `strategies/` directory. It must expose three things at module level:

```python
NAME = "my_strategy"           # shown in the menu
DESCRIPTION = "one-liner"      # shown in the menu

async def run() -> None:       # called when selected
    ...
```

Inside `run()`, fetch data, configure a backtest engine, and run it. Two patterns are available:

- **Bar data (Kalshi)** — `BacktestNode` + `ParquetDataCatalog`. See [`strategies/kalshi_ema_cross.py`](strategies/kalshi_ema_cross.py).
- **Trade tick data (Polymarket)** — `BacktestEngine` with in-memory data. See [`strategies/polymarket_ema_cross.py`](strategies/polymarket_ema_cross.py).

For custom strategy logic, subclass `Strategy` and `StrategyConfig` from `nautilus_trader.trading.strategy`.

## Running a Backtest

```bash
make backtest
```

This starts `main.py`, which scans `strategies/`, shows a numbered menu, and runs the selected strategy. Equivalent to `uv run python main.py`.

## Testing

```bash
make test
```

Runs the end-to-end test suite against the live APIs. Each test redirects file outputs (catalogs, tearsheets) to an isolated pytest temp directory so nothing in the working tree is mutated.

## Updating the Subtree

```bash
make update
```

Unlike git submodules, subtrees copy upstream code directly into this repo — there's no live link. `make update` runs `git subtree pull` against the upstream `nautilus_pm` repo.

## Roadmap

- [ ] mult-market support witin strategies
- [ ] live paper trading mode
- [ ] better position sizing capabilities
- [ ] fee modeling *** exchange fees, maker/taker fees, etc
- [ ] slippage modeling ***
- [ ] liquidity aware sizing

## Known Issues

- [ ] the API's rate limit a lot, and this can get annoying when running backtests on many markets.

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
