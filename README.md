# prediction-market-backtesting

[![CI](https://github.com/evan-kolberg/prediction-market-backtesting/actions/workflows/ci.yml/badge.svg)](https://github.com/evan-kolberg/prediction-market-backtesting/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![Rust 1.63+](https://img.shields.io/badge/rust-1.63%2B-%23000000.svg?logo=rust&logoColor=white)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![DuckDB](https://img.shields.io/badge/DuckDB-%23FFF000.svg?logo=duckdb&logoColor=black)](https://duckdb.org)

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)
![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting)
![GitHub repo size](https://img.shields.io/github/repo-size/evan-kolberg/prediction-market-backtesting)

An event-driven backtesting engine for prediction market trading strategies. Replays historical trades from [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com) in chronological order, simulating order fills, portfolio tracking, and market lifecycle events. The hot loop (broker, portfolio, lifecycle) is compiled to native code via [PyO3](https://pyo3.rs) while strategy callbacks remain in Python. Inspired by [NautilusTrader](https://github.com/nautechsystems/nautilus_trader), plotting inspired by [minitrade](https://github.com/dodid/minitrade).

These two graphs below are the output of the gambling strategy. Losing money has never looked so good.
![Gambling strategy on Polymarket](media/gambling_strategy_polymarket_1pct.png)
![Gambling strategy on Kalshi](media/gambling_strategy_kalshi_1pct.png)

Built on top of [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) for data indexing and analysis.

## Table of Contents

- [How the Engine Works](#how-the-engine-works)
  - [How orders fill](#how-orders-fill)
  - [How slippage works](#how-slippage-works)
  - [A note on what backtests can and can't tell you](#a-note-on-what-backtests-can-and-cant-tell-you)
- [Roadmap](#roadmap)
- [Current issues](#current-issues)
- [Prerequisites](#prerequisites)
- [Setup](#setup-created-on-macos)
  - [1. Clone the repository](#1-clone-the-repository)
  - [2. Install dependencies](#2-install-dependencies)
  - [3. Build the engine](#3-build-the-engine)
  - [4. Download the data](#4-download-the-data)
  - [5. Run a backtest](#5-run-a-backtest)
  - [6. Front test (live paper trading)](#6-front-test-live-paper-trading)
- [Available Commands](#available-commands)
- [Writing a Strategy](#writing-a-strategy)
  - [Strategy API](#strategy-api)
  - [Lifecycle Hooks](#lifecycle-hooks)
  - [Properties](#properties)
- [Project Structure](#project-structure)
- [Data](#data)
- [License](#license)

## How the Engine Works

The backtest replays historical trades one by one in the order they happened. For each trade, the engine checks whether any of your pending orders should fill, updates your portfolio, and then calls your strategy's `on_trade` so it can react and place new orders. The hot loop — all the order matching and portfolio math — runs in compiled Rust for speed, while your strategy logic stays in Python.

### How orders fill

When your strategy places a limit buy order — say, buy YES at $0.20 — the engine waits for a trade that matches. Two conditions have to be true:

1. **Price**: the trade price has to be at or below your limit ($0.20 or lower)
2. **Taker side**: the taker in that trade has to be on the *opposite* side from you

That second point matters a lot and most backtesting frameworks miss it. In a real order book, your limit buy sits on the bid side. It only fills when someone comes in and *sells* to you — meaning the taker is a NO buyer (equivalently, a YES seller). If a YES buyer comes in and hits the ask, your bid just sits there untouched. The engine models this correctly. Skipping it makes strategies look more liquid than they actually are, because you'd be counting fills on trades that wouldn't have touched your order at all.

Once an order fills, the fill price gets adjusted for slippage before it lands in your portfolio.

### How slippage works

Slippage combines two real effects:

**Spread cost** — bid-ask spreads in prediction markets get much wider at extreme prices. Near 50/50 the spread might be 1 cent. Near 5% or 95% it can be 5–10 cents. That sounds small, but 5 cents of spread on a 10-cent YES contract is 50% of your position cost right off the bat. The model captures this with a spread multiplier that grows as you move away from 50%: at 50% it's 1×, at 15% it's about 2×, at 5% it's about 5×.

**Market impact** — bigger orders move the price against you. If you try to buy 100 contracts in a market that normally trades 10 at a time, you're going to eat through the book and pay more for each successive contract. The model uses square-root scaling for this: an order 4× the typical trade size pays 2× the base impact, and 100× pays 10×. This is the standard approach in quantitative finance (Almgren-Chriss/Kyle-lambda model). The typical trade size is tracked per-market with an exponential moving average that updates as trades come in, so liquid markets stay cheap to trade and thin markets stay expensive.

Both effects stack on top of a configurable base slippage (default 0.5%).

### A note on what backtests can and can't tell you

Running `buy_low` (buy YES when priced below 20%, hold to resolution) against all of this modeling still shows positive returns. That's not a sign the model is wrong — it turns out that in the historical data, markets priced below 20% YES resolved YES about 23% of the time. That's slightly above the implied probability, and it's genuine historical edge. No friction model can realistically close that gap; you'd need to charge fantasy-level slippage to force EV negative.

The point of good liquidity modeling isn't to guarantee any given strategy loses. It's to catch strategies that only *look* profitable because they assume perfect fills — like placing a huge order in a thin market at exactly the quoted price with zero spread cost. Those strategies get correctly penalized now. Whether historical edge holds up out-of-sample is a different question that no backtest can answer for you.

## Roadmap

- [x] **Interactive charts** — Bokeh-based HTML charts with linked equity curve, P&L, market prices, drawdown, and cash panels
- [x] **Slippage, latency, & liquidity modeling** — taker-side-aware fill logic, price-proportional spread cost, and square-root market impact. See "How the Engine Works" above.
- [x] **Front-testing** — paper trade strategies against live WebSocket data from Kalshi and Polymarket
- [ ] **Time span selection** — restrict backtests to a specific date range (e.g. `--start 2024-01-01 --end 2024-12-31`)
- [ ] **Market filtering** — filter by market type, category, or specific market IDs
- [ ] **Advanced order types** — market orders, stop-losses, take-profit, and time-in-force options
- [ ] **Multi-strategy comparison** — run multiple strategies side-by-side and generate comparative reports

## Current issues

- [ ] High memory usage (42 GB when loading top 1% volume Polymarket data). The bulk of memory comes from the data feed and plotting pipeline — further work needed on streaming/chunked processing.
- [ ] Live paper-trading with Polymarket & Kalshi has not yet been verified to work fully. It is a WIP.

## Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) — fast Python package manager `brew install uv`
- [zstd](https://github.com/facebook/zstd) — required for data decompression `brew install zstd`
- [GNU Make](https://www.gnu.org/software/make/) - needed for using makefiles `brew install make`
- [Rust](https://rustup.rs/) — required for the compiled engine `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh`

## Setup (created on macOS)
> It is still entirely possible to run this on Windows & Linux, but the terminal commands will look different. Until they are fully supported, I recommend you use something like Claude Code or GitHub Copilot to handle the initial setup on your system.

### 1. Clone the repository

```bash
git clone --recurse-submodules https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting
```

If you already cloned without `--recurse-submodules`:

```bash
git submodule update --init --recursive
```

### 2. Install dependencies

uv manages virtual environments automatically — no manual activation needed. Each project (root and submodule) has its own `pyproject.toml` and isolated environment. uv resolves and installs dependencies on first `uv run`.

```bash
uv sync
```

### 3. Build the engine

```bash
make build-rust
```

> **Note:** Requires a Rust toolchain ([rustup](https://rustup.rs/)) and [maturin](https://www.maturin.rs/) (`pip install maturin` or `uv pip install maturin`).

### 4. Download the data

This downloads and extracts the historical trade dataset (~36 GB compressed, ~53.57 uncompressed) into the submodule's `data/` directory. A symlink at the root points there.

```bash
make setup
```

> **Note:** This step installs `zstd` and `aria2c` if not already present (via Homebrew on macOS or apt on Linux), then downloads and extracts the dataset. You only need to do this once.

### 5. Run a backtest

```bash
make backtest
```

This launches an interactive menu where you select a strategy, platform, and market sample size. Results are printed to the terminal and an event log is saved to `output/`.

<img src="media/running_backtest.gif" alt="Running a backtest" width="600">

To run a specific strategy directly:

```bash
make backtest <strat_name>
```

### 6. Front test (live paper trading)

```bash
make fronttest
```

This connects to live WebSocket feeds from Kalshi or Polymarket and paper trades your strategy against real-time market data. No real money is used — fills are simulated using the same matching logic as the backtest engine.

You'll be prompted to select a strategy, platform, and market IDs to watch. Press Enter without providing an ID to auto-select a random active market.

To run a specific strategy directly:

```bash
make fronttest <strat_name>
```

**Kalshi** requires API credentials — set `KALSHI_API_KEY` and `KALSHI_PRIVATE_KEY_PATH` environment variables. **Polymarket** uses public WebSocket data and needs no authentication.

## Available Commands

| Command | Description |
|---|---|
| `make backtest [name]` | Run a backtest interactively or by strategy name |
| `make fronttest [name]` | Front test a strategy against live market data (paper trading) |
| `make build-rust` | Compile the engine |
| `make setup` | Initialize submodule and download trade data |
| `make test` | Run the test suite |
| `make lint` | Check code style with Ruff |
| `make format` | Auto-format code with Ruff |

Any target not defined in the root Makefile is forwarded to the [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) submodule:

| Command | Description |
|---|---|
| `make index` | Build/rebuild DuckDB indexes over the raw parquet data |
| `make analyze` | Run the full analysis suite and write results to `output/` |
| `make package` | Package analysis outputs for distribution |

## Writing a Strategy

Create a new file in `src/backtesting/strategies/` and subclass `Strategy`:

```python
from src.backtesting.models import TradeEvent
from src.backtesting.strategy import Strategy


class MyStrategy(Strategy):
    def __init__(self, initial_cash: float = 10_000.0):
        super().__init__(
            name="my_strategy",
            description="Description shown in the menu",
            initial_cash=initial_cash,
        )

    def on_trade(self, trade: TradeEvent) -> None:
        """Called for every historical trade event."""
        if trade.yes_price < 0.10:
            self.buy_yes(trade.market_id, price=0.10, quantity=10.0)
```

Strategies are auto-discovered — drop a `.py` file in the `strategies/` directory and it appears in the backtest menu.

### Strategy API

| Method | Description |
|---|---|
| `buy_yes(market_id, price, quantity)` | Place a limit buy on YES contracts |
| `buy_no(market_id, price, quantity)` | Place a limit buy on NO contracts |
| `sell_yes(market_id, price, quantity)` | Place a limit sell on YES contracts |
| `sell_no(market_id, price, quantity)` | Place a limit sell on NO contracts |
| `cancel_order(order_id)` | Cancel a pending order |
| `cancel_all(market_id=None)` | Cancel all pending orders |

### Lifecycle Hooks

| Hook | When it fires |
|---|---|
| `initialize()` | Once before the simulation starts |
| `on_trade(trade)` | Every historical trade event |
| `on_fill(fill)` | When one of your orders fills |
| `on_market_open(market)` | When a market's open time is reached |
| `on_market_close(market)` | When a market's close time is reached |
| `on_market_resolve(market, result)` | When a market resolves to YES or NO |
| `finalize()` | Once after the simulation ends |

### Properties

| Property | Description |
|---|---|
| `self.portfolio` | Current portfolio snapshot (cash, equity, positions) |
| `self.open_orders` | List of currently pending orders |
| `self.markets` | All available market metadata |

## Project Structure

```
├── main.py                          # CLI entry point
├── Makefile                         # build commands (proxies to submodule)
├── pyproject.toml                   # python dependencies
├── data -> prediction-market-analysis/data  # symlink to trade data
├── crates/
│   └── backtesting_engine/          # compiled rust core (PyO3)
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs               # PyO3 module definition
│           ├── engine.rs            # hot loop, event logging, FFI
│           ├── broker.rs            # order matching (HashMap by market_id)
│           ├── portfolio.rs         # position tracking, resolution, snapshots
│           └── models.rs            # internal rust data types
├── src/
│   └── backtesting/
│       ├── rust_engine.py           # python wrapper for the Rust core
│       ├── front_test_engine.py     # live paper-trading engine
│       ├── paper_broker.py          # pure-Python broker for paper trading
│       ├── strategy.py              # abstract strategy base class
│       ├── models.py                # data models (TradeEvent, Order, Fill, etc.)
│       ├── metrics.py               # performance metric calculations
│       ├── plotting.py              # interactive Bokeh charts
│       ├── logger.py                # event logging
│       ├── progress.py              # progress bar display
│       ├── _archive/                # pure-Python engine (fallback)
│       │   ├── engine.py
│       │   ├── broker.py
│       │   └── portfolio.py
│       ├── feeds/
│       │   ├── base.py              # abstract data feed interface
│       │   ├── kalshi.py            # kalshi parquet data feed
│       │   ├── kalshi_live.py       # live Kalshi eebSocket feed
│       │   ├── polymarket.py        # polymarket parquet data feed
│       │   └── polymarket_live.py   # live Polymarket eebSocket feed
│       └── strategies/              # auto-discovered strategy files
│           └── gambling_addiction.py# typical gambling tactics
├── tests/                           # test suite
├── output/                          # backtest logs and results
└── prediction-market-analysis/      # data & analysis submodule
```

## Data

Historical trade data is sourced from the [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) project. The dataset is stored as parquet files and queried via DuckDB.

| Platform | Data |
|---|---|
| Kalshi | Markets metadata + individual trades with prices in cents (1–99) |
| Polymarket | On-chain CTF Exchange trade executions (OrderFilled events from Polygon) joined with block timestamps. Not CLOB order book data — only filled trades are available. |

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left)](https://www.star-history.com/#evan-kolberg/prediction-market-backtesting&type=date&legend=top-left)
