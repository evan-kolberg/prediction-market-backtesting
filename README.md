# prediction-market-backtesting

[![CI](https://github.com/evan-kolberg/prediction-market-backtesting/actions/workflows/ci.yml/badge.svg)](https://github.com/evan-kolberg/prediction-market-backtesting/actions/workflows/ci.yml)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code style: Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![DuckDB](https://img.shields.io/badge/DuckDB-%23FFF000.svg?logo=duckdb&logoColor=black)](https://duckdb.org)

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)
![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting)
![GitHub repo size](https://img.shields.io/github/repo-size/evan-kolberg/prediction-market-backtesting)

An event-driven backtesting engine for prediction market trading strategies. Replays historical trades from [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com) in chronological order, simulating order fills, portfolio tracking, and market lifecycle events.

<p align="center">
  <img src="media/running_backtest.gif" alt="Running a backtest" width="720">
</p>

<p align="center">
  <img src="media/backtest_chart.png" alt="Interactive backtest chart" width="720">
</p>

Built on top of [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) for data indexing and analysis.

## Features

- **Event-driven simulation** — replays every historical trade, firing market open/close/resolve events and checking limit order fills at each tick
- **Multi-platform support** — Kalshi and Polymarket data feeds with normalized trade and market models
- **Strategy API** — subclass `Strategy`, implement `on_trade()`, and call `buy_yes()` / `buy_no()` to place orders
- **Portfolio tracking** — position management, mark-to-market pricing, equity curve snapshots, and resolution payouts
- **Performance metrics** — total return, Sharpe ratio, Sortino ratio, max drawdown, win rate, profit factor
- **Interactive charts** — Bokeh-based HTML charts with linked equity curve, P&L, market prices, drawdown, and cash panels
- **Interactive CLI** — terminal menu for selecting strategies, platforms, and sample sizes
- **Analysis passthrough** — run any analysis command from the submodule directly via `make`

## Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) — fast Python package manager
- [zstd](https://github.com/facebook/zstd) — required for data decompression
- [GNU Make](https://www.gnu.org/software/make/)

## Quick Start

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

### 3. Download the data

This downloads and extracts the historical trade dataset (~36 GB compressed) into the submodule's `data/` directory. A symlink at the root points there.

```bash
make setup
```

> **Note:** This step installs `zstd` and `aria2c` if not already present (via Homebrew on macOS or apt on Linux), then downloads and extracts the dataset. You only need to do this once.

### 4. Run a backtest

```bash
make backtest
```

This launches an interactive menu where you select a strategy, platform, and market sample size. Results are printed to the terminal and an event log is saved to `output/`.

To run a specific strategy directly:

```bash
make backtest buy_low
make backtest calibration_arb
```

## Available Commands

### Backtesting (root)

| Command | Description |
|---|---|
| `make backtest [name]` | Run a backtest interactively or by strategy name |
| `make setup` | Initialize submodule and download trade data |
| `make test` | Run the test suite |
| `make lint` | Check code style with Ruff |
| `make format` | Auto-format code with Ruff |

### Analysis (proxied from submodule)

Any target not defined in the root Makefile is forwarded to the [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) submodule:

| Command | Description |
|---|---|
| `make index` | Build/rebuild DuckDB indexes over the raw parquet data |
| `make analyze` | Run the full analysis suite and write results to `output/` |
| `make package` | Package analysis outputs for distribution |

## Writing a Strategy

Create a new file in `src/backtesting/examples/` and subclass `Strategy`:

```python
from src.backtesting.models import TradeEvent
from src.backtesting.strategy import Strategy


class MyStrategy(Strategy):
    def __init__(self):
        super().__init__(
            name="my_strategy",
            description="Description shown in the menu",
        )

    def on_trade(self, trade: TradeEvent) -> None:
        """Called for every historical trade event."""
        if trade.yes_price < 0.10:
            self.buy_yes(trade.market_id, price=0.10, quantity=10.0)
```

Strategies are auto-discovered — drop a `.py` file in the examples directory and it appears in the backtest menu.

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
├── Makefile                         # Build commands (proxies to submodule)
├── pyproject.toml                   # Python dependencies
├── data -> prediction-market-analysis/data  # Symlink to trade data
├── src/
│   └── backtesting/
│       ├── engine.py                # Simulation loop orchestrator
│       ├── broker.py                # Order matching and fill simulation
│       ├── portfolio.py             # Position and cash management
│       ├── strategy.py              # Abstract strategy base class
│       ├── models.py                # Data models (TradeEvent, Order, Fill, etc.)
│       ├── metrics.py               # Performance metric calculations
│       ├── logger.py                # Event logging
│       ├── progress.py              # Progress bar display
│       ├── feeds/
│       │   ├── base.py              # Abstract data feed interface
│       │   ├── kalshi.py            # Kalshi parquet data feed
│       │   └── polymarket.py        # Polymarket parquet data feed
│       └── examples/
│           ├── buy_low.py           # Example: buy YES below threshold
│           └── calibration_arb.py   # Example: exploit calibration mispricings
├── tests/                           # Test suite
├── output/                          # Backtest logs and results
└── prediction-market-analysis/      # Data & analysis submodule
```

## Data

Historical trade data is sourced from the [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) project. The dataset is stored as parquet files and queried via DuckDB.

| Platform | Data |
|---|---|
| Kalshi | Markets metadata + individual trades with prices in cents (1–99) |
| Polymarket | CLOB markets + on-chain CTF Exchange trades joined with block timestamps |

## Roadmap

Planned features and improvements:

- **Time span selection** — restrict backtests to a specific date range (e.g. `--start 2024-01-01 --end 2024-12-31`)
- **Market filtering** — filter by market type, category, or specific market IDs
- **Advanced order types** — market orders, stop-losses, take-profit, and time-in-force options
- **Strategy parameters** — CLI flags and config files for tuning strategy hyperparameters without code changes
- **Walk-forward optimization** — automated parameter sweeps with in-sample / out-of-sample splits
- **Multi-strategy comparison** — run multiple strategies side-by-side and generate comparative reports
- **Slippage & latency modeling** — configurable fill delay and price impact simulation
- **Live paper trading** — forward-test strategies against real-time market data without placing actual orders
- **Additional platforms** — support for more prediction market exchanges as data becomes available
- **Richer charting** — volume overlays, rolling Sharpe, win-rate heatmaps, and exportable PDF reports

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=evan-kolberg/prediction-market-backtesting&type=Date)](https://star-history.com/#evan-kolberg/prediction-market-backtesting&Date)
