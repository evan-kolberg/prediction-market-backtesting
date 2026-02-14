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

An event-driven backtesting engine for prediction market trading strategies. Replays historical trades from [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com) in chronological order, simulating order fills, portfolio tracking, and market lifecycle events. Engine is inspired by [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) and plotting is inspired by [minitrade](https://github.com/dodid/minitrade).


<figure align="center">
  <img src="media/running_backtest.gif"
       alt="Running a backtest"
       width="720"
       style="border-radius: 14px;">
  <figcaption><em>Running a backtest simulation.</em></figcaption>
</figure>
<figure align="center">
  <img src="media/gambling_strategy_kalshi_1pct.png"
       alt="Gambling strategy on Kalshi"
       width="720"
       style="border-radius: 14px;">
  <figcaption><em>Performance of a naïve strategy on Kalshi.</em></figcaption>
</figure>
<figure align="center">
  <img src="media/gambling_strategy_polymarket_1pct.png"
       alt="Gambling strategy on Polymarket"
       width="720"
       style="border-radius: 14px;">
  <figcaption><em>Performance of the same strategy on Polymarket.</em></figcaption>
</figure>



Built on top of [prediction-market-analysis](https://github.com/Jon-Becker/prediction-market-analysis) for data indexing and analysis.

## Roadmap

- [x] **Interactive charts** — Bokeh-based HTML charts with linked equity curve, P&L, market prices, drawdown, and cash panels
- [ ] **Slippage, latency, & liquidity modeling** — these will impact live-deployed strategies, so it's very important to backtest with this taken into account. In low liquidity markets, large orders will eat through the order book and it's important to be aware of this price impact.
- [ ] **Time span selection** — restrict backtests to a specific date range (e.g. `--start 2024-01-01 --end 2024-12-31`)
- [ ] **Market filtering** — filter by market type, category, or specific market IDs
- [ ] **Advanced order types** — market orders, stop-losses, take-profit, and time-in-force options
- [ ] **Walk-forward optimization** — automated parameter sweeps with in-sample / out-of-sample splits
- [ ] **Multi-strategy comparison** — run multiple strategies side-by-side and generate comparative reports

## Current issues

- [ ] Insanely high mem usage (42 gigs when loading top 1% volume polymarket data). Even with 48 gigs of ram, this is painful. Kalsi is fine, even at 100% markets since data collection was done differently (~19 gigs ram).


## Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) — fast Python package manager `brew install uv`
- [zstd](https://github.com/facebook/zstd) — required for data decompression `brew install zstd`
- [GNU Make](https://www.gnu.org/software/make/) - needed for using makefiles `brew install make`

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

This downloads and extracts the historical trade dataset (~36 GB compressed, ~53.57 uncompressed) into the submodule's `data/` directory. A symlink at the root points there.

```bash
make setup
```

> **Note:** This step installs `zstd` and `aria2c` if not already present (via Homebrew on macOS or apt on Linux), then downloads and extracts the dataset. You only need to do this once.

### 4. Run a backtest

```bash
make backtest
```

This launches an interactive menu where you select a strategy, platform, and market sample size. Results are printed to the terminal and an event log is saved to `output/`.

<p align="center">
  <img src="media/backtest.gif" alt="Running a backtest" width="360" style="border-radius: 8px;">
</p>

To run a specific strategy directly:

```bash
make backtest buy_low
make backtest calibration_arb
make backtest gambling_addiction
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

Create a new file in `src/backtesting/strategies/` and subclass `Strategy`:

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
│       └── strategies/
│           ├── buy_low.py           # Buy YES below threshold, hold to resolution
│           ├── calibration_arb.py   # Exploit calibration mispricings at extremes
│           └── gambling_addiction.py # Martingale + mean-reversion gambling tactics
├── tests/                           # Test suite
├── output/                          # Backtest logs and results
└── prediction-market-analysis/      # Data & analysis submodule
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
