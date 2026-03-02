# prediction-market-backtesting

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

![GitHub stars](https://img.shields.io/github/stars/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub forks](https://img.shields.io/github/forks/evan-kolberg/prediction-market-backtesting?style=social)
![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting)

Backtesting framework for prediction market trading strategies on [Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), powered by [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with custom exchange adapters.

> Looking for the old standalone engine? See the [`legacy`](https://github.com/evan-kolberg/prediction-market-backtesting/tree/legacy) branch.

## Architecture

This repo uses [nautilus_pm](https://github.com/ben-gramling/nautilus_pm) as a git subtree — a fork of NautilusTrader with custom Kalshi and Polymarket adapters. Data is fetched via REST APIs (no more 50 GB downloads).

```
prediction-market-backtesting/
  nautilus_pm/                    # NautilusTrader fork (git subtree)
    nautilus_trader/
      adapters/
        kalshi/                   # Kalshi REST adapter
        polymarket/               # Polymarket REST adapter
    examples/
      backtest/
        kalshi_ema_bars.py        # Kalshi backtest example
        polymarket_simple_quoter.py
```

## Setup

### Prerequisites

- Python 3.12+
- [Rust toolchain](https://rustup.rs/) (for building NautilusTrader)

### Install

```bash
git clone https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting/nautilus_pm
pip install -e .
```

### Run a backtest

```bash
python examples/backtest/kalshi_ema_bars.py
```

## Updating the subtree

To pull latest changes from the upstream `nautilus_pm` repo:

```bash
git subtree pull --prefix=nautilus_pm https://github.com/ben-gramling/nautilus_pm.git develop --squash
```

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
