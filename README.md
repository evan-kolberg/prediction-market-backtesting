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

Relay VPS statistics:

[![PMXT relay](https://209-209-10-83.sslip.io/v1/badge/status.svg?v=4)](https://209-209-10-83.sslip.io/v1/stats)
[![Processing](https://209-209-10-83.sslip.io/v1/badge/processing.svg?v=4)](https://209-209-10-83.sslip.io/v1/queue)
[![CPU load](https://209-209-10-83.sslip.io/v1/badge/load.svg?v=4)](https://209-209-10-83.sslip.io/v1/system)
[![I/O wait](https://209-209-10-83.sslip.io/v1/badge/iowait.svg?v=4)](https://209-209-10-83.sslip.io/v1/system)
[![RAM](https://209-209-10-83.sslip.io/v1/badge/mem.svg?v=4)](https://209-209-10-83.sslip.io/v1/system)
[![Disk](https://209-209-10-83.sslip.io/v1/badge/disk.svg?v=4)](https://209-209-10-83.sslip.io/v1/system)

[![ClickHouse](https://209-209-10-83.sslip.io/v1/badge/clickhouse.svg?v=8)](https://209-209-10-83.sslip.io/v1/system)
[![Worker service](https://209-209-10-83.sslip.io/v1/badge/worker.svg?v=8)](https://209-209-10-83.sslip.io/v1/system)
[![Mirror service](https://209-209-10-83.sslip.io/v1/badge/mirroring.svg?v=4)](https://209-209-10-83.sslip.io/v1/queue)
[![API service](https://209-209-10-83.sslip.io/v1/badge/api.svg?v=8)](https://209-209-10-83.sslip.io/v1/system)


[![Hours mirrored](https://209-209-10-83.sslip.io/v1/badge/mirrored.svg?v=4)](https://209-209-10-83.sslip.io/v1/stats)
[![Hours processed](https://209-209-10-83.sslip.io/v1/badge/processed.svg?v=4)](https://209-209-10-83.sslip.io/v1/stats)
[![Queue lag](https://209-209-10-83.sslip.io/v1/badge/lag.svg?v=4)](https://209-209-10-83.sslip.io/v1/queue)
[![Completion rate](https://209-209-10-83.sslip.io/v1/badge/rate.svg?v=4)](https://209-209-10-83.sslip.io/v1/stats)
[![Latest hour](https://209-209-10-83.sslip.io/v1/badge/latest.svg?v=4)](https://209-209-10-83.sslip.io/v1/queue)


[![Current file](https://209-209-10-83.sslip.io/v1/badge/file.svg?v=4)](https://209-209-10-83.sslip.io/v1/events?limit=50)
[![Rows processed](https://209-209-10-83.sslip.io/v1/badge/rows.svg?v=4)](https://209-209-10-83.sslip.io/v1/events?limit=50)

Backtesting framework for prediction market strategies on
[Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built on
top of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with
custom exchange adapters. Plotting inspired by [minitrade](https://github.com/dodid/minitrade). This repo is still in active development, and **a full release should happen within the next one to two months.**

Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Charting preview](https://raw.githubusercontent.com/evan-kolberg/prediction-market-backtesting/main/docs/assets/charting-preview.jpeg)
[```/nautilus_pm/nautilus_trader/analysis/legacy_backtesting/plotting.py```](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/nautilus_pm/nautilus_trader/analysis/legacy_backtesting/plotting.py)


Detailed guides have been filed away in the [docs index](https://evan-kolberg.github.io/prediction-market-backtesting/) for better organization and long-term sustainability.

PMXT defaults:
- timing output is on by default in `make backtest` and `uv run python main.py`
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out
- local PMXT filtered cache is enabled by default at `~/.cache/nautilus_trader/pmxt`

## Table of Contents

- [Docs Index](https://evan-kolberg.github.io/prediction-market-backtesting/)
- [Setup](https://evan-kolberg.github.io/prediction-market-backtesting/setup/)
  - [Prerequisites](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#prerequisites)
  - [Install](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#install)
  - [First Run](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#first-run)
  - [PMXT Defaults](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#pmxt-defaults)
  - [Updating The Vendored Subtree](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#updating-the-vendored-subtree)
- [Backtests And Runners](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/)
  - [Repo Layout](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#repo-layout)
  - [Runner Contract](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#runner-contract)
  - [Running Backtests](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#running-backtests)
  - [Common Environment Variables](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#common-environment-variables)
  - [PMXT Notes](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#pmxt-notes)
- [Execution Modeling](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/)
  - [Fees](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#fees)
  - [Slippage](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#slippage)
  - [Limits](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#limits)
  - [PMXT L2 Behavior](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#pmxt-l2-behavior)
- [PMXT BYOD And Local Data](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/)
  - [What Works Today](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#what-works-today)
  - [Supported Local File Layout](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#supported-local-file-layout)
  - [Required Parquet Columns](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#required-parquet-columns)
  - [Required JSON Payload Shape](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#required-json-payload-shape)
  - [Relay Mode](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#relay-mode)
  - [What Is Not Plug-And-Play Yet](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#what-is-not-plug-and-play-yet)
- [PMXT Fetch Sources And Timing](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/)
  - [Example Output](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/#example-output)
  - [Timing Expectations By Source](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/#timing-expectations-by-source)
  - [How To See This Output](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/#how-to-see-this-output)
- [PMXT Relay](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-relay/)
- [Plotting](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/)
  - [Output Paths](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#output-paths)
  - [Multi-Market References](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#multi-market-references)
- [Testing](https://evan-kolberg.github.io/prediction-market-backtesting/testing/)
  - [Standard Repo Gate](https://evan-kolberg.github.io/prediction-market-backtesting/testing/#standard-repo-gate)
  - [Useful Smoke Checks](https://evan-kolberg.github.io/prediction-market-backtesting/testing/#useful-smoke-checks)
- [Project Status](https://evan-kolberg.github.io/prediction-market-backtesting/project-status/)
  - [Roadmap](https://evan-kolberg.github.io/prediction-market-backtesting/project-status/#roadmap)
  - [Known Issues](https://evan-kolberg.github.io/prediction-market-backtesting/project-status/#known-issues)
  - [Recently Fixed](https://evan-kolberg.github.io/prediction-market-backtesting/project-status/#recently-fixed)
- [License Notes](https://evan-kolberg.github.io/prediction-market-backtesting/license/)
  - [Scope](https://evan-kolberg.github.io/prediction-market-backtesting/license/#scope)
  - [NautilusTrader Attribution](https://evan-kolberg.github.io/prediction-market-backtesting/license/#nautilustrader-attribution)
  - [Practical Meaning](https://evan-kolberg.github.io/prediction-market-backtesting/license/#practical-meaning)


## Star History

<a href="https://www.star-history.com/?repos=evan-kolberg%2Fprediction-market-backtesting&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/image?repos=evan-kolberg/prediction-market-backtesting&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/image?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/image?repos=evan-kolberg/prediction-market-backtesting&type=date&legend=top-left" />
 </picture>
</a>
