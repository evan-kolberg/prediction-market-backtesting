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
![NautilusTrader](https://img.shields.io/badge/NautilusTrader-1.225.0-1E3A5F)
![GitHub last commit](https://img.shields.io/github/last-commit/evan-kolberg/prediction-market-backtesting)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/evan-kolberg/prediction-market-backtesting)
![GitHub code size](https://img.shields.io/github/languages/code-size/evan-kolberg/prediction-market-backtesting)
![GitHub top language](https://img.shields.io/github/languages/top/evan-kolberg/prediction-market-backtesting)
![GitHub open issues](https://img.shields.io/github/issues/evan-kolberg/prediction-market-backtesting)

[![Relay Status](https://209-209-10-83.sslip.io/v1/badge/status.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![r2.pmxt.dev](https://209-209-10-83.sslip.io/v1/badge/upstream.svg)](https://209-209-10-83.sslip.io/v1/queue)
[![CPU Load](https://209-209-10-83.sslip.io/v1/badge/cpu.svg)](https://209-209-10-83.sslip.io/v1/system)
[![I/O Wait](https://209-209-10-83.sslip.io/v1/badge/iowait.svg)](https://209-209-10-83.sslip.io/v1/system)
[![RAM](https://209-209-10-83.sslip.io/v1/badge/mem.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Disk](https://209-209-10-83.sslip.io/v1/badge/disk.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Mirror Service](https://209-209-10-83.sslip.io/v1/badge/worker.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Hours Mirrored](https://209-209-10-83.sslip.io/v1/badge/mirrored.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![Latest File](https://209-209-10-83.sslip.io/v1/badge/latest-file.svg)](https://209-209-10-83.sslip.io/v1/stats)

Backtesting framework for prediction market strategies on
[Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built on
top of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with
custom exchange adapters. Plotting inspired by [minitrade](https://github.com/dodid/minitrade). This repo is still in active development.


Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Charting preview](https://raw.githubusercontent.com/evan-kolberg/prediction-market-backtesting/main/docs/assets/charting-preview.jpeg)
[Plotting docs](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/)

**If you find any bugs, unexpected behavior, or missing simulation features, PLEASE post an [issue](https://github.com/evan-kolberg/prediction-market-backtesting/issues/new) or [discussion](https://github.com/evan-kolberg/prediction-market-backtesting/discussions/new/choose).**

Detailed guides have been filed away in the [docs index](https://evan-kolberg.github.io/prediction-market-backtesting/) for better organization and long-term sustainability.

## Table of Contents

- [Docs Index](https://evan-kolberg.github.io/prediction-market-backtesting/)
- [Setup](https://evan-kolberg.github.io/prediction-market-backtesting/setup/)
  - [Prerequisites](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#prerequisites)
  - [Install](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#install)
  - [First Run](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#first-run)
  - [Timing And Cache Defaults](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#timing-and-cache-defaults)
  - [Nautilus Overlay Layout](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#nautilus-overlay-layout)
- [Backtests And Runners](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/)
  - [Repo Layout](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#repo-layout)
  - [Runner Contract](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#runner-contract)
  - [HTML And Report Modes](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#html-and-report-modes)
  - [Optimization Runners](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#optimization-runners)
  - [Designing Good Runner Files](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#designing-good-runner-files)
  - [Multi-Market Strategy Configs](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#multi-market-strategy-configs)
  - [Running Backtests](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#running-backtests)
  - [Editing Runner Inputs](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#editing-runner-inputs)
  - [Data Vendor Notes](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#data-vendor-notes)
    - [Native Vendors](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#native-vendors)
    - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#pmxt)
- [Execution Modeling](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/)
  - [Fees](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#fees)
  - [Slippage](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#slippage)
  - [Passive Orders And Queue Position](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#passive-orders-and-queue-position)
  - [Latency](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#latency)
  - [Limits](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#limits)
  - [Vendor L2 Behavior](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#vendor-l2-behavior)
    - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#pmxt)
- [Data Vendors, Local Mirrors, And Raw PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/)
  - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#pmxt)
    - [Runner Source Modes](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#runner-source-modes)
    - [Lower-Level Loader Env Vars](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#lower-level-loader-env-vars)
    - [What Works Today](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#what-works-today)
    - [Supported Local File Layout](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#supported-local-file-layout)
    - [Required Parquet Columns](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#required-parquet-columns)
    - [Required JSON Payload Shape](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#required-json-payload-shape)
  - [Relay Mode](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#relay-mode)
  - [What Is Not Plug-And-Play Yet](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-byod/#what-is-not-plug-and-play-yet)
- [Mirror And Relay Ops](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-relay/)
  - [Active Mirror Service](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-relay/#active-mirror-service)
  - [PC-Side Alternative](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-relay/#pc-side-alternative)
  - [Archived Relay Snapshot](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-relay/#archived-relay-snapshot)
- [Vendor Fetch Sources And Timing](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/)
  - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/#pmxt)
  - [Example Output](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/#example-output)
  - [Timing Expectations By Source](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/#timing-expectations-by-source)
  - [How To See This Output](https://evan-kolberg.github.io/prediction-market-backtesting/pmxt-fetch-sources/#how-to-see-this-output)
- [Plotting](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/)
  - [Scaling Model](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#scaling-model)
  - [Output Types](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#output-types)
  - [Output Paths](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#output-paths)
  - [Example Summary Output](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#example-summary-output)
  - [Multi-Market References](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#multi-market-references)
- [Testing](https://evan-kolberg.github.io/prediction-market-backtesting/testing/)
  - [Standard Repo Gate](https://evan-kolberg.github.io/prediction-market-backtesting/testing/#standard-repo-gate)
  - [Useful Smoke Checks](https://evan-kolberg.github.io/prediction-market-backtesting/testing/#useful-smoke-checks)
  - [Docs Validation](https://evan-kolberg.github.io/prediction-market-backtesting/testing/#docs-validation)
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
