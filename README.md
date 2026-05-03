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
![GitHub contributors](https://img.shields.io/github/contributors/evan-kolberg/prediction-market-backtesting)
![GitHub pull requests](https://img.shields.io/github/issues-pr/evan-kolberg/prediction-market-backtesting)
![GitHub closed issues](https://img.shields.io/github/issues-closed/evan-kolberg/prediction-market-backtesting)
![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/evan-kolberg/prediction-market-backtesting)

**New in Version 3:**
- Telonex vendor support
- Local Telonex download script
- Many bug fixes & accuracy improvements
- Book replay order book deltas with trade ticks

**New in Version 2:**
- Nautilus 1.225.0, via PyPI in lieu of a subtree
- Better backtest runner classes via EXPERIMENT objects
- IPython notebook support (.ipynb files)
- Joint portfolio multi replay runners
- Growing support for statistical optimizers
- New aggregate charts
- Massive improvements charting gen speed
- an attempt at a Tree-structured Parzen Estimator via Optuna

Looking for the old version? That was renamed to [Version 1](https://github.com/evan-kolberg/prediction-market-backtesting/tree/v1)

Backtesting framework for prediction market strategies on
[Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built on
top of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with
custom exchange adapters. Plotting inspired by [minitrade](https://github.com/dodid/minitrade). This repo is still in active development.


Fantastic single & multi-market charting. Featuring: equity (total & individual markets), profit / loss ticks, P&L periodic bars, market allocation, YES price (with green buy and red sell fills), drawdown, sharpe (with above/below shading), cash / equity, monthly returns, and cumulative brier advantage.
![Charting preview](https://raw.githubusercontent.com/evan-kolberg/prediction-market-backtesting/main/docs/assets/charting-preview.jpeg)

**If you find any bugs, unexpected behavior, or missing simulation features, PLEASE post an [issue](https://github.com/evan-kolberg/prediction-market-backtesting/issues/new) or [discussion](https://github.com/evan-kolberg/prediction-market-backtesting/discussions/new/choose).**

Detailed guides have been filed away in the [docs index](https://evan-kolberg.github.io/prediction-market-backtesting/) for better organization and long-term sustainability.

## Table of Contents

- [Docs Index](https://evan-kolberg.github.io/prediction-market-backtesting/)
  - [Acknowledgements](https://evan-kolberg.github.io/prediction-market-backtesting/#acknowledgements)
- [Setup](https://evan-kolberg.github.io/prediction-market-backtesting/setup/)
  - [Prerequisites](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#prerequisites)
  - [Install](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#install)
  - [First Run](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#first-run)
  - [Timing And Cache Defaults](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#timing-and-cache-defaults)
  - [Extension Architecture](https://evan-kolberg.github.io/prediction-market-backtesting/setup/#extension-architecture)
- [Backtests And Runners](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/)
  - [Repo Layout](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#repo-layout)
  - [Runner Contract](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#runner-contract)
  - [HTML And Report Modes](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#html-and-report-modes)
  - [Optimization Runners](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#optimization-runners)
  - [Designing Good Runner Files](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#designing-good-runner-files)
  - [Multi-Market Strategy Configs](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#multi-market-strategy-configs)
  - [Running Backtests](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#running-backtests)
  - [Notebook Runners](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#notebook-runners)
  - [Editing Runner Inputs](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#editing-runner-inputs)
  - [Data Vendor Notes](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#data-vendor-notes)
    - [Native Vendors](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#native-vendors)
    - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#pmxt)
    - [Telonex](https://evan-kolberg.github.io/prediction-market-backtesting/backtests/#telonex)
- [V4 Rust Data Loading Plan](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/)
  - [Goals](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#goals)
  - [Non-Goals](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#non-goals)
  - [Realism Invariants](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#realism-invariants)
  - [Architecture](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#architecture)
  - [Current Loader Diagnosis](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#current-loader-diagnosis)
  - [Unified Ingestion Contract](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#unified-ingestion-contract)
    - [Replay Load Request](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#replay-load-request)
    - [Replay Work Plan](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#replay-work-plan)
    - [Source Envelope](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#source-envelope)
    - [Canonical Output](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#canonical-output)
  - [Message Bus](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#message-bus)
    - [Log Line Format](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#log-line-format)
    - [Event Schema](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#event-schema)
    - [Stages](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#stages)
    - [Status Values](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#status-values)
    - [Sinks](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#sinks)
    - [Rust Emission](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#rust-emission)
  - [Unified Data Model](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#unified-data-model)
    - [Canonical Book Delta Table](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#canonical-book-delta-table)
    - [Canonical Trade Tick Table](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#canonical-trade-tick-table)
    - [Replay Manifest](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#replay-manifest)
  - [Materialized Cache And Catalog Plan](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#materialized-cache-and-catalog-plan)
    - [Short-Term Cache](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#short-term-cache)
    - [Nautilus Catalog Evaluation](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#nautilus-catalog-evaluation)
  - [Rust Crate Plan](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#rust-crate-plan)
  - [Conversion Inventory](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#conversion-inventory)
  - [Performance-Priority Conversion Targets](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#performance-priority-conversion-targets)
  - [Conversion Roadmap](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#conversion-roadmap)
    - [Slice 0: Message Bus Foundation](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-0-message-bus-foundation)
    - [Slice 1: Rust Smoke Module](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-1-rust-smoke-module)
    - [Slice 2: Canonical Replay Schema](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-2-canonical-replay-schema)
    - [Slice 3: Trade Tick Conversion](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-3-trade-tick-conversion)
    - [Slice 4: Telonex Snapshot-To-Deltas](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-4-telonex-snapshot-to-deltas)
    - [Slice 5: PMXT Payload Decode](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-5-pmxt-payload-decode)
    - [Slice 6: Replay Merge And Sort](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-6-replay-merge-and-sort)
    - [Slice 7: Nautilus Object Boundary](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-7-nautilus-object-boundary)
    - [Slice 8: Catalog Backtest Prototype](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-8-catalog-backtest-prototype)
    - [Slice 9: Optimizer Data Reuse](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#slice-9-optimizer-data-reuse)
  - [Vendor-Specific Plans](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#vendor-specific-plans)
    - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#pmxt)
    - [Telonex](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#telonex)
    - [Polymarket Native](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#polymarket-native)
    - [Kalshi](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#kalshi)
  - [Verification Matrix](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#verification-matrix)
  - [Rollout Controls](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#rollout-controls)
  - [Performance Metrics](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#performance-metrics)
  - [Open Decisions](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#open-decisions)
  - [First Implementation Order](https://evan-kolberg.github.io/prediction-market-backtesting/v4-rust-data-loading-plan/#first-implementation-order)
- [Polymarket Account Ledger Replay](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/)
  - [Runner And Notebook](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#runner-and-notebook)
  - [What The Strategy Does](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#what-the-strategy-does)
  - [Why Exact Reproduction Fails](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#why-exact-reproduction-fails)
  - [Copy-Trading Interpretation](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#copy-trading-interpretation)
  - [External Source Check](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#external-source-check)
  - [Observed Result](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#observed-result)
  - [Latest Terminal Output](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#latest-terminal-output)
  - [How To Use This Experiment](https://evan-kolberg.github.io/prediction-market-backtesting/account-ledger-replay/#how-to-use-this-experiment)
- [Research](https://evan-kolberg.github.io/prediction-market-backtesting/research/)
  - [Overview](https://evan-kolberg.github.io/prediction-market-backtesting/research/#overview)
  - [Warm PMXT Cache Before Notebook Runs](https://evan-kolberg.github.io/prediction-market-backtesting/research/#warm-pmxt-cache-before-notebook-runs)
  - [Scoring](https://evan-kolberg.github.io/prediction-market-backtesting/research/#scoring)
  - [Joint-Portfolio Mode](https://evan-kolberg.github.io/prediction-market-backtesting/research/#joint-portfolio-mode)
  - [Samplers](https://evan-kolberg.github.io/prediction-market-backtesting/research/#samplers)
    - [Random Grid (`sampler="random"`)](https://evan-kolberg.github.io/prediction-market-backtesting/research/#random-grid-samplerrandom)
    - [TPE (`sampler="tpe"`)](https://evan-kolberg.github.io/prediction-market-backtesting/research/#tpe-samplertpe)
  - [Caveats](https://evan-kolberg.github.io/prediction-market-backtesting/research/#caveats)
  - [Notebook Output Persistence](https://evan-kolberg.github.io/prediction-market-backtesting/research/#notebook-output-persistence)
- [Execution Modeling](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/)
  - [Fees](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#fees)
    - [Maker Rebates](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#maker-rebates)
  - [Slippage](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#slippage)
  - [Passive Orders And Queue Position](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#passive-orders-and-queue-position)
  - [Latency](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#latency)
  - [Limits](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#limits)
  - [Vendor L2 Behavior](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#vendor-l2-behavior)
    - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#pmxt)
    - [Telonex](https://evan-kolberg.github.io/prediction-market-backtesting/execution-modeling/#telonex)
- [Data Vendors And Local Mirrors](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/)
  - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#pmxt)
    - [Runner Source Modes](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#runner-source-modes)
    - [Lower-Level Loader Env Vars](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#lower-level-loader-env-vars)
    - [What Works Today](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#what-works-today)
    - [Supported Local File Layout](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#supported-local-file-layout)
    - [Required Parquet Columns](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#required-parquet-columns)
    - [Required JSON Payload Shape](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#required-json-payload-shape)
  - [Telonex](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#telonex)
    - [Download Local Telonex Files](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#download-local-telonex-files)
  - [What Is Not Plug-And-Play Yet](https://evan-kolberg.github.io/prediction-market-backtesting/data-vendors/#what-is-not-plug-and-play-yet)
- [Vendor Fetch Sources And Timing](https://evan-kolberg.github.io/prediction-market-backtesting/vendor-fetch-sources/)
  - [PMXT](https://evan-kolberg.github.io/prediction-market-backtesting/vendor-fetch-sources/#pmxt)
  - [Example Output](https://evan-kolberg.github.io/prediction-market-backtesting/vendor-fetch-sources/#example-output)
  - [Telonex](https://evan-kolberg.github.io/prediction-market-backtesting/vendor-fetch-sources/#telonex)
  - [Timing Expectations By Source](https://evan-kolberg.github.io/prediction-market-backtesting/vendor-fetch-sources/#timing-expectations-by-source)
  - [How To See This Output](https://evan-kolberg.github.io/prediction-market-backtesting/vendor-fetch-sources/#how-to-see-this-output)
- [Plotting](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/)
  - [Scaling Model](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#scaling-model)
  - [Downsampling](https://evan-kolberg.github.io/prediction-market-backtesting/plotting/#downsampling)
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
