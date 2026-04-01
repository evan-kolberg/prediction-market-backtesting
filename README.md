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

[![PMXT relay](https://209-209-10-83.sslip.io/v1/badge/status.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![Relay CPU](https://209-209-10-83.sslip.io/v1/badge/cpu.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay mem](https://209-209-10-83.sslip.io/v1/badge/mem.svg)](https://209-209-10-83.sslip.io/v1/system)
[![Relay disk](https://209-209-10-83.sslip.io/v1/badge/disk.svg)](https://209-209-10-83.sslip.io/v1/system)

[![PMXT mirrored](https://209-209-10-83.sslip.io/v1/badge/mirrored.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT processed](https://209-209-10-83.sslip.io/v1/badge/processed.svg)](https://209-209-10-83.sslip.io/v1/stats)
[![PMXT latest](https://209-209-10-83.sslip.io/v1/badge/latest.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![PMXT lag](https://209-209-10-83.sslip.io/v1/badge/lag.svg?v=3)](https://209-209-10-83.sslip.io/v1/queue)
[![PMXT rate](https://209-209-10-83.sslip.io/v1/badge/rate.svg?v=1)](https://209-209-10-83.sslip.io/v1/stats)

[![PMXT file](https://209-209-10-83.sslip.io/v1/badge/prebuild-file.svg?v=1)](https://209-209-10-83.sslip.io/v1/events?limit=50)
[![PMXT rows](https://209-209-10-83.sslip.io/v1/badge/prebuild-progress.svg?v=1)](https://209-209-10-83.sslip.io/v1/events?limit=50)

Backtesting framework for prediction market strategies on
[Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com), built on
top of [NautilusTrader](https://github.com/nautechsystems/nautilus_trader) with
custom exchange adapters. This repo is still in active development, and the
public PMXT relay is moving from the old tiny-file path to a raw-hour ->
ClickHouse path.

Setup, runner, and ops detail now live in `docs/` instead of bloating the root
README. PMXT timing output is enabled by default, `BACKTEST_ENABLE_TIMING=0` is
the quiet opt-out, and local PMXT filtered cache is enabled by default at
`~/.cache/nautilus_trader/pmxt`. The relay CPU badge is loadavg-based, so it
can hit `100%` during ClickHouse merges or disk wait without meaning the API is
down. Execution caveats and market-data modeling notes live in
[`docs/execution-modeling.md`](docs/execution-modeling.md).

![Charting preview](https://raw.githubusercontent.com/evan-kolberg/prediction-market-backtesting/main/docs/assets/charting-preview.jpeg)

## Table of Contents

- [Docs Index](docs/index.md)
- [Setup](docs/setup.md)
- [Backtests And Runners](docs/backtests.md)
- [Execution Modeling](docs/execution-modeling.md)
- [PMXT BYOD And Local Data](docs/pmxt-byod.md)
- [PMXT Fetch Sources And Timing](docs/pmxt-fetch-sources.md)
- [PMXT Relay](docs/pmxt-relay.md)
- [Plotting](docs/plotting.md)
- [Testing](docs/testing.md)
- [Project Status](docs/project-status.md)
- [License Notes](docs/license.md)

---

