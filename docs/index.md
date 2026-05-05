# Docs Index

This repository is documented around two active operating assumptions:

- vendor-backed historical data is local-first: mirror PMXT raw archive hours
  and Telonex full-book parquet parts onto local disk, then replay through
  L2-native adapters
- public Python backtest runners expose `run()` and build their explicit
  experiment inputs inline: `MarketDataConfig`, `BookReplay`, strategy configs,
  `ExecutionModelConfig`, `MarketReportConfig`, and either
  `build_replay_experiment(...)` or `ParameterSearchExperiment(...)`

PMXT uses local raw files plus remote archives instead of a separate service
surface. Telonex uses a local Hive-partitioned full-book mirror, materialized
`OrderBookDeltas` replay cache, and `api:` as an explicit fallback. The primary
operating docs are grouped under Core Framework; optimizer research and ledger
replay are grouped under Advanced / Experiments.

## Start Here

- [Setup](setup.md)

## Core Framework

- [Backtests And Runners](backtests.md)
- [Data Loading](data-loading.md)
- [Data Vendors And Local Mirrors](data-vendors.md)
- [Vendor Fetch Sources And Timing](vendor-fetch-sources.md)
- [Execution Modeling](execution-modeling.md)
- [Plotting](plotting.md)
- [Testing](testing.md)

## Advanced / Experiments

- [Research And Optimizers](research.md)
- [Polymarket Account Ledger Replay](account-ledger-replay.md)

## Project

- [Project Status](project-status.md)
- [License Notes](license.md)

### Acknowledgements

I'd like to thank everybody who I talked to along the way, as well as everybody who has starred, forked, filed issues, and asked questions about this project. Being only 19, I started with very little knowledge about the inner workings of markets on a microstructure level, and now have a lot more experience in strategy research and optimization. This repository started when I wanted to test my friend's hypotheses, and serves as my attempt at an all-in-one backtesting solution for prediction markets, with easy access to data and abstractions around a well-known backtesting framework. 
