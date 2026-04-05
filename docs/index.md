# Docs Index

This repository is documented around two active operating assumptions:

- vendor-backed historical data is local-first: mirror raw archive hours onto
  local disk, process them locally, and keep shared infrastructure focused on
  raw mirroring and file serving
- public backtest runners are flat experiment specs built around `DATA`,
  `SIMS`, `STRATEGY_CONFIGS`, and a `PredictionMarketBacktest` object with a
  `.run()` entrypoint

PMXT is the first fully documented vendor path. Mirror and relay operations are
filed under the vendor/local-processing docs instead of being treated as a
top-level product surface.

- [Setup](setup.md)
- [Backtests And Runners](backtests.md)
- [Execution Modeling](execution-modeling.md)
- [Data Vendors, Local Mirrors, And Raw PMXT](pmxt-byod.md)
- [Mirror And Relay Ops](pmxt-relay.md)
- [Vendor Fetch Sources And Timing](pmxt-fetch-sources.md)
- [Plotting](plotting.md)
- [Testing](testing.md)
- [Project Status](project-status.md)
- [License Notes](license.md)
