# Project Status

## Roadmap

- [ ] live paper trading mode
- [x] multi-market support within strategies
- [x] better position sizing
- [x] fee modeling [PR#4](https://github.com/ben-gramling/nautilus_pm/pull/4)
- [ ] fuller slippage modeling for maker realism still needs L3 data [PR#6](https://github.com/ben-gramling/nautilus_pm/pull/6), [PR#9](https://github.com/evan-kolberg/prediction-market-backtesting/pull/9)
- [x] Polymarket L2 order-book backtests [PR#10](https://github.com/evan-kolberg/prediction-market-backtesting/pull/10)
- [x] public PMXT relay on a VPS for faster backtests [PR#17](https://github.com/evan-kolberg/prediction-market-backtesting/pull/17), [PR#18](https://github.com/evan-kolberg/prediction-market-backtesting/pull/18), [PR#19](https://github.com/evan-kolberg/prediction-market-backtesting/pull/19), [PR#20](https://github.com/evan-kolberg/prediction-market-backtesting/pull/20), [PR#21](https://github.com/evan-kolberg/prediction-market-backtesting/pull/21), [PR#22](https://github.com/evan-kolberg/prediction-market-backtesting/pull/22), [PR#24](https://github.com/evan-kolberg/prediction-market-backtesting/pull/24), [PR#25](https://github.com/evan-kolberg/prediction-market-backtesting/pull/25), [PR#34](https://github.com/evan-kolberg/prediction-market-backtesting/pull/34), [PR#35](https://github.com/evan-kolberg/prediction-market-backtesting/pull/35), [PR#36](https://github.com/evan-kolberg/prediction-market-backtesting/pull/36), [PR#40](https://github.com/evan-kolberg/prediction-market-backtesting/pull/40)
- [ ] Kalshi L2 order-book backtests need data we do not have yet
- [x] richer charting [PR#5](https://github.com/ben-gramling/nautilus_pm/pull/5)

## Known Issues

No repo-level open issues are tracked here right now.

## Recently Fixed

- [x] relay misses now fall back client-side to `r2.pmxt.dev` instead of forcing server-side scans [PR#22](https://github.com/evan-kolberg/prediction-market-backtesting/pull/22)
- [x] proxied relay clients now keep distinct rate-limit buckets when the proxy is trusted [PR#25](https://github.com/evan-kolberg/prediction-market-backtesting/pull/25)
- [x] stale request-rate buckets are pruned instead of accumulating forever [PR#25](https://github.com/evan-kolberg/prediction-market-backtesting/pull/25)
- [x] relay observability now includes rolling completed-hour rate and live progress badges [PR#34](https://github.com/evan-kolberg/prediction-market-backtesting/pull/34), [PR#35](https://github.com/evan-kolberg/prediction-market-backtesting/pull/35)
- [x] relay DB writes now retry across transient SQLite lock contention instead of killing services [PR#36](https://github.com/evan-kolberg/prediction-market-backtesting/pull/36)
- [x] PMXT L2 replay now orders book updates ahead of quote ticks so longer windows do not lose book state [PR#26](https://github.com/evan-kolberg/prediction-market-backtesting/pull/26)
- [x] `polymarket_simple_quoter.py` now works when run directly because repo-root bootstrap happens early enough [PR#26](https://github.com/evan-kolberg/prediction-market-backtesting/pull/26)
