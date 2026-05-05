# Project Status

## Roadmap

- [x] multi-market support within strategies [PR#30](https://github.com/evan-kolberg/prediction-market-backtesting/pull/30), [PR#53](https://github.com/evan-kolberg/prediction-market-backtesting/pull/53), [PR#54](https://github.com/evan-kolberg/prediction-market-backtesting/pull/54), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
- [x] better position sizing
- [x] fee modeling [PR#4](https://github.com/ben-gramling/nautilus_pm/pull/4), [PR#42](https://github.com/evan-kolberg/prediction-market-backtesting/pull/42)
- [ ] fuller slippage modeling for maker realism still needs L3 data [PR#6](https://github.com/ben-gramling/nautilus_pm/pull/6), [PR#9](https://github.com/evan-kolberg/prediction-market-backtesting/pull/9), [PR#50](https://github.com/evan-kolberg/prediction-market-backtesting/pull/50)
- [x] Polymarket L2 order-book backtests [PR#10](https://github.com/evan-kolberg/prediction-market-backtesting/pull/10), [PR#45](https://github.com/evan-kolberg/prediction-market-backtesting/pull/45), [PR#57](https://github.com/evan-kolberg/prediction-market-backtesting/pull/57)
- [x] PMXT raw archive workflow: local raw mirrors, direct archive fallback,
  filtered replay cache, and incremental downloader reruns [PR#17](https://github.com/evan-kolberg/prediction-market-backtesting/pull/17), [PR#22](https://github.com/evan-kolberg/prediction-market-backtesting/pull/22), [PR#40](https://github.com/evan-kolberg/prediction-market-backtesting/pull/40), [PR#47](https://github.com/evan-kolberg/prediction-market-backtesting/pull/47), [PR#56](https://github.com/evan-kolberg/prediction-market-backtesting/pull/56), [PR#61](https://github.com/evan-kolberg/prediction-market-backtesting/pull/61), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
- [ ] Kalshi L2 order-book backtests need L2 historical book data we do not have
  yet. The next exchange expansion targets are Limitless.exchange and
  Opinion.trade after the Polymarket loading path stays stable.
- [x] richer charting and honest multi-run HTML/report outputs [PR#5](https://github.com/ben-gramling/nautilus_pm/pull/5), [PR#52](https://github.com/evan-kolberg/prediction-market-backtesting/pull/52), [PR#68](https://github.com/evan-kolberg/prediction-market-backtesting/pull/68), [PR#74](https://github.com/evan-kolberg/prediction-market-backtesting/pull/74), [PR#80](https://github.com/evan-kolberg/prediction-market-backtesting/pull/80), [PR#83](https://github.com/evan-kolberg/prediction-market-backtesting/pull/83)
- [x] manifest-based runner architecture and repo-level optimizer surface [PR#67](https://github.com/evan-kolberg/prediction-market-backtesting/pull/67)
- [x] repo-level runner/report contracts, docs validation, and launcher/docs hardening [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64), [PR#65](https://github.com/evan-kolberg/prediction-market-backtesting/pull/65), [PR#68](https://github.com/evan-kolberg/prediction-market-backtesting/pull/68), [PR#69](https://github.com/evan-kolberg/prediction-market-backtesting/pull/69), [PR#71](https://github.com/evan-kolberg/prediction-market-backtesting/pull/71), [PR#76](https://github.com/evan-kolberg/prediction-market-backtesting/pull/76), [PR#77](https://github.com/evan-kolberg/prediction-market-backtesting/pull/77), [PR#78](https://github.com/evan-kolberg/prediction-market-backtesting/pull/78), [PR#80](https://github.com/evan-kolberg/prediction-market-backtesting/pull/80), [PR#81](https://github.com/evan-kolberg/prediction-market-backtesting/pull/81)

## Known Issues

- Kalshi is not currently exposed as a public runnable backtest path. The repo
  still contains Kalshi instrument, trade/candlestick loader, fee-model, and
  research helper components, but the built-in replay adapter registry only
  exposes Polymarket PMXT and Telonex book replay adapters. Users should treat
  Kalshi as experimental adapter plumbing until real Kalshi L2 historical book
  data, a Kalshi replay adapter, and a public runner are added.

## Recently Fixed

- [x] v4 staged replay loading now prepares Polymarket metadata first, loads all
  book data second, and loads execution trade ticks last. PMXT filtered-cache
  misses are grouped by raw archive hour, Telonex API/file work has separate
  worker caps, and warm 100-market Telonex loads avoid redundant full-week book
  sorting/count scans.
- [x] PR#119 upgrades public Polymarket runners to L2-native `BookReplay`
  semantics with `OrderBookDeltas` plus real `TradeTick` execution evidence,
  keeps Nautilus on `BookType.L2_MBP` with `trade_execution=True`, removes
  standalone quote/trade replay framing, speeds Telonex API-day cache reads with
  `.fast.parquet` sidecars, prunes local Telonex mirror scans through the DuckDB
  manifest, bounds the Telonex downloader writer queue, periodically closes
  open Telonex part writers, and makes PMXT raw downloads incremental by
  skipping existing local files
  [PR#119](https://github.com/evan-kolberg/prediction-market-backtesting/pull/119)
- [x] PR#119 now also materializes Telonex `OrderBookDeltas` under
  `book-deltas-v1`, prints richer terminal statistics from per-market result
  payloads plus portfolio-level Nautilus `BacktestResult` stats, and keeps
  public Python runner inputs inline inside `run()` instead of module-level
  constants [PR#119](https://github.com/evan-kolberg/prediction-market-backtesting/pull/119)
- [x] prediction-market backtests now use settlement-aware result assembly,
  finite synthetic taker depth, lower default touched-limit fill probability,
  fill-time Kalshi fee waivers, zero-fee Polymarket maker modeling, safer
  trade-tick liquidity caps, and corrected Telonex outcome selection; docs,
  UML, tests, and public runner configs were updated alongside the execution
  honesty fixes [PR#118](https://github.com/evan-kolberg/prediction-market-backtesting/pull/118)
- [x] Telonex source loading now falls through to the next source when one
  fails (so a missing `local:` mirror cleanly hands off to `api:`) instead of
  aborting the whole replay [PR#105](https://github.com/evan-kolberg/prediction-market-backtesting/pull/105)
- [x] v3 adds a Telonex joint-portfolio runner, local Telonex daily-Parquet
  downloader, Hive-partitioned Parquet output with a DuckDB resume manifest,
  and daily-file timing output for Telonex `local:` / `api:` sources [PR#104](https://github.com/evan-kolberg/prediction-market-backtesting/pull/104)
- [x] v3 removes the active PMXT relay path, relay badges, and relay service
  package. PMXT runners now use local raw files plus direct `r2v2.pmxt.dev` /
  `r2.pmxt.dev` archive fallback, and Telonex is available as a separate
  Polymarket vendor [PR#103](https://github.com/evan-kolberg/prediction-market-backtesting/pull/103)
- [x] relay schema-bootstrap now commits its `UPDATE` so a second writer can
  take the WAL lock instead of deadlocking on first start [PR#102](https://github.com/evan-kolberg/prediction-market-backtesting/pull/102)
- [x] PMXT relay latest-hour badge now prints the mirrored filename
  (`polymarket_orderbook_YYYY-MM-DDTHH`) instead of only the naked hour, the
  mirror worker validates the local file size against upstream before reusing
  an existing raw file (so stale placeholder downloads can no longer survive
  as `ready`), `count_raw_dump_files` excludes undersized parquet files from
  the public coverage denominator, and startup adoption purges orphan raw
  files under the nonempty byte threshold that aren't tracked as ready in the
  index DB [PR#101](https://github.com/evan-kolberg/prediction-market-backtesting/pull/101)
- [x] PMXT relay archive-coverage redesign exposes per-source priority
  metrics, mirrored-vs-archive accounting, and clearer empty-hour handling
  [PR#100](https://github.com/evan-kolberg/prediction-market-backtesting/pull/100), [PR#95](https://github.com/evan-kolberg/prediction-market-backtesting/pull/95), [PR#94](https://github.com/evan-kolberg/prediction-market-backtesting/pull/94), [PR#93](https://github.com/evan-kolberg/prediction-market-backtesting/pull/93), [PR#90](https://github.com/evan-kolberg/prediction-market-backtesting/pull/90), [PR#89](https://github.com/evan-kolberg/prediction-market-backtesting/pull/89)
- [x] PMXT runners support full per-entry source ordering and split raw-archive
  sources, so `MarketDataConfig.sources` can interleave `local:` and `archive:`
  entries in any order the runner needs
  [PR#98](https://github.com/evan-kolberg/prediction-market-backtesting/pull/98), [PR#92](https://github.com/evan-kolberg/prediction-market-backtesting/pull/92)
- [x] public runner validation harness covers every flat runner under
  `backtests/` to keep direct script paths and the menu deterministic
  [PR#91](https://github.com/evan-kolberg/prediction-market-backtesting/pull/91)
- [x] ruff-driven cleanups across relay, adapters, and plotting; docs now
  include an embedded-HTML example, acknowledgements, and a fixed index
  anchor URL [PR#88](https://github.com/evan-kolberg/prediction-market-backtesting/pull/88), [PR#87](https://github.com/evan-kolberg/prediction-market-backtesting/pull/87)
- [x] multi-market runners now default to `EMIT_HTML=False` and the artifact
  pipeline downsamples price points to 5 000 before building dense equity
  curves, cutting wall time from ~320s to ~26s on an 8-market basket
  [PR#84](https://github.com/evan-kolberg/prediction-market-backtesting/pull/84)
- [x] HTML chart files are now downsampled to ~5 000 points before Bokeh
  serialization, reducing a 446 K-bar chart from 31 MB to under 1 MB;
  redundant ColumnDataSource columns and intermediate DataFrames were also
  deduplicated, and new regression tests enforce that 100 K-bar backtests
  produce HTML under 5 MB
  [PR#83](https://github.com/evan-kolberg/prediction-market-backtesting/pull/83)
- [x] aggregate summary report builders now skip serializing unused per-market
  price series, fill events, and overlay curves when the selected summary
  panels do not render them
  [PR#83](https://github.com/evan-kolberg/prediction-market-backtesting/pull/83)
- [x] docs deploy workflow now triggers on the active `v2` branch instead of the
  removed `main` branch, and the GitHub Pages environment allows `v2` deploys
  [PR#81](https://github.com/evan-kolberg/prediction-market-backtesting/pull/81)
- [x] plotting docs rewritten around a clearer detail-vs-summary mental model,
  stale `blob/main` GitHub links fixed across all docs, and a regression test
  guards against stale branch links returning
  [PR#80](https://github.com/evan-kolberg/prediction-market-backtesting/pull/80)
- [x] backtest runner examples refreshed to match current runner contracts
  [PR#78](https://github.com/evan-kolberg/prediction-market-backtesting/pull/78)
- [x] legacy Kalshi trade-tick runners pinned `end_time` to a known-good close
  window so direct script paths and the repo pytest gate stay deterministic,
  the docs/examples now point at current runnable entrypoints, and shared
  startup reporting no longer understates factory-backed runs [PR#76](https://github.com/evan-kolberg/prediction-market-backtesting/pull/76), [PR#77](https://github.com/evan-kolberg/prediction-market-backtesting/pull/77)
- [x] public runners now use typed replay specs plus one experiment builder,
  adapter-owned replay loading, and a repo-layer optimizer surface instead of
  the older shared `SIMS` / `BACKTEST` contract
  [PR#67](https://github.com/evan-kolberg/prediction-market-backtesting/pull/67)
- [x] plotting now scales as one detailed HTML per loaded sim plus one aggregate summary HTML per basket, the repo no longer relies on concatenated mega-pages, and the prediction-market runner internals are split into clearer execution, artifact, reporting, and data-source seams [Issue #73](https://github.com/evan-kolberg/prediction-market-backtesting/issues/73), [PR#74](https://github.com/evan-kolberg/prediction-market-backtesting/pull/74)
- [x] direct script HTML outputs now resolve from the repo root, fixed-basket multi-market runners emit aggregate reports again, and the repo runner/report surface stays explicit about per-sim detail charts versus aggregate multi-market reports [PR#68](https://github.com/evan-kolberg/prediction-market-backtesting/pull/68)
- [x] setup/backtest/fetch-source docs now match the unified `main.py` launcher
  and current PMXT terminal/reporting output, and the orphaned `_trade_tick_ui.py`
  helper is gone [PR#69](https://github.com/evan-kolberg/prediction-market-backtesting/pull/69)
- [x] root README scope and agent guidance now keep detailed operational docs out of the README body and in `docs/` instead [PR#70](https://github.com/evan-kolberg/prediction-market-backtesting/pull/70), [PR#71](https://github.com/evan-kolberg/prediction-market-backtesting/pull/71)
- [x] PMXT L2 replay now orders book updates deterministically so longer
  windows do not lose book state [PR#26](https://github.com/evan-kolberg/prediction-market-backtesting/pull/26)
- [x] relay misses fall back client-side to `r2.pmxt.dev`, trusted proxy clients keep distinct rate-limit buckets, and stale buckets are pruned instead of accumulating forever [PR#22](https://github.com/evan-kolberg/prediction-market-backtesting/pull/22), [PR#25](https://github.com/evan-kolberg/prediction-market-backtesting/pull/25), [PR#42](https://github.com/evan-kolberg/prediction-market-backtesting/pull/42)
- [x] relay observability and survivability improved with progress badges, ClickHouse ingest, retry handling around transient lock contention, mirror pruning, and incremental raw-hour adoption [PR#34](https://github.com/evan-kolberg/prediction-market-backtesting/pull/34), [PR#35](https://github.com/evan-kolberg/prediction-market-backtesting/pull/35), [PR#36](https://github.com/evan-kolberg/prediction-market-backtesting/pull/36), [PR#40](https://github.com/evan-kolberg/prediction-market-backtesting/pull/40), [PR#56](https://github.com/evan-kolberg/prediction-market-backtesting/pull/56), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
- [x] PMXT public workflows are now raw-first: local raw mirrors, archive fallback, mirror-only relay behavior, and downloader output all line up across runners and docs [PR#45](https://github.com/evan-kolberg/prediction-market-backtesting/pull/45), [PR#47](https://github.com/evan-kolberg/prediction-market-backtesting/pull/47), [PR#57](https://github.com/evan-kolberg/prediction-market-backtesting/pull/57), [PR#60](https://github.com/evan-kolberg/prediction-market-backtesting/pull/60), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
- [x] public runners now model queue position and static latency where this repo uses them, reducing dependence on zero-latency assumptions [PR#50](https://github.com/evan-kolberg/prediction-market-backtesting/pull/50), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
- [x] replay/report outputs now distinguish requested windows from loaded windows and keep honesty-focused defaults visible in normal runs [PR#52](https://github.com/evan-kolberg/prediction-market-backtesting/pull/52), [PR#56](https://github.com/evan-kolberg/prediction-market-backtesting/pull/56), [PR#63](https://github.com/evan-kolberg/prediction-market-backtesting/pull/63), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
- [x] the interactive menu again shows full runner contents, direct runner imports work in both script and package modes, and the root `_script_helpers.py` shim is gone [PR#53](https://github.com/evan-kolberg/prediction-market-backtesting/pull/53), [PR#62](https://github.com/evan-kolberg/prediction-market-backtesting/pull/62), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
- [x] PMXT timing output, source labels, and raw-hour progress reporting are clearer and better aligned with the actual runner behavior [PR#55](https://github.com/evan-kolberg/prediction-market-backtesting/pull/55), [PR#59](https://github.com/evan-kolberg/prediction-market-backtesting/pull/59), [PR#60](https://github.com/evan-kolberg/prediction-market-backtesting/pull/60)
- [x] repo CI and docs validation now match the documented local gate, and PR docs builds validate without trying to deploy Pages [PR#58](https://github.com/evan-kolberg/prediction-market-backtesting/pull/58), [PR#64](https://github.com/evan-kolberg/prediction-market-backtesting/pull/64)
