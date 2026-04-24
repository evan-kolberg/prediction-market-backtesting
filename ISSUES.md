# Backtesting Audit Issues

Round 4: Deep quant inspection — L2 utilization, setup, reporting, realism

---

## Fixes Applied This Round

- **Setup:** Added `aiohttp` to `make install` — PMXT remote archive loading now works on fresh install
- **Reporting:** Replaced PnL table with raw Nautilus `stats_pnls`/`stats_returns` readout; `engine_result` threaded through `build_result` to include per-instrument and portfolio stats in every result dict
- **Runners:** Stripped all comments and docstrings from backtest runner files

---

## CRITICAL

### C1 — No trade-size-dependent slippage / market impact modeling

`PredictionMarketTakerFillModel` applies fixed slippage regardless of order size. No market impact — strategy can buy 1,000 shares at the ask without moving the book. **Systematically overstates PnL for any non-trivial order size.**

### C2 — PMXT race condition: remote URL swap under ThreadPoolExecutor

`_load_remote_market_batches` (pmxt.py:203-222) mutates `self._pmxt_remote_base_url` in a try/finally while concurrent threads load hours. No lock. Default `prefetch_workers=16`.

### C3 — PMXT missing hours silently skipped with no warning

`load_order_book_and_quotes` skips hours returning `None` with no warning, gap marker, or error. Stale book state propagates silently.

---

## HIGH

### H1 — Zero default latency on all execution paths

`StaticLatencyConfig` defaults all to 0. `build_latency_model()` returns `None`. Polymarket CLOB round-trip is 200-500ms.

### H2 — Trade-tick strategies have zero liquidity visibility

`visible_size=None` bypasses the liquidity cap entirely. Only balance cap limits order size.

### H3 — Resolution metadata accessible to strategy during simulation

`instrument.info["result"]` available from `on_start()` — look-ahead vector.

### H4 — LIMIT order fill modeling = 25% flat probability

No FIFO queue, no time-at-front-of-queue. `prob_fill_on_limit=0.25`.

### H5 — Polymarket fee model assumes all LIMIT = maker

Aggressively priced limits crossing the spread should pay taker fee. No way to distinguish.

### H6 — PMXT float64 timestamp precision loss (~380ns resolution)

Two conversion paths (ts_event via ms-string vs ts_init via Decimal) can diverge by ~150ns.

### H7 — Cross-hour book state carries stale data through missing-hour gaps

If missing hour contained a book_snapshot reset, subsequent deltas apply against stale book.

### H8 — Telonex runner defaults to "quotes" channel (BBO), not full L2

`MarketDataConfig` without explicit channel falls back to `_TELONEX_DEFAULT_CHANNEL = "quotes"`. No validation.

### H9 — No gap detection for missing days or intra-day temporal gaps

Silent skips in both PMXT and Telonex loaders.

### H10 — No data cleaning / outlier filtering

No zero-size/zero-price filtering, no volume anomaly detection, no stale-book detection.

---

## MEDIUM

### M1 — PMXT fsspec/aiohttp is dead code but import blocks all PMXT

`_pmxt_fs` never used; downloads use `urlopen`. But missing fsspec/aiohttp kills `__init__`.

### M2 — No maximum position limit relative to book depth or open interest

Balance cap allows arbitrarily large orders.

### M3 — Settlement PnL returns 0.0 for empty fill_events

Conflates "no trades" with "PnL is exactly zero."

### M4 — Duplicate instruments in joint-portfolio — no deduplication

Same instrument_id across replays causes double-counting or silent overwrites.

### M5 — Telonex API cache has no TTL, size limit, or revalidation

Stale data persists forever. No ETag or version check.

### M6 — Settlement PnL applied post-hoc without resolution mechanics

No modeling of liquidity dry-up, settlement timing, or venue-specific resolution.

### M7 — `AggressorSide.NO_AGGRESSOR` silently substituted for unexpected side values

No logging when API returns non-BUY/non-SELL side strings.

### M8 — `_probability_frame` warnings not persisted in result dict

RuntimeWarning emitted to stderr but not in `warnings` list.

---

## LOW

### L1 — PMXT: sequence=0, order_id=0 on all book deltas

Deduplication impossible if two snapshots arrive at same ts_event.

### L2 — Telonex size-zero book levels silently dropped

Cannot distinguish "level with zero size" from "level removed."

### L3 — Polymarket prices at 0.0/1.0 pass validation but violate tick range

[0.001, 0.999] is the valid instrument range.

### L4 — Telonex DuckDB manifest uses relative paths — moving root breaks references

Orphan cleaner would delete everything.
