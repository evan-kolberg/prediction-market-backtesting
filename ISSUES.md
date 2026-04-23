# Backtesting Audit Issues

Round 1: Initial audit (3 inspectors + judge)
Round 2: Post-fix verification audit

---

## CRITICAL

### C1 — Trade-print-size used as synthetic book depth proxy (overly conservative for trade-tick strategies)

**Round 2 finding** · `fill_model.py:81-88`, `core.py:79-88`

The fix for the unlimited book (`_UNLIMITED_BOOK_SIZE=1_000_000`) replaced it with `visible_size` from order tags. For trade-tick strategies, `visible_size` comes from `tick.size` (the last trade print size). But **trade size is not resting book depth**. A single 5-share trade sets `visible_size=5`, so a 25-share market order only fills 5 shares. Conversely, a 200-share block trade (which may have exhausted all liquidity) makes the book look 200 deep.

This flips from overly optimistic (100% fill guarantee) to overly pessimistic for large orders and overly optimistic after block trades.

Additionally, `_cap_entry_size_to_visible_liquidity` in `core.py:79-88` clips `desired_size` to `visible_size`, which doubles the problem: orders are first capped to trade-print size, then the fill model further limits fills to the same trade-print depth.

**Recommended fix:** Use a configurable `synthetic_book_depth_multiplier` or `max(visible_size, default_book_depth)`. For trade-tick strategies, consider passing `visible_size=None` to the liquidity cap (since trade print size != book depth) and using `max(visible_size, order_quantity)` in the fill model.

### C2 — Settlement PnL not applied when `simulated_through_ns` is missing

**Round 1 finding** · `_result_policies.py:82-101`

If `simulated_through` key is missing from the result dict (e.g., when the backtest runner doesn't populate it), `simulated_through_ns` is `None`, and the comparison `simulated_through_ns < settlement_observable_ns` is short-circuited. The settlement PnL is then applied even if the market resolved after the backtest window — a form of look-ahead.

**Status:** Partially addressed — `apply_backtest_run_state` populates `simulated_through` from `engine_result.backtest_end`, but any code path that bypasses `build_backtest_run_state` would be vulnerable.

---

## HIGH

### H1 — `_synthetic_book_size` fallback to `_MIN_SYNTHETIC_BOOK_SIZE=1.0`

**Round 2 finding** · `fill_model.py:85-88`

If `parse_visible_liquidity` returns `None` and `_order_quantity` returns `None`, the fallback is `1.0` — meaning exactly 1 share of synthetic depth. Any market order larger than 1 share would get a 1-share partial fill, producing nonsense results.

### H2 — Look-ahead in rolling indicators was present (now fixed)

**Round 1 finding** · `mean_reversion.py`, `vwap_reversion.py`, `panic_fade.py`

Strategies appended the current price to the rolling window *before* computing the reference indicator, creating a one-tick look-ahead.

**Status: CONFIRMED FIXED in Round 2.** All strategies now compute indicators from the prior window before appending.

### H3 — RSI used arithmetic mean instead of Wilder smoothing (now fixed)

**Round 1 finding** · `rsi_reversion.py:82-106`

**Status: CONFIRMED FIXED in Round 2.** Now uses proper Wilder exponential smoothing with seed period.

### H4 — No maker fee modeling (now fixed)

**Round 1 finding** · `polymarket/fee_model.py`

**Status: CONFIRMED FIXED in Round 2.** Limit orders now return zero commission (Polymarket maker fee = 0).

### H5 — Kalshi fee waivers baked into instrument at load time (now fixed)

**Round 1 finding** · `kalshi/providers.py`, `kalshi/fee_model.py`

**Status: CONFIRMED FIXED in Round 2.** Fee waiver is now checked per-fill using `order.ts_init` timestamp. Minor residual concern: in backtest mode `ts_init` may differ from real wall-clock time due to latency model configuration.

### H6 — Joint portfolio runner config mismatch (now fixed)

**Round 1 finding** · `joint_portfolio_runner.py:113-127`

**Status: CONFIRMED FIXED in Round 2.** Config now uses correct `QuoteTickDeepValueHoldConfig` keys.

### H7 — Survivorship bias — only pre-selected surviving markets are backtested

**Round 1 finding** · All `backtests/*.py` runners

All runners hardcode pre-selected, already-resolved markets. Cancelled, delisted, or zero-liquidity markets are never included. The `_result_policies.py` now warns about curated replay selection.

**Status: WARNED but not fixed.** `apply_repo_research_disclosures` attaches a warning, but the fundamental bias remains.

### H8 — No portfolio-level risk limits

**Round 1 finding** · `core.py` (entire `LongOnlyPredictionMarketStrategy` class)

No max position size, drawdown circuit-breaker, or daily loss limit. Backtested drawdowns can be unrealistically deep without halting.

**Status: WARNED but not fixed.** `_PORTFOLIO_RISK_WARNING` is attached to results.

### H9 — `_risk_exit` did not account for fees in breakeven calculation (now fixed)

**Round 1 finding** · `core.py:199-208`

**Status: CONFIRMED FIXED in Round 2.** `_risk_exit` now uses `_entry_price_with_fees()` and `_exit_price_after_fees(price)`.

### H10 — Settlement PnL used post-backtest outcome without disclosure (now fixed)

**Round 1 finding** · `_result_policies.py`

**Status: CONFIRMED FIXED in Round 2.** The policy now checks `settlement_observable_ns` vs `simulated_through_ns` and keeps mark-to-market PnL when resolution wasn't observable during the replay window, with a warning.

---

## MEDIUM

### M1 — `compute_binary_settlement_pnl` incorrect for short positions

**Round 1 finding** (unfixed) · `backtest_utils.py:299-328`

If `open_qty` goes negative (short YES), the formula `cash + (resolved_outcome * open_qty) - commissions` is wrong. E.g., short 100 YES at 0.80 that resolves No: `cash=80, open_qty=-100, resolved_outcome=0 → PnL = 80 + 0 = 80`, but the short should also account for not needing to buy back (since it resolved worthless, the short is worth 100 × 0.80 proceeds + 100 × (1 - 0) settlement credit = 80 + 100 = 180 minus cost basis, but the current formula misses the short settlement mechanics).

Low priority since all strategies are long-only, but the function is available for custom use.

### M2 — All trade-tick strategies pass `tick.size` as `visible_size`

**Round 2 finding** · `vwap_reversion.py:176`, `mean_reversion.py:191`, `ema_crossover.py:168`, `breakout.py:251`, `rsi_reversion.py:175`, `panic_fade.py:163`, `late_favorite_limit_hold.py:158-163`

Same root cause as C1. Trade print size is not visible ask depth. Propagates to both the liquidity cap in `core.py` and the synthetic book depth in `fill_model.py`.

### M3 — Equity series derivation differs between single-market and multi-market modes

**Round 1/2 finding** (unfixed) · `artifacts.py:327-334` vs `artifacts.py:140-157`

Fill-event reconstruction vs engine-based account state snapshots can produce slightly different equity curves for the same strategy/market.

### M4 — `_cap_entry_size_to_visible_liquidity` clips order size to trade-print size

**Round 2 finding** · `core.py:79-88`

When `visible_size` is `tick.size` (from trade-tick strategies), `min(desired_size, visible_size)` clips order size to the last trade print. Combined with C1, orders are double-clipped.

### M5 — Entry/exit slippage inverted for short-selling strategies

**Round 1 finding** (latent) · `fill_model.py:96-102`

The model assumes BUY = entry, SELL = exit via `_is_entry_order`. This is now correctly handled via order tags (`parse_order_intent`), with `order.side == OrderSideEnum.BUY` only as fallback. Short-entry strategies (sell to open) would hit the wrong slippage path. Low risk since all strategies are long-only, but the architecture is fragile.

### M6 — Commission monkey-patch can be missed

**Round 1 finding** (latent) · `__init__.py:6-20`

If someone constructs a `BacktestEngine` directly without calling `install_commission_patch()`, the upstream linear formula would be used, overestimating fees near p=0.50 and underestimating near extremes.

### M7 — EMA crossover warmup initialization bias

**Round 1 finding** (unfixed) · `ema_crossover.py:86-97`

Both EMAs initialized from SMA of their respective seed windows. The first crossover signal after warmup completes may be an artifact of initialization rather than genuine momentum. Matters most for short backtest windows.

### M8 — Floating-point drift in entry price/cost accumulation

**Round 1 finding** (partially mitigated) · `core.py:108-229`

Accumulated `float` arithmetic over many fills. VWAP reversion has a periodic recompute guard (every 256 ticks), but the core base strategy has no such protection for `_entry_price`.

### M9 — Polymarket API offset ceiling truncates historical data

**Round 1 finding** (unfixed) · `loaders.py:806-820`

For high-volume markets, the ~100K offset ceiling means incomplete trade history. A `RuntimeWarning` is emitted but backtests proceed with partial data.

### M10 — `_probability_frame` clips to [0,1] with warning but no detail

**Round 2 finding** · `backtest_utils.py:204-214`

Warning doesn't identify which records or how many were affected, making it hard to trace data corruption.

---

## LOW

### L1 — Timestamp tie-breaking at 1s resolution

**Round 1 finding** · `loaders.py:880-884`, `kalshi/loaders.py:368-372`

Polymarket/Kalshi trade APIs provide 1s timestamps. Synthetic nanosecond offsets for same-second trades may not match real CLOB execution order.

### L2 — Stop-loss/take-profit previously excluded fees (now fixed)

**Round 1 finding** · `core.py:199-208`

**Status: CONFIRMED FIXED in Round 2.**

### L3 — Train/holdout windows on same market limits generalization

**Round 1 finding** · `polymarket_quote_tick_ema_optimizer.py:57-79`

Holdout is a different time slice of the same market, not an out-of-sample market.

### L4 — `_pending` flag blocks all signals during order lifecycle

**Round 1/2 finding** · `core.py:107,192-230`

Design choice, not a bug. Strategies cannot react to new signals between order submission and terminal event. The `LateFavoriteLimitHoldStrategy` now partially works around this via `on_order_accepted`.

### L5 — No `on_market_resolution` handler in strategy base class

**Round 1 finding** · `core.py`

Strategies that hold positions at resolution don't receive an explicit resolution event. Post-hoc settlement PnL is computed by `BinarySettlementPnlPolicy`, but in-flight strategy logic can't react to resolution.

### L6 — `_parse_numeric` overly permissive

**Round 1 finding** · `backtest_utils.py:29-46`

Replaces underscores, Unicode minus, splits on whitespace. Robust for human-readable formats but could silently accept malformed data.

### L7 — `LateFavoriteLimitHoldStrategy.on_order_accepted` muddles `_pending` / `_entered_once` semantics

**Round 2 finding** · `late_favorite_limit_hold.py:140-142`

Two flags with overlapping semantics. `_pending` is cleared on acceptance, but `_entered_once` is the actual gate. A subclass that doesn't check `_entered_once` could process signals during the GTC rest period.

### L8 — Brier advantage uses lagging rolling mean as "user probability"

**Round 1 finding** · `backtest_utils.py:320-348`

The "user" Brier score is a smoothed version of market price that always lags, confounding strategy timing with inherent smoothing lag. This measures SMA lag, not forecasting skill.

---

## Round 3: Deep audit (4 agents — research/analysis, edge cases, config/tests, data integrity)

### HIGH (new from Round 3)

### H11 — Hardcoded `side="yes"` in `_serialize_fill_events` distorts equity curves for NO-side markets

**Round 3 finding** · `research.py:314`

`_serialize_fill_events` always sets `"side": "yes"` regardless of the actual instrument/position side. For NO-side instruments, the serialized event discards the real side. On deserialization, `_deserialize_fill_events` re-infers the side from the `market_id` using `_infer_market_side` string heuristics. If the caller passes a label lacking the NO-side suffix pattern (e.g., human-readable slug rather than raw instrument ID), the side is inferred as YES incorrectly.

**Impact:** For NO-side positions, `_signed_quantity` returns the wrong sign, causing position quantities to be inverted in equity curve reconstruction. Equity curves, drawdown, and position counts in charts are wrong for any NO-side markets.

### H12 — Joint-portfolio drawdown uses `bfill()`, assigning future equity to the past

**Round 3 finding** · `_optimizer.py:629`

`_joint_portfolio_drawdown` reindexes each market's equity series onto the union timeline using `.ffill().bfill()`. The `bfill()` propagates the first known equity value *backwards* to timestamps before any trade occurred on that market. A market that starts trading on day 5 gets its day-5 equity value copied to days 1-4, inflating the joint-portfolio equity at early timestamps.

**Impact:** Artificially inflates the running peak, *understating* the true maximum drawdown. Since optimizer scoring uses `pnl - 0.5 * max_drawdown_currency`, understated drawdown inflates scores for strategies with staggered market entries, potentially selecting suboptimal parameters.

---

### MEDIUM (new from Round 3)

### M11 — Drawdown sign convention inconsistent between modules

**Round 3 finding** · `legacy_plot_adapter.py:621` vs `research.py:1006-1008,1239-1241`

`_build_metrics` computes drawdown as `(running_max - equity) / running_max` → **positive** value, and `max_drawdown = drawdown.max()`. `save_aggregate_backtest_report` computes drawdown as `(equity - running_peak) / running_peak` → **negative** value, and `max_drawdown = drawdowns.min()`.

**Impact:** `BacktestResult.metrics["max_drawdown"]` has inconsistent sign depending on which code path built it. Downstream comparisons or thresholds will behave incorrectly for one of the two paths.

### M12 — `fill_price_map` overwritten per fill — only last fill price survives as fallback

**Round 3 finding** · `legacy_plot_adapter.py:389`

`_replay_fill_position_deltas` sets `fill_price_map[market_id] = float(fill.price)` on every fill, overwriting the previous entry. After processing, only the *last* fill price remains. This value is used as `fallback_price` on line 511 when `_aligned_market_prices` lacks price data.

**Impact:** For markets without external price feeds, equity between the first and last fill is incorrectly valued at the last fill price rather than the entry price.

### M13 — Pre-timeline fills clamped to bar 0, distorting early position deltas

**Round 3 finding** · `legacy_plot_adapter.py:386-387`, `research.py:169-170`

When `np.searchsorted` returns 0 (fill timestamp before first dense bar), `max(0, bar_idx)` clamps to 0. Any fill preceding the first dense timeline bar is packed into bar 0's position delta.

**Impact:** Usually no issue because the timeline includes fill timestamps. But if market prices are supplied with timestamps before the first fill, the equity at bar 0 would include position deltas that logically belong to a later point.

### M14 — `.replace(0, pd.NA)` masks 100% drawdowns, reporting max_drawdown as 0 instead of 1.0

**Round 3 finding** · `legacy_plot_adapter.py:612`, `research.py:1006`

`running_max = equity.cummax().replace(0, pd.NA)` is intended to avoid division by zero, but if equity drops to exactly 0 (total drawdown), the point is masked and drawdown is reported as 0% instead of 100%.

**Impact:** A true total drawdown event would be silently underreported.

---

### LOW (new from Round 3)

### L9 — Polymarket trade pagination early-termination assumes API ordering

**Round 3 finding** · `polymarket_native.py:259`

`if start_ts is not None and max(trade["timestamp"] for trade in data) < start_ts: break` assumes trades are returned in descending timestamp order. If the API returns ascending order, this condition never fires and the loop continues until offset ceiling.

### L10 — Telonex `_book_events_from_frame` uses upper-bound-only timestamp filter

**Round 3 finding** · `telonex.py:1089-1090`

Book events use `ts_ns <= end_ns` only, with lower-bound filtering inside the loop. This is intentional for book reconstruction but inconsistent with the simpler `_quote_ticks_from_frame` path.

### L11 — Stale notebook outputs

**Round 3 finding** · `backtests/*.ipynb`

Optimizer and runner notebooks contain rendered output cells from previous executions. Not a logic bug but a staleness risk for anyone reviewing without re-running.

---

### CRITICAL (new from Round 3 edge-case audit)

### C3 — Duplicate instruments in joint-portfolio run cause double-counting or silent overwrites

**Round 3 finding** · `_prediction_market_backtest.py:197-199`

`run_async()` adds every loaded sim's instrument via `engine.add_instrument()` and `engine.add_data()`. If two replays reference the same market (same instrument_id), there is no deduplication check. The engine either rejects the duplicate or silently overwrites, leading to incorrect PnL or double-counted records.

### C4 — `compute_binary_settlement_pnl` returns 0.0 for empty fill_events instead of None

**Round 3 finding** · `backtest_utils.py:299-328`

When `fill_events` is empty, the loop never runs, and the function returns `0.0 + (resolved_outcome * 0.0) - 0.0 = 0.0`. Downstream in `apply_binary_settlement_pnl`, this 0.0 overwrites the original `pnl` and marks `settlement_pnl_applied=True`. This conflates "no trades, no PnL" with "trades happened and PnL is exactly zero."

### C5 — `PolymarketFeeModel.get_commission` crashes on `None` `instrument.taker_fee`

**Round 3 finding** · `polymarket/fee_model.py:79`

`taker_fee_dec = instrument.taker_fee` is used without a None check. `None * Decimal(10_000)` raises `TypeError`. The Kalshi fee model has the same issue at line 134: `if instrument.taker_fee > 0:` crashes when `taker_fee` is `None`.

---

### HIGH (new from Round 3 edge-case audit)

### H13 — Polymarket price validation is a hard crash, not a skip

**Round 3 finding** · `loaders.py:907-911`

`if not (0.0 <= _raw_price <= 1.0): raise ValueError(...)` aborts the entire trade-parsing loop on a single corrupted trade. Unlike Kalshi's `_normalize_price` which attempts recovery, there is no tolerance or skip logic.

### H14 — Kalshi `_extract_yes_price` raises `KeyError` on missing price fields

**Round 3 finding** · `kalshi/loaders.py:132`

A single malformed trade dict missing all expected price keys raises `KeyError`, crashing the backtest rather than skipping the bad record.

### H15 — PMXT `_scoped_source_entry` mutates instance state non-atomically under ThreadPoolExecutor

**Round 3 finding** · `pmxt.py:254-273`, `pmxt.py:747-807`

The PMXT runner temporarily mutates `self._pmxt_raw_root` and `self._pmxt_remote_base_url` in a try/finally while using `ThreadPoolExecutor` for parallel hour-loading. Thread A's scoped entry can overwrite thread B's intended URL before thread B reads it.

**Impact:** Under `prefetch_workers > 1`, parallel hour-loads may temporarily see the wrong source URL.

---

### MEDIUM (new from Round 3 edge-case audit)

### M15 — `_probability_frame` warnings not propagated to backtest result `warnings` list

**Round 3 finding** · `backtest_utils.py:204-214`

The `RuntimeWarning` for out-of-range prices is emitted to stderr but not persisted in the result dict's `warnings` list. Downstream Brier scores and chart visuals use clipped values with no audit trail.

### M16 — Very small `initial_cash` produces silent no-op backtests

**Round 3 finding** · `core.py:75-76`, `_prediction_market_backtest.py:143`

With `initial_cash=0.01` and typical prices, `_cap_entry_size_to_free_balance` computes `affordable_size` that rounds to 0 via `make_qty(round_down=True)`, producing `None` from `_entry_quantity`. The strategy never trades and no warning is emitted.

### M17 — `_effective_entry_reference_price` returns `Decimal(1)` for trade-tick strategies, systematically under-sizing positions

**Round 3 finding** · `core.py:91-101`

When no visible ask exists (always the case for trade-tick strategies), the worst-case reference price of 1.0 makes unit_cost ~1.0, so `affordable_size ≈ balance * 0.97`. This is conservative but misleading — actual fills at 0.10 would allow far more shares than the strategy orders.

### M18 — `_parse_numeric` defaults to 0.0 for missing fill prices in settlement PnL

**Round 3 finding** · `backtest_utils.py:30-47,314-316`

`_parse_numeric(event.get("price"), default=0.0)` returns 0.0 for missing price keys. A fill with price=0.0 is treated as a free fill in the settlement calculation, distorting PnL without warning.

---

### LOW (new from Round 3 edge-case audit)

### L12 — `extract_realized_pnl` returns 0.0 for empty position reports, conflating with zero PnL

**Round 3 finding** · `backtest_utils.py:50-57`

### L13 — `build_brier_inputs` silently returns empty for `window <= 0` with no error

**Round 3 finding** · `backtest_utils.py:331-339`

### L14 — ContextVar-based loader configs not inherited by ThreadPoolExecutor on Python < 3.12

**Round 3 finding** · `polymarket_native.py:51-52`, `kalshi_native.py:36-37`, `pmxt.py:74`

PMXT worker threads may not see explicitly configured source settings, falling back to env vars.

### L15 — `ReplayWindow` with `start_ns >= end_ns` not validated at construction

**Round 3 finding** · `replay.py:38-41`

---

### CRITICAL (new from Round 3 data integrity audit)

### C6 — PMXT: Float64 nanosecond timestamp conversion causes sub-microsecond precision loss

**Round 3 finding** · `pmxt.py:897,919,963`

`int(payload.timestamp * 1_000_000_000)` — a float64 has only 53 bits of mantissa, but a nanosecond timestamp after ~1979 exceeds 2^53. This causes `float_to_int` rounding for every timestamp, trashing sub-microsecond precision. Two events within the same float64 bucket may be misordered, producing incorrect book state reconstruction. Affects `ts_event` in every `OrderBookDeltas` and `QuoteTick` from PMXT data.

### C7 — PMXT: `parse_to_quote` uses `bids[-1]`/`asks[-1]` on unsorted book levels — wrong top-of-book if payload is not worst-to-best ordered

**Round 3 finding** · `pmxt.py:934`

`parse_to_quote` takes the last element as best bid/ask, assuming bids are ascending and asks are descending. The PMXT `_to_book_snapshot` passes lists as-is without ordering normalization. Telonex's loader sorts to normalize, but PMXT does not. If payloads ever have best-first ordering, snapshots produce inverted spreads. Every snapshot-derived QuoteTick would have wrong mid-prices.

---

### HIGH (new from Round 3 data integrity audit)

### H16 — PMXT: Cross-hour book state carried incorrectly — stale snapshots from hour N+1 reset book after hour N updates

**Round 3 finding** · `pmxt.py:1016-1057`

Book state is maintained across hours, but per-hour sorting only orders payloads within that hour. If hour N+1 starts with a snapshot from the end of hour N, that snapshot resets the book after price_changes from hour N were already applied. The final `events.sort()` (line 1058) reorders emitted events correctly, but the `QuoteTick` values captured via `_quote_from_book` during the streaming pass are permanently wrong.

### H17 — Polymarket prices at exactly 0.0 or 1.0 pass validation but violate `POLYMARKET_MIN_PRICE`/`POLYMARKET_MAX_PRICE`

**Round 3 finding** · `loaders.py:907-911`

Validation allows 0.0 and 1.0, but the instrument's valid tick range is [0.001, 0.999]. Trades at these boundaries represent fully resolved markets and can cause `Price` construction errors or produce un-tickable prices that contaminate the backtest price series.

### H18 — Polymarket `AggressorSide.NO_AGGRESSOR` silently substituted for unexpected side values

**Round 3 finding** · `loaders.py:886-892`

Any non-"BUY"/non-"SELL" `side` string (API version changes, liquidations, AMM trades) silently degrades to `NO_AGGRESSOR`, which changes fill modeling behavior and causes strategies relying on trade direction to miss signals. No logging or warning.

---

### MEDIUM (new from Round 3 data integrity audit)

### M19 — PMXT: QuoteTick values captured during wrong book state are not fixed by final sort

**Round 3 finding** · `pmxt.py:1016-1058`

Related to H16. The sort reorders events correctly but does not fix the `QuoteTick` bid/ask values that were already captured from wrong book state during streaming.

### M20 — Kalshi `_normalize_price` may misinterpret integer `1` as cent-scale (0.01) instead of dollar-scale (1.0)

**Round 3 finding** · `kalshi/loaders.py:78-100`

If the API returns price as bare integer `1` (no decimal marker), `has_decimal_marker=False` and the code normalizes `1` to `1/100 = 0.01` instead of recognizing it as $1.00 (=1.0 probability).

### M21 — Kalshi timestamp tiebreaker assigns sub-millisecond ordering by fetch order, not actual execution order

**Round 3 finding** · `kalshi/loaders.py:368-372`

Trades with identical second-resolution timestamps get incrementing nanosecond offsets in fetch order. If the Kalshi API does not return trades in chronological order within a second, the tiebreaker produces wrong intra-second ordering.

### M22 — Telonex `timestamp_ms` column cast to `int64` before multiplication truncates sub-millisecond precision

**Round 3 finding** · `telonex.py:854-855`

If the Parquet file stores `timestamp_ms` as float (e.g., `1710007200123.456`), `to_numpy(dtype="int64")` truncates before multiplying. Currently a latent risk since the column is typically integer.

---

### LOW (new from Round 3 data integrity audit)

### L16 — PMXT token filtering uses regex substring match — fragile but safe due to closing `"` in pattern

**Round 3 finding** · `pmxt.py:287-291,467-472`

A schema change that removes JSON key formatting could break the match.

### L17 — Kalshi outcome hardcoded to "Yes" — no NO-side token support

**Round 3 finding** · `kalshi/providers.py:123`

Design limitation — no way to directly backtest the No side of a Kalshi market.

### L18 — Polymarket `ts_init = ts_event` means zero processing latency in replay

**Round 3 finding** · `loaders.py:919`

Appropriate for historical replay but means latency-aware fill models see zero delay.

### L19 — PMXT fetches one extra hour before start for pre-window snapshot

**Round 3 finding** · `pmxt.py:127`

Performance concern only; pre-window data is correctly not emitted.

### L20 — Telonex `_diff_to_deltas` uses `sequence=0` for all events

**Round 3 finding** · `telonex.py:1039-1050`

Theoretical risk of book update deduplication in NautilusTrader if engine uses `sequence` for dedup.

---

### HIGH (new from Round 3 config/test audit)

### H19 — Six strategies have zero `__post_init__` validation

**Round 3 finding** · `deep_value.py:31-42`, `panic_fade.py:45-80`, `ema_crossover.py:41-69`, `rsi_reversion.py:41-69`, `threshold_momentum.py:29-57`, `final_period_momentum.py:29-57`

These config dataclasses accept any value (negative, NaN, absurdly large, type-mismatched) silently. Fields like `entry_price_max`, `trade_size`, `fast_period`, `slow_period`, `period`, `entry_rsi`, `exit_rsi`, `take_profit`, `stop_loss` all unvalidated. `fast_period >= slow_period` logically invalid but accepted.

### H20 — BreakoutConfig validates only 5 of 10 fields

**Round 3 finding** · `breakout.py:62-124`

`breakout_buffer`, `mean_reversion_buffer`, `min_holding_periods`, `reentry_cooldown`, and `max_entry_price` have no validation. Negative `breakout_buffer` inverts entry logic; `max_entry_price` outside [0,1] is meaningless for binary markets.

### H21 — `entry_slippage_pct`/`exit_slippage_pct` have no upper bound

**Round 3 finding** · `_execution_config.py:64-67`

Only checks `< 0.0`. Values above 1.0 (100%) are accepted, producing fill prices outside the [0,1] prediction-market range and silently corrupting PnL.

---

### MEDIUM (new from Round 3 config/test audit)

### M23 — Protocol `_MeanReversionConfig` missing `window`/`vwap_window` fields

**Round 3 finding** · `mean_reversion.py:32-38`

The Protocol declares only 6 fields, but configs have `window` or `vwap_window`. The base class uses `getattr(self.config, self._window_field)` as a workaround. New configs satisfying the Protocol but lacking `window`/`vwap_window` will fail at runtime with `AttributeError`, not at type-check time.

### M24 — Protocol `_BreakoutConfig` missing `bar_type` field

**Round 3 finding** · `breakout.py:34-45`

`BarBreakoutConfig` requires `bar_type: BarType` but the Protocol doesn't declare it.

### M25 — `Decimal(0)`/`Decimal(1)` used without string argument throughout core.py

**Round 3 finding** · `core.py` (15+ sites), `fee_model.py:132`, `providers.py:46,92`

Inconsistent with project convention of `Decimal(str(value))`. `Decimal(0)` from integer is fine for exact values but inconsistent with the `Decimal("0.97")` pattern used elsewhere.

### M26 — Float round-trip precision loss via `as_double()` → `_decimal_or_none`

**Round 3 finding** · `core.py:142`

`_decimal_or_none(free_balance.as_double())` converts Decimal → float → Decimal[str(float)], potentially losing precision for large balances.

### M27 — Five strategies have zero behavioral unit tests

**Round 3 finding** · test suite

`deep_value`, `ema_crossover`, `rsi_reversion`, `threshold_momentum`, and `final_period_momentum` have only import/existence tests, no behavioral validation. Only `late_favorite_limit_hold` has parameterized validation-rejection tests.

---

### LOW (new from Round 3 config/test audit)

### L21 — Runner `trade_size` diverges from class defaults (e.g., VWAP defaults `Decimal(1)` but runners use `Decimal(100)`)

**Round 3 finding** · `vwap_reversion.py:60` vs runners

### L22 — `StaticLatencyConfig` defaults to 0ms latency but all runners use 75ms

**Round 3 finding** · `_execution_config.py:24-27`

Anyone constructing a config without explicit latency gets zero-latency behavior.

### L23 — Redundant `OrderSide` import in `deep_value.py`

**Round 3 finding** · `deep_value.py:24`

Already imported by parent class `LongOnlyPredictionMarketStrategy`.

### L24 — `_UNLIMITED_BOOK_SIZE` fully removed — no stale references found

**Round 3 finding** · Codebase-wide grep confirmed clean.

---

## Issue Count Summary

| Severity | Count |
|----------|-------|
| CRITICAL | 7 (C1-C7) |
| HIGH | 21 (H1-H21) |
| MEDIUM | 27 (M1-M27) |
| LOW | 24 (L1-L24) |
| **Total** | **79** |
