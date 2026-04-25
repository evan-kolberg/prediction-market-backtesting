# L2 Migration & Cleanup Plan

Working branch: `fix/l2-realism-and-cache-speedup` off `v3`.
Date authored: 2026-04-25.

This plan is the handoff for finishing the in-flight cleanup. Read top to bottom before editing. The user (Evan) has explicit directives — follow them, don't improvise.

---

## User's Three Directives

1. **L2-with-trades replay, per Nautilus docs** — https://nautilustrader.io/docs/latest/concepts/backtesting/#combining-l2-book-data-with-trade-ticks. Real `OrderBookDeltas` + real `TradeTick`s fed to the engine. Engine: `BookType.L2_MBP`, `bar_execution=False`, `trade_execution=True`. We have been using `QuoteTick` (L1) which is wrong for our data type.
2. **All TradeTick replay traces gone.** TradeTick-driven strategies (vwap_reversion as a TradeTick consumer, breakout-on-prints, etc.) were producing low-quality backtests. **TradeTick is now execution-only — strategies never subscribe to `on_trade_tick`.** Delete TradeTick* strategies/configs/runners/tests; do not preserve or rename.
3. **Per-market HTML output gone (`emit_html=...`).** Only the summary report at the end with the 15 panels. Strip the `emit_html` flag and the per-market detail-HTML emission code path entirely.

Plus a fourth (in-flight perf bug): **telonex cache is slower than the API.** Earlier commits (`9c73dd6`, `2d38140`) optimized the wrong path (Hive blob store) — user has only the `~/.cache/nautilus_trader/telonex/api-days/` per-day cache, no blob store. Real bottleneck not yet diagnosed; needs measurement, not blind optimization. **Blocked on user clarification** — what exactly is being measured (full backtest wall-clock? cache-warm vs cache-cold run?).

---

## Strategies use Nautilus-native APIs only

User preference (2026-04-25): **don't roll custom signals.** Strategies must use what Nautilus exposes naturally:

- **Top-of-book / depth**: `self.cache.order_book(instrument_id)` → `book.best_bid_price()`, `book.best_ask_price()`, `book.best_bid_size()`, `book.best_ask_size()`, `book.midpoint()`, `book.spread()`, `book.bids(depth=N)`, `book.asks(depth=N)`.
- **Indicators**: `nautilus_trader.indicators.*` (e.g. `ExponentialMovingAverage`, `RelativeStrengthIndex`, `BollingerBands`, `AverageTrueRange`, `MovingAverageConvergenceDivergence`, `VolumeWeightedAveragePrice`). Register on the appropriate feed (book delta, bar, or trade) per the indicator's accepted input.
- **Subscriptions**: `subscribe_order_book_deltas(...)` (signal source) and/or bars built from book updates. **Never** `subscribe_trade_ticks()` for strategy signals.

If a strategy cannot be expressed using Nautilus's native APIs without rolling custom rolling-window stats, **delete it rather than reinvent**. We are not building a parallel signal framework.

---

## Current State (as of authoring)

### Already done on this branch
- TradeTick runners deleted: `kalshi_trade_tick_*.py`, `polymarket_trade_tick_*.py`.
- TradeTick tests deleted: `test_polymarket_trade_tick_multi_runner.py`, `test_quote_tick_runner_contract.py`, `test_quote_tick_strategy_configs.py`, `test_trade_loader_integrity.py`, `test_trade_tick_runner_*.py`, `test_trade_tick_strategy_regressions.py`.
- Quote-tick runners renamed → `*_book_*` (e.g. `polymarket_book_ema_crossover.py`).
- `_replay_specs.py` cleaned: only `BookReplay` remains (no `QuoteReplay` / `TradeReplay`).
- `_prediction_market_backtest.py` cleaned of `QuoteTick` references; uses `min_book_events` not `min_quotes`/`min_trades`.
- `replay_adapters.py` exposes `PolymarketPMXTBookReplayAdapter` and `PolymarketTelonexBookReplayAdapter` with `BookType.L2_MBP`, `fill_model_mode="passive_book"`, `liquidity_consumption=True`.
- Telonex `RunnerPolymarketTelonexBookDataLoader` rename in flight (uncommitted: just the class rename, not behavior).

### Broken / inconsistent
- `tests/test_book_runner_contract.py` is **untracked and broken** — imports nonexistent `QuoteReplay`, references `polymarket_quote_tick_*.py` runners that no longer exist, asserts `EXPECTED_RUNNER_EMIT_HTML = True` which contradicts directive #3. This file is a half-converted carbon copy of the deleted `test_quote_tick_runner_contract.py`. Either delete it or fully convert.
- `tests/test_book_strategy_configs.py` — untracked, status unknown, must verify.
- Runner `polymarket_book_independent_25_replay_runner.py:259-260` still wires `strategies:QuoteTickVWAPReversionStrategy` / `QuoteTickVWAPReversionConfig` — strategy name is L1 but the data feed is now L2. Mismatched.
- Runners still split `DETAIL_PLOT_PANELS` (15) vs `SUMMARY_PLOT_PANELS` (7). User wants summary = 15 panels, no detail.
- `replay_adapters.py:289, 294, 411, 418` still reference `QuoteTick` (only for price-range validation — replace with top-of-book derived from book deltas).
- Telonex `_book_events_from_frame` still synthesizes `QuoteTick`s from snapshot diffs (line ~1188-1202, `_quote_from_levels`). Drop this — emit only `OrderBookDeltas`.
- All 10 strategy files in `strategies/` define three variants (`Bar*`, `QuoteTick*`, `TradeTick*`). Per directive #2, `TradeTick*` dies. `QuoteTick*` either dies or converts to `Book*` per Nautilus-native preference.

---

## Phase A — emit_html rip-out (mechanical, contained)

**Goal:** delete the `emit_html` flag and the per-market detail-HTML emission path. Summary report stays. Set `SUMMARY_PLOT_PANELS = DETAIL_PLOT_PANELS` (15 panels) in runners; drop the `DETAIL_PLOT_PANELS` constant from runners that only need the summary.

### Files to edit
- `prediction_market_extensions/backtesting/_experiments.py` — drop `emit_html` from `ReplayExperiment` field, `build_replay_experiment` arg, `replay_experiment_from_backtest`, `build_backtest_for_experiment`.
- `prediction_market_extensions/backtesting/_independent_multi_replay_runner.py` — delete `_resolve_independent_replay_chart_output_path`; drop `emit_html` and per-market `chart_output_path` from `_single_replay_backtest_kwargs`.
- `prediction_market_extensions/backtesting/_artifact_paths.py` — delete `resolve_independent_replay_detail_chart_output_path` (now unused). Keep `sanitize_chart_label` only if still referenced elsewhere; otherwise delete the file.
- `prediction_market_extensions/backtesting/_prediction_market_runner.py` — drop `emit_html`, `chart_output_path`, `return_chart_layout` args (the per-market HTML-emission branch goes away). Keep `detail_plot_panels` only if the summary builder still consumes it.
- `prediction_market_extensions/backtesting/_backtest_runtime.py` — drop `emit_html` arg, drop the `if emit_html or return_chart_layout:` branch around line 296, remove the per-market HTML write at line 310. Keep `return_chart_layout` only if a non-HTML path uses it.
- `prediction_market_extensions/backtesting/_optimizer.py` — drop `emit_html` field from optimizer config + kwarg threading (lines 81, 406-423, 452).
- `prediction_market_extensions/backtesting/prediction_market/artifacts.py` — drop `emit_html` field; delete the `if self.emit_html:` write block at line 239; delete `resolve_chart_output_path`. Per-market detail chart is GONE; only summary survives.
- All 5 runners in `backtests/polymarket_book_*.py` and `backtests/polymarket_telonex_book_*.py` — drop `emit_html=...`, drop per-market `chart_output_path` if any, set `SUMMARY_PLOT_PANELS = DETAIL_PLOT_PANELS` (or inline a 15-panel tuple), remove `DETAIL_PLOT_PANELS` if no longer referenced.
- All 4 notebooks `backtests/*.ipynb` — strip `emit_html=False/True` cells.

### Tests to update / delete
- `tests/test_book_runner_contract.py` — delete or rewrite. If kept, drop the `EXPECTED_RUNNER_EMIT_HTML` assertion and align with new runner shape.
- `tests/test_optimizer.py:226, 248, 255` — drop `emit_html` checks.
- `tests/test_polymarket_pmxt_runner.py:48, 59, 94, 148, 189` — drop.
- `tests/test_prediction_market_equivalence.py:108, 126, 168, 190, 242, 264` — drop.
- `tests/test_replay_adapter_architecture.py:104` — drop.
- `tests/test_polymarket_pmxt_multi_runner.py:123, 132, 134` — drop.
- `tests/test_prediction_market_runner.py:282, 298` — drop.
- `tests/test_joint_portfolio_artifacts.py:68, 138, 186` — drop.
- `tests/test_backtest_script_entrypoints.py:165, 172, 279, 310, 333, 364, 406, 430, 451` — drop.

### Verification
- `uv run pytest tests/test_book_runner_contract.py tests/test_optimizer.py tests/test_replay_adapter_architecture.py tests/test_backtest_script_entrypoints.py -x` should pass.
- `rg "emit_html" prediction_market_extensions/ backtests/ tests/` returns only deletion residue (zero hits in source).
- One runner can be invoked with `uv run python backtests/polymarket_book_ema_crossover.py` without crashing on missing `emit_html` arg.

### Commit
`refactor: rip emit_html and per-market HTML emission; summary-only with 15 panels`

---

## Phase B — Strategy TradeTick deletion + QuoteTick→Book conversion

**Goal:** strategies/* contains only `Bar*` and `Book*` variants. `Book*` uses Nautilus-native APIs only.

### Per strategy (10 files)

For each of `breakout`, `deep_value`, `ema_crossover`, `final_period_momentum`, `late_favorite_limit_hold`, `mean_reversion`, `panic_fade`, `rsi_reversion`, `threshold_momentum`, `vwap_reversion`:

1. **Delete** `TradeTick<Name>Config` and `TradeTick<Name>Strategy`. No replacements.
2. **Convert** `QuoteTick<Name>*` → `Book<Name>*`:
   - Rename class + config.
   - Change `__init__` subscription from `subscribe_quote_ticks(...)` → `subscribe_order_book_deltas(instrument_id=..., book_type=BookType.L2_MBP)`.
   - Replace `on_quote_tick(self, tick)` → `on_order_book_deltas(self, deltas)` or `on_order_book(self, book)`.
   - Top-of-book: `book = self.cache.order_book(self.config.instrument_id); book.best_bid_price()` etc. (NOT `tick.bid_price`).
   - Indicators: register Nautilus built-ins (e.g. `self.ema_fast = ExponentialMovingAverage(period); self.register_indicator_for_quote_ticks(...)`). For book-driven indicators, check Nautilus's accepted input. If an indicator only accepts `Bar`, either:
     - Subscribe to bars built from the book (`subscribe_bars(BarType.from_str("..."))`), or
     - Skip / delete that strategy.
   - **Do NOT** maintain custom `deque`s, hand-rolled stddev, hand-rolled VWAP. If the strategy needed one, either find the Nautilus indicator that does it, or delete the strategy.
3. **Bar* variants** (`Bar<Name>Config`, `Bar<Name>Strategy`) — leave alone unless they reference TradeTick, in which case clean up their imports.
4. **Imports**: drop `from nautilus_trader.model.data import QuoteTick, TradeTick` lines. Replace with `from nautilus_trader.model.data import OrderBookDeltas` (and `Bar` if Bar variant survives).
5. **Validation hooks** (`strategies/_validation.py`) — usually shared; no change unless they reference deleted variants.

### `strategies/__init__.py`
Drop all `TradeTick*` and `QuoteTick*` exports. Add `Book*` exports. Keep `Bar*` exports.

### Strategies likely to die outright (Nautilus-native impossible without custom stats)
- `vwap_reversion` if it consumed trade ticks for VWAP — Nautilus has `VolumeWeightedAveragePrice` but it accepts `Bar`s. Either rewrite as `Bar*` only (build VWAP bars from real trade ticks now flowing through the engine), or delete.
- `breakout` if it computed rolling stddev manually — Nautilus has `BollingerBands` (accepts `Bar`). Rewrite as `Bar*` only or delete the `Book*` variant.
- `mean_reversion` — same.

Verify each by reading the existing implementation. **If a strategy's L1/Trade variant maintained custom rolling stats and there's no Nautilus indicator accepting book deltas directly, delete the L1/Trade variants and keep only the Bar variant.**

### Tests
- `tests/test_strategy_configs.py`, `tests/test_strategy_behavior_additional.py`, `tests/test_breakout_strategy_behavior.py`, `tests/test_strategy_core.py`, `tests/test_strategy_config_validation.py`, `tests/test_limit_hold_regressions.py` — drop TradeTick/QuoteTick assertions; replace with Book equivalents where possible.
- `tests/test_book_strategy_configs.py` (untracked) — verify or rewrite.

### Runner `STRATEGY_CONFIGS` updates
Every runner in `backtests/` that references `strategies:QuoteTickXxxStrategy` → switch to `strategies:BookXxxStrategy`. If the strategy was deleted, switch to the surviving `Bar*` variant or another strategy.

### Commit
`refactor(strategies): delete TradeTick variants, convert QuoteTick→Book using Nautilus-native APIs only`

---

## Phase C — Replay adapter: real trades for matching

**Goal:** `load_replay()` returns interleaved `(OrderBookDeltas | TradeTick)` records sorted by `ts_event`. Trades come from the real Polymarket Data API (or Telonex trades channel if it exists), not synthesized.

### Trade fetch
- Use `PolymarketDataLoader.load_trades(start, end)` at `prediction_market_extensions/adapters/polymarket/loaders.py:514`. Already returns `list[TradeTick]` from `https://data-api.polymarket.com/trades`.
- Cache trades to disk: `~/.cache/nautilus_trader/polymarket_trades/<condition_id>/<token_id>/<YYYY-MM-DD>.parquet` (mirrors PMXT cache layout). Lookup before API call. Reuse `pq.read_table` / `pq.write_table` patterns from PMXT loader.
- For Telonex: check whether Telonex has a `trades_full` channel by probing the API (`curl -H "Authorization: Bearer $TELONEX_API_KEY" https://api.telonex.io/v1/downloads/polymarket/trades_full/...`). If yes, plumb a `TELONEX_TRADES_CHANNEL` constant + a load path. If no, fall back to the public Polymarket Data API.

### Adapter rewrite (`replay_adapters.py`)
Both `PolymarketPMXTBookReplayAdapter` and `PolymarketTelonexBookReplayAdapter`:
1. Drop the `QuoteTick` references (lines 289, 294, 411, 418 — used for price-range validation).
2. Replace price-range validation with: derive top-of-book mid from the *first* `OrderBookDeltas` snapshot or scan top-of-book mids across the deltas stream. (`book = OrderBook(...); book.apply_delta(d); mid = (book.best_bid_price() + book.best_ask_price()) / 2`. Or compute lazily: extract bid/ask price from F_LAST snapshot deltas.)
3. After loading book deltas, fetch real trades via `loader.load_trades(start, end)` (PolymarketDataLoader instance) and merge by `ts_event`. Single sorted `tuple[OrderBookDeltas | TradeTick, ...]`.
4. `_validate_replay_window` continues to gate on book event count + price range. May want a separate `min_trade_count` knob (default 0).

### Loader changes
- `pmxt.py` and `telonex.py`: `load_order_book_and_quotes` → `load_order_book_and_trades`. Drop the `QuoteTick` synthesis path entirely (`_quote_ticks_from_frame`, `_book_events_from_frame`'s `include_quotes` arg, `_quote_from_levels`).
- New helper `load_trades(start, end)` on the runner-side loader subclass that delegates to the existing `PolymarketDataLoader.load_trades` (compose, don't duplicate).

### Engine profile (already correct)
`L2_BOOK_ENGINE_PROFILE` already has `book_type=BookType.L2_MBP`, `fill_model_mode="passive_book"`, `liquidity_consumption=True`. Verify the engine config also sets `bar_execution=False, trade_execution=True` — find this in `_backtest_runtime.py` engine construction or `ReplayEngineProfile` consumer.

### Tests
- `tests/test_replay_adapters.py`, `tests/test_replay_adapter_architecture.py` — assert records contain both `OrderBookDeltas` and `TradeTick`, merged sorted.
- `tests/test_pmxt_data_source.py`, `tests/test_telonex_data_source.py` — drop QuoteTick generation tests.
- New: a test that mocks `load_trades` and asserts trades are interleaved with book deltas in the loaded records.

### Commit
`feat(replay): real trade ticks from Polymarket Data API, interleaved with L2 book deltas`

---

## Phase D — Runner cleanup

**Goal:** Every `backtests/*.py` runner uses L2-with-trades + Book strategies + summary-only reporting.

### Per runner
- Update `STRATEGY_CONFIGS` to reference `strategies:Book*` paths.
- Drop `min_quotes`, `min_trades` → use `min_book_events` (and optionally `min_trade_count`).
- Confirm `MarketReportConfig.count_key="book_events"`, `count_label="Book Events"`.
- Set `SUMMARY_PLOT_PANELS = DETAIL_PLOT_PANELS` (15 panels) or inline equivalent. Drop the 7-panel split.
- Remove any per-market `chart_output_path` argument.

### Notebooks
`backtests/pmxt_quote_tick_joint_portfolio_runner.ipynb` and `telonex_quote_tick_joint_portfolio_runner.ipynb` — rename to `*_book_*` and update content to match. The two `generic_*_research.ipynb` need their `emit_html=` cells stripped.

### Commit
`refactor(runners): book-strategy wiring, summary 15-panel reporting, drop quote/trade framings`

---

## Phase E — Telonex cache slow vs PMXT (DIAGNOSED; ready to implement)

**Root cause confirmed.** Telonex cache files store bids/asks as nested `list<struct<price:string, size:string>>`. Pandas materialization of nested arrow types into Python lists-of-dicts is ~14× slower than the read itself. PMXT cache stores `(update_type: string, data: string)` — two flat scalar columns — so pandas materialization is essentially free.

### Bench numbers (head-to-head on similar-sized files)

Telonex sample: `2026-03-01.parquet` (535KB, 13,322 rows). PMXT sample: `polymarket_orderbook_2026-04-06T22.parquet` (420KB, 14,184 rows).

| Read path | PMXT | Telonex (current) |
|---|---|---|
| `pq.ParquetFile.read()` (arrow Table only) | **0.88 ms** | 17.25 ms (20×) |
| `pd.read_parquet(path)` | 5.55 ms | **233 ms (43×)** |
| `pq.read_table(path)` | 1.09 ms | **FAILS** — `ArrowTypeError: Unable to merge: Field outcome has incompatible types: string vs dictionary<values=string, indices=int32>` |

The 233ms `pd.read_parquet` is the dominant per-file cost. Plus `pq.read_table` outright fails on telonex because pyarrow treats `outcome=Yes/` parent dirs as Hive partitions and the `outcome` column is dict-encoded inconsistently across sibling files.

### Cache format alternatives benchmarked

Same telonex day file, converted to flat schemas, then read:

| Format | `pd.read_parquet` | `pq.ParquetFile.read` | Disk size | Notes |
|---|---|---|---|---|
| Original `list<struct<string,string>>` | 233 ms | 17 ms | 535 KB | nested decode kills pandas |
| `list<float32>` (separate prices/sizes lists) | 8.9 ms | 3.1 ms | 233 KB | **lossy — breaks diff** (see below) |
| `list<string>` (separate prices/sizes lists) | 31 ms | 8.6 ms | 232 KB | preserves source strings; `7×` faster than current |
| Scalar row-per-level (no nested) | 4.3 ms | — | 398 KB | row count blows up 100× (1.4M rows) |

**Critical constraint:** `_book_events_from_frame` keys diffs on `str(level.price)` (line ~1144-1162) to detect changes between snapshots. Converting prices to `float32` corrupts the source string representation (`"0.061"` → `"0.06099999964237213"`) and breaks the diff identity check. **Must preserve original strings.** That eliminates the `list<float32>` format despite its speed.

**Recommended format: `list<string>` for prices/sizes**, separate columns. ~7× speedup for `pd.read_parquet` (31ms vs 233ms), preserves precision, simple.

### Implementation

Add a tier-2 cache **alongside** the existing `<date>.parquet` (which keeps the raw API response as source of truth):

- File path: `<date>.fast.parquet` next to `<date>.parquet`.
- Schema:
  - `timestamp_us: int64`
  - `bid_prices: list<string>`
  - `bid_sizes: list<string>`
  - `ask_prices: list<string>`
  - `ask_sizes: list<string>`
- Compression: `zstd`.

#### Code changes (`prediction_market_extensions/backtesting/data_sources/telonex.py`)

1. **New helper** `_fast_api_cache_path(...)` — returns `<date>.fast.parquet` path mirroring `_api_cache_path`.
2. **New helper** `_load_fast_cache_day(...)` — `_safe_read_parquet(fast_path)` if exists, else None.
3. **New helper** `_write_fast_cache_day(*, frame, ...)` — convert nested `bids`/`asks` columns to four `list<string>` columns, atomic write to `<date>.fast.parquet`.
4. **Update `_load_api_day` cache-hit chain (around line 896):**
   ```
   fast = self._load_fast_cache_day(...)
   if fast is not None: return fast
   slow = self._load_api_cache_day(...)
   if slow is not None:
       try: self._write_fast_cache_day(frame=slow, ...)
       except Exception as exc: warnings.warn(...)
       return slow
   # ... existing API download path ...
   payload = b"".join(chunks)
   self._write_api_cache_day(payload=payload, ...)  # raw, existing
   frame = pd.read_parquet(BytesIO(payload))
   try: self._write_fast_cache_day(frame=frame, ...)
   except Exception: pass
   return frame
   ```
5. **Update `_book_events_from_frame` (line ~1204) to dispatch on column shape:**
   ```
   has_flat = 'bid_prices' in frame.columns
   if has_flat:
       bid_prices_values = frame['bid_prices'].to_numpy()[mask]
       bid_sizes_values  = frame['bid_sizes'].to_numpy()[mask]
       ask_prices_values = frame['ask_prices'].to_numpy()[mask]
       ask_sizes_values  = frame['ask_sizes'].to_numpy()[mask]
   else:
       bids_values = frame[bids_column].to_numpy()[mask]
       asks_values = frame[asks_column].to_numpy()[mask]
   # in inner loop:
   if has_flat:
       bids = self._book_levels_from_arrays(prices=bid_prices_values[idx], sizes=bid_sizes_values[idx], side='bid')
       asks = self._book_levels_from_arrays(prices=ask_prices_values[idx], sizes=ask_sizes_values[idx], side='ask')
   else:
       bids = self._book_levels_from_value(bids_values[idx], side='bid')
       asks = self._book_levels_from_value(asks_values[idx], side='ask')
   ```
6. **New static method `_book_levels_from_arrays(*, prices, sizes, side)`** — produces `tuple[PolymarketBookLevel, ...]` directly from string arrays:
   ```python
   pairs = [(float(p), p, s) for p, s in zip(prices, sizes) if float(s) > 0]
   pairs.sort(key=lambda t: t[0], reverse=(side == 'ask'))
   return tuple(PolymarketBookLevel(price=p_str, size=s_str) for _, p_str, s_str in pairs)
   ```
7. **`_load_blob_range` path** uses the same nested format but is the Hive blob store, which user does not have populated. Apply the same flat conversion here for symmetry only after confirming a user actually uses blob.

### Migration

- **Lazy.** First read of an existing `<date>.parquet` builds `<date>.fast.parquet` opportunistically. Subsequent reads hit the fast file. No upfront cost.
- Keep raw `<date>.parquet` as source of truth (handy for debugging, also lets the fast cache be regenerated).
- Optional follow-up: a one-shot `migrate_telonex_cache_to_fast.py` script that walks the cache and pre-builds all fast variants. Not required; lazy migration is fine.

### Verification

- Run a backtest twice with the same window. First run: builds fast caches (incurs one-time ~290ms per file: 233 read + 50 convert + 5 write). Second run: hits fast caches (~30ms per file vs 233ms). Wall-clock should drop substantially on the second run.
- `pq.read_table` no longer fails on the fast variant (no nested struct, no dict-encoded `outcome` column).
- Existing tests in `tests/test_telonex_data_source.py` should still pass (the loader's external behavior is unchanged; only the cache format changed).
- Add a regression test: round-trip a known frame through the fast cache write/read, assert `_book_events_from_frame` produces the same `OrderBookDeltas` list with both formats.

### Commit
`fix(telonex): tier-2 fast cache (list<string> schema) — ~7× faster reads, matches PMXT's flat-schema strategy`

---

## Phase F — Telonex downloader memory leak

**Status:** Reported by user 2026-04-25. Not yet investigated.

**Symptoms:** The telonex download script grows memory unboundedly. **22GB on 3 hours of downloading.** User's hypothesis: the writer falls behind, the in-memory queue between download and write threads grows without bound.

**Likely fix shape:** every hour (or every N items / N MB), force-drain the queue and flush to disk before accepting more downloads. Bound the queue or add backpressure so the producer waits when the writer is full.

### What needs to happen
1. **Locate the downloader.** Probably under `prediction_market_extensions/backtesting/data_sources/` or a top-level `scripts/telonex_*.py` / `prediction_market_extensions/adapters/.../downloader.py`. Grep for the writer/queue pattern.
2. **Identify the producer/consumer model.** Is it `concurrent.futures`? `asyncio.Queue`? A custom thread + queue? Note the queue type and size limit (likely unbounded).
3. **Apply backpressure:**
   - Bound the queue: `Queue(maxsize=N)` where N reflects acceptable RAM (e.g., maxsize chosen so queued payloads × avg size ≤ a few hundred MB).
   - Producer `put()` blocks when the queue is full — natural backpressure.
4. **Add periodic flush:** every hour (or every K items), explicitly join the writer queue (`queue.join()`), `gc.collect()`, and log queue depth + RSS so future regressions are visible.
5. **Profile RSS during a real download** before and after. Confirm steady-state memory.

### Verification
- Download for ≥3 hours; RSS plateaus instead of growing linearly.
- Log shows queue depth oscillating around the cap, never stuck at max for long.

### Commit
`fix(telonex-downloader): bound writer queue + periodic flush; eliminates unbounded memory growth`

---

## Cross-cutting cleanup

After phases A–D land:
- `rg "QuoteTick|quote_tick|TradeTick|trade_tick|emit_html" --type py --type ipynb` — should return zero hits in source. (Some nautilus_trader imports of these types may still appear inside adapter code where TradeTick is fed to the engine for matching — that's allowed.)
- Update `AGENTS.md` if it references deleted strategies/runners.
- Delete dead test files that exercised deleted code (already partially done; verify `tests/` directory).
- Update `tests/conftest.py` if it imports anything removed.
- Run full suite: `uv run pytest tests/ -x`. Fix until green.
- Run one end-to-end backtest: `uv run python backtests/polymarket_book_ema_crossover.py` — no crashes, summary HTML written, no per-market HTML written.

---

## Memory / preferences (already saved, do not override)

- `feedback_l2_only.md` — strictly L2; never reference QuoteTick/L1/quote_tick.
- `feedback_no_trade_tick_replay.md` — TradeTick is execution-only; never a signal source.
- `feedback_commit_coauthor.md` — Claude as commit co-author by default; add Codex trailer if both agents contributed.
- `project_active_branch.md` — v3 is active; v4_polymarket abandoned.

---

## Suggested execution order

1. **Phase A** (emit_html) — fully isolated, easiest, lands first. Creates a clean baseline.
2. **Phase E** (telonex cache tier-2) — fully isolated from A–D; can run in parallel. Diagnosed and ready to implement.
3. **Phase F** (telonex downloader memory) — fully isolated; can run in parallel. Just needs investigation + a queue cap.
4. **Phase B** (strategies) — depends on knowing which strategies survive. May involve deletions; that's fine.
5. **Phase C** (real trades) — depends on B for the strategy signal contract.
6. **Phase D** (runners) — depends on B and C. Mechanical once those land.

Phases A, E, F are independent of each other and of B–D; safe to fork to separate agents/sessions. B → C → D are sequential.

---

## Open questions for user (pin these — don't guess)

1. **Telonex trades channel** — does Telonex expose a trades download endpoint we can use, or is the Polymarket public Data API our only trade source? (Affects Phase C.)
2. **Strategy survivors** — should `vwap_reversion`, `breakout`, `mean_reversion` survive as `Bar*`-only (using Nautilus indicators on Bars built from real trades), or be deleted entirely?
3. **Notebook format** — convert `*_quote_tick_*.ipynb` notebooks in place, or delete and recreate?
4. **Phase E migration** — is lazy migration (first read of an existing slow cache builds the fast cache) acceptable, or do you want a one-shot pre-migration script that walks `~/.cache/nautilus_trader/telonex/api-days/` and pre-builds all `.fast.parquet` siblings up front?
