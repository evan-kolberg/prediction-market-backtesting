# Vendor Fetch Sources And Timing

Timing output is enabled by default for public runners that use
`@timing_harness`. Set `BACKTEST_ENABLE_TIMING=0` only when you explicitly want
quiet output.

## PMXT

PMXT book runners fetch historical L2 order-book data one UTC hour at a time.
The hour lookup order is:

1. Local filtered cache.
2. Each explicit raw source in `MarketDataConfig.sources`, left to right.
3. Confirmed miss.

The public PMXT runners usually use:

```python
sources=(
    "local:/Volumes/storage/pmxt_data",
    "archive:r2v2.pmxt.dev",
    "archive:r2.pmxt.dev",
)
```

After a successful raw-source fetch, the market/token/hour slice is written to
the filtered cache under `~/.cache/nautilus_trader/pmxt`. Warm filtered-cache
reads should be sub-millisecond to low-millisecond per hour because the cache
stores a compact filtered parquet slice rather than the full raw archive hour.

## Example Output

A representative PMXT run prints:

```text
PMXT source: explicit priority (cache -> local /Volumes/storage/pmxt_data -> archive https://r2v2.pmxt.dev -> archive https://r2.pmxt.dev)
Loading PMXT Polymarket market will-ludvig-aberg-win-the-2026-masters-tournament (token_index=0, window_start=2026-04-05T00:00:00+00:00, window_end=2026-04-07T23:59:59+00:00)...
  2026-04-05T00:00:00+00:00      ...          ... rows  cache polymarket_orderbook_2026-04-05T00.parquet
  2026-04-06T12:00:00+00:00      ...          ... rows  local raw
  2026-04-07T23:00:00+00:00      ...            0 rows  none
Fetching hours (69/72 done, 3 active): ...
```

Important fields:

- `PMXT source:` shows exact source priority.
- `cache` means the filtered market/token/hour cache satisfied the request.
- `local raw` means a local raw archive hour was scanned and filtered.
- `r2 raw` means a remote raw archive hour was downloaded and filtered.
- `none` means the hour was not found in any configured source.
- Active progress shows currently running scans or transfers.

## Telonex

Telonex book runners read full-depth daily book snapshots from
`book_snapshot_full`.

Typical source config:

```python
MarketDataConfig(
    platform=Polymarket,
    data_type=Book,
    vendor=Telonex,
    sources=(
        "local:/Volumes/storage/telonex_data",
        "api:",
    ),
)
```

The effective lookup order for converted replay records is:

1. Telonex materialized replay caches under `book-deltas-v1` and
   `trade-ticks-v1`.
2. Explicit entries in `MarketDataConfig.sources`, left to right.
3. For execution trade ticks only, Polymarket's public trades cache/API remains
   the final fallback after Telonex local/API misses.

The `Telonex source:` line shows that implicit cache layer:

```text
Telonex book source: explicit priority (cache -> local /Volumes/storage/telonex_data -> api https://api.telonex.io (key set))
Telonex trade source: explicit priority (cache -> local /Volumes/storage/telonex_data -> api https://api.telonex.io (key set) -> polymarket cache -> api https://data-api.polymarket.com/trades)
```

Local reads use the DuckDB manifest when present. The manifest maps requested
market/outcome/channel/day ranges to concrete parquet part paths, so the loader
does not need to glob or scan unrelated partitions. If a candidate local part is
empty or unreadable, it is ignored and the loader can fall through to the next
source.

API reads are daily. A first API run writes both the raw nested daily parquet
and a `.fast.parquet` sidecar. Warm cache reads prefer the sidecar, which
stores `bid_prices`, `bid_sizes`, `ask_prices`, and `ask_sizes` as
`list<string>` columns. That keeps price/size precision and avoids slow nested
list-of-struct pandas decoding.

After any raw/cache/local/API day is converted to `OrderBookDeltas`, the loader
writes a materialized replay parquet. Non-empty Telonex `onchain_fills` or
`trades` days are also materialized as `TradeTick`s. Empty Telonex onchain-fill
results are not terminal for execution matching; the loader continues to
Telonex `trades` and then Polymarket's public trade fallback before deciding a
day has no trade ticks. Repeated runs for the same market, token, instrument id,
day, and clipped window report `telonex deltas cache ...` or
`telonex onchain_fills cache ...` / `telonex trades cache ...` and skip
local/API decoding. Local/API trade-tick labels also include the exact Telonex
channel, such as `telonex local onchain_fills` or `telonex local trades`.

## Timing Expectations By Source

| Source | Expected behavior | When it happens |
|---|---|---|
| PMXT filtered cache | Fastest PMXT path; compact filtered parquet per market/token/hour | Second run onward for the same market, token, and hour |
| Local PMXT raw archive | Local disk bound; scans full raw hour then filters to market/token | Hour is missing from filtered cache but exists in `local:/...` |
| Remote PMXT raw archive | Network and full-hour parquet bound | Hour is missing locally and archive fallback is configured |
| Telonex deltas cache | Fastest Telonex path; materialized Nautilus `OrderBookDeltas` | Same market/token/day/window was already converted once |
| Telonex materialized trade-tick cache | Fastest Telonex execution path; materialized Nautilus `TradeTick`s | Same market/token/day/window Telonex fills or trades were already converted once |
| Telonex fast API cache | Local disk bound; avoids nested payload materialization | API day was previously downloaded and sidecar exists or was lazily migrated |
| Local Telonex mirror | Local disk bound; manifest-pruned parquet parts | `/Volumes/storage/telonex_data` has the requested full-book day |
| Telonex API | Network and daily parquet bound | Cache/local mirror misses and `TELONEX_API_KEY` is available |
| None | Fast miss | Hour/day does not exist in any source |

## How To See This Output

Run any public PMXT or Telonex runner directly:

```bash
uv run python backtests/polymarket_book_ema_crossover.py
uv run python backtests/polymarket_book_joint_portfolio_runner.py
uv run python backtests/polymarket_telonex_book_joint_portfolio_runner.py
```

Run all public Python backtests:

```bash
uv run python scripts/run_all_backtests.py
```

Use the timing harness helper when you want only source/timing diagnostics for a
runner:

```bash
uv run python prediction_market_extensions/backtesting/_timing_test.py backtests/polymarket_book_ema_crossover.py
```

Timing output is additive to Nautilus logs. It should remain enabled by default
so local/cache/archive/API source behavior is visible in normal runs.
