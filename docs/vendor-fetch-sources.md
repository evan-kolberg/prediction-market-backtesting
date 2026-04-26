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
    "local:/Volumes/LaCie/pmxt_raws",
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
PMXT source: explicit priority (cache -> local /Volumes/LaCie/pmxt_raws -> archive https://r2v2.pmxt.dev -> archive https://r2.pmxt.dev)
Loading PMXT Polymarket market will-ludvig-aberg-win-the-2026-masters-tournament (token_index=0, window_start=2026-04-05T00:00:00+00:00, window_end=2026-04-07T23:59:59+00:00)...
  2026-04-05T00:00:00+00:00      ...          ... rows  cache 2026-04-05T00
  2026-04-06T12:00:00+00:00      ...          ... rows  local raw
  2026-04-07T23:00:00+00:00      ...            0 rows  none
Fetching hours (69/72 done, 3 active): ...
```

Important fields:

- `PMXT source:` shows exact source priority.
- `cache` means the filtered market/token/hour cache satisfied the request.
- `local raw` means a local raw archive hour was scanned and filtered.
- `archive` means a remote raw archive hour was downloaded and filtered.
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
        "local:/Volumes/LaCie/telonex_data",
        "api:",
    ),
)
```

The effective lookup order for converted replay records is:

1. Telonex materialized `OrderBookDeltas` cache under `book-deltas-v1`.
2. Telonex API-day cache, when enabled.
3. Local mirror entries listed in `MarketDataConfig.sources`.
4. API entries listed in `MarketDataConfig.sources`.

The `Telonex source:` line shows that implicit cache layer:

```text
Telonex source: explicit priority (cache -> local /Volumes/LaCie/telonex_data -> api https://api.telonex.io (key set))
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
writes a materialized deltas parquet. Repeated runs for the same market, token,
instrument id, day, and clipped window report `telonex deltas cache ...` and
skip full-book snapshot diffing.

## Timing Expectations By Source

| Source | Expected behavior | When it happens |
|---|---|---|
| PMXT filtered cache | Fastest PMXT path; compact filtered parquet per market/token/hour | Second run onward for the same market, token, and hour |
| Local PMXT raw archive | Local disk bound; scans full raw hour then filters to market/token | Hour is missing from filtered cache but exists in `local:/...` |
| Remote PMXT raw archive | Network and full-hour parquet bound | Hour is missing locally and archive fallback is configured |
| Telonex deltas cache | Fastest Telonex path; materialized Nautilus `OrderBookDeltas` | Same market/token/day/window was already converted once |
| Telonex fast API cache | Local disk bound; avoids nested payload materialization | API day was previously downloaded and sidecar exists or was lazily migrated |
| Local Telonex mirror | Local disk bound; manifest-pruned parquet parts | `/Volumes/LaCie/telonex_data` has the requested full-book day |
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
