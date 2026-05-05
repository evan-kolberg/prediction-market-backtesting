# Data Loading

This framework treats data loading as part of backtest realism, not just an I/O
detail. A fast load is only useful if the replay still exposes the same L2 book
state, trade prints, missing-hour gaps, and source failures that a live strategy
would have faced.

## Mental Model

Public Polymarket book runners load L2 `OrderBookDeltas`, then interleave real
`TradeTick` records for execution matching. Strategies should read the L2 book;
trade ticks exist so Nautilus can advance queue position and match fills.

Every runner has explicit source intent in `MarketDataConfig.sources`:

```python
MarketDataConfig(
    platform=Polymarket,
    data_type=Book,
    vendor=PMXT,
    sources=(
        "local:/Volumes/storage/pmxt_data",
        "archive:r2v2.pmxt.dev",
        "archive:r2.pmxt.dev",
    ),
)
```

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

PMXT source entries accept `local:` and `archive:`. Telonex source entries accept
`local:` and `api:`. The fast cache layers are implicit and should not be listed
as explicit sources.

## Staged Loading

Multi-market backtests do not load each replay end-to-end in isolation. They use
staged loading:

1. Resolve market metadata and instruments.
2. Plan the source work for all requested windows.
3. Load cache/local/archive/API source data with a larger source-stage worker
   pool.
4. Convert loaded source data into Nautilus replay records with a smaller
   materialization worker pool.
5. Merge book deltas and execution trade ticks, then hand records to Nautilus.

This is faster because repeated source work is shared. PMXT can scan one raw
hour once, filter it for many market/token requests, and write each filtered
slice to cache. Telonex can fan out day reads while still limiting the expensive
conversion stage. The memory-heavy step is deliberately narrower so high source
concurrency does not mean dozens of full replay objects are materialized at the
same time.

Key controls:

```bash
BACKTEST_REPLAY_LOAD_WORKERS=32
BACKTEST_REPLAY_MATERIALIZE_WORKERS=4
PMXT_PREFETCH_WORKERS=6
PMXT_CACHE_PREFETCH_WORKERS=32
TELONEX_PREFETCH_WORKERS=128
TELONEX_API_WORKERS=32
TELONEX_FILE_WORKERS=28
```

The defaults are tuned for speed without letting RAM or file descriptors grow
without a bound. Raise source workers only after checking disk/network
throughput; raise materialization workers only if RAM headroom is clear.

## PMXT Flow

PMXT is the hourly raw archive path. Each raw parquet hour may contain many
markets and tokens, so the loader first tries the compact filtered cache and only
falls back to large raw files when necessary.

Lookup order for a market/token/hour:

1. PMXT filtered cache:
   `~/.cache/nautilus_trader/pmxt/<condition>/<token>/polymarket_orderbook_YYYY-MM-DDTHH.parquet`
2. Explicit `local:` raw roots, left to right.
3. Explicit `archive:` remote roots, left to right.
4. Confirmed miss.

Local raw roots accept both layouts:

```text
<raw_root>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
<raw_root>/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

If a `local:` root is configured but lacks an hour, the loader logs a local skip.
If an `archive:` source follows it, the loader downloads the remote hour, filters
the requested market/token, writes the filtered cache, and attempts to persist a
raw archive copy back under the first local raw root. If the local root is not
writable or does not exist, that raw persistence step is skipped; the archive
download can still satisfy the replay.

If only `local:` is configured and the hour is absent, no archive download is
attempted. The replay records a missing-hour gap and resets book state until a
fresh `book_snapshot` appears. This avoids carrying an incremental book update
across a hole in history.

## Telonex Flow

Telonex is the full-depth daily snapshot path. Public Telonex runners use the
`book_snapshot_full` channel.

Book lookup order for a market/outcome/day:

1. Materialized `OrderBookDeltas` cache under
   `~/.cache/nautilus_trader/telonex/book-deltas-v1`.
2. Explicit `local:` Telonex mirror entries.
3. Explicit `api:` entries.
4. Confirmed miss.

The local mirror created by `make download-telonex-data` contains a DuckDB
manifest and Hive-partitioned parquet parts:

```text
/Volumes/storage/telonex_data/
  telonex.duckdb
  data/
    channel=book_snapshot_full/
      year=2026/
        month=04/
          part-000001.parquet
```

When the manifest is available, the loader uses it to jump directly to candidate
parts for the requested market, outcome, channel, and day. It does not glob the
entire mirror. If the manifest is missing, it falls back to older local layouts.

When an `api:` source is reached, the loader first checks the Telonex API-day
cache under `~/.cache/nautilus_trader/telonex/api-days`. API cache files have a
raw nested form and, when available, a `.fast.parquet` sidecar optimized for
replay reads. A first API miss downloads the daily payload, writes the API-day
cache, converts snapshots to `OrderBookDeltas`, then writes the materialized
`book-deltas-v1` cache for warm replays.

Execution ticks follow the same realism rule: use the best configured Telonex
source first, but do not stop early on empty `onchain_fills`. The loader tries
materialized Telonex trade cache, Telonex `onchain_fills`, Telonex `trades`, and
then Polymarket's public trade cache/API fallback.

## Caching

PMXT has one main replay-speed cache:

```text
~/.cache/nautilus_trader/pmxt
```

It stores compact filtered parquet slices keyed by condition id, token id, and
hour. Warm PMXT cache loads avoid scanning the raw hourly archive entirely.

Telonex has three cache families:

```text
~/.cache/nautilus_trader/telonex/api-days
~/.cache/nautilus_trader/telonex/book-deltas-v1
~/.cache/nautilus_trader/telonex/trade-ticks-v1
```

`api-days` avoids refetching daily Telonex API payloads. `book-deltas-v1` and
`trade-ticks-v1` avoid reconverting source payloads into Nautilus records.

Polymarket public trade fallback has its own cache:

```text
~/.cache/nautilus_trader/polymarket_trades
```

Cache clearing:

```bash
make clear-telonex-cache && make clear-pmxt-cache && make clear-polymarket-cache
```

The clear targets are intentionally scoped to replay caches. They should not
delete configured local raw PMXT mirrors or local Telonex mirrors.

## Downloading Local Data

Mirror PMXT raw archive hours:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The PMXT downloader is incremental. Existing local hours are skipped unless
overwrite behavior is requested, so reruns fill gaps without replacing completed
raw files.

Mirror a bounded Telonex window:

```bash
TELONEX_API_KEY=... make download-telonex-data TELONEX_DOWNLOAD_FLAGS='\
  --market-slug us-recession-by-end-of-2026 \
  --outcome-id 0 \
  --channels book_snapshot_full onchain_fills trades \
  --start-date 2026-01-19 \
  --end-date 2026-02-01'
```

Mirror Telonex for all markets:

```bash
uv run python scripts/telonex_download_data.py \
  --destination /Volumes/storage/telonex_data \
  --all-markets \
  --channels book_snapshot_full onchain_fills trades
```

Use `--max-days` for bounded smoke tests before a full mirror. The Telonex
manifest records completed and empty days, so interrupted downloads can resume
without repeating completed work.

## Progress And Timing

Timing output is enabled by default for `make backtest`, `uv run python
main.py`, and direct public runners that use `@timing_harness`.

The useful progress lines are plain log lines:

```text
PMXT book progress [####--------------------] 1.0/6 hours (15.9%; started=6, done=0, active=6) prefetch: r2 raw 92.0 MiB/403.1 MiB 11.9s | +4 more
Telonex book progress [##############----------] 4.0/7 days (57.1%; started=7, done=4, active=0)
```

Source labels tell you what actually happened:

- `cache`: PMXT filtered cache hit.
- `local raw`: PMXT local raw hour was scanned.
- `r2 raw`: PMXT archive hour was downloaded.
- `telonex deltas cache`: materialized Telonex book replay hit.
- `telonex local`: Telonex local mirror supplied the day.
- `telonex api`: Telonex API/cache path supplied the day.
- `none`: no configured source had the requested hour/day.

Quiet opt-outs:

```bash
BACKTEST_ENABLE_TIMING=0
BACKTEST_LOADER_PROGRESS=0
BACKTEST_LOADER_PROGRESS_LINES=0
```

Use the first variable to disable the repo timing harness entirely. Use the
loader-specific variables only when you want timing but not loader progress.

## Failure Semantics

Missing PMXT hours warn and reset book state. This is intentional: carrying
incremental L2 changes across a missing full snapshot would make the replay more
confident than the data supports.

Missing PMXT local files do not automatically mean failure if an archive source
is configured after the local source. The archive can satisfy the replay and
optionally backfill the local raw root.

Missing or empty Telonex local days fall through to the next configured source.
Unreadable parquet files warn and are skipped. Empty Telonex `onchain_fills`
fall through to Telonex `trades`, then Polymarket public trades.

Source failures should stay visible in normal logs. Do not hide errors or
warnings that could make a backtest look more complete than the data really is.
