# Data Vendors And Local Mirrors

This page documents the active local-first vendor paths. Both public vendor
adapters are Polymarket book adapters: they produce `OrderBookDeltas` for L2
book state and the replay adapter interleaves real `TradeTick` records for
execution.

## PMXT

PMXT is the hourly raw archive path for Polymarket L2 order-book data.

The preferred workflow is raw-first:

- Mirror raw PMXT archive hours onto local disk.
- Point runners at those raw hours with `local:/...`.
- Let public archive sources fill gaps when the local mirror is incomplete.
- Let the filtered PMXT cache make repeated market/token/hour slices fast.

### Runner Source Modes

Public PMXT runners select sources directly in their inline `MarketDataConfig`:

```python
MarketDataConfig(
    platform=Polymarket,
    data_type=Book,
    vendor=PMXT,
    sources=(
        "local:/Volumes/LaCie/pmxt_data",
        "archive:r2v2.pmxt.dev",
        "archive:r2.pmxt.dev",
    ),
)
```

Lookup order:

1. Local filtered cache at `~/.cache/nautilus_trader/pmxt`.
2. Explicit raw sources in `MarketDataConfig.sources`, left to right.
3. Confirmed miss.

`MarketDataConfig.sources` is intentionally strict: use only `local:` and
`archive:` for PMXT. Bare hosts, bare paths, and legacy aliases are rejected.

### Lower-Level Loader Env Vars

Runner files should carry their source priority inline. These lower-level env
vars remain available for custom integrations:

- `PMXT_LOCAL_ARCHIVE_DIR`
- `PMXT_RAW_ROOT`
- `PMXT_REMOTE_BASE_URL`
- `PMXT_CACHE_DIR`
- `PMXT_DISABLE_CACHE`

### What Works Today

The public PMXT path loads one market/token/hour from raw archives and converts
those rows into Nautilus `OrderBookDeltas`.

The loader decodes:

- `book_snapshot` as a fresh full book snapshot.
- `price_change` as an incremental price-level update.

If an hour is missing, the loader warns and resets local book state. It does
not apply later incremental `price_change` rows across a missing-hour gap until
a fresh `book_snapshot` rebuilds the book.

To mirror raw archive hours locally:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The downloader walks direct hourly filenames from `2026-02-21T16:00:00Z`
through the current floored UTC hour newest-first. It probes `r2v2.pmxt.dev`
and `r2.pmxt.dev`, chooses the larger archive object when both exist, and
writes the same archive filename under a dated local path.

The downloader is incremental. Without `--overwrite`, an existing local hour is
treated as complete and skipped before any network transfer is attempted. This
keeps reruns safe for large mirrors and prevents accidental replacement of
already-downloaded raws.

### Supported Local File Layout

The filtered cache lives at:

```text
~/.cache/nautilus_trader/pmxt
```

Override it with:

```bash
PMXT_CACHE_DIR=/custom/path
```

Disable it with either:

```bash
PMXT_CACHE_DIR=0
PMXT_DISABLE_CACHE=1
```

For local raw PMXT archive hours, the loader accepts:

```text
<raw_root>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
<raw_root>/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

Pin it in a runner with:

```python
sources=("local:/data/pmxt/raw",)
```

### Required Parquet Columns

Raw PMXT archive parquet must contain:

- `market_id`
- `update_type`
- `data`

The loader filters raw hours to `market_id` at parquet scan time, then filters
the remaining rows to `token_id` inside the JSON payload.

`PMXT_PREFETCH_WORKERS` controls how many archive hours are read ahead while a
market window is loading. Local raw mirrors default to `4` workers through the
repo data-source wrapper; raise it, for example `PMXT_PREFETCH_WORKERS=8`, only
after checking that the local disk has enough read bandwidth.

### Required JSON Payload Shape

For `book_snapshot`, the loader expects the `data` JSON to include:

```json
{
  "update_type": "book_snapshot",
  "market_id": "0x...",
  "token_id": "123...",
  "timestamp": 1710000000.123,
  "bids": [["0.45", "100.0"]],
  "asks": [["0.47", "120.0"]]
}
```

For `price_change`, the loader expects:

```json
{
  "update_type": "price_change",
  "market_id": "0x...",
  "token_id": "123...",
  "timestamp": 1710000001.456,
  "change_price": "0.46",
  "change_size": "25.0",
  "change_side": "buy"
}
```

Prices and sizes are preserved as decimal strings until the Nautilus instrument
constructs typed prices and quantities.

## Telonex

Telonex is a Polymarket full-book snapshot vendor path. Public Telonex runners
use `data_type=Book`, `vendor=Telonex`, and the `book_snapshot_full` channel.
They read the local mirror first, then include an `api:${TELONEX_API_KEY}`
fallback whose key is expanded by the loader at runtime.

Telonex source syntax:

- `local:/path/to/telonex` reads the local blob mirror.
- `api:` uses `https://api.telonex.io` with `TELONEX_API_KEY`.
- `api:https://host.example` points at a compatible custom base URL.

The API path reads the key from `TELONEX_API_KEY` unless a private runner source
provides an explicit `api:<key>` value. Do not commit private keys.

API-day downloads are cached by default at:

```text
~/.cache/nautilus_trader/telonex
```

Each cached API day has two forms:

- `<YYYY-MM-DD>.parquet`: the raw nested Telonex API payload.
- `<YYYY-MM-DD>.fast.parquet`: a flat list-string sidecar optimized for replay
  reads.

The fast sidecar preserves price and size strings while avoiding expensive
pandas materialization of nested list-of-struct columns. If a raw cache file is
encountered without a sidecar, the loader migrates it lazily. The API-day cache
is consulted only when an `api:` source is reached, so a `local:` mirror listed
first still has priority over a stale API cache hit.

Replay conversion has a separate materialized cache under:

```text
~/.cache/nautilus_trader/telonex/book-deltas-v1
```

That cache stores Nautilus `OrderBookDeltas` after full-book snapshots have
already been diffed. It is keyed by exchange, channel, market slug, outcome,
instrument id, day, and clipped replay window. Warm runs report
`telonex deltas cache` and skip local/API snapshot decoding entirely.

Clear only the API cache with:

```bash
make clear-telonex-cache
```

Do not point `TELONEX_CACHE_ROOT` at the local mirror. The clear target refuses
configured local data stores and parents containing those stores.

Recommended local mirror root:

```text
/Volumes/LaCie/telonex_data/
  telonex.duckdb
  data/
    channel=book_snapshot_full/
      year=2026/
        month=04/
          part-000001.parquet
```

The DuckDB manifest records completed and empty market/outcome/channel/day
jobs. The loader uses it to select only readable parquet parts for the requested
market, outcome, channel, and date range. If the manifest is missing or invalid,
the loader falls back to legacy path scans.

### Download Local Telonex Files

Small window:

```bash
TELONEX_API_KEY=... make download-telonex-data TELONEX_DOWNLOAD_FLAGS='\
  --market-slug us-recession-by-end-of-2026 \
  --outcome-id 0 \
  --channels book_snapshot_full \
  --start-date 2026-01-19 \
  --end-date 2026-02-01'
```

Full Polymarket mirror:

```bash
uv run python scripts/telonex_download_data.py \
  --destination /Volumes/LaCie/telonex_data \
  --all-markets \
  --channels book_snapshot_full
```

For a bounded smoke test of the all-market path, add `--max-days 100`; the cap
is applied after manifest resume pruning.

`book_snapshot_full` is the canonical book source. Do not download
`book_snapshot_5` and `book_snapshot_25` unless you intentionally want the
shallow vendor files too; they duplicate the same book-state family at lower
depth.

Downloader behavior:

- Default destination is `/Volumes/LaCie/telonex_data`.
- Default channel is `book_snapshot_full`.
- Default `--workers` is 16.
- `--max-days` caps post-resume day jobs for smoke tests.
- Runner API day loading uses `TELONEX_PREFETCH_WORKERS`, default `128`.
- `--parse-workers` or `TELONEX_PARSE_WORKERS` controls the bounded Arrow
  decode pool.
- `--writer-queue-items` or `TELONEX_WRITER_QUEUE_ITEMS` bounds parsed day
  results waiting for the writer. Default: `128`.
- `--pending-commit-items` or `TELONEX_PENDING_COMMIT_ITEMS` bounds completed
  day results held before manifest commit. Default: `128`.
- Transient `408`, `425`, `429`, and `5xx` responses retry with exponential
  backoff.
- Completed days and empty days are tracked in `telonex.duckdb` for crash-safe
  resume.
- The writer queue and pending-commit list are bounded, and an hourly forced
  writer drain closes open Parquet part writers, commits their manifest rows,
  releases Arrow memory, and prints RSS/open-part diagnostics. Raising the
  queue limits can improve throughput on high-RAM machines while still
  preventing unbounded growth.
- Hit `Ctrl-C` once to stop gracefully; in-flight work drains and the manifest
  is flushed before exit.

## What Is Not Plug-And-Play Yet

- Arbitrary third-party vendor raw formats.
- Automatic normalization from another vendor into PMXT raw archive hours.
- True L3/MBO priority reconstruction from public Polymarket L2 data.

If you have custom global raw dumps, the safe paths are:

1. If they already match PMXT raw archive shape, point `local:/...` at them.
2. Otherwise normalize them outside this repo into the PMXT raw schema.
3. Or add a new vendor adapter that directly emits `OrderBookDeltas`.
