# Data Vendors And Local Mirrors

This page is intentionally strict about what is supported today.

## PMXT

The repository direction is raw-first:

- mirror raw PMXT archive hours onto local disk when you want local-first replay
- point runners at those raws directly
- use the public PMXT archive URLs when local raw files are missing

### Runner Source Modes

The preferred PMXT quote-tick path is runner-side source selection through
`MarketDataConfig(..., sources=...)`. Public runners pin those source values
directly in code so the file is self-contained and directly runnable.

Example:

```python
DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=(
        "local:/data/pmxt/raw",
        "archive:r2v2.pmxt.dev",
        "archive:r2.pmxt.dev",
    ),
)
```

With PMXT, the active public contract is:

1. local filtered cache
2. each explicit raw source in the order you list it

`DATA.sources` is intentionally strict here: use only `local:` and `archive:`.
Unprefixed hosts, paths, and legacy alias prefixes are rejected.

The vendored Nautilus PMXT loader still exposes lower-level env switches for
custom integrations. In this repository's v3 setup, the supported remote path
is direct raw archive parquet.

### Lower-Level Loader Env Vars

The public runner layer is pinned in code, but the underlying loader env vars
still work for custom integrations:

- `PMXT_LOCAL_ARCHIVE_DIR`
- `PMXT_RAW_ROOT`
- `PMXT_REMOTE_BASE_URL` (comma-separate multiple archives, e.g. `https://r2v2.pmxt.dev,https://r2.pmxt.dev`)
- `PMXT_CACHE_DIR`
- `PMXT_DISABLE_CACHE`

### What Works Today

The public PMXT runner layer reads one market/token/hour from these places:

1. local filtered cache
2. each explicit raw source in the order you list it in `DATA.sources`

The current "bring your own data" story is therefore:

- set `DATA.sources` in your runner to
  `("local:/path/to/raw-hours", "archive:r2v2.pmxt.dev", "archive:r2.pmxt.dev")`
- or point `PMXT_LOCAL_ARCHIVE_DIR` / `PMXT_RAW_ROOT` at a directory of raw
  PMXT hour files you already mirrored locally

When the runner falls back to a remote raw source, it downloads that hour to a
temporary local parquet file, filters it locally, and deletes the temp
artifact afterward. Persistent raw disk growth only happens when you
intentionally configure a local raw mirror.

If you want local-only PMXT replays, set `PMXT_LOCAL_ARCHIVE_DIR` or
`PMXT_RAW_ROOT` to your raw-hour directory and leave remote archive sources out
of `DATA.sources`.

The loader still does not expose a first-class runner flag for arbitrary vendor
raw dumps or automatic normalization from other vendors.

To mirror raw archive hours locally for this repo's runners, use:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The downloader walks direct hourly filenames from `2026-02-21T16:00:00Z`
through the current floored UTC hour newest-first, probes `r2v2.pmxt.dev` and
`r2.pmxt.dev`, and keeps the larger archive object when both exist for the same
hour. It reports the direct-hour count, hours missing from all configured
sources, and requested hours still missing locally. Existing local files are
refreshed when they are empty or when an upstream source advertises a larger
object. It also prints per-hour completion lines plus the active transfer.
Example output:

```text
PMXT raw source: direct hour probes (archive best-of https://r2v2.pmxt.dev, https://r2.pmxt.dev)
Downloading PMXT raw hours to /path/to/pmxt_raws (requested_hours=3, window_start=2026-02-27T11, window_end=2026-02-27T13)...
  2026-02-27T13  12.431s   445.9 MiB  archive
  2026-02-27T12   0.000s    existing  skip
Downloading raw hours (2/3 done, 1 active):  67%|████████████████████████████████████████████████████████████▏                              | [00:41<00:20]active: archive 2026-02-27T11 392.0/445.9 MiB 14.8s
```

Those values vary with the direct-hour window and whatever hour is currently in
flight.

### Supported Local File Layout

The loader-managed filtered cache still lives at:

```text
~/.cache/nautilus_trader/pmxt
```

You can override it with:

```bash
PMXT_CACHE_DIR=/custom/path
```

Or disable it with:

```bash
PMXT_CACHE_DIR=0
PMXT_DISABLE_CACHE=1
```

For local raw PMXT archive hours, the loader accepts either of these layouts:

```text
<raw_root>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
<raw_root>/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

Enable that source with low-level env vars:

```bash
PMXT_LOCAL_ARCHIVE_DIR=/custom/raw-hours
```

The lower-level loader `raw-local` mode expects the archive-style layout:

```text
/data/pmxt/raw/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

Enable that mode with:

```bash
PMXT_DATA_SOURCE=raw-local
PMXT_LOCAL_RAWS_DIR=/data/pmxt/raw
```

Or pin it directly in a runner:

```python
sources=("local:/data/pmxt/raw",)
```

### Required Parquet Columns

Local raw PMXT archive parquet must contain:

- `market_id`
- `update_type`
- `data`

The loader filters raw hours to `market_id` at parquet scan time, then filters
the remaining rows to `token_id` inside the JSON payload.

### Required JSON Payload Shape

For `book_snapshot`, the loader decodes `data` with these fields:

```json
{
  "update_type": "book_snapshot",
  "market_id": "0x...",
  "token_id": "123...",
  "side": "buy",
  "best_bid": "0.45",
  "best_ask": "0.47",
  "timestamp": 1710000000.123,
  "bids": [["0.45", "100.0"]],
  "asks": [["0.47", "120.0"]]
}
```

For `price_change`, the loader decodes `data` with these fields:

```json
{
  "update_type": "price_change",
  "market_id": "0x...",
  "token_id": "123...",
  "side": "buy",
  "best_bid": "0.45",
  "best_ask": "0.47",
  "timestamp": 1710000001.456,
  "change_price": "0.46",
  "change_size": "25.0",
  "change_side": "buy"
}
```

The loader filters to `token_id` by regex-matching inside the `data` JSON, so
that field must be present and string-encoded exactly as expected.

## Telonex

Telonex is a separate Polymarket vendor path. The public runner surface still
uses `data_type=quote_tick`, but the Telonex adapter pins the
`book_snapshot_full` channel, converts full-depth book snapshots into L2
`OrderBookDeltas`, and emits derived `QuoteTick`s for strategies and reports.
It does not use PMXT hourly raw files or the PMXT filtered cache. Runner API
downloads use a separate Telonex API-day cache at
`~/.cache/nautilus_trader/telonex` by default.

Telonex source syntax is also explicit:

- `local:/path/to/telonex` reads already-downloaded Telonex Parquet files from
  disk
- `api:` uses the default `https://api.telonex.io` download endpoint
- `api:https://host.example` points the adapter at a custom compatible base URL

The API path reads the key from `TELONEX_API_KEY`. Do not put API keys in
`DATA.sources`, notebooks, docs, or committed files. Telonex free trials count
each daily Parquet download, so warm local files first when you are experimenting
and use `api:` only for intentional downloads.

When a runner falls back to `api:`, the downloaded daily Parquet payload is
written to the Telonex API-day cache before it is parsed. A second run for the
same base URL, channel, market, outcome, and date reads that cache without
asking Telonex for another presigned URL. Override the cache root with
`TELONEX_CACHE_ROOT=/path/to/cache`, disable it with `TELONEX_CACHE_ROOT=0`, or
clear only that cache with:

```bash
make clear-telonex-cache
```

Do not point `TELONEX_CACHE_ROOT` at a local mirror. The clear target refuses
the configured local Telonex data destination, the PMXT raw mirror root, paths
inside those stores, and parents containing those stores.

Recommended local layout:

```text
/path/to/telonex/
  polymarket/
    market-slug/
      0/
        quotes.parquet
        trades.parquet
        book_snapshot_5.parquet
        book_snapshot_25.parquet
        book_snapshot_full.parquet
        onchain_fills.parquet
      1/
        quotes.parquet
```

The downloader consolidates each `(market, outcome, channel)` group by default
so a full mirror does not create one tiny file per market/outcome/day. With
`--no-consolidate`, daily files are kept under
`polymarket/<market_slug>/<outcome>/<channel>/<YYYY-MM-DD>.parquet`. The loader
also accepts the earlier channel-first daily layout and a few flat filename
fallbacks for test fixtures and ad hoc downloads.

### Download Local Telonex Files

Use the local downloader to warm the same directory the public Telonex runner
uses by default:

```bash
TELONEX_API_KEY=... make download-telonex-data TELONEX_DOWNLOAD_FLAGS='\
  --market-slug us-recession-by-end-of-2026 \
  --outcome-id 0 \
  --start-date 2026-01-19 \
  --end-date 2026-02-01'
```

To mirror every Telonex Polymarket market without storing redundant shallow
book snapshots, download the quote, trade, full-depth book, and onchain-fill
channels into Hive-partitioned Parquet (with a DuckDB manifest for
resumability):

```bash
uv run python scripts/telonex_download_data.py \
  --destination /Volumes/LaCie/telonex_data \
  --all-markets \
  --channels quotes trades book_snapshot_full onchain_fills
```

`book_snapshot_full` carries full-depth snapshots. Use it as the canonical
book-snapshot source and derive 5-level or 25-level views from it when needed;
downloading `book_snapshot_5` and `book_snapshot_25` alongside
`book_snapshot_full` duplicates the same book-state family.

The default `--workers 128` is the in-flight coroutine ceiling in the shared
async `httpx` pool. The downloader decodes day Parquet payloads directly into
Arrow tables and writes consolidated ~1 GiB blob parts; it does not create
millions of tiny day files. On a fast host, benchmark `--workers 64`, `128`,
and `256` before scaling up because high concurrency can hit socket/file
descriptor pressure or outrun the single consolidated Parquet writer.
`--parse-workers` controls the bounded Arrow decode pool (default:
`min(8, cpu_count)`, also configurable with `TELONEX_PARSE_WORKERS`). Transient
`408/425/429/5xx` responses retry with exponential backoff. Hit `Ctrl-C` once
to stop gracefully; the same command resumes. Five interrupt signals are
required to force-exit before the graceful drain finishes.

The downloader fetches the Telonex markets catalog on every run, so newly
listed markets and extended channel availability windows are planned on resume.
Cached 404 day markers are rechecked after 7 days by default; use
`--recheck-empty-after-days 0` to recheck 404s every run, or
`--recheck-empty-after-days -1` to keep 404s forever unless `--overwrite` is
used.

The default destination is `/Volumes/LaCie/telonex_data`. Override it with
`TELONEX_DATA_DESTINATION=/path/to/telonex_data` or call the script directly:

```bash
uv run python scripts/telonex_download_data.py \
  --destination /Volumes/LaCie/telonex_data \
  --market-slug us-recession-by-end-of-2026 \
  --outcome-id 0 \
  --start-date 2026-01-19 \
  --end-date 2026-02-01
```

The script reads the API key only from `TELONEX_API_KEY` and writes data under
`<destination>/data/` with a DuckDB manifest at
`<destination>/telonex.duckdb`. The manifest tracks `completed_days` and
`empty_days` for crash-safe resume. The `local:/Volumes/LaCie/telonex_data`
source reads back through the same blob.

For `--all-markets`, progress is visible in three phases: loading the markets
dataset, planning concrete day-file downloads from each market availability
window, and downloading straight into the blob. The process is resumable:
`Ctrl-C` once to stop gracefully, then re-run the same command to skip
everything already recorded and continue. Five interrupt signals are required
to force-exit before the graceful drain finishes. Transient HTTP failures
(408/425/429/5xx) and connection errors are retried with exponential backoff.

## What Is Not Plug-And-Play Yet

- arbitrary third-party vendor raw formats
- automatic normalization from another vendor into PMXT raw archive hours

If you have your own global raw dumps today, the safe path is:

1. if they are already PMXT raw archive hours, point `PMXT_LOCAL_ARCHIVE_DIR`
   at them directly
2. otherwise normalize them into the PMXT raw archive shape outside this repo
3. or add a new vendor adapter that knows how to read them directly

That keeps the strategy and runner layer unchanged.
