# Data Vendors, Local Mirrors, And Raw PMXT

This page is intentionally strict about what is supported today.

## PMXT

The repository direction is raw-first:

- mirror raw PMXT archive hours onto local disk when you want local-first replay
- point runners at those raws directly
- treat shared servers as raw mirrors only

Mirror deployment and archived self-hosted relay guidance live on the separate
[Mirror And Relay Ops](pmxt-relay.md) page so the vendor docs can stay focused
on source order, raw layout, and loader expectations.

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
        "archive:r2.pmxt.dev",
        "relay:mirror.example.com",
    ),
)
```

With PMXT, the active public contract is:

1. local filtered cache
2. each explicit raw source in the order you list it

`DATA.sources` is intentionally strict here: use only `local:`,
`archive:`, and `relay:`. Unprefixed hosts, paths, and legacy alias prefixes
are rejected.

The vendored Nautilus PMXT loader still exposes lower-level env switches for
custom integrations. In this repository's current mirror-first setup, the
supported shared-server path is raw parquet serving, not relay-hosted filtered
parquet.

### Lower-Level Loader Env Vars

The public runner layer is pinned in code, but the underlying loader env vars
still work for custom integrations:

- `PMXT_LOCAL_ARCHIVE_DIR`
- `PMXT_RAW_ROOT`
- `PMXT_REMOTE_BASE_URL`
- `PMXT_RELAY_BASE_URL`
- `PMXT_CACHE_DIR`
- `PMXT_DISABLE_CACHE`

### What Works Today

The public PMXT runner layer reads one market/token/hour from these places:

1. local filtered cache
2. each explicit raw source in the order you list it in `DATA.sources`

The current "bring your own data" story is therefore:

- set `DATA.sources` in your runner to
  `("local:/path/to/raw-hours", "archive:r2.pmxt.dev", "relay:relay.example.com")`
- or point `PMXT_LOCAL_ARCHIVE_DIR` / `PMXT_RAW_ROOT` at a directory of raw
  PMXT hour files you already mirrored locally
- or run your own raw mirror and point `PMXT_RELAY_BASE_URL` at it

When the runner falls back to a remote raw source, it downloads that hour to a
temporary local parquet file, filters it locally, and deletes the temp
artifact afterward. Persistent raw disk growth only happens when you
intentionally configure a local raw mirror.

The important distinction is:

- local raw mirrors and remote raw mirrors are the first-class paths
- the public runner layer only uses raw mirror relay endpoints
- the shared relay path for this repo is raw-only

If you want local-only PMXT replays, set both:

```bash
PMXT_RELAY_BASE_URL=0
PMXT_LOCAL_ARCHIVE_DIR=/path/to/pmxt-hours
```

The loader still does not expose a first-class runner flag for arbitrary vendor
raw dumps or automatic normalization from other vendors.

To mirror raw archive hours locally for this repo's runners, use:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The downloader prints a live progress bar while it walks archive hours. Example
output:

```text
Downloading PMXT raws:  13%|███████████████████████▍| 137/1017 [41:27<3:37:59, 14.86s/hour, archive 2026-02-27T11:00:00+00:00 392.0/445.9 MiB]
```

Those values vary with the archive listing and whatever hour is currently in
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

The runner-level `raw-local` mode expects the archive-style layout:

```text
/data/pmxt/raw/YYYY/MM/DD/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

Enable that source with:

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

## Relay Mode

The public runner layer does not assume a repo-wide default relay host. If you
want a relay fallback, set it explicitly in `DATA.sources` or with the loader
env var below.

Override it with:

```bash
PMXT_RELAY_BASE_URL=https://your-relay-host
```

Disable relay usage entirely with:

```bash
PMXT_RELAY_BASE_URL=0
```

Mirror-only deployments are the preferred server shape now. In that mode the
server keeps mirroring raw hours and may serve `/v1/raw/...`, but `/v1/filtered`
and filtered-hour listing endpoints are disabled or expected to miss.

## What Is Not Plug-And-Play Yet

- arbitrary third-party vendor raw formats
- automatic normalization from another vendor into PMXT raw archive hours

If you have your own global raw dumps today, the safe path is:

1. if they are already PMXT raw archive hours, point `PMXT_LOCAL_ARCHIVE_DIR`
   at them directly
2. otherwise normalize them into the PMXT raw archive shape outside this repo
3. or add a new vendor adapter that knows how to read them directly

That keeps the strategy and runner layer unchanged.
