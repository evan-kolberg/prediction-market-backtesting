# PMXT BYOD And Local Data

This page is intentionally strict about what is supported today.

## Runner Source Modes

The PMXT quote-tick example runners expose a runner-side source selector so
users are not pinned to one relay or one directory layout.

Set `PMXT_DATA_SOURCE` to one of:

- `auto`
- `relay`
- `raw-remote`
- `raw-local`
- `filtered-local`

Examples:

```bash
PMXT_DATA_SOURCE=raw-remote \
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

```bash
PMXT_DATA_SOURCE=raw-local \
PMXT_LOCAL_MIRROR_DIR=/data/pmxt/raw \
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

```bash
PMXT_DATA_SOURCE=filtered-local \
PMXT_LOCAL_FILTERED_DIR=/data/pmxt/filtered \
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

`raw-local` expects a local PMXT raw mirror. `filtered-local` is strict local
mode and will not fall back to the public relay or remote archive if an hour is
missing.

## Lower-Level Loader Env Vars

The runner source selector is the easiest public entrypoint, but the underlying
loader env vars still work too:

- `PMXT_LOCAL_ARCHIVE_DIR`
- `PMXT_RELAY_BASE_URL`
- `PMXT_CACHE_DIR`
- `PMXT_DISABLE_CACHE`

## What Works Today

The current PMXT loader can read one market/token/hour from three places, in
this order:

1. local filtered cache
2. relay-hosted filtered parquet
3. local raw PMXT archive hour
4. raw PMXT archive hour on `r2.pmxt.dev`

The current "bring your own data" story is therefore:

- pre-populate the local PMXT filtered cache with PMXT-compatible market-hour
  parquet files
- or point `PMXT_LOCAL_ARCHIVE_DIR` at a directory of raw PMXT hour files you
  already mirrored locally
- or use `PMXT_DATA_SOURCE=raw-local` with `PMXT_LOCAL_MIRROR_DIR`
- or run your own relay and point `PMXT_RELAY_BASE_URL` at it

If you want local-only PMXT replays, set both:

```bash
PMXT_RELAY_BASE_URL=0
PMXT_LOCAL_ARCHIVE_DIR=/path/to/pmxt-hours
```

The loader still does not expose a first-class runner flag for arbitrary vendor
raw dumps or automatic normalization from other vendors.

## Supported Local File Layout

When the loader reads from local filtered cache, it expects:

```text
<cache_root>/<condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

Default cache root:

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

Enable that source with:

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
PMXT_LOCAL_MIRROR_DIR=/data/pmxt/raw
```

The runner-level `filtered-local` mode expects:

```text
/data/pmxt/filtered/<condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

Enable that source with:

```bash
PMXT_DATA_SOURCE=filtered-local
PMXT_LOCAL_FILTERED_DIR=/data/pmxt/filtered
```

## Required Parquet Columns

Filtered cache parquet must contain exactly the columns the loader already
consumes:

- `update_type`
- `data`

`update_type` should be a string such as `book_snapshot` or `price_change`.

`data` should be a JSON string payload for one Polymarket L2 event.

Local raw PMXT archive parquet must contain:

- `market_id`
- `update_type`
- `data`

The loader filters raw hours to `market_id` at parquet scan time, then filters
the remaining rows to `token_id` inside the JSON payload.

## Required JSON Payload Shape

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

The default relay is:

```text
https://209-209-10-83.sslip.io
```

Override it with:

```bash
PMXT_RELAY_BASE_URL=https://your-relay-host
```

Disable relay usage entirely with:

```bash
PMXT_RELAY_BASE_URL=0
```

## What Is Not Plug-And-Play Yet

- arbitrary third-party vendor raw formats
- automatic normalization from another vendor into the PMXT filtered shape

If you have your own global raw dumps today, the safe path is:

1. if they are already PMXT raw archive hours, point `PMXT_LOCAL_ARCHIVE_DIR`
   at them directly
2. otherwise convert them into the filtered market-hour parquet layout above
3. stage them into `PMXT_CACHE_DIR`
4. or ingest them into your own relay that serves the same `/v1/filtered/...`
   API shape

That keeps the strategy and runner layer unchanged.
