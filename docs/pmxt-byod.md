# PMXT BYOD and Data Sources

The PMXT L2 runners can now be pointed at different historical data sources
without changing code inside the runner files.

## Source Modes

Set `PMXT_DATA_SOURCE` before running any PMXT quote-tick backtest:

- `auto`: existing behavior. Use local cache first, then the relay, then the remote raw PMXT archive.
- `relay`: force relay-first behavior even if `PMXT_RELAY_BASE_URL` was disabled earlier in your shell.
- `raw-remote`: skip the relay and scan the raw PMXT archive directly.
- `raw-local`: scan a local raw PMXT mirror instead of `r2.pmxt.dev`.
- `filtered-local`: read already-filtered local parquet files only.

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

If you already set lower-level loader env vars such as `PMXT_RAW_ROOT`,
`PMXT_RELAY_BASE_URL`, or `PMXT_CACHE_DIR` manually, the runner leaves those in
place unless you explicitly set `PMXT_DATA_SOURCE`.

## Local Raw Mirror Layout

`PMXT_DATA_SOURCE=raw-local` expects the same hourly directory layout used by
the relay's `raw/` tree:

```text
/data/pmxt/raw/
  2026/03/21/polymarket_orderbook_2026-03-21T12.parquet
  2026/03/21/polymarket_orderbook_2026-03-21T13.parquet
  ...
```

Each raw hourly parquet file must keep these columns:

- `market_id`
- `update_type`
- `data`

The runner filters by `market_id` at parquet scan time, then filters the JSON
payload in `data` down to the selected `token_id`.

## Local Filtered Layout

`PMXT_DATA_SOURCE=filtered-local` expects the same per-market/token/hour layout
as the runner cache and relay prebuilt output:

```text
/data/pmxt/filtered/
  <condition_id>/<token_id>/polymarket_orderbook_YYYY-MM-DDTHH.parquet
```

Each filtered parquet file must contain only:

- `update_type`
- `data`

This mode is strict local BYOD. If a required hour is missing from that root,
the runner will not fall back to the public relay or remote PMXT archive.

## L2 JSON Payload Requirements

The loader currently understands two PMXT update types:

- `book_snapshot`
- `price_change`

For `book_snapshot`, the JSON string in `data` must decode into fields with this
shape:

```json
{
  "update_type": "book_snapshot",
  "market_id": "0x...",
  "token_id": "123...",
  "side": "BUY",
  "best_bid": "0.42",
  "best_ask": "0.43",
  "timestamp": 1771767624.001295,
  "bids": [["0.42", "150"], ["0.41", "80"]],
  "asks": [["0.43", "120"], ["0.44", "95"]]
}
```

For `price_change`, the JSON string in `data` must decode into fields with this
shape:

```json
{
  "update_type": "price_change",
  "market_id": "0x...",
  "token_id": "123...",
  "side": "BUY",
  "best_bid": "0.42",
  "best_ask": "0.43",
  "timestamp": 1771767624.101295,
  "change_price": "0.43",
  "change_size": "25",
  "change_side": "SELL"
}
```

Notes:

- `timestamp` is a UTC Unix timestamp in seconds.
- Price and size values are strings because that is what the current PMXT
  decoder expects.
- `best_bid` and `best_ask` may be `null`.
- Rows within each hourly file must stay in original event order. The loader
  rebuilds the order book by replaying those rows sequentially.
- The loader needs at least one earlier snapshot hour before the requested
  window so it can reconstruct initial L2 state.
