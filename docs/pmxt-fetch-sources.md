# Vendor Fetch Sources And Timing

## PMXT

When running a PMXT-backed quote-tick backtest, the loader fetches historical
L2 order-book data one hour at a time. In the current codebase, the hour lookup
order is:

1. local filtered cache
2. local raw PMXT archive
3. relay-hosted filtered parquet, if the configured relay supports it
4. remote raw PMXT archive
5. relay-hosted raw PMXT archive hour
6. none

Two practical notes matter here:

- the shared public relay for this repository is mirror-first, so raw-hour
  serving is the supported shared-server path
- the filtered-relay tier still exists in the vendored loader for people who
  run their own legacy or full-stack PMXT relay

After a successful fetch from a raw source, the result is written to the local
filtered cache so subsequent runs are fast.

## Example Output

The timing harness prints one line per completed hour. A representative PMXT
run looks like this:

```text
Loading PMXT Polymarket market market-slug (token_index=0, window_start=2026-03-19T07:35:57.277659+00:00, window_end=2026-03-24T07:35:57.277659+00:00, window_hours=120.0)...
  2026-03-19T11:00:00+00:00   0.001s     214 rows  /Users/you/.cache/nautilus_trader/pmxt/.../polymarket_orderbook_2026-03-19T11.parquet
  2026-03-20T03:00:00+00:00   0.487s    2443 rows  /Volumes/data/pmxt_raws/2026/03/20/polymarket_orderbook_2026-03-20T03.parquet
  2026-03-20T05:00:00+00:00  31.842s      91 rows  https://r2.pmxt.dev
  2026-03-20T06:00:00+00:00   0.711s      88 rows  https://mirror.example.com/v1/raw/2026/03/20/polymarket_orderbook_2026-03-20T06.parquet
  2026-03-24T07:00:00+00:00   0.404s       0 rows  none
Fetching hours: 100%|██████████████████████████████████████| 122/122 [00:34<00:00]
```

The important signal is the rightmost source column:

- cache hits are immediate
- local raw mirrors are disk-bound
- remote raw hours are network and file-size bound
- mirror-only relays serve raw parquet, not server-built filtered parquet

## Timing Expectations By Source

| Source | Typical time | When it happens |
|---|---|---|
| Local cache | <0.05s | Second run onward for the same market/token/hour |
| Local raw PMXT archive | local disk bound | You mirrored raw PMXT hours locally and pointed `DATA_SOURCES` or `PMXT_RAW_ROOT` at them |
| Relay filtered parquet | remote parquet read | Only if you point the loader at a relay that still serves `/v1/filtered/...` |
| Remote raw PMXT archive | network and file-size bound | Hour is missing from local cache and local raw mirror, so the client downloads the upstream raw parquet to a temp file and filters it locally |
| Relay raw mirror | network and file-size bound | A mirror-only relay serves `/v1/raw/...`, so the client downloads the raw parquet to a temp file and filters it locally |
| None | <1s | Hour does not exist yet |

## How To See This Output

Timing is enabled by default in the interactive menu and direct script runners
that use `@timing_harness`.

Turn it off explicitly with:

```bash
BACKTEST_ENABLE_TIMING=0 make backtest
```

Or run any PMXT runner directly:

```bash
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

You can also time a runner explicitly through the harness test helper:

```bash
uv run python backtests/_shared/_timing_test.py backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

Public PMXT examples are pinned to known-good sample windows in code so the
direct script paths stay runnable without editing the file first. If your local
raw mirror lives somewhere else, update `DATA.sources` in the runner file.
