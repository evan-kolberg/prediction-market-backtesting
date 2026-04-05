# Vendor Fetch Sources And Timing

## PMXT

When running a PMXT-backed quote-tick backtest, the loader fetches historical
L2 order-book data one hour at a time. In the current codebase, the hour lookup
order is:

1. local filtered cache
2. each explicit raw source in `DATA.sources`, left to right
3. none

`DATA.sources` is prefix-driven on purpose: use `local:`, `archive:`, and
`relay:` only. Bare hosts, bare paths, and alias prefixes are not accepted.

Two practical notes matter here:

- the shared public relay for this repository is mirror-first, so raw-hour
  serving is the supported shared-server path
- the public runner layer disables relay-hosted filtered parquet
- PMXT upstream raw hours live at flat object URLs like
  `https://r2.pmxt.dev/polymarket_orderbook_YYYY-MM-DDTHH.parquet`, while the
  local mirror serves those same files under dated `/v1/raw/YYYY/MM/DD/...`
  paths

After a successful fetch from a raw source, the result is written to the local
filtered cache so subsequent runs are fast.

## Example Output

The timing harness now keeps one aggregate progress bar for the requested
window and refreshes the active prefetch status in place. A representative
`make backtest` PMXT run looks like this:

```text
make backtest
uv run python main.py

Running: polymarket_quote_tick_pmxt_deep_value_hold

PMXT source: explicit priority (cache -> local /Volumes/LaCie/pmxt_raws -> archive https://r2.pmxt.dev -> relay https://209-209-10-83.sslip.io)
Loading PMXT Polymarket market will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 (token_index=0, window_start=2026-02-21T16:00:00+00:00, window_end=2026-02-23T10:00:00+00:00)...
Fetching hours (43/44 started, 43 active):  49%|█████████████████████████████████████████████████████████████                                                                | [12:34<20:04], prefetch: | relay raw 2026-02-22T22 52.0/654.6 MiB 170.4s | r2 raw 2026-02-23T10 548.0/565.1 MiB 15.7s
```

The important signals are:

- the `PMXT source:` line shows the exact cache, local, archive, and relay priority
  the runner will use
- `started` and `active` show how much of the window has been dispatched and is
  still in flight
- the `prefetch:` segment shows the currently active remote raw-hour transfers,
  including source, hour, bytes, and elapsed time
- no per-hour completion lines are printed anymore; the aggregate bar is the
  intended output shape

## Timing Expectations By Source

| Source | Typical time | When it happens |
|---|---|---|
| Local cache | <0.05s | Second run onward for the same market/token/hour |
| Local raw PMXT archive | local disk bound | You mirrored raw PMXT hours locally and pointed `DATA_SOURCES` or `PMXT_RAW_ROOT` at them |
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
raw mirror or relay lives somewhere else, update `DATA.sources` in the runner
file.
