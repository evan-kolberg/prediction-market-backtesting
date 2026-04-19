# Vendor Fetch Sources And Timing

## PMXT

When running a PMXT-backed quote-tick backtest, the loader fetches historical
L2 order-book data one hour at a time. In the current codebase, the hour lookup
order is:

1. local filtered cache
2. each explicit raw source in `DATA.sources`, left to right
3. none

`DATA.sources` is prefix-driven on purpose: use `local:` and `archive:` only.
Bare hosts, bare paths, and alias prefixes are not accepted.

Two practical notes matter here:

- PMXT upstream raw hours live at flat object URLs like
  `https://r2v2.pmxt.dev/polymarket_orderbook_YYYY-MM-DDTHH.parquet`, while the
  local mirror stores those same files under dated `YYYY/MM/DD/...` paths

After a successful fetch from a raw source, the result is written to the local
filtered cache so subsequent runs are fast.

## Example Output

The timing harness prints one completion line per resolved hour and keeps an
aggregate progress bar for any hours that are still in flight. A representative
`make backtest` PMXT run looks like this:

```text
make backtest
uv run python main.py

Running: polymarket_quote_tick_joint_portfolio_runner
Running: polymarket_quote_tick_ema_crossover

PMXT source: explicit priority (cache -> local /Volumes/LaCie/pmxt_raws -> archive https://r2v2.pmxt.dev -> archive https://r2.pmxt.dev)
Loading PMXT Polymarket market will-ludvig-aberg-win-the-2026-masters-tournament (token_index=0, window_start=2026-04-05T00:00:00+00:00, window_end=2026-04-07T23:59:59+00:00)...
  2026-04-05T00:00:00+00:00      ...          ... rows  cache 2026-04-05T00
  2026-04-05T01:00:00+00:00      ...          ... rows  cache 2026-04-05T01
  2026-04-05T02:00:00+00:00      ...          ... rows  cache 2026-04-05T02
  2026-04-06T12:00:00+00:00      ...          ... rows  local raw 2026-04-06T12
  2026-04-07T23:00:00+00:00      ...            0 rows  none
Fetching hours (69/72 done, 3 active):  96%|█████████████████████████████████████████████████████████████████████████████████████████████████████████▏| [...<...], prefetch: - local raw 2026-04-07T22 scan ... | +1 more
```

The important signals are:

- the `PMXT source:` line shows the exact cache, local, and archive priority
  the runner will use
- each per-hour line shows the hour, load time, filtered row count, and the
  source that satisfied that hour
- `cache`, `local raw`, and `none` tell you whether the hour came from warm
  cache, the local raw mirror, or a confirmed miss
- `done` and `active` on the aggregate bar show how much of the window has
  completed and how many hours are still in flight
- the `prefetch:` segment shows the currently active raw-hour scans or
  transfers, including source, hour, bytes, and elapsed time

The exact timings, row counts, and active prefetch details vary with cache
warmth, mirror speed, and the requested window.

This is what the repo's current PMXT basket-runner output looks like once
Nautilus logs, the market summary table, and HTML artifact paths are all
printed in the same terminal session:

## Timing Expectations By Source

| Source | Typical time | When it happens |
|---|---|---|
| Local cache | <0.05s | Second run onward for the same market/token/hour |
| Local raw PMXT archive | local disk bound | You mirrored raw PMXT hours locally and pointed `DATA.sources` at `local:/...`, or used `PMXT_RAW_ROOT` for a lower-level loader workflow |
| Remote raw PMXT archive | network and file-size bound | Hour is missing from local cache and local raw mirror, so the client downloads the upstream raw parquet to a temp file and filters it locally |
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
uv run python backtests/polymarket_quote_tick_ema_crossover.py
```

You can also time a runner explicitly through the harness test helper:

```bash
uv run python prediction_market_extensions/backtesting/_timing_test.py backtests/polymarket_quote_tick_ema_crossover.py
```

Public PMXT examples are pinned to known-good sample windows in code so the
direct script paths stay runnable without editing the file first. If your local
raw mirror lives somewhere else, update `DATA.sources` in the runner file.
Public Kalshi trade-tick examples similarly pin `end_time` to a known-good
close window, while native trade-tick runners that omit `end_time` still use
rolling lookbacks.
