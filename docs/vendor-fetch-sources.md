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

PMXT source: explicit priority (cache -> local /Volumes/LaCie/pmxt_data -> archive https://r2v2.pmxt.dev -> archive https://r2.pmxt.dev)
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

## Telonex

Telonex quote-tick runners read the `book_snapshot_full` channel as L2 book
data from consolidated local Parquet files at
`polymarket/<market>/<outcome>/<channel>.parquet`, or fetch missing daily files
from `api:` when the local mirror is not present. Use local files first when
you have warmed them:

```python
DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=Telonex,
    sources=(
        "local:/Volumes/LaCie/telonex_data",
        "api:",
    ),
)
```

The timing harness uses the same `BACKTEST_ENABLE_TIMING` switch for Telonex.
Because the API source is daily, the progress bar says `Fetching days` instead
of `Fetching hours`. Active status lines show the actual source class:
`telonex local` for local mirrors, `telonex cache` for cached API-day payloads,
or `telonex api` for network downloads. API downloads show byte progress when
the response exposes a content length; every source reports scan rows, matched
book/quote rows, and a completed line for each date.

If a local Telonex blob partition contains a Parquet part that is not readable
yet, for example because a downloader is actively writing the mirror, the runner
rejects that local blob slice and tries the next source instead of silently
replaying partial L2 books. With the public source priority, that means API-day
cache first, then local, then `api:`.

When `TELONEX_CACHE_ROOT` is enabled, the `Telonex source:` line includes the
implicit cache layer before the configured `local:` and `api:` entries, for
example:

```text
Telonex source: explicit priority (cache -> local /Volumes/LaCie/telonex_data -> api https://api.telonex.io (key set))
```

## Timing Expectations By Source

| Source | Typical time | When it happens |
|---|---|---|
| Local cache | <0.05s | Second run onward for the same market/token/hour |
| Local raw PMXT archive | local disk bound | You mirrored raw PMXT hours locally and pointed `DATA.sources` at `local:/...`, or used `PMXT_RAW_ROOT` for a lower-level loader workflow |
| Remote raw PMXT archive | network and file-size bound | Hour is missing from local cache and local raw mirror, so the client downloads the upstream raw parquet to a temp file and filters it locally |
| Local Telonex daily Parquet | local disk bound | You warmed `/Volumes/LaCie/telonex_data` and listed `local:/Volumes/LaCie/telonex_data` before `api:` |
| Telonex API-day cache | local disk bound | The same Telonex API day was downloaded earlier and cached under `TELONEX_CACHE_ROOT` |
| Telonex API daily Parquet | network and file-size bound | The local daily file is missing or a not-yet-readable local blob partition was rejected, and the API-day cache is missing, so the runner falls back to `api:` with `TELONEX_API_KEY` in the environment |
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

Or run the Telonex joint-portfolio example directly:

```bash
uv run python backtests/polymarket_telonex_quote_tick_joint_portfolio_runner.py
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
