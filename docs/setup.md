# Setup

## Prerequisites

- Python 3.12+ (`3.13` recommended)
- [Rust toolchain](https://rustup.rs/) `>= 1.93.1`
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Install

```bash
git clone https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting

# conda's linker flags conflict with the rust build
unset CONDA_PREFIX

uv venv --python 3.13
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb textual
```

You can also use:

```bash
make install
```

After setup, run commands with `uv run ...`. No manual
`source .venv/bin/activate` step is required.

## First Run

Interactive backtest menu:

```bash
make backtest
```

The interactive menu uses `Textual` with a left-side runner list, a right-side
details/preview pane, single-letter shortcuts, and search via `/`. Arrow keys
move the selection, `Enter` runs the highlighted runner, and `Esc` clears the
current filter and returns focus to the list. The preview pane now shows the
full runner file contents rather than an excerpt.

Direct entrypoint:

```bash
uv run python main.py
```

Direct runner files also work:

```bash
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

Public runner files carry their market, source, and execution assumptions in
code. PMXT quote-tick runners also pin absolute sample windows; native
trade-tick runners use rolling lookbacks unless you set `end_time` in `SIMS`
or `default_end_time` on the backtest. To use a different local PMXT mirror
path or a different market, edit the runner file directly or copy it into
`backtests/private/`. If you already have mirrored PMXT raw hours locally, add
`local:/path/to/raw-hours` to the runner's `DATA.sources`.

To mirror PMXT raw archive hours locally, run:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The download is long-running and prints a live progress bar. Example output:

```text
Downloading PMXT raws:  13%|███████████████████████▍| 137/1017 [41:27<3:37:59, 14.86s/hour, archive 2026-02-27T11:00:00+00:00 392.0/445.9 MiB]
```

The counts, hour timestamp, source label, and byte totals vary with the
current archive and the window you are mirroring.

## Timing And Cache Defaults

- timing output is on by default in `make backtest`, `uv run python main.py`,
  and direct script runners that opt into `@timing_harness`
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out
- PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`
- public PMXT runners pin `local:/Volumes/LaCie/pmxt_raws` first,
  `archive:r2.pmxt.dev` second, and `relay:209-209-10-83.sslip.io` third
- PMXT `DATA.sources` entries are explicit and prefix-driven: `local:`,
  `archive:`, `relay:`
- normal Nautilus logs are still printed; the timing harness is additive

## Updating The Vendored Subtree

```bash
make update
```

Unlike submodules, the subtree is copied into this repo. There is no live link
to upstream.
