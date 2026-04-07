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

If you want to build the docs locally, also install the MkDocs theme used by
this repo:

```bash
uv pip install mkdocs-shadcn
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

![Unified backtest runner menu](assets/backtests-menu-textual.png)

What that view is telling you:

- the left pane only shows flat runner entrypoints under `backtests/` and
  `backtests/private/`
- the right pane shows the exact file path, runner metadata, and current file
  contents for the highlighted entry
- shortcuts are assigned per visible runner, so filtering changes the set of
  hotkeys you can use immediately

Direct entrypoint:

```bash
uv run python main.py
```

Direct runner files also work:

```bash
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
uv run python backtests/polymarket_trade_tick_sports_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_pmxt_multi_sim_runner.py
```

Those direct runs write HTML artifacts into the repo-local `output/` directory
when the runner keeps `CHART_OUTPUT_PATH="output"`.

Public runner files carry their market, source, and execution assumptions in
code. PMXT quote-tick runners also pin absolute sample windows; native
trade-tick runners use rolling lookbacks unless you set `end_time` in
`REPLAYS` or `default_end_time` in the experiment. To use a different local PMXT mirror
path or a different market, edit the runner file directly or copy it into
`backtests/private/`. If you already have mirrored PMXT raw hours locally, add
`local:/path/to/raw-hours` to the runner's `DATA.sources`.

Repo-layer source syntax is explicit on purpose:

- Kalshi native trade-tick runners use `rest:...`
- Polymarket native trade-tick runners use `gamma:...`, `trades:...`, and `clob:...`
- PMXT quote-tick runners use `local:...`, `archive:...`, and `relay:...`

To mirror PMXT raw archive hours locally, run:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

The download is long-running and prints per-hour completion lines plus the
currently active transfer. Example output:

```text
PMXT raw source: explicit priority (archive https://r2.pmxt.dev -> relay https://209-209-10-83.sslip.io)
Downloading PMXT raw hours to /path/to/pmxt_raws (requested_hours=3, window_start=2026-02-27T11, window_end=2026-02-27T13)...
  2026-02-27T11  12.431s   445.9 MiB  archive
  2026-02-27T12   0.000s    existing  skip
Downloading raw hours (2/3 done, 1 active):  67%|████████████████████████████████████████████████████████████▏                              | [00:41<00:20]active: relay 2026-02-27T13 392.0/445.9 MiB 14.8s
```

The counts, hour labels, source label, and byte totals vary with the current
archive and the window you are mirroring.

If you want to see the full loader and reporting flow in one place, the PMXT
multi-sim output below is representative of the current repo-layer behavior:
Nautilus logs stay visible, the summary table is printed in-terminal, and the
per-sim detail HTML paths plus the aggregate multi-market summary HTML path are
printed after the run.

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
