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
uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client duckdb
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

Direct entrypoint:

```bash
uv run python main.py
```

Direct runner files also work:

```bash
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

Public runner files now carry their own pinned market, window, and source
values. To use a different local PMXT mirror path or a different market, edit
the runner file directly or copy it into `backtests/private/`.

## Timing And Cache Defaults

- timing output is on by default in `make backtest`, `uv run python main.py`,
  and direct script runners that opt into `@timing_harness`
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out
- PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`
- normal Nautilus logs are still printed; the timing harness is additive

## Updating The Vendored Subtree

```bash
make update
```

Unlike submodules, the subtree is copied into this repo. There is no live link
to upstream.
