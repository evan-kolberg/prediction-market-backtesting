# Research

This page describes the optimizer methodology used by the research notebooks in
`backtests/`. Treat the notebook and optimizer flow as a research scaffold, not
as proof that a strategy is robust.

## Overview

The repo treats strategy hyperparameter search as a walk-forward experiment:

1. Define one or more training windows.
2. Evaluate candidate parameterizations on every training window.
3. Score each window with the objective below.
4. Rank candidates by median train score.
5. If holdout windows are configured, rerun only the train top-k on holdout and
   rank by median holdout score, using train score as the tie-breaker.

Each trial runs in an isolated subprocess so a crashing strategy cannot poison
the driver process. Artifacts such as leaderboard CSV, summary JSON, and summary
HTML land under `output/`.

## Warm PMXT Cache Before Notebook Runs

Run the notebook's market slugs and timestamps once through a regular runner
before launching a long notebook sweep. Regular runners print PMXT source and
download progress while the local filtered cache warms. Notebook optimizers
mostly surface trial-level output, so a cold archive fill can look quiet for a
long time.

Cache warming is about market, token, and hour coverage; the warmup strategy
does not need to match the notebook strategy.

If you plan to run many-market research, download PMXT raw dumps first:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

Then point the runner at that mirror:

```python
sources=(
    "local:/path/to/pmxt_raws",
    "archive:r2v2.pmxt.dev",
    "archive:r2.pmxt.dev",
)
```

For the bundled TPE example, warm the current PMXT basket first:

```bash
uv run python backtests/polymarket_book_joint_portfolio_runner.py
```

For Telonex notebook research, warm the local full-book mirror first:

```bash
TELONEX_API_KEY=... uv run python scripts/telonex_download_data.py \
  --destination /Volumes/LaCie/telonex_data \
  --all-markets \
  --channels book_snapshot_full
```

Use the exact slugs and windows you plan to study before scaling notebook runs.

## Scoring

Per-window score:

```text
score = pnl - 0.5 * max_drawdown_currency - penalties
```

Penalties are applied per window for early termination, insufficient coverage,
and trials that fail to meet `min_fills_per_window`. The leaderboard ranks by
the median of per-window scores across training windows. If holdout windows are
configured, holdout median is the primary rank and train median is the
tie-breaker.

## Joint-Portfolio Mode

When `ParameterSearchConfig.base_replays` contains more than one market, every
trial evaluates the same parameter set across all replays simultaneously.

Portfolio scoring behavior:

- PnL is summed across markets.
- Fill counts are summed across markets.
- Drawdown is computed on the summed equity curve.
- Requested-coverage ratio is averaged across markets.

Drawdown is intentionally not computed by summing per-market drawdowns.
Diversification can reduce joint drawdown, and the optimizer should reflect
that.

## Samplers

### Random Grid (`sampler="random"`)

Draws up to `max_trials` unique combinations uniformly at random from the
Cartesian product of `parameter_grid`. This is memoryless, unbiased, and cannot
express continuous or log-scale ranges.

Notebook: `backtests/generic_optimizer_research.ipynb`.

### TPE (`sampler="tpe"`)

Uses Optuna's Tree-structured Parzen Estimator. TPE accepts `parameter_space`
specs:

- `{"type": "int", "low": int, "high": int, "log": bool}`
- `{"type": "float", "low": float, "high": float, "log": bool}`
- `{"type": "categorical", "choices": [...]}`

Log-uniform sampling is useful for parameters spanning orders of magnitude.
TPE benefits from seeing prior results before proposing the next trial, so this
repo runs it sequentially.

Notebook: `backtests/generic_tpe_research.ipynb`.

## Caveats

- Overfitting is the default outcome of parameter search. Inspect holdout
  score, not just train score.
- Strategy results are only as good as source coverage, latency assumptions,
  fee modeling, and execution realism.
- Current public Polymarket runners use L2 MBP data, not L3 MBO data. Queue
  position is a heuristic based on visible depth and trade ticks.
- Reproducibility is seeded via `random_seed`, but engine and subprocess
  nondeterminism can still introduce small score jitter.
- Continuous parameter ranges are not automatically better. Use them only when
  the strategy response is smooth in that dimension.

## Notebook Output Persistence

Notebook runners execute through the menu and `nbclient`. Generated outputs are
written back into the notebook only for the run artifacts needed to inspect the
latest result. Large HTML outputs may be linked instead of inlined to keep the
notebook usable.
