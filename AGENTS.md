# Repository Notes For Agents

This repo is a professional prediction-market backtesting framework built on
NautilusTrader. The primary objective is **maximum backtest realism** — every
design choice must be weighed against whether it makes simulated results more
or less trustworthy. Do not paper over failures that could make a reader trust
misleading results. Play devil's advocate on realism gaps; chase the best
features from the NautilusTrader documentation.

## Scope And Worktree Safety

- Expect a dirty worktree. Never revert or overwrite existing changes you did
  not make unless the user explicitly asks.
- Keep changes tightly scoped to the request. Avoid unrelated refactors and
  formatting churn.
- NEVER push directly to `main`, `v4`, `v3`, or any default/base/release
  branch. Push only to a separate PR branch.

## README And Docs

- Do not touch other parts of the root README. Only touch the table of
  contents.
- All other documentation changes belong under `docs/`; do not bloat the root
  README body.
- Only change the README TOC when it needs to be in sync with docs.
- Do not trim the README TOC. All docs and subheaders should be properly
  recorded there.
- Do not add extra things to the root README.
- `docs/project-status.md` is the place for roadmap, known issues, and recently
  fixed history; include PR links there instead of in the root README body.
- When touching MkDocs config, theme CSS, code-fence behavior, or docs assets,
  verify with:

```bash
uv run mkdocs build --strict
```

## L2 Book Architecture

This framework is L2-native. All replay data is L2 order book deltas
(`OrderBookDeltas`), not L1 quotes or trade ticks.

- The replay spec is **`BookReplay`** — never `QuoteReplay` or `TradeReplay`.
- The data type string is **`"book"`** — never `"quote_tick"` or `"trade_tick"`.
- `min_book_events` — never `min_quotes` or `min_trades`.
- NautilusTrader **ignores `QuoteTick` for `L2_MBP`** backtesting. Our data is
  L2 order book data; naming it "quote tick" was incorrect and has been fixed.
- **Trade ticks are integrated into book replay for order matching**, per
  [NautilusTrader backtesting docs](https://nautilustrader.io/docs/latest/concepts/backtesting/#combining-l2-book-data-with-trade-ticks).
  Standalone trade-tick replay does not exist and must not be re-introduced.
- No backwards-compatibility aliases. Do not add `QuoteReplay = BookReplay` or
  similar shims. We move forward with correctness.

### Book Data Signals

The `cache.order_book(instrument_id)` API exposes these L2 signals that
strategies should use:

- `spread()`, `midpoint()`, `bids()`, `asks()`
- `get_avg_px_for_quantity()`, `get_quantity_for_price()`
- `simulate_fills()` — for pre-trade impact estimation
- **Imbalance**, **microprice** — derived from bid/ask depth asymmetry
- `OrderBookImbalance` — NautilusTrader ships a built-in indicator

New strategies should consume the full depth of the book, not just BBO.

### Per-Market HTML Removed

Individual per-market HTML report generation (`emit_html`) has been removed.
Summary/portfolio HTML with all panels (including per-market result lines in
tables and plots) is retained. Do not re-add per-market file generation.

## Backtest Realism Priorities

Treat these as high-value issues:

- broken direct runner entrypoints
- `make backtest` or menu behavior drifting from docs
- vendor archive and local-source correctness
- API handler event-loop blocking
- memory growth that can accumulate forever
- timestamp or datetime warnings in normal runs
- examples that use stale markets, fragile dates, or false performance claims
- optimizer math, scoring, or ranking behavior that misleads research decisions
- **PMXT missing-hour gaps** — must warn and reset book state (already fixed)
- **resolution metadata look-ahead** — `instrument.info` must not contain
  `result` keys accessible to strategy during simulation (already fixed)
- **zero latency defaults** — realistic CLOB round-trip latencies needed

Treat expected losing strategies separately from bugs. Negative PnL or
`AccountBalanceNegative` is not automatically a code defect.

Local PMXT filtered-cache growth is intentional. Do not "fix" it by default.

## Backtest Runner Conventions

- Direct script runners must work as imports and as direct scripts:

```bash
uv run python path/to/runner.py
```

- Use the shared `_script_helpers` bootstrap pattern for repo-root imports
  instead of one-off `sys.path` hacks.
- Timing/progress output should stay enabled by default in `main.py`.
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out.
- Nautilus output should stay enabled by default at the repo layer.
- The default repo-layer `nautilus_log_level` is `INFO`; do not downgrade it to
  `WARNING` or quieter unless the user explicitly asks.
- Treat changes that hide Nautilus runner output by default as regressions.
- Public runner files should carry their real inputs: `DATA`, `REPLAYS`,
  `STRATEGY_CONFIGS`, `EXECUTION`, `EXPERIMENT`, plot panels, summary report
  paths, and optimizer windows.

## Optimizers And Research Notebooks

- The parameter-search objective is per-window:

```text
score = pnl - 0.5 * max_drawdown_currency - penalties
```

- Rank training candidates by the median of per-window scores. If holdout
  windows are configured, rerun only the train top-k on holdout and rank by
  median holdout score, with train score as the tie-breaker.
- For joint-portfolio optimizers, compute drawdown on the summed equity curve,
  not by summing per-market drawdowns. This is intentional because
  diversification can reduce joint drawdown.
- TPE/continuous `parameter_space` behavior must be tested separately from
  discrete random-grid `parameter_grid` behavior.
- Notebook optimizer examples use bundled slugs only as examples. Users should
  plug in the markets and windows they actually want to study, then warm those
  exact slugs before scaling notebook runs.
- Before running notebook optimizers, run the same slugs and timestamps through
  a regular runner so PMXT source and cache-fill progress is visible. Otherwise
  a cold local-cache -> R2/archive fill can take a long time with little
  notebook output.
- For many-market research, prefer downloading PMXT raw dumps first:

```bash
make download-pmxt-raws DESTINATION=/path/to/pmxt_raws
```

Keeping raw hours on disk avoids refetching the same hourly raw files from R2
for each new market.

## PMXT Data Defaults

- PMXT filtered cache is enabled by default at:

```text
~/.cache/nautilus_trader/pmxt
```

- Public PMXT runners pin local raw first, then archive, usually:

```text
local:/Volumes/storage/pmxt_data
archive:r2v2.pmxt.dev
archive:r2.pmxt.dev
```

- Root setup docs should include `duckdb`.
- `docs/setup.md` and PMXT docs should describe cache default-on behavior and
  timing output default-on behavior, with `BACKTEST_ENABLE_TIMING=0` as the
  opt-out.

## Verification

Baseline local gates before opening or merging a PR:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/ -q
```

Useful representative smoke checks:

```bash
uv run python backtests/polymarket_book_ema_crossover.py
uv run python backtests/polymarket_book_independent_multi_replay_runner.py
```

If core internals, optimizer math, loader behavior, runner bootstrap, plotting,
reporting, or backtest behavior changed, run focused tests plus representative
runner smokes before submitting a PR. Include book Polymarket (PMXT and
Telonex), multi-runner, optimizer, and HTML/report paths when the touched code
can affect them.

When touching `main.py`, timing, PMXT loader behavior, or default backtest
selection/parameters, verify both:

```bash
uv run python path/to/affected_runner.py
make backtest
```

If the user says `test everything`, `end-to-end`, `all backtests`, or asks
whether "everything works", verify the current worktree. Do not claim "all
backtests passed" until every runnable entrypoint under `backtests/` has
returned 0 on the current tree.

If the worktree is dirty, explicitly separate:

- what was verified in the current worktree
- what is actually included in your change, PR, or commit

If the user reports a specific failing command, rerun that exact command first.
Do not substitute a nearby script and call it equivalent.

Clean up temporary sweep artifacts and long-running background verification
processes before finishing.

## Review And Issue Hunting

When asked to review or look for issues, prioritize:

1. vendor source correctness and survivability
2. backtest runner correctness
3. docs/setup drift
4. organizational consistency

Explicitly check:

- Does `make backtest` still behave as expected?
- Do direct runner paths still work?
- Are stale buckets, temp files, or background artifacts growing forever?
- Are normal runs still warning-free?

## PR Hygiene

- DO NOT MERGE TO `main`, `v4`, `v3`, or any other base branch unless the user
  gives an explicit merge command for that exact PR. Opening or updating a PR is
  not permission to merge it.
- NEVER push straight to a base branch. All repo changes must go through:
  branch -> draft PR -> review -> green CI -> explicit user merge command.
- Use branch -> draft PR -> review -> green CI -> explicit user merge command.
- Before pushing code-structure changes to a PR branch whose base is `main`,
  `v4`, `v3`, or any other version branch, run
  `uv run python scripts/generate_codebase_uml.py`.
  Include the refreshed root `CODEBASE_UML.md` in the PR whenever code structure
  changed, so agents and reviewers can use an up-to-date project map.
- Review the PR diff after opening it.
- Wait for GitHub Actions to pass.
- Wait for explicit user confirmation before merging to `main`.
