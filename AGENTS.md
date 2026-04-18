# Repository Notes For Agents

This repo is a prediction-market backtesting research project. Optimize for
honest simulation, durable runner behavior, and clear operational docs. Do not
paper over failures that could make a reader trust misleading results.

## Scope And Worktree Safety

- Expect a dirty worktree. Never revert or overwrite existing changes you did
  not make unless the user explicitly asks.
- Keep changes tightly scoped to the request. Avoid unrelated refactors and
  formatting churn.
- Never push directly to `main`.

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
- If relay behavior or env vars change, update both `pmxt_relay/README.md` and
  `pmxt_relay/systemd/pmxt-relay.env.example`.
- When touching MkDocs config, theme CSS, code-fence behavior, or docs assets,
  verify with:

```bash
uv run mkdocs build --strict
```

## Backtest Realism Priorities

Treat these as high-value issues:

- broken direct runner entrypoints
- `make backtest` or menu behavior drifting from docs
- public relay correctness, fairness, and survivability
- relay docs or env vars that do not match live reality
- API handler event-loop blocking
- memory growth that can accumulate forever
- timestamp or datetime warnings in normal runs
- examples that use stale markets, fragile dates, or false performance claims
- optimizer math, scoring, or ranking behavior that misleads research decisions

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
  a cold local-cache -> R2/archive -> relay fill can take a long time with
  little notebook output.
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

- Public PMXT runners pin local raw first, then archive, then relay, usually:

```text
local:/Volumes/LaCie/pmxt_raws
archive:r2v2.pmxt.dev
archive:r2.pmxt.dev
relay:209-209-10-83.sslip.io
```

- The shared public relay is a raw mirror service. Filtered relay behavior is
  legacy or self-hosted.
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
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_trade_tick_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_ema_crossover.py
uv run python backtests/polymarket_quote_tick_independent_multi_replay_runner.py
```

If core internals, optimizer math, loader behavior, runner bootstrap, plotting,
reporting, or backtest behavior changed, run focused tests plus representative
runner smokes before submitting a PR. Include trade-tick Kalshi, trade-tick
Polymarket, quote-tick Polymarket, multi-runner, optimizer, and HTML/report
paths when the touched code can affect them.

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

1. public relay correctness and survivability
2. backtest runner correctness
3. docs/setup drift
4. organizational consistency

Explicitly check:

- Does `make backtest` still behave as expected?
- Do direct runner paths still work?
- Do public relay endpoints return quickly and consistently?
- Do service names, env vars, and deploy paths in docs match reality?
- Can reverse proxies collapse client identity or headers?
- Are stale buckets, temp files, or background artifacts growing forever?
- Are normal runs still warning-free?

## Relay Facts

- Live checkout path: `/opt/prediction-market-backtesting`
- Relay env file: `/etc/pmxt-relay.env`
- Relay services:
  - `pmxt-relay-api.service`
  - `pmxt-relay-worker.service`
- Public relay URL: `https://209-209-10-83.sslip.io`
- Trusted proxy env var: `PMXT_RELAY_TRUSTED_PROXY_IPS`

If relay code changes are deployed, verify both service state and HTTP health:

```bash
systemctl is-active pmxt-relay-api.service pmxt-relay-worker.service
curl -fsS https://209-209-10-83.sslip.io/healthz
curl -fsS https://209-209-10-83.sslip.io/v1/stats
curl -fsS https://209-209-10-83.sslip.io/v1/system
```

If a PR changes live relay behavior in `pmxt_relay/`, do not stop at local
tests if deploy access is available. Deploy and verify the real box.

When touching the VPS over SSH:

- prefer one persistent PTY SSH session for the deploy/observe cycle
- avoid many short-lived SSH sessions; fail2ban is enabled
- run `systemctl`, `curl`, and observation loops inside that one PTY unless
  there is a strong reason not to

Typical deploy steps:

```bash
rsync updated relay files to /opt/prediction-market-backtesting
update /etc/pmxt-relay.env if env semantics changed
systemctl restart pmxt-relay-api.service pmxt-relay-worker.service
systemctl is-active pmxt-relay-api.service pmxt-relay-worker.service
```

After deploy, observe for a few minutes instead of doing a single spot check.
Watch for repeated endpoint timeouts, services drifting from `active`, rising
error counts, CPU staying pinned while API responsiveness degrades, or memory
climbing without settling.

Relay `/v1/system` CPU is based on 1-minute load average divided by CPU count,
capped at 100. A high percentage can reflect worker load or I/O wait; confirm
with `uptime`, `/proc/loadavg`, `vmstat`, and top processes before concluding
the box is unhealthy.

## PR Hygiene

- Use branch -> draft PR -> review -> green CI -> explicit user merge command.
- Review the PR diff after opening it.
- Wait for GitHub Actions to pass.
- Wait for explicit user confirmation before merging to `main`.
