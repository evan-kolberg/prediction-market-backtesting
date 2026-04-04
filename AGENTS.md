# Repository Notes For Agents

## Keep Scope Tight

- Treat subtree updates in `nautilus_pm/` as separate work. Do not mix an upstream subtree pull with normal bugfix/docs PRs.
- Keep repo-specific fixes in this repo layer unless there is a clear reason to update the vendored subtree.

## Local Verification

Run these before opening or merging a PR:

```bash
uv run ruff check --exclude nautilus_pm .
uv run ruff format --check --exclude nautilus_pm .
uv run pytest tests/ -q
```

Useful smoke checks:

```bash
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_trade_tick_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

## What Matters Most

Optimize for real bugs, regressions, and stale operational assumptions, not
cosmetic churn.

Backtest realism is the highest priority in this repo. The point of the
project is to simulate honestly, surface failure modes clearly, and avoid
results that flatter toy assumptions or mislead users.

Work with a high level of scrutiny. If a result, metric, health signal, or
example could cause a careful reader to believe something false about the
system, treat that as a bug until it is disproven.

High-value things to catch:

- broken direct runner entrypoints
- stale README/setup/deploy docs
- relay behavior that differs from what the docs claim
- multi-user/public relay fairness or stability issues
- event-loop blocking in API handlers
- memory growth that can accumulate forever
- timestamp / datetime warnings that pollute normal runs
- anything that would make public backtests slower, less reliable, or misleading

Lower-value things:

- minor wording nits with no operational consequence
- “strategy lost money” outcomes that are just normal backtest behavior
- unbounded PMXT local cache growth, which is intentional here

## Review / Issue Hunting

When asked to “look for issues,” prioritize:

1. public relay correctness and survivability
2. backtest runner correctness
3. docs/setup drift
4. organizational consistency

Things to explicitly check:

- Does `make backtest` still behave the way the repo expects?
- Do direct `uv run python path/to/script.py` runner paths still work?
- Do public relay endpoints return quickly and consistently?
- Do service names, env vars, and deploy paths in docs still match reality?
- Are there places where reverse proxies collapse client identity or headers?
- Are there stale buckets, temp files, or background artifacts that can grow forever?
- Are “warning-free normal runs” still true after the change?

When reviewing behavior, separate:

- real runtime/infrastructure bugs
- expected strategy outcomes such as negative PnL or `AccountBalanceNegative`

The latter is not automatically a code bug.

## End-To-End Claims

- If the user says `test everything`, `end-to-end`, `all backtests`, or asks whether `everything works`, verify the current worktree, not just `HEAD` or the PR diff.
- Do not claim `all backtests passed` until every runnable entrypoint under `backtests/` has returned `0` on the current tree.
- If the worktree is dirty, explicitly separate:
  - current-worktree verification
  - what is actually included in the PR/commit
- If the user reports a specific failing command, rerun that exact command first. Do not substitute a nearby script and call it equivalent.
- When touching PMXT loader behavior, runner bootstrap, `main.py`, or default backtest selection/parameters, verify both:
  - the direct script path for the affected runner
  - the interactive/menu path, e.g. `make backtest`
- Do not merge a backtest-affecting PR after only partial smokes if the user asked for full end-to-end validation.
- Clean up temporary sweep artifacts and long-running background verification processes before finishing.

## Backtest Runner Conventions

- Timing/progress output should stay enabled by default in `main.py`.
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out.
- Direct script runners must work both as package imports and as direct script execution via `uv run python path/to/script.py`.
- Use the shared `_script_helpers` bootstrap pattern for repo-root imports instead of one-off `sys.path` hacks.

## PMXT Cache

- Local PMXT filtered parquet cache is enabled by default.
- Treat unbounded local PMXT cache growth as intentional, not a bug to fix by default.

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

## Deploy Expectations

If a PR changes live relay behavior in `pmxt_relay/`, do not stop at local
tests if deploy access is available. Deploy and verify the real box.

When touching the VPS over SSH:

- prefer one persistent PTY SSH session for the whole deploy/observe cycle
- avoid opening many short-lived SSH sessions in a row; fail2ban is enabled on
  the box and repeated reconnect churn is avoidable
- do long-running `systemctl`, `curl`, and observation loops inside that one
  PTY unless there is a strong reason not to

Typical deploy steps:

```bash
rsync updated relay files to /opt/prediction-market-backtesting
update /etc/pmxt-relay.env if env semantics changed
systemctl restart pmxt-relay-api.service pmxt-relay-worker.service
systemctl is-active pmxt-relay-api.service pmxt-relay-worker.service
```

If restart is messy:

- verify the final state again after systemd settles
- do not assume “restart command returned” means the workers are healthy

After deploy, observe the relay for a few minutes instead of doing a single
spot check.

## Post-Deploy Observation

Sample for a few minutes:

- `/healthz`
- `/v1/stats`
- `/v1/system`
- `systemctl is-active ...`

What to watch for:

- repeated endpoint timeouts, not just one transient blip after restart
- services drifting from `active` to `inactive` / `deactivating`
- rising error counts in `/v1/stats`
- CPU staying pinned with API responsiveness degrading
- memory climbing without settling

One transient timeout immediately after restart is less important than a repeat.

## Relay Metrics Note

- `/v1/system` CPU is based on `1-minute loadavg / cpu_count`, capped at `100`.
- A high CPU percentage can reflect worker or ClickHouse pressure or I/O wait, not necessarily an API failure.
- Confirm with `uptime`, `/proc/loadavg`, `vmstat`, and top processes before concluding the box is unhealthy.

## Docs Invariants

- Root setup docs should include `duckdb`.
- Root README should describe PMXT cache as enabled by default.
- Root README and PMXT docs should describe timing output as default-on, with `BACKTEST_ENABLE_TIMING=0` as the opt-out.
- If relay behavior or env vars change, update both `pmxt_relay/README.md` and `pmxt_relay/systemd/pmxt-relay.env.example`.

Examples in README should be durable:

- prefer placeholders or bundled sample windows over fragile date-specific examples
- if an example is shown as a direct script path, confirm that exact invocation works

## PR Hygiene

- Never push directly to `main`.
- Every commit intended for `main` must go through a pull request.
- The required path is: branch -> draft PR -> review -> merge.
- If the work belongs in the roadmap/known-issues history, add the relevant PR link in `README.md`.
- Review the PR diff after opening it, wait for GitHub Actions to pass, then merge.

## Testing Standard

Do not stop at unit tests if the change affects user-facing behavior.

Good default test mix for this repo:

- repo lint gate
- full pytest suite
- at least one direct-script backtest smoke
- at least one menu/default PMXT smoke when touching `main.py`, timing, or PMXT loader behavior
- live relay verification when touching deployed relay code
- full runnable `backtests/` sweep when the user explicitly asks for all backtests or when answering whether everything works end-to-end

If the user asks whether “everything works,” the answer should be based on:

- local tests
- representative smoke runs
- deploy verification if the live relay was touched

not just static code inspection.
