# Testing

## Standard Repo Gate

Run these before opening or merging a PR:

```bash
uv run ruff check --exclude nautilus_pm .
uv run ruff format --check --exclude nautilus_pm .
uv run pytest tests/ -q
```

You can also use the equivalent Make targets:

```bash
make check
make test
```

## Useful Smoke Checks

```bash
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_trade_tick_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

Quote-tick PMXT runners use the source path pinned in `DATA.sources` inside the
file. Public PMXT runners now pin `local:/Volumes/LaCie/pmxt_raws` first,
`archive:r2.pmxt.dev` second, and `relay:209-209-10-83.sslip.io` third. If that
local mirror path is absent, the loader falls through to archive and relay.
Those prefixes are the contract; do not use unprefixed hosts or ad hoc aliases.

Coverage is mixed by design:

- fast unit tests for strategy, loader, cache, and relay logic
- relay processor and API integration tests against temp dirs
- smoke tests that exercise real backtest flows
- generated chart output redirected to temp dirs during tests so the working
  tree stays clean

## Docs Validation

When you change docs, README navigation, or MkDocs config, also run:

```bash
uv run mkdocs build --strict
```
