# Testing

## Standard Repo Gate

Run these before cutting a commit you want to keep on the next-version branch:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/ -q
```

You can also use the equivalent Make targets:

```bash
make check
make test
```

## Useful Smoke Checks

```bash
uv run python backtests/kalshi_trade_tick_ema_crossover.py
uv run python backtests/polymarket_trade_tick_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
uv run python backtests/polymarket_quote_tick_pmxt_multi_sim_runner.py
```

Those cover the main user-facing paths in the current tree: one pinned Kalshi
trade-tick runner, one native Polymarket trade-tick runner, one single-market
PMXT quote-tick runner, and one PMXT multi-sim runner.

Quote-tick PMXT runners use the source path pinned in `DATA.sources` inside the
file. Public PMXT runners now pin `local:/Volumes/LaCie/pmxt_raws` first,
`archive:r2.pmxt.dev` second, and `relay:209-209-10-83.sslip.io` third. If that
local mirror path is absent, the loader falls through to archive and relay.
Those prefixes are the contract; do not use unprefixed hosts or ad hoc aliases.

Coverage is mixed by design:

- fast unit tests for strategy, loader, cache, and relay logic
- relay processor and API integration tests against temp dirs
- smoke tests that exercise real backtest flows
- generated chart output either redirected to temp dirs or explicitly cleaned up
  so the working tree stays clean

If you are specifically validating HTML/report behavior, include at least:

- one single-market runner that should emit one `*_legacy.html`
- one fixed-basket multi-market runner that should emit per-market
  `*_legacy.html` files plus one `*_multi_market.html`
- the PMXT multi-sim runner, which should emit per-sim `*_legacy.html` files
  plus one `*_multi_market.html`

## Docs Validation

When you change docs, README navigation, or MkDocs config, also run:

```bash
uv run mkdocs build --strict
```
