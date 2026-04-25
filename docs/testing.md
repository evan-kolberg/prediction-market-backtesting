# Testing

## Standard Repo Gate

Run these before cutting a commit intended for the PR:

```bash
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/ -q
```

Equivalent Make targets:

```bash
make check
make test
```

## Useful Smoke Checks

Public Python runner smoke checks:

```bash
uv run python backtests/polymarket_book_ema_crossover.py
uv run python backtests/polymarket_book_ema_optimizer.py
uv run python backtests/polymarket_book_joint_portfolio_runner.py
uv run python backtests/polymarket_telonex_book_joint_portfolio_runner.py
```

All public Python runners:

```bash
uv run python scripts/run_all_backtests.py
```

Menu path:

```bash
make backtest
```

The runner smokes cover:

- PMXT L2 book replay from filtered cache, local raw mirror, and archive
  fallback.
- Telonex L2 full-book replay from local mirror, API-day cache, and API
  fallback when configured.
- Real trade-tick integration for execution.
- `BookType.L2_MBP`, `liquidity_consumption=True`, `trade_execution=True`, and
  optional `queue_position=True`.
- Summary report generation without per-market HTML files.

PMXT public runners pin `local:/Volumes/LaCie/pmxt_data` first, then
`archive:r2v2.pmxt.dev` and `archive:r2.pmxt.dev`. Telonex public runners pin
`local:/Volumes/LaCie/telonex_data`, then `api:`.

Coverage is mixed by design:

- Fast unit tests for strategies, loaders, cache, and source selection.
- Focused tests for replay adapter architecture and execution wiring.
- Smoke tests for direct backtest entrypoints.
- Docs build validation for Markdown, MkDocs config, and theme changes.

If you are validating runner realism changes, include focused tests plus at
least one PMXT runner and one Telonex runner. If the user asks whether
"everything works" or asks for end-to-end validation, run every public Python
entrypoint under `backtests/` and do not claim success until each returns 0.

## Docs Validation

When docs, README navigation, MkDocs config, theme CSS, code-fence behavior, or
docs assets change, run:

```bash
uv run mkdocs build --strict
```
