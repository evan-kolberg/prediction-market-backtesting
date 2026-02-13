# Contributing

Thanks for your interest in contributing to prediction-market-backtesting! This guide covers the basics for getting started.

## Development Setup

```bash
git clone --recurse-submodules https://github.com/evan-kolberg/prediction-market-backtesting.git
cd prediction-market-backtesting
make setup
uv sync --dev
```

## Code Style

This project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting. Check and auto-fix with:

```bash
make lint     # check
make format   # auto-fix
```

Configuration is in `pyproject.toml` — 120 character line length, Python 3.9 target.

## Running Tests

```bash
make test
```

Tests live in `tests/` and run with pytest.

## Types of Contributions

### New Strategies

The easiest way to contribute is by adding a new strategy:

1. Create a new file in `src/backtesting/examples/`
2. Subclass `Strategy` and implement `on_trade()`
3. Strategies are auto-discovered — no registration needed
4. Run `make backtest` to verify it works
5. Open a PR with your strategy and a brief description of the logic

### Engine Improvements

Improvements to the core engine (`broker.py`, `engine.py`, `portfolio.py`, `metrics.py`) are welcome. Please include tests for any new functionality.

### New Data Feeds

To add a new platform, subclass `BaseFeed` in `src/backtesting/feeds/` and implement `markets()`, `trades()`, and `trade_count()`.

### Bug Fixes

If you find a bug, please open an issue first. If you have a fix, include a test that reproduces the issue.

## Pull Requests

1. Fork the repository and create a branch with a descriptive name
2. Make your changes with clear, focused commits
3. Ensure `make lint` and `make test` pass
4. Open a PR with a description of what changed and why

### Stale PRs

Pull requests with no activity for 30 days may be closed. Keep yours active by responding to review feedback.

## Issues

Bug reports and feature requests are welcome via [GitHub Issues](https://github.com/evan-kolberg/prediction-market-backtesting/issues). Include enough detail to reproduce the problem or understand the request.
