# Contributing

Hey there, stranger. Thanks for thinking of contributing!

## Code Style 😎

Make sure the CI workflow will pass prior to making a PR. 

```bash
cargo check
cargo test
uv sync
uv run maturin develop --release --manifest-path crates/backtesting_engine/Cargo.toml
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/ -v
```


Configuration is in `pyproject.toml` — 120 character line length, Python 3.9 target.

## Running Tests

```bash
make test
```

Tests live in `tests/` and run with pytest.

## Types of Contributions

- Anything on the roadmap
- Any new or currently known issues
- Anything that holistically improves the framework 

## Pull Requests

1. Fork the repository and create a branch with a descriptive name
2. Make your changes with clear, focused commits
3. Ensure `make lint` and `make test` pass
4. Open a PR with a description of what changed


## Issues

Bug reports and feature requests are welcome via [GitHub Issues](https://github.com/evan-kolberg/prediction-market-backtesting/issues). Include enough detail to reproduce the problem or understand the request.
