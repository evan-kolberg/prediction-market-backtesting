.PHONY: backtest lint format test setup build-rust

ANALYSIS = VIRTUAL_ENV= $(MAKE) -C prediction-market-analysis
RUN = uv run main.py

backtest:
	$(RUN) backtest $(filter-out $@,$(MAKECMDGOALS))

lint:
	uv run ruff check .
	uv run ruff format --check .

format:
	uv run ruff check --fix .
	uv run ruff format .

test:
	uv run pytest tests/ -v

build-rust:
	cd crates/backtesting_engine && maturin develop --release

setup:
	git submodule update --init --recursive
	$(ANALYSIS) setup

%:
	$(ANALYSIS) $@ $(filter-out $@,$(MAKECMDGOALS))
