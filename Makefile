.PHONY: backtest lint format test setup

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

setup:
	git submodule update --init --recursive
	cd prediction-market-analysis && bash scripts/install-tools.sh
	cd prediction-market-analysis && bash scripts/download.sh

%:
	@:
