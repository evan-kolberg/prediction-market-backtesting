.PHONY: backtest lint format test setup analyze

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

setup:
	git submodule update --init --recursive
	$(ANALYSIS) setup

analyze:
	$(ANALYSIS) analyze

analysis-%:
	$(ANALYSIS) $* $(filter-out $@,$(MAKECMDGOALS))

%:
	@:
