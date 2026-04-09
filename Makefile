.PHONY: backtest install update test check clear-pmxt-cache download-pmxt-raws

PMXT_CACHE_ROOT ?= $(if $(XDG_CACHE_HOME),$(XDG_CACHE_HOME),$(HOME)/.cache)/nautilus_trader/pmxt
DESTINATION ?=
PMXT_RAW_DOWNLOAD_FLAGS ?=

backtest:
	uv run python main.py

install:
	unset CONDA_PREFIX && uv venv --python 3.13 && uv pip install "nautilus_trader[polymarket,visualization]==1.225.0" bokeh plotly numpy py-clob-client duckdb textual

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest tests/ -q

test: check

clear-pmxt-cache:
	rm -rf "$(PMXT_CACHE_ROOT)"
	mkdir -p "$(PMXT_CACHE_ROOT)"
	du -sh "$(PMXT_CACHE_ROOT)"

download-pmxt-raws:
	@if [ -z "$(DESTINATION)" ]; then echo "Set DESTINATION=/path"; exit 2; fi
	uv run python scripts/pmxt_download_raws.py \
		--destination "$(DESTINATION)" \
		$(PMXT_RAW_DOWNLOAD_FLAGS)

update:
	@echo "No vendored Nautilus subtree remains in this branch."
	@echo "Bump the upstream nautilus_trader version and port prediction_market_extensions/ as needed."
