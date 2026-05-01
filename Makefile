.PHONY: backtest install update test check clear-pmxt-cache clear-telonex-cache clear-polymarket-cache download-pmxt-raws download-telonex-data

PMXT_CACHE_ROOT ?= $(if $(XDG_CACHE_HOME),$(XDG_CACHE_HOME),$(HOME)/.cache)/nautilus_trader/pmxt
PMXT_LOCAL_DATA_ROOT ?= /Volumes/LaCie/pmxt_raws
TELONEX_CACHE_ROOT ?= $(if $(XDG_CACHE_HOME),$(XDG_CACHE_HOME),$(HOME)/.cache)/nautilus_trader/telonex
POLYMARKET_CACHE_ROOT ?= $(if $(XDG_CACHE_HOME),$(XDG_CACHE_HOME),$(HOME)/.cache)/nautilus_trader/polymarket_trades
DESTINATION ?=
PMXT_RAW_DOWNLOAD_FLAGS ?=
TELONEX_DATA_DESTINATION ?= /Volumes/LaCie/telonex_data
TELONEX_DOWNLOAD_FLAGS ?=

backtest:
	uv run python main.py

install:
	unset CONDA_PREFIX && uv venv --python 3.13 && uv pip install "nautilus_trader[polymarket,visualization]==1.225.0" bokeh plotly numpy py-clob-client duckdb textual nbformat nbclient ipykernel optuna python-dotenv aiohttp

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest tests/ -q

test: check

clear-pmxt-cache:
	@python3 scripts/_cache_clear_guard.py --name PMXT_CACHE_ROOT --target "$(PMXT_CACHE_ROOT)" --unsafe "$(PMXT_LOCAL_DATA_ROOT)" --unsafe "$(TELONEX_DATA_DESTINATION)" --unsafe "$(DESTINATION)"
	rm -rf "$(PMXT_CACHE_ROOT)"
	mkdir -p "$(PMXT_CACHE_ROOT)"
	du -sh "$(PMXT_CACHE_ROOT)"

clear-telonex-cache:
	@python3 scripts/_cache_clear_guard.py --name TELONEX_CACHE_ROOT --target "$(TELONEX_CACHE_ROOT)" --unsafe "$(TELONEX_DATA_DESTINATION)" --unsafe "$(PMXT_LOCAL_DATA_ROOT)" --unsafe "$(DESTINATION)"
	rm -rf "$(TELONEX_CACHE_ROOT)"
	mkdir -p "$(TELONEX_CACHE_ROOT)"
	du -sh "$(TELONEX_CACHE_ROOT)"

clear-polymarket-cache:
	@python3 scripts/_cache_clear_guard.py --name POLYMARKET_CACHE_ROOT --target "$(POLYMARKET_CACHE_ROOT)" --unsafe "$(TELONEX_DATA_DESTINATION)" --unsafe "$(PMXT_LOCAL_DATA_ROOT)" --unsafe "$(DESTINATION)"
	rm -rf "$(POLYMARKET_CACHE_ROOT)"
	mkdir -p "$(POLYMARKET_CACHE_ROOT)"
	du -sh "$(POLYMARKET_CACHE_ROOT)"

download-pmxt-raws:
	@if [ -z "$(DESTINATION)" ]; then echo "Set DESTINATION=/path"; exit 2; fi
	uv run python scripts/pmxt_download_raws.py \
		--destination "$(DESTINATION)" \
		$(PMXT_RAW_DOWNLOAD_FLAGS)

download-telonex-data:
	uv run python scripts/telonex_download_data.py \
		--destination "$(TELONEX_DATA_DESTINATION)" \
		$(TELONEX_DOWNLOAD_FLAGS)

update:
	@echo "No vendored Nautilus subtree remains in this branch."
	@echo "Bump the upstream nautilus_trader version and port prediction_market_extensions/ as needed."
