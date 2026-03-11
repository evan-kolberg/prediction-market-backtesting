.PHONY: backtest install update test

backtest:
	uv run python main.py

install:
	unset CONDA_PREFIX && uv venv --python 3.13 && uv pip install -e nautilus_pm/ bokeh plotly numpy py-clob-client

test:
	uv run pytest tests/ -v

update:
	git subtree pull --prefix=nautilus_pm https://github.com/ben-gramling/nautilus_pm.git charting --squash
