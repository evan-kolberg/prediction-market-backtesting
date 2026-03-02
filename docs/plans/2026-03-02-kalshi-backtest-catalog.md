# Kalshi Catalog-Based Backtest Example Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create `examples/backtest/kalshi_ema_bars.py` — a two-phase script that fetches Kalshi hourly bars into a `ParquetDataCatalog` and runs an EMA-cross backtest via `BacktestNode`/`BacktestRunConfig`.

**Architecture:** Phase 1 (async) fetches hourly bars from the Kalshi REST API using `KalshiDataLoader` and writes the `BinaryOption` instrument + `Bar` objects to a local parquet catalog. Phase 2 (sync) configures `BacktestVenueConfig` + `BacktestDataConfig` pointing at that catalog and runs the backtest through `BacktestNode`. The two phases are independent: re-running only Phase 2 is fast since data is already on disk.

**Tech Stack:** `KalshiDataLoader` (loaders.py), `ParquetDataCatalog`, `BacktestNode`, `BacktestRunConfig`, `BacktestVenueConfig`, `BacktestDataConfig`, `BacktestEngineConfig`, `ImportableStrategyConfig`, `EMACrossLongOnly`.

---

### Task 1: Scaffold the script with imports, constants, and empty stubs

**Files:**
- Create: `examples/backtest/kalshi_ema_bars.py`

**Context:**

The bar type that Kalshi's `Hours1` interval produces is `1-HOUR-LAST-EXTERNAL` (uses `AggregationSource.EXTERNAL`).
`BacktestDataConfig.bar_spec = "1-HOUR-LAST"` tells the catalog engine to look for bars tagged `{instrument_id}-1-HOUR-LAST-EXTERNAL`.
`dispose_on_completion=False` is **required** on `BacktestRunConfig` so we can call `node.get_engine(run_config.id)` for reports after running.

**Step 1: Create the file**

```python
#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

import asyncio
from decimal import Decimal

import pandas as pd

from nautilus_trader.adapters.kalshi.loaders import KalshiDataLoader
from nautilus_trader.backtest.config import BacktestDataConfig
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.config import BacktestRunConfig
from nautilus_trader.backtest.config import BacktestVenueConfig
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import ImportableStrategyConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog


# ---------------------------------------------------------------------------
# Configure these constants for your backtest
# ---------------------------------------------------------------------------
MARKET_TICKER = "KXBTCD24"        # Kalshi market ticker
BAR_INTERVAL = "Hours1"           # Minutes1 | Hours1 | Days1
CATALOG_PATH = "./kalshi_catalog"  # Local directory for parquet catalog
START = "2024-01-01"               # ISO 8601 UTC date string
END = "2024-12-31"                 # ISO 8601 UTC date string
FAST_EMA = 10
SLOW_EMA = 20
TRADE_SIZE = Decimal("1")          # Number of contracts per trade


async def fetch_and_catalog() -> None:
    """Phase 1 – fetch bars from Kalshi API and write to local catalog."""
    pass  # TODO


def run_backtest() -> None:
    """Phase 2 – run EMA-cross backtest against the catalog data."""
    pass  # TODO


if __name__ == "__main__":
    asyncio.run(fetch_and_catalog())
    run_backtest()
```

**Step 2: Verify syntax compiles**

```bash
python -m py_compile examples/backtest/kalshi_ema_bars.py && echo "OK"
```

Expected output: `OK`

**Step 3: Commit**

```bash
git add examples/backtest/kalshi_ema_bars.py
git commit -m "feat(kalshi): scaffold catalog-based backtest example"
```

---

### Task 2: Implement `fetch_and_catalog()`

**Files:**
- Modify: `examples/backtest/kalshi_ema_bars.py`

**Context:**

`KalshiDataLoader.from_market_ticker(ticker)` is an async classmethod that fetches the market dict from `GET /markets/{ticker}` and returns a ready-to-use loader. The `instrument` attribute is the `BinaryOption`.

`loader.load_bars(start, end, interval)` accepts `pd.Timestamp` (with UTC tz) or `None`.

`ParquetDataCatalog(path)` creates the directory if it doesn't exist. `write_data([instrument])` writes the instrument definition. `write_data(bars)` writes bar data partitioned by bar type.

**Step 1: Replace the `pass` in `fetch_and_catalog()` with the implementation**

```python
async def fetch_and_catalog() -> None:
    """Phase 1 – fetch bars from Kalshi API and write to local catalog."""
    print(f"Fetching {BAR_INTERVAL} bars for {MARKET_TICKER} from {START} to {END}...")
    loader = await KalshiDataLoader.from_market_ticker(MARKET_TICKER)

    bars = await loader.load_bars(
        start=pd.Timestamp(START, tz="UTC"),
        end=pd.Timestamp(END, tz="UTC"),
        interval=BAR_INTERVAL,
    )

    catalog = ParquetDataCatalog(CATALOG_PATH)
    catalog.write_data([loader.instrument])
    catalog.write_data(bars)

    print(f"Wrote instrument {loader.instrument.id} and {len(bars)} bars to {CATALOG_PATH}")
```

**Step 2: Verify syntax still compiles**

```bash
python -m py_compile examples/backtest/kalshi_ema_bars.py && echo "OK"
```

Expected: `OK`

**Step 3: Commit**

```bash
git add examples/backtest/kalshi_ema_bars.py
git commit -m "feat(kalshi): implement fetch_and_catalog phase"
```

---

### Task 3: Implement `run_backtest()`, wire up `__main__`, and validate

**Files:**
- Modify: `examples/backtest/kalshi_ema_bars.py`

**Context:**

`BacktestDataConfig`:
- `data_cls` must be the string `"nautilus_trader.model.data:Bar"` (the config is frozen and resolves the class via `resolve_path` internally)
- `instrument_id` can be a plain string like `"KXBTCD24.KALSHI"`
- `bar_spec="1-HOUR-LAST"` → the catalog query looks for bars tagged `KXBTCD24.KALSHI-1-HOUR-LAST-EXTERNAL`

`BacktestRunConfig.dispose_on_completion=False` is **required** — otherwise the engine is torn down before we can retrieve it for reports.

`node.get_engine(run_config.id)` returns the `BacktestEngine` after the run. Call `.trader.generate_*_report()` on it.

`ImportableStrategyConfig.config` values are serialised as strings/scalars. Pass `trade_size` as `str(TRADE_SIZE)` and `bar_type` as the full string `"{MARKET_TICKER}.KALSHI-1-HOUR-LAST-EXTERNAL"`.

**Step 1: Replace the `pass` in `run_backtest()` with the implementation**

```python
def run_backtest() -> None:
    """Phase 2 – run EMA-cross backtest against the catalog data."""
    instrument_id = f"{MARKET_TICKER}.KALSHI"
    bar_type = f"{instrument_id}-1-HOUR-LAST-EXTERNAL"

    venue_config = BacktestVenueConfig(
        name="KALSHI",
        oms_type="NETTING",
        account_type="CASH",
        base_currency="USD",
        starting_balances=["10000 USD"],
    )

    data_config = BacktestDataConfig(
        catalog_path=CATALOG_PATH,
        data_cls="nautilus_trader.model.data:Bar",
        instrument_id=instrument_id,
        bar_spec="1-HOUR-LAST",
        start_time=START,
        end_time=END,
    )

    strategy_config = ImportableStrategyConfig(
        strategy_path="nautilus_trader.examples.strategies.ema_cross_long_only:EMACrossLongOnly",
        config_path="nautilus_trader.examples.strategies.ema_cross_long_only:EMACrossLongOnlyConfig",
        config={
            "instrument_id": instrument_id,
            "bar_type": bar_type,
            "fast_ema_period": FAST_EMA,
            "slow_ema_period": SLOW_EMA,
            "trade_size": str(TRADE_SIZE),
        },
    )

    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(log_level="INFO"),
        strategies=[strategy_config],
    )

    run_config = BacktestRunConfig(
        venues=[venue_config],
        data=[data_config],
        engine=engine_config,
        dispose_on_completion=False,  # keep engine alive so we can read reports
    )

    node = BacktestNode(configs=[run_config])
    node.run()

    engine = node.get_engine(run_config.id)
    kalshi_venue = Venue("KALSHI")

    import pandas as pd  # noqa: PLC0415  (local import keeps top-level imports lean)
    with pd.option_context("display.max_rows", 100, "display.max_columns", None, "display.width", 300):
        print(engine.trader.generate_account_report(kalshi_venue))
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    node.dispose()
```

Wait — `pd` is already imported at the top of the file. Remove the local `import pandas as pd` line from the function body and the `# noqa` comment:

```python
def run_backtest() -> None:
    """Phase 2 – run EMA-cross backtest against the catalog data."""
    instrument_id = f"{MARKET_TICKER}.KALSHI"
    bar_type = f"{instrument_id}-1-HOUR-LAST-EXTERNAL"

    venue_config = BacktestVenueConfig(
        name="KALSHI",
        oms_type="NETTING",
        account_type="CASH",
        base_currency="USD",
        starting_balances=["10000 USD"],
    )

    data_config = BacktestDataConfig(
        catalog_path=CATALOG_PATH,
        data_cls="nautilus_trader.model.data:Bar",
        instrument_id=instrument_id,
        bar_spec="1-HOUR-LAST",
        start_time=START,
        end_time=END,
    )

    strategy_config = ImportableStrategyConfig(
        strategy_path="nautilus_trader.examples.strategies.ema_cross_long_only:EMACrossLongOnly",
        config_path="nautilus_trader.examples.strategies.ema_cross_long_only:EMACrossLongOnlyConfig",
        config={
            "instrument_id": instrument_id,
            "bar_type": bar_type,
            "fast_ema_period": FAST_EMA,
            "slow_ema_period": SLOW_EMA,
            "trade_size": str(TRADE_SIZE),
        },
    )

    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(log_level="INFO"),
        strategies=[strategy_config],
    )

    run_config = BacktestRunConfig(
        venues=[venue_config],
        data=[data_config],
        engine=engine_config,
        dispose_on_completion=False,
    )

    node = BacktestNode(configs=[run_config])
    node.run()

    engine = node.get_engine(run_config.id)
    kalshi_venue = Venue("KALSHI")

    with pd.option_context("display.max_rows", 100, "display.max_columns", None, "display.width", 300):
        print(engine.trader.generate_account_report(kalshi_venue))
        print(engine.trader.generate_order_fills_report())
        print(engine.trader.generate_positions_report())

    node.dispose()
```

**Step 2: Verify syntax and ruff lint**

```bash
python -m py_compile examples/backtest/kalshi_ema_bars.py && echo "OK"
ruff check examples/backtest/kalshi_ema_bars.py
```

Expected: `OK` and no ruff errors. If ruff reports issues, fix them (run `ruff check --fix examples/backtest/kalshi_ema_bars.py`).

**Step 3: Verify the full script structure reads cleanly**

Read the final file and confirm it has:
- License header
- All imports at top (stdlib → third-party → nautilus)
- 8 constants after the imports
- `fetch_and_catalog()` with the KalshiDataLoader → catalog write logic
- `run_backtest()` with BacktestNode + reports
- `if __name__ == "__main__":` block calling both phases

**Step 4: Final commit**

```bash
git add examples/backtest/kalshi_ema_bars.py
git commit -m "feat(kalshi): implement run_backtest phase with BacktestNode"
```

---

### Task 4: Final validation and GitHub issue update

**Files:**
- No code changes — validation and bookkeeping only

**Step 1: Run ruff on all changed Kalshi files**

```bash
ruff check nautilus_trader/adapters/kalshi/ examples/backtest/kalshi_ema_bars.py
```

Expected: no errors.

**Step 2: Run the existing Kalshi unit tests**

```bash
pytest tests/unit_tests/adapters/kalshi/ -v
```

Expected: all 19 tests pass (18 loader + 1 provider).

**Step 3: Update GitHub issue**

Update issue #2 (`https://github.com/ben-gramling/nautilus_pm/issues/2`) with:
- A comment summarising what was built (two-phase script, Phase 1 = KalshiDataLoader → ParquetDataCatalog, Phase 2 = BacktestNode + EMACrossLongOnly)
- Add label `status: complete / unmerged`

**Note on end-to-end execution:** Running the script against the live Kalshi API requires valid Kalshi API credentials (the `nautilus_pyo3.HttpClient` uses the key from environment/config). The script structure and all wiring can be validated without a live key by inspecting the code; a live integration test requires credentials.
