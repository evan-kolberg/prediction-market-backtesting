# Backtests And Runners

## Repo Layout

- `strategies/` contains reusable strategy classes and configs
- `strategies/private/` is for git-ignored local strategy modules
- `backtests/` contains flat public runner entrypoints
- `backtests/_shared/` contains shared runner plumbing, data-source adapters,
  strategy-config binding, timing, and UI helpers
- `backtests/private/` is for git-ignored local runners

Only `backtests/*.py` and `backtests/private/*.py` are discoverable runner
entrypoints. Any other subdirectory under `backtests/` should be support code
only.

Good public examples:

- reusable EMA logic:
  [`strategies/ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/ema_crossover.py)
- reusable late-favorite limit-hold logic:
  [`strategies/late_favorite_limit_hold.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/late_favorite_limit_hold.py)
- Kalshi native trade-tick runner:
  [`backtests/kalshi_trade_tick_breakout.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_breakout.py)
- Polymarket native trade-tick runner:
  [`backtests/polymarket_trade_tick_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_vwap_reversion.py)
- Polymarket quote-tick runner with PMXT vendor data:
  [`backtests/polymarket_quote_tick_pmxt_ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_ema_crossover.py)
- fixed-basket multi-market runner:
  [`backtests/polymarket_trade_tick_sports_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_sports_vwap_reversion.py)

Those public runners are intended as readable research demos, not profitability
claims. Result payloads now separate the requested replay window from the data
window that actually loaded, including `planned_start`, `planned_end`,
`loaded_start`, `loaded_end`, `coverage_ratio` for loaded-data coverage, and
`requested_coverage_ratio` for requested-window coverage.

## Runner Contract

Public runners should read like flat experiment specs. The canonical shape is:

```python
from decimal import Decimal

from _script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._prediction_market_backtest import MarketReportConfig
from backtests._shared._prediction_market_backtest import MarketSimConfig
from backtests._shared._prediction_market_backtest import PredictionMarketBacktest
from backtests._shared._prediction_market_backtest import run_reported_backtest
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick

NAME = "polymarket_quote_tick_pmxt_ema_crossover"
DESCRIPTION = "EMA crossover momentum on one Polymarket market"

DATA = MarketDataConfig(
    platform=Polymarket,
    data_type=QuoteTick,
    vendor=PMXT,
    sources=(
        "local:/Volumes/LaCie/pmxt_raws",
        "archive:r2.pmxt.dev",
        "relay:209-209-10-83.sslip.io",
    ),
)

SIMS = (
    MarketSimConfig(
        market_slug="market-slug",
        token_index=0,
        start_time="2026-03-19T07:35:57.277659Z",
        end_time="2026-03-24T07:35:57.277659Z",
    ),
)

STRATEGY_CONFIGS = [
    {
        "strategy_path": "strategies:QuoteTickEMACrossoverStrategy",
        "config_path": "strategies:QuoteTickEMACrossoverConfig",
        "config": {
            "trade_size": Decimal("100"),
            "fast_period": 64,
            "slow_period": 256,
            "entry_buffer": 0.0005,
            "take_profit": 0.010,
            "stop_loss": 0.010,
        },
    },
]

REPORT = MarketReportConfig(
    count_key="quotes",
    count_label="Quotes",
    pnl_label="PnL (USDC)",
)

EXECUTION = ExecutionModelConfig(
    queue_position=True,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)

BACKTEST = PredictionMarketBacktest(
    name=NAME,
    data=DATA,
    sims=SIMS,
    strategy_configs=STRATEGY_CONFIGS,
    initial_cash=100.0,
    probability_window=256,
    min_quotes=500,
    min_price_range=0.005,
    execution=EXECUTION,
)

@timing_harness
def run() -> None:
    run_reported_backtest(
        backtest=BACKTEST,
        report=REPORT,
        empty_message="No sims met the quote-tick requirements.",
    )
```

Every public runner should expose:

- `NAME`
- `DESCRIPTION`
- `DATA`
- `SIMS`
- `STRATEGY_CONFIGS`
- `REPORT` when the runner prints a summary table or writes combined reports
- `EXECUTION` when the runner models non-default queue position or exchange latency
- `BACKTEST`
- `run()`

## Designing Good Runner Files

A runner file should answer the experiment questions directly:

- which venue or platform is being replayed
- which data modality is being used
- which vendor supplies that modality
- which source priority should be used
- which market or basket of markets is being replayed
- what the capital and execution assumptions are
- which strategy config or configs should be bound into the run

Keep the top-level file declarative. Keep shared mechanics in `backtests/_shared/`.

That division is deliberate:

- `DATA` selects the platform, modality, vendor, and source priority
- `SIMS` is the instrument basket, whether that basket contains one market or many
- `STRATEGY_CONFIGS` is the stable strategy payload passed into the backtest object
- `EXECUTION` holds optional queue-position and latency assumptions
- `BACKTEST` owns loading, engine construction, and execution

## Multi-Market Strategy Configs

`PredictionMarketBacktest` supports either one strategy instance per sim or one
batch-level strategy config that references the full basket.

Useful config sentinels:

- `__SIM_INSTRUMENT_ID__` binds to the current sim instrument
- `__ALL_SIM_INSTRUMENT_IDS__` binds to every loaded sim instrument in the basket
- `__SIM_METADATA__:<key>` binds metadata from `MarketSimConfig.metadata`

That lets a runner expose `SIMS` explicitly and still pass one clean
`STRATEGY_CONFIGS` payload into the runner object.

## Running Backtests

Interactive menu:

```bash
make backtest
```

The menu uses `Textual`, so you get a scrollable runner list on the left and a
details/preview pane on the right. Single-letter shortcuts still launch
runners directly, `/` focuses the filter box, `Esc` clears the filter, and the
highlighted runner's full file contents stay visible while you browse.

Equivalent direct command:

```bash
uv run python main.py
```

Direct script execution is usually better once you know the runner you want:

```bash
uv run python backtests/kalshi_trade_tick_breakout.py
uv run python backtests/polymarket_trade_tick_vwap_reversion.py
uv run python backtests/polymarket_quote_tick_pmxt_ema_crossover.py
```

Public runners keep their experiment inputs in code. PMXT quote-tick runners
pin absolute sample windows; native trade-tick runners pin market/source
selection and use rolling lookbacks unless you also set `end_time`. If you want
a different market, window, cash value, or vendor source priority, edit
`DATA`, `SIMS`, or `STRATEGY_CONFIGS` in the runner file, or copy the file into
`backtests/private/` and customize it there.

## Editing Runner Inputs

The public runner layer no longer depends on shell env vars for experiment
definition. The file itself should carry the actual values.

Use these top-level objects as the edit surface:

- `DATA` for platform, modality, vendor, and source priority
- `SIMS` for one market or a basket of markets
- `STRATEGY_CONFIGS` for strategy paths and parameter payloads
- `EXECUTION` for optional queue-position and latency heuristics
- `BACKTEST` for shared execution requirements like cash, quote/trade minimums,
  probability window, and Nautilus log level

Low-level loader env vars still exist for custom integrations and private
workflows:

- `KALSHI_REST_BASE_URL`
- `POLYMARKET_GAMMA_BASE_URL`, `POLYMARKET_TRADE_API_BASE_URL`,
  `POLYMARKET_CLOB_BASE_URL`
- `PMXT_RAW_ROOT`, `PMXT_REMOTE_BASE_URL`, `PMXT_RELAY_BASE_URL`,
  `PMXT_CACHE_DIR`, `PMXT_DISABLE_CACHE`
- `BACKTEST_ENABLE_TIMING=0`

## Data Vendor Notes

### Native Vendors

- `native` means the loader is using venue-native APIs or venue-native historical
  adapters
- public runners pin native source selection in `DATA.sources`
- low-level native loader URLs can still be overridden outside the public runner
  layer if you are building a custom workflow

### PMXT

- PMXT is the first documented quote-tick vendor adapter in this repo
- the preferred sustained workflow is raw-first: point runners at a local raw
  mirror when you have one, otherwise let them pull from archive and relay
- use `archive:archive.example.com` when you want the runner to fetch raw
  archive hours explicitly
- use `local:/path/to/raw-hours` when you want the runner to fetch from a
  local PMXT raw mirror explicitly
- use `relay:relay.example.com` when you want the runner to fetch raw hours
  from a relay explicitly
- after the cache layer, PMXT quote-tick runners try the explicit raw sources
  in the exact order you list them
- PMXT source parsing is strict on purpose; only `local:`, `archive:`,
  and `relay:` are accepted in `DATA.sources`
- the local PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`
- the shared public relay is now treated as a raw mirror service; filtered relay
  behavior is legacy or self-hosted
- direct script execution keeps normal Nautilus output visible, and runners that
  opt into `@timing_harness` keep timing output too

For vendor-specific data-source behavior and timings, use:

- [Data Vendors, Local Mirrors, And Raw PMXT](pmxt-byod.md)
- [Vendor Fetch Sources And Timing](pmxt-fetch-sources.md)
- [Mirror And Relay Ops](pmxt-relay.md)
