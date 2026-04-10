# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-04-05.
# See the repository NOTICE file for provenance and licensing scope.

"""
Research-only EMA parameter search on one Polymarket market using PMXT historical L2 data.
"""

# ruff: noqa: E402

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

from prediction_market_extensions.backtesting._execution_config import ExecutionModelConfig
from prediction_market_extensions.backtesting._execution_config import StaticLatencyConfig
from prediction_market_extensions.backtesting._experiments import ParameterSearchExperiment
from prediction_market_extensions.backtesting._experiments import run_experiment
from prediction_market_extensions.backtesting._prediction_market_runner import MarketDataConfig
from prediction_market_extensions.backtesting._replay_specs import QuoteReplay
from prediction_market_extensions.backtesting._timing_harness import timing_harness
from prediction_market_extensions.backtesting.data_sources import PMXT, Polymarket, QuoteTick
from prediction_market_extensions.backtesting.optimizers import ParameterSearchConfig
from prediction_market_extensions.backtesting.optimizers import ParameterSearchWindow


NAME = "polymarket_quote_tick_ema_optimizer"

DESCRIPTION = "Random-search EMA optimizer with explicit train and holdout windows on PMXT L2 data"
MARKET_SLUG = "will-ludvig-aberg-win-the-2026-masters-tournament"

EMIT_HTML = True
CHART_OUTPUT_PATH = "output"

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

BASE_REPLAY = QuoteReplay(market_slug=MARKET_SLUG, token_index=0)

TRAIN_WINDOWS = (
    ParameterSearchWindow(
        name="sample-a-full-window",
        start_time="2026-04-05T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    ParameterSearchWindow(
        name="sample-b-2026-04-06-day",
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-06T23:59:59Z",
    ),
    ParameterSearchWindow(
        name="sample-c-2026-04-07-late",
        start_time="2026-04-07T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
)

HOLDOUT_WINDOWS = (
    ParameterSearchWindow(
        name="sample-d-close-window",
        start_time="2026-04-07T00:00:00Z",
        end_time="2026-04-07T11:59:59Z",
    ),
)

STRATEGY_SPEC = {
    "strategy_path": "strategies:QuoteTickEMACrossoverStrategy",
    "config_path": "strategies:QuoteTickEMACrossoverConfig",
    "config": {
        "trade_size": Decimal("5"),
        "fast_period": "__SEARCH__:fast_period",
        "slow_period": "__SEARCH__:slow_period",
        "entry_buffer": "__SEARCH__:entry_buffer",
        "take_profit": "__SEARCH__:take_profit",
        "stop_loss": "__SEARCH__:stop_loss",
    },
}

PARAMETER_GRID = {
    "fast_period": (32, 64, 96),
    "slow_period": (128, 256, 384),
    "entry_buffer": (0.00025, 0.0005),
    "take_profit": (0.005, 0.01),
    "stop_loss": (0.005, 0.01),
}

EXECUTION = ExecutionModelConfig(
    queue_position=True,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)

PARAMETER_SEARCH = ParameterSearchConfig(
    name=NAME,
    data=DATA,
    base_replay=BASE_REPLAY,
    strategy_spec=STRATEGY_SPEC,
    parameter_grid=PARAMETER_GRID,
    train_windows=TRAIN_WINDOWS,
    holdout_windows=HOLDOUT_WINDOWS,
    max_trials=18,
    random_seed=7,
    holdout_top_k=5,
    initial_cash=100.0,
    probability_window=256,
    min_quotes=500,
    min_price_range=0.005,
    min_fills_per_window=1,
    execution=EXECUTION,
    emit_html=EMIT_HTML,
    chart_output_path=CHART_OUTPUT_PATH,
)


SEARCH = PARAMETER_SEARCH
OPTIMIZER = PARAMETER_SEARCH
OPTIMIZATION = PARAMETER_SEARCH

EXPERIMENT = ParameterSearchExperiment(name=NAME, description=DESCRIPTION, parameter_search=SEARCH)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
