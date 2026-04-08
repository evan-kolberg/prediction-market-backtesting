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

from backtests._shared._execution_config import ExecutionModelConfig
from backtests._shared._execution_config import StaticLatencyConfig
from backtests._shared._experiments import OptimizationExperiment
from backtests._shared._experiments import run_experiment
from backtests._shared._optimizer import OptimizationConfig
from backtests._shared._optimizer import OptimizationWindow
from backtests._shared._prediction_market_runner import MarketDataConfig
from backtests._shared._replay_specs import PolymarketPMXTQuoteReplay
from backtests._shared._timing_harness import timing_harness
from backtests._shared.data_sources import PMXT, Polymarket, QuoteTick


NAME = "polymarket_quote_tick_pmxt_ema_optimizer"

DESCRIPTION = "Random-search EMA optimizer with explicit train and holdout windows on PMXT L2 data"

EMIT_HTML = False
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

BASE_REPLAY = PolymarketPMXTQuoteReplay(
    market_slug="will-ludvig-aberg-win-the-2026-masters-tournament",
    token_index=0,
)

TRAIN_WINDOWS = (
    OptimizationWindow(
        name="sample-a-full-window",
        start_time="2026-04-05T00:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
    OptimizationWindow(
        name="sample-b-2026-04-06-day",
        start_time="2026-04-06T00:00:00Z",
        end_time="2026-04-06T23:59:59Z",
    ),
    OptimizationWindow(
        name="sample-c-2026-04-07-late",
        start_time="2026-04-07T12:00:00Z",
        end_time="2026-04-07T23:59:59Z",
    ),
)

HOLDOUT_WINDOWS = (
    OptimizationWindow(
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

OPTIMIZATION = OptimizationConfig(
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


EXPERIMENT = OptimizationExperiment(
    name=NAME,
    description=DESCRIPTION,
    optimization=OPTIMIZATION,
)


@timing_harness
def run() -> None:
    run_experiment(EXPERIMENT)


if __name__ == "__main__":
    run()
