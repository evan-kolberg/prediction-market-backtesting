# Derived from NautilusTrader prediction-market example code.
# Distributed under the GNU Lesser General Public License Version 3.0 or later.
# Modified in this repository on 2026-05-02.
# See the repository NOTICE file for provenance and licensing scope.

from __future__ import annotations

from decimal import Decimal

if __package__ in {None, ""}:
    from _script_helpers import ensure_repo_root
else:
    from ._script_helpers import ensure_repo_root

ensure_repo_root(__file__)

WINDOW_START = "2026-04-21T00:00:00Z"
WINDOW_END = "2026-04-27T23:59:59.999999999Z"
WINDOW_START_NS = 1776729600000000000
WINDOW_END_NS = 1777334399999999999

POPULAR_MARKET_SLUGS = (
    "will-jesus-christ-return-before-2027",
    "will-oprah-winfrey-win-the-2028-democratic-presidential-nomination",
    "will-lebron-james-win-the-2028-us-presidential-election",
    "will-chelsea-clinton-win-the-2028-democratic-presidential-nomination",
    "will-bernie-sanders-win-the-2028-democratic-presidential-nomination-879",
    "will-andrew-yang-win-the-2028-democratic-presidential-nomination",
    "will-lebron-james-win-the-2028-democratic-presidential-nomination",
    "will-tim-walz-win-the-2028-us-presidential-election",
    "will-hillary-clinton-win-the-2028-democratic-presidential-nomination",
    "will-george-clooney-win-the-2028-democratic-presidential-nomination",
    "will-mike-pence-win-the-2028-republican-presidential-nomination",
    "will-tim-walz-win-the-2028-democratic-presidential-nomination-475",
    "will-byron-donalds-win-the-2028-republican-presidential-nomination",
    "will-kim-kardashian-win-the-2028-democratic-presidential-nomination",
    "will-phil-murphy-win-the-2028-democratic-presidential-nomination-611",
    "will-beto-orourke-win-the-2028-democratic-presidential-nomination",
    "will-mrbeast-win-the-2028-democratic-presidential-nomination",
    "will-uzbekistan-win-the-2026-fifa-world-cup-773",
    "will-the-iranian-regime-fall-by-june-30",
    "strait-of-hormuz-traffic-returns-to-normal-by-april-30",
    "will-liz-cheney-win-the-2028-democratic-presidential-nomination-551",
    "will-zohran-mamdani-win-the-2028-democratic-presidential-nomination-445",
    "will-kim-kardashian-win-the-2028-us-presidential-election",
    "will-person-a-win-the-2028-democratic-presidential-nomination",
    "will-curaao-win-the-2026-fifa-world-cup",
    "will-trump-acquire-greenland-before-2027",
    "will-greg-abbott-win-the-2028-us-presidential-election",
    "will-kristi-noem-win-the-2028-republican-presidential-nomination",
    "will-john-thune-win-the-2028-republican-presidential-nomination",
    "will-vivek-ramaswamy-win-the-2028-us-presidential-election",
    "will-jasmine-crockett-win-the-2028-democratic-presidential-nomination",
    "will-gina-raimondo-win-the-2028-democratic-presidential-nomination-676",
    "will-stephen-smith-win-the-2028-us-presidential-election",
    "will-usa-win-the-2026-fifa-world-cup-467",
    "will-sarah-huckabee-sanders-win-the-2028-republican-presidential-nomination",
    "will-tom-brady-win-the-2028-republican-presidential-nomination",
    "will-tulsi-gabbard-win-the-2028-us-presidential-election",
    "will-barack-obama-win-the-2028-democratic-presidential-nomination-265",
    "will-roy-cooper-win-the-2028-democratic-presidential-nomination-286",
    "will-raphael-warnock-win-the-2028-democratic-presidential-nomination-914",
    "will-katie-britt-win-the-2028-republican-presidential-nomination",
    "will-elon-musk-win-the-2028-republican-presidential-nomination",
    "will-new-zealand-win-the-2026-fifa-world-cup-635",
    "will-kim-kardashian-win-the-2028-republican-presidential-nomination",
    "will-the-us-confirm-that-aliens-exist-before-2027-789-924-249",
    "will-the-san-antonio-spurs-win-the-2026-nba-finals",
    "will-frank-donovan-be-the-leader-of-venezuela-end-of-2026",
    "will-saudi-arabia-win-the-2026-fifa-world-cup",
    "will-south-africa-win-the-2026-fifa-world-cup",
    "will-gavin-newsom-win-the-2028-democratic-presidential-nomination-568",
    "will-jared-polis-win-the-2028-democratic-presidential-nomination-837",
    "will-atletico-madrid-win-the-202526-champions-league",
    "will-elise-stefanik-win-the-2028-republican-presidential-nomination",
    "will-michelle-obama-win-the-2028-democratic-presidential-nomination-777",
    "will-jordan-win-the-2026-fifa-world-cup-233",
    "will-corey-booker-win-the-2028-democratic-presidential-nomination-125",
    "will-glenn-youngkin-win-the-2028-us-presidential-election",
    "will-nikki-haley-win-the-2028-us-presidential-election",
    "will-elon-musk-win-the-2028-us-presidential-election",
    "will-china-invade-taiwan-before-2027",
    "will-france-win-the-2026-fifa-world-cup-924",
    "will-jon-stewart-win-the-2028-democratic-presidential-nomination-518",
    "will-qatar-win-the-2026-fifa-world-cup",
    "will-iran-win-the-2026-fifa-world-cup-788",
    "will-mark-cuban-win-the-2028-democratic-presidential-nomination-329",
    "will-egypt-win-the-2026-fifa-world-cup",
    "will-australia-win-the-2026-fifa-world-cup-816",
    "will-south-korea-win-the-2026-fifa-world-cup-485",
    "will-cape-verde-win-the-2026-fifa-world-cup",
    "will-canada-win-the-2026-fifa-world-cup-755",
    "will-steve-bannon-win-the-2028-republican-presidential-nomination",
    "will-the-los-angeles-lakers-win-the-2026-nba-finals",
    "will-stephen-a-smith-win-the-2028-democratic-presidential-nomination-914",
    "will-tunisia-win-the-2026-fifa-world-cup-165",
    "will-the-us-invade-iran-before-2027",
    "will-algeria-win-the-2026-fifa-world-cup",
    "will-ivory-coast-win-the-2026-fifa-world-cup",
    "will-club-brugge-win-the-202526-champions-league",
    "will-john-fetterman-win-the-2028-democratic-presidential-nomination-941",
    "will-zohran-mamdani-win-the-2028-us-presidential-election",
    "will-ecuador-win-the-2026-fifa-world-cup-986",
    "will-greg-abbott-win-the-2028-republican-presidential-nomination",
    "will-josh-hawley-win-the-2028-republican-presidential-nomination",
    "will-japan-win-the-2026-fifa-world-cup-112",
    "will-andy-beshear-win-the-2028-us-presidential-election",
    "will-spain-win-the-2026-fifa-world-cup-963",
    "will-matt-gaetz-win-the-2028-republican-presidential-nomination",
    "will-morocco-win-the-2026-fifa-world-cup-464",
    "will-judy-shelton-be-confirmed-as-fed-chair",
    "will-rand-paul-win-the-2028-republican-presidential-nomination",
    "will-paraguay-win-the-2026-fifa-world-cup-967",
    "will-the-toronto-raptors-win-the-2026-nba-finals",
    "will-portugal-win-the-2026-fifa-world-cup-912",
    "will-scotland-win-the-2026-fifa-world-cup",
    "will-mexico-win-the-2026-fifa-world-cup-529",
    "will-the-iranian-regime-fall-by-the-end-of-2026",
    "will-haiti-win-the-2026-fifa-world-cup",
    "will-brazil-win-the-2026-fifa-world-cup-183",
    "will-netherlands-win-the-2026-fifa-world-cup-739",
    "will-gavin-newsom-win-the-2028-us-presidential-election",
)


def run() -> None:
    from prediction_market_extensions.backtesting._execution_config import (
        ExecutionModelConfig,
        StaticLatencyConfig,
    )
    from prediction_market_extensions.backtesting._experiments import (
        build_replay_experiment,
        run_experiment,
    )
    from prediction_market_extensions.backtesting._prediction_market_backtest import (
        MarketReportConfig,
    )
    from prediction_market_extensions.backtesting._prediction_market_runner import (
        MarketDataConfig,
    )
    from prediction_market_extensions.backtesting._replay_specs import BookReplay
    from prediction_market_extensions.backtesting._timing_harness import timing_harness
    from prediction_market_extensions.backtesting.data_sources import Book, Polymarket, PMXT

    replays = tuple(
        BookReplay(
            market_slug=slug,
            token_index=0,
            start_time=WINDOW_START,
            end_time=WINDOW_END,
            metadata={
                "sim_label": slug,
                "replay_window_start_ns": WINDOW_START_NS,
                "replay_window_end_ns": WINDOW_END_NS,
            },
        )
        for slug in POPULAR_MARKET_SLUGS
    )

    @timing_harness
    def _run() -> None:
        run_experiment(
            build_replay_experiment(
                name="polymarket_pmxt_book_100_replay_runner",
                description=(
                    "Joint-portfolio PMXT book backtest over 100 popular Polymarket markets"
                ),
                data=MarketDataConfig(
                    platform=Polymarket,
                    data_type=Book,
                    vendor=PMXT,
                    sources=(
                        "local:/Volumes/storage/pmxt_data",
                        "archive:r2v2.pmxt.dev",
                        "archive:r2.pmxt.dev",
                    ),
                ),
                replays=replays,
                strategy_configs=[
                    {
                        "strategy_path": "strategies:BookMicropriceImbalanceStrategy",
                        "config_path": "strategies:BookMicropriceImbalanceConfig",
                        "config": {
                            "trade_size": Decimal(5),
                            "depth_levels": 3,
                            "entry_imbalance": 0.62,
                            "exit_imbalance": 0.48,
                            "min_microprice_edge": 0.0015,
                            "max_spread": 0.08,
                            "max_entry_price": 0.95,
                            "max_expected_slippage": 0.02,
                            "min_holding_updates": 0,
                            "reentry_cooldown_updates": 0,
                            "min_holding_seconds": 30.0,
                            "reentry_cooldown_seconds": 60.0,
                            "take_profit": 0.02,
                            "stop_loss": 0.025,
                        },
                    }
                ],
                initial_cash=1_000.0,
                probability_window=30,
                min_book_events=25,
                min_price_range=0.0,
                execution=ExecutionModelConfig(
                    queue_position=True,
                    latency_model=StaticLatencyConfig(
                        base_latency_ms=75.0,
                        insert_latency_ms=10.0,
                        update_latency_ms=5.0,
                        cancel_latency_ms=5.0,
                    ),
                ),
                report=MarketReportConfig(
                    count_key="book_events",
                    count_label="Book Events",
                    pnl_label="PnL (USDC)",
                    market_key="sim_label",
                    summary_report=True,
                    summary_report_path=(
                        "output/polymarket_pmxt_book_100_replay_runner_joint_portfolio.html"
                    ),
                    summary_plot_panels=(
                        "total_equity",
                        "equity",
                        "market_pnl",
                        "periodic_pnl",
                        "yes_price",
                        "allocation",
                        "total_drawdown",
                        "drawdown",
                        "total_rolling_sharpe",
                        "rolling_sharpe",
                        "total_cash_equity",
                        "cash_equity",
                        "monthly_returns",
                        "total_brier_advantage",
                        "brier_advantage",
                    ),
                ),
                empty_message=(
                    "No PMXT 100-market joint-portfolio windows met the book requirements."
                ),
                partial_message=(
                    "Completed {completed} of {total} PMXT 100-market joint-portfolio replays."
                ),
                return_summary_series=True,
            )
        )

    _run()


if __name__ == "__main__":
    run()
