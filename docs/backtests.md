# Backtests And Runners

## Repo Layout

- `strategies/` contains reusable strategy classes and configs
- `strategies/private/` is for git-ignored local strategy modules
- `backtests/` contains runnable public backtest entrypoints and helpers
- `backtests/private/` is for git-ignored local runners

Good public examples:

- reusable EMA logic:
  [`strategies/ema_crossover.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/ema_crossover.py)
- reusable final-period momentum logic:
  [`strategies/final_period_momentum.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/final_period_momentum.py)
- reusable late-favorite limit-hold logic:
  [`strategies/late_favorite_limit_hold.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/late_favorite_limit_hold.py)
- Kalshi trade-tick runner:
  [`backtests/kalshi_trade_tick/kalshi_breakout.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick/kalshi_breakout.py)
- PMXT Polymarket quote-tick runners live under `backtests/polymarket_quote_tick/`
- archived legacy Polymarket trade-tick runners live under `backtests/polymarket_trade_tick/`

## Runner Contract

Any module shown in the menu should expose:

```python
NAME = "my_strategy"
DESCRIPTION = "one-liner"

async def run() -> None:
    ...
```

Keep reusable signal logic in `strategies/`, then import it into a thin runner.
If you add reusable strategy classes or configs, re-export them from
[`strategies/__init__.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/strategies/__init__.py).

Common public runner patterns:

- Kalshi trade-tick:
  [`backtests/kalshi_trade_tick/_kalshi_single_market_trade_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick/_kalshi_single_market_trade_runner.py)
- Polymarket trade-tick:
  [`backtests/polymarket_trade_tick/_polymarket_single_market_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick/_polymarket_single_market_runner.py)
- Polymarket PMXT L2:
  [`backtests/polymarket_quote_tick/_polymarket_single_market_pmxt_runner.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick/_polymarket_single_market_pmxt_runner.py)

## Running Backtests

Interactive menu:

```bash
make backtest
```

Equivalent direct command:

```bash
uv run python main.py
```

Direct script execution is usually better once you know the runner you want:

```bash
MARKET_TICKER=<kalshi-market-ticker> uv run python backtests/kalshi_trade_tick/kalshi_breakout.py
uv run python backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
MARKET_SLUG=<polymarket-market-slug> uv run python backtests/polymarket_trade_tick/polymarket_vwap_reversion.py
```

If you omit the market env vars, most public runners fall back to defaults
bundled in the module so examples stay runnable.

## Common Environment Variables

- `MARKET_TICKER` for Kalshi single-market runners
- `MARKET_SLUG` for Polymarket trade-tick single-market runners
- `TOKEN_INDEX` to choose which Polymarket outcome token to backtest
- `LOOKBACK_DAYS` for rolling trade-tick windows
- `START_TIME`, `END_TIME`, or `LOOKBACK_HOURS` to override the PMXT quote-tick
  runner window from the shell without editing the script
- `PMXT_RELAY_BASE_URL` to override the default public relay or disable it with
  `PMXT_RELAY_BASE_URL=0`
- `PMXT_LOCAL_ARCHIVE_DIR` to use your own local PMXT raw hour mirror before
  falling back to `r2.pmxt.dev` after the public relay's raw-hour passthrough
- `PMXT_CACHE_DIR` or `PMXT_DISABLE_CACHE` for the local PMXT cache
- `TRADE_SIZE` and `INITIAL_CASH` for sizing
- `TARGET_RESULTS` for multi-market runners

## PMXT Notes

- PMXT timing output is enabled by default in the menu and direct `main.py` path
- `BACKTEST_ENABLE_TIMING=0` is the explicit quiet opt-out
- PMXT filtered cache is enabled by default at `~/.cache/nautilus_trader/pmxt`
- the public relay defaults to `https://209-209-10-83.sslip.io`
- the public Polymarket quote-tick examples are pinned to a known-good public
  relay sample window so direct script runs work out of the box
- if you mirror PMXT raw hours locally, set `PMXT_LOCAL_ARCHIVE_DIR=/path/to/raw-hours`
  and optionally `PMXT_RELAY_BASE_URL=0` for a local-only run

For PMXT data-source behavior and timings, use:

- [PMXT BYOD And Local Data](pmxt-byod.md)
- [PMXT Fetch Sources And Timing](pmxt-fetch-sources.md)
- [PMXT Relay Deploy And Ops](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/pmxt_relay/README.md)
