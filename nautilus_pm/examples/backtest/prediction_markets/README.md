# Prediction Market Backtests

This folder contains runnable backtest scripts for Kalshi and Polymarket.

## Purpose

Each script should only do orchestration:

- choose instruments/markets using adapter research helpers,
- instantiate strategy configs from `nautilus_trader.examples.strategies.prediction_market`,
- run backtests and report outputs.

## Current Scripts

- `kalshi_ema_crossover.py`
- `kalshi_breakout.py`
- `kalshi_rsi_reversion.py`
- `kalshi_panic_fade.py`
- `kalshi_sports_final_period_momentum.py`
- `kalshi_ema_bars.py`
- `kalshi_spread_capture.py`
- `polymarket_ema_crossover.py`
- `polymarket_rsi_reversion.py`
- `polymarket_vwap_reversion.py`
- `polymarket_panic_fade.py`
- `polymarket_simple_quoter.py`
- `polymarket_spread_capture.py`
- `polymarket_deep_value_resolution_hold.py`
- `polymarket_sports_final_period_momentum.py`

## Multi-Market Example

- `polymarket_sports_final_period_momentum.py` is the public reference example for
  running one strategy across many markets.
- It defaults to `TARGET_RESULTS=50`, emits the usual per-market legacy charts, and
  also writes one legacy multi-market summary chart.

## Single-Market Comparison Set

Use these scripts to compare strategies on shared markets:

- Kalshi default market: `KXNEXTIRANLEADER-45JAN01-MKHA`
- Polymarket default market:
  `will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026`

Kalshi:

- `kalshi_spread_capture.py`
- `kalshi_ema_crossover.py`
- `kalshi_breakout.py`
- `kalshi_rsi_reversion.py`
- `kalshi_panic_fade.py`

Polymarket:

- `polymarket_spread_capture.py`
- `polymarket_ema_crossover.py`
- `polymarket_rsi_reversion.py`
- `polymarket_vwap_reversion.py`
- `polymarket_panic_fade.py`
- `polymarket_deep_value_resolution_hold.py`
- `polymarket_simple_quoter.py`

Default chart readability settings for this comparison set:

- Kalshi and Polymarket chart prices stay full-density until the legacy chart path needs to downsample large datasets.
- Set `CHART_RESAMPLE_RULE` if you want smoother chart lines for debugging.
- Polymarket defaults use a lower-activity public market so the public trades API
  stays within its historical offset ceiling while still producing non-flat charts.

## Conventions

- Keep venue-specific data access in adapter research modules.
- Keep strategy classes in the shared prediction-market strategy package.
- Keep shared single-market defaults in `_defaults.py` and import them from scripts.
- Avoid helper duplication across scripts; prefer shared utilities.
- Private runners can live under nested folders such as `private_strategies/`; the backtest menu discovers them recursively.
- Multi-market scripts should prefer one legacy Bokeh multi-market HTML report instead of concatenated per-market pages.
