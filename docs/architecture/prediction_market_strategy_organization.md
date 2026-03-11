# Prediction Market Strategy Organization

## Goal

Scale to dozens of prediction-market strategies without duplicating helper logic
or mixing venue adapters with strategy code.

## External Patterns Benchmarked

- QuantConnect LEAN Algorithm Framework:
  separates alpha, portfolio construction, risk management, and execution.
- Freqtrade:
  keeps strategy classes as reusable units while data/exchange plumbing lives
  in the framework/runtime.
- Backtrader/Zipline:
  event-driven strategy classes with engine/data handling outside strategy modules.

## Repository Structure

- Adapter layer:
  `nautilus_trader.adapters.{kalshi,polymarket}.research`
  contains market discovery + historical data loading.
- Strategy layer:
  `nautilus_trader.examples.strategies.prediction_market`
  contains reusable, venue-agnostic strategy classes/configs.
- Backtest orchestration:
  `examples/backtest/prediction_markets`
  contains scripts that select markets and run strategies.

## Implementation Rules

- No market discovery helpers in strategy modules.
- No venue-specific API logic in strategy modules.
- Shared order lifecycle helpers belong in `prediction_market/core.py`.
- New strategies should expose config + strategy classes and be exported in
  `prediction_market/__init__.py`.

## Charting Architecture

Prediction-market charting follows the same separation principle as strategy execution:

- NautilusTrader remains the computation engine and source of truth for fills, equity,
  account balances, and positions.
- `examples/backtest/prediction_markets` scripts orchestrate the run and select the output path.
- `nautilus_trader.adapters.prediction_market.research` runs the `BacktestEngine`,
  extracts native Nautilus outputs, and prepares PM-specific chart inputs.
- `nautilus_trader.adapters.prediction_market.backtest_utils` builds market price series,
  Brier inputs, realized PnL summaries, and realized-outcome inference.
- `nautilus_trader.analysis.legacy_plot_adapter` translates those Nautilus outputs into the
  legacy prediction-market plotting model and then standardizes the final layout.

This means PM charting is a rendering adapter around Nautilus, not a separate backtest
engine and not a bypass of Nautilus internals.

### Why the PM adapter exists

The generic Plotly tearsheet framework is the long-term visualization system, but the
prediction-market examples currently benefit from a denser PM-specific layout:

- YES-price chart with fills,
- profit / loss markers,
- periodic PnL,
- monthly returns,
- cumulative Brier advantage.

The adapter preserves Nautilus-native computation while reusing that PM-specific layout.

### Brier advantage behavior

The cumulative Brier advantage chart is computed from market probability, strategy
probability, and a realized outcome. Because the realized outcome is required, the chart is:

- rendered as a true cumulative series for resolved markets,
- rendered as an unavailable placeholder for unresolved markets.

That placeholder is intentional and preserves computational correctness.

## New Strategy Modules Added

- `ema_crossover.py`
- `breakout.py`
- `rsi_reversion.py`
- `vwap_reversion.py`
- `panic_fade.py`
