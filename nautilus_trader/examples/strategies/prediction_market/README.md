# Prediction Market Strategy Package

This package contains reusable, venue-agnostic strategy classes for prediction markets.

## Design Boundaries

- `nautilus_trader.adapters.{kalshi,polymarket}.research`:
  market discovery + historical data loading (venue-specific I/O).
- `nautilus_trader.examples.strategies.prediction_market`:
  pure signal + risk + order intent logic (strategy logic).
- `examples/backtest/prediction_markets/*.py`:
  orchestration scripts (choose markets, wire strategy configs, run backtests, print results).

## Modules

- `core.py`: shared single-instrument long-only lifecycle and order plumbing.
- `mean_reversion.py`: rolling-average spread capture.
- `ema_crossover.py`: trend-following crossover.
- `breakout.py`: volatility breakout with bounded entry near resolution.
- `final_period_momentum.py`: late-game threshold breakout in the final minutes.
- `rsi_reversion.py`: oscillator pullback entries.
- `vwap_reversion.py`: trade-tick VWAP dislocation fade.
- `panic_fade.py`: capitulation/rebound logic with time-based exits.

## Extension Rules

- Keep strategy modules free of API calls and market discovery logic.
- Add new market filters/loaders in adapter `research.py`, not in strategy files.
- Expose strategy classes/configs through `__init__.py` for consistent imports.
- Use `core.py` to avoid repeated order-state helpers across strategy modules.
