# Prediction Market Strategy Package

This package contains reusable, venue-agnostic strategy classes for prediction markets.

## Design Boundaries

- `nautilus_trader.adapters.{kalshi,polymarket}.research`:
  market discovery + historical data loading (venue-specific I/O).
- `strategies/`:
  pure signal + risk + order intent logic (strategy logic).
- `backtests/*.py`:
  orchestration scripts (choose markets, wire strategy configs, run backtests, print results).

## Modules

- `core.py`: shared single-instrument long-only lifecycle and order plumbing.
- `mean_reversion.py`: rolling-average spread capture.
- `microprice_imbalance.py`: L2 spread, depth-imbalance, and microprice pressure.
- `ema_crossover.py`: trend-following crossover.
- `breakout.py`: volatility breakout with bounded entry near resolution.
- `final_period_momentum.py`: late-game threshold breakout in the final minutes.
- `late_favorite_limit_hold.py`: limit-entry late-favorite hold meant for resolved settlement backtests.
- `rsi_reversion.py`: oscillator pullback entries.
- `vwap_reversion.py`: trade-tick VWAP dislocation fade.
- `panic_fade.py`: capitulation/rebound logic with time-based exits.

## Extension Rules

- Keep strategy modules free of API calls and market discovery logic.
- Add new market filters/loaders in adapter `research.py`, not in strategy files.
- Expose strategy classes/configs through `__init__.py` for consistent imports.
- Use `core.py` to avoid repeated order-state helpers across strategy modules.
