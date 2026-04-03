# Plotting

Single-market plotting is built into the shared runner flow used by the public
prediction-market backtests.

Good examples:

- [`backtests/kalshi_trade_tick_breakout.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_breakout.py)
- [`backtests/kalshi_trade_tick_panic_fade.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/kalshi_trade_tick_panic_fade.py)
- [`backtests/polymarket_quote_tick_pmxt_panic_fade.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_panic_fade.py)
- [`backtests/polymarket_quote_tick_pmxt_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_quote_tick_pmxt_vwap_reversion.py)

## Output Paths

Charts are written to `output/`, typically with names like:

- `output/<backtest>_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_ema_crossover_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_breakout_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_rsi_reversion_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_spread_capture_<market>_legacy.html`
- `output/polymarket_quote_tick_pmxt_sports_vwap_reversion_combined_legacy.html`
- `output/polymarket_quote_tick_pmxt_sports_vwap_reversion_multi_market.html`

## Multi-Market References

The clearest multi-market plotting references are the flat Polymarket trade-tick
runner files:

- [`backtests/polymarket_trade_tick_sports_final_period_momentum.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_sports_final_period_momentum.py)
- [`backtests/polymarket_trade_tick_sports_vwap_reversion.py`](https://github.com/evan-kolberg/prediction-market-backtesting/blob/main/backtests/polymarket_trade_tick_sports_vwap_reversion.py)

Those runners write one combined summary chart to `output/`, typically with
names like:

- `output/polymarket_trade_tick_sports_final_period_momentum_multi_market.html`
- `output/polymarket_trade_tick_sports_vwap_reversion_multi_market.html`
