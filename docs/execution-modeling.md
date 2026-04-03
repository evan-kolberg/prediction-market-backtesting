# Execution Modeling

Backtests here replay venue data from Kalshi and Polymarket into
NautilusTrader. The main things that move realized backtest performance beyond
the raw venue data are:

- exchange fee models
- taker slippage assumptions
- engine behavior such as IOC handling, price rounding, cash-account limits,
  and `AccountBalanceNegative`

## Fees

- Kalshi uses a nonlinear expected-earnings fee model
- Polymarket uses the current taker fee curve from venue metadata, plus CLOB
  `fee-rate` enrichment when the market payload itself still reports zero fees
- Polymarket maker fees are treated as zero; taker fees vary by market category
  and are not hardcoded to the older sports-vs-crypto heuristic
- if a venue reports zero fees for a market, the backtest also applies zero
  fees

## Slippage

- shared prediction-market backtests default to a custom taker fill model
- non-limit orders get a deterministic one-tick adverse fill
- Polymarket uses the market's own tick size in trade-tick mode
- Kalshi uses one cent as the effective order tick for taker slippage
- limit orders keep default Nautilus matching behavior
- PMXT-backed Polymarket L2 backtests do not use the synthetic one-tick taker
  fill model; they replay historical `OrderBookDeltas` with `book_type=L2_MBP`
  and `liquidity_consumption=True`

## Limits

- Kalshi public backtests here are trade-tick replay only
- Polymarket PMXT-backed backtests are full L2 order-book replay
- neither path models queue position for passive orders because public MBP data
  does not expose true queue position
- taker-heavy strategies that harvest tiny price changes can look much worse
  once fees and one-tick slippage are turned on
- PMXT L2 helps with taker modeling, but maker realism still needs L3 data

## Vendor L2 Behavior

### PMXT

- the loader prefers local filtered cache first, then any local raw PMXT
  mirror, then relay-hosted filtered hours if the configured relay still serves
  them, then the configured remote raw archive, then relay-hosted raw hours
- the current shared relay direction is mirror-only, so the durable shared
  server path is raw parquet serving rather than server-side filtered-hour
  processing
- local PMXT filtered cache is enabled by default and grows with the number of
  unique `(condition_id, token_id, hour)` tuples you replay
- `BACKTEST_ENABLE_TIMING=0` is the opt-out if you want a quieter PMXT run

For concrete timings and source tiers, see [Vendor Fetch Sources And
Timing](pmxt-fetch-sources.md).
