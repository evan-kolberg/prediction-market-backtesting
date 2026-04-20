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

## Passive Orders And Queue Position

- public runners can now opt into Nautilus `queue_position=True`
- the public PMXT quote-tick runners in this repo now enable that heuristic by
  default, and the public trade-tick late-favorite passive runner now does too
- this is still a heuristic, not true venue queue reconstruction
- Kalshi and Polymarket trade-tick replay can use trade prints to move queue
  ahead estimates on passive limit orders
- PMXT quote-tick replay can also enable queue tracking, but that path replays
  L2 book updates and quotes rather than historical trade ticks, so fills still
  depend more heavily on book-level clears and price moves than on true trade
  consumption
- public MBP data does not expose hidden liquidity, exact priority inside a
  level, or venue-specific matching quirks

## Latency

- public runners can now attach a static Nautilus latency model through the
  runner config
- the public PMXT quote-tick runners in this repo now ship with a static
  latency model enabled by default
- the current repo-layer surface is a static millisecond model with separate
  base, insert, update, and cancel delays
- this helps test whether a market-making or quote-chasing strategy only works
  because orders are assumed to land instantly

## Limits

- repo-owned backtests keep cash-account risk checks enabled by default
- result payloads now distinguish the requested replay window from the loaded
  data window via `planned_start`, `planned_end`, `loaded_start`,
  `loaded_end`, `coverage_ratio`, and `requested_coverage_ratio`
- Kalshi public backtests here are trade-tick replay only
- Polymarket PMXT-backed backtests are full L2 order-book replay
- Polymarket Telonex-backed backtests are quote-tick replay only
- taker-heavy strategies that harvest tiny price changes can look much worse
  once fees and one-tick slippage are turned on
- PMXT L2 helps with taker modeling, but robust maker realism still needs L3
  or MBO-style data

## Vendor L2 Behavior

### PMXT

- the loader prefers local filtered cache first, then raw sources in the order
  configured by the runner with `local:` and `archive:`
- for the public PMXT runners in this repo, that usually means local raw
  mirror first, then the configured remote archives
- local PMXT filtered cache is enabled by default and grows with the number of
  unique `(condition_id, token_id, hour)` tuples you replay
- `BACKTEST_ENABLE_TIMING=0` is the opt-out if you want a quieter PMXT run

### Telonex

- Telonex support is quote-tick only in this repo
- `local:` reads already-downloaded Telonex Parquet files, while `api:` uses
  the Telonex download endpoint with `TELONEX_API_KEY` from the environment
- because Telonex quote ticks do not carry full order-book depth here, the
  adapter uses the quote-tick execution profile rather than PMXT's L2 passive
  book profile
- use PMXT when a strategy depends on L2 replay behavior; use Telonex for
  quote-tick strategies where the daily Parquet source is the desired input

For concrete timings and source tiers, see [Vendor Fetch Sources And
Timing](vendor-fetch-sources.md).
