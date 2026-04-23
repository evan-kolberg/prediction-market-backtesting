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
- Kalshi fee waivers are checked at fill time against the order timestamp, so a
  waiver expiring mid-window no longer zeroes fees for the full replay

## Slippage

- shared prediction-market backtests default to a custom taker fill model
- market orders get a deterministic adverse fill shifted from the last trade print
- `slippage_ticks` (default 1): shifts the synthetic book by N ticks adverse
  - Polymarket 1 tick = instrument `price_increment`
  - Kalshi 1 tick = $0.01
- `entry_slippage_pct` (default 0.0): shifts BUY fills by a percentage of the
  ask price (e.g., 0.02 on a $0.50 ask fills at $0.51)
- `exit_slippage_pct` (default 0.0): shifts SELL fills by a percentage of the
  bid price (e.g., 0.03 on a $0.50 bid fills at $0.485)
- entry and exit percentages let you model the higher cost of exiting a binary
  option position (thinner book, more urgency) versus entering
- entry vs exit is inferred from repo-owned order tags and `reduce_only`
  first, with order side only as a fallback; this keeps long exits and future
  short-cover flows on the correct slippage path
- when both tick-based and percentage-based slippage are non-zero, they stack
  additively: fill price = ask + tick_shift + pct_shift (for buys) or
  bid - tick_shift - pct_shift (for sells)
- configure these on the `ExecutionModelConfig` in your runner:
  ```python
  EXECUTION = ExecutionModelConfig(
      queue_position=False,
      slippage_ticks=1,
      entry_slippage_pct=0.02,
      exit_slippage_pct=0.03,
      latency_model=StaticLatencyConfig(...),
  )
  ```
- trade-tick market orders no longer use the last trade print size as a proxy
  for resting book depth
- when no visible book depth exists, the synthetic taker book now uses a
  finite fallback depth based on order size plus a configurable floor
  (`min_synthetic_book_size`, default `10`)
- when real visible depth exists (for example quote-tick top-of-book size), the
  repo preserves that observed liquidity instead of inflating it with the
  fallback floor
- limit orders still use Nautilus passive-book heuristics, but the repo now
  defaults touched-limit fill probability to `0.25` instead of `1.0`
- `entry_slippage_pct` and `exit_slippage_pct` are validated to stay within
  `[0.0, 1.0]` so runner configs cannot silently manufacture impossible
  prediction-market fills
- PMXT-backed Polymarket L2 backtests do not use the synthetic taker
  fill model; they replay historical `OrderBookDeltas` with `book_type=L2_MBP`
  and `liquidity_consumption=True`
- Telonex-backed Polymarket quote backtests request the `book_snapshot_full`
  channel, convert full-depth snapshots into L2 `OrderBookDeltas` plus derived
  `QuoteTick`s, and run with `book_type=L2_MBP`

## Passive Orders And Queue Position

- public PMXT and Telonex quote-tick runners enable Nautilus
  `queue_position=True` by default because they replay L2 book data
- public Kalshi and Polymarket trade-tick runners keep `queue_position=False`
  because trade prints do not provide book depth to queue against
- this is still a heuristic, not true venue queue reconstruction
- Kalshi and Polymarket trade-tick replay still uses the static latency model,
  but queue-position simulation is intentionally disabled for those runners
- PMXT and Telonex quote-tick replay can also enable queue tracking, but those
  paths replay MBP book updates and quotes rather than MBO queue events, so
  fills still depend more heavily on book-level size changes and price moves
  than on true venue priority reconstruction
- public MBP data does not expose hidden liquidity, exact priority inside a
  level, or venue-specific matching quirks

## Latency

- public runners can now attach a static Nautilus latency model through the
  runner config
- the public PMXT, Telonex, Kalshi trade-tick, and Polymarket trade-tick
  runners in this repo now ship with a static latency model enabled by default
- the current repo-layer surface is a static millisecond model with separate
  base, insert, update, and cancel delays
- this helps test whether a market-making or quote-chasing strategy only works
  because orders are assumed to land instantly

## Limits

- repo-owned backtests keep cash-account risk checks enabled by default
- result payloads now distinguish the requested replay window from the loaded
  data window via `planned_start`, `planned_end`, `loaded_start`,
  `loaded_end`, `coverage_ratio`, and `requested_coverage_ratio`
- when a binary market resolves after the replay window, the result now keeps
  mark-to-market PnL and emits an explicit warning instead of silently applying
  post-window settlement
- when settlement is observable inside the replay window, result payloads use
  binary settlement PnL and preserve the last-tick mark as `market_exit_pnl`
- if settlement metadata exists but `simulated_through` is missing, the result
  now keeps mark-to-market PnL and emits an explicit warning instead of
  guessing that post-window settlement was observable
- empty fill sets no longer overwrite `pnl` with a synthetic settlement value
- NO-side fill events now preserve their actual side through report
  serialization so reconstructed equity curves do not silently flip sign
- Kalshi public backtests here are trade-tick replay only
- Polymarket PMXT-backed backtests are full L2 order-book replay
- Polymarket Telonex-backed backtests use full-depth Telonex book snapshots
  when available
- taker-heavy strategies that harvest tiny price changes can look much worse
  once fees and one-tick slippage are turned on
- PMXT and Telonex L2 help with taker and passive-book modeling, but robust
  maker realism still needs L3 or MBO-style data
- run outputs now also warn that replay sets are curated and that no
  portfolio-level drawdown or daily-loss circuit breaker is configured by
  default, so those limitations stay visible in normal runs

## Vendor L2 Behavior

### PMXT

- the loader prefers local filtered cache first, then raw sources in the order
  configured by the runner with `local:` and `archive:`
- for the public PMXT runners in this repo, that usually means local raw
  mirror first, then the configured remote archives
- local PMXT filtered cache is enabled by default and grows with the number of
  unique `(condition_id, token_id, hour)` tuples you replay
- `BACKTEST_ENABLE_TIMING=0` is the opt-out if you want a quieter PMXT run
- PMXT payload timestamps are converted to nanoseconds with decimal-string
  arithmetic instead of float64 multiplication, so sub-microsecond ordering is
  no longer rounded away during replay construction
- PMXT book snapshots now normalize bids/asks into the ordering expected by the
  Nautilus Polymarket schema before deriving `QuoteTick`s
- stale cross-hour PMXT payloads are ignored if their timestamp/order would
  move the replayed book backwards in time

### Telonex

- Telonex support uses the `quote_tick` runner surface, but the adapter pins
  the `book_snapshot_full` channel and replays L2 book data alongside derived
  quotes
- `local:` reads already-downloaded Telonex Parquet files, while `api:` uses
  the Telonex download endpoint with `TELONEX_API_KEY` from the environment
- Telonex quote-tick runners use a passive L2 book execution profile with
  `queue_position=True`, `liquidity_consumption=True`, and static latency
- the Telonex replay adapter now resolves the Polymarket outcome name from the
  selected instrument metadata when `replay.outcome` is omitted, so vendor
  comparisons do not silently fall back to `outcome_id=token_index`
- Telonex timestamp-ms inputs preserve fractional milliseconds when present,
  and generated delta sequences are now monotonic within each snapshot diff
- use Telonex when the full-depth daily Parquet/API source is the desired
  Polymarket input; use PMXT when you specifically want the PMXT hourly archive

For concrete timings and source tiers, see [Vendor Fetch Sources And
Timing](vendor-fetch-sources.md).
