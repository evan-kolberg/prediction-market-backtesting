# Execution Modeling

Backtests in this repo are designed around maximum replay realism with the data
we actually have. The active Polymarket path is L2 market-by-price book replay:
strategies consume book state, Nautilus maintains an `L2_MBP` order book, and
real trade ticks are included only as execution evidence for matching.

Nautilus documents the relevant behavior in its backtesting guide:
<https://nautilustrader.io/docs/latest/concepts/backtesting/>. The important
repo-level interpretation is:

- `QuoteTick` is not a valid L2 replay input here. Nautilus ignores quote ticks
  for `BookType.L2_MBP` book updates.
- `OrderBookDeltas` update the L2 book.
- `TradeTick` records trigger matching and queue-position updates when
  `trade_execution=True`.
- Strategies should not subscribe to trade ticks for signals in public runners.
  Trade prints are execution evidence, not the strategy data feed.

## Fees

- Polymarket uses the current taker fee curve from venue metadata, plus CLOB
  fee-rate enrichment when the market payload itself still reports zero fees.
- Polymarket maker fees are treated as zero.
- Polymarket maker rebates are modeled for passive limit-order fills as a
  negative commission. The credit uses the same fee-equivalent curve as taker
  fees, then applies the documented rebate share: 20% for crypto markets and
  25% for other fee-enabled categories.
- If a venue reports zero fees for a market, the backtest applies zero fees
  and zero maker rebates rather than inventing a fallback.
- If a fee-enabled market cannot be mapped to a documented rebate category from
  market metadata or a documented fee rate, the backtest applies no maker
  rebate for that market.
- Kalshi fee logic remains in the extension layer, but the public v3 runner
  surface is Polymarket book replay.

### Maker Rebates

Polymarket's Maker Rebates program pays daily USDC rebates to liquidity
providers whose resting orders are taken. The documented fee-equivalent value
for each filled maker order is:

```text
fee_equivalent = C x feeRate x p x (1 - p)
```

The backtest credits maker fills with:

```text
maker_rebate = fee_equivalent x maker_rebate_share
```

This is represented as a negative commission on `LIMIT` fills. It preserves the
per-fill economics of the rebate pool without pretending to know wallet-level
daily payout timing or the complete set of competing makers.

Two important realism boundaries remain:

- The Polymarket $1 minimum accrued payout threshold is not modeled because
  Nautilus fee callbacks do not maintain wallet/day-level settlement state.
- Polymarket Liquidity Rewards are separate from Maker Rebates. They depend on
  resting-order scoring, market-wide samples, min-size/max-spread configs, and
  daily reward allocations. They are not credited as PnL until the framework can
  reconstruct that market-wide state without look-ahead.

## Slippage

There are two execution paths:

- L2 book replay uses Nautilus passive-book execution. Marketable orders walk
  the replayed book, so fills can consume multiple price levels when top-of-book
  size is insufficient.
- The custom `PredictionMarketTakerFillModel` is retained for non-book adapter
  paths, but PMXT and Telonex book runners do not use it.

For book runners, the relevant engine profile is:

```python
L2_BOOK_ENGINE_PROFILE = ReplayEngineProfile(
    book_type=BookType.L2_MBP,
    fill_model_mode="passive_book",
    liquidity_consumption=True,
)
```

`PredictionMarketBacktest._build_engine()` then wires the venue with:

```python
engine.add_venue(
    ...,
    book_type=BookType.L2_MBP,
    liquidity_consumption=True,
    queue_position=execution.queue_position,
    bar_execution=False,
    trade_execution=True,
)
```

That means:

- `OrderBookDeltas` are the only records that move the displayed L2 book.
- `TradeTick` records can fill resting orders between book updates.
- Bars do not drive execution in these runners.
- Fills consume visible book liquidity until a later book update refreshes that
  level.

## Passive Orders And Queue Position

Public PMXT and Telonex book runners set `queue_position=True` because they
replay L2 book depth and real trade ticks. This is more realistic than filling
every touched limit order immediately, but it is still an MBP heuristic, not
true venue FIFO reconstruction.

For a resting `LIMIT` order, Nautilus snapshots the same-side displayed book
quantity at the order price when the order is accepted. That quantity is the
estimated queue ahead of the simulated order. Later trade ticks at that price
decrement the queue ahead; only excess traded quantity after the queue clears
can fill the simulated order.

Practical implications:

- A buy limit resting at the bid needs seller-aggressor trade prints at that
  price, or book movement that makes the order marketable, before it fills.
- A sell limit resting at the ask needs buyer-aggressor trade prints at that
  price, or a crossing book move, before it fills.
- Historical trades with `NO_AGGRESSOR` metadata can affect both sides, which
  prevents impossible queue stalls but can overstate fill probability.
- Queue position is per order and per price level. It does not model hidden
  liquidity, cancels ahead of us, pro-rata allocation, or true L3/MBO priority.

This is the best available realism level with public PMXT/Telonex L2 MBP data:
full L2 book state, liquidity consumption, real trade prints for fill evidence,
queue-position tracking, and explicit latency.

## Latency

Public runner configs use `ExecutionModelConfig` with optional
`StaticLatencyConfig`:

```python
ExecutionModelConfig(
    queue_position=True,
    latency_model=StaticLatencyConfig(
        base_latency_ms=75.0,
        insert_latency_ms=10.0,
        update_latency_ms=5.0,
        cancel_latency_ms=5.0,
    ),
)
```

Zero-latency assumptions are optimistic for CLOB strategies. Keep latency
enabled unless you are intentionally testing a lower-bound execution scenario.

## Limits

- L2 MBP is not L3 MBO. We know aggregate size at a price level, not individual
  orders or exact priority.
- Public Polymarket data does not expose hidden liquidity or all venue-specific
  matching details.
- Trade ticks improve maker-fill realism, but they only prove that liquidity
  traded at a price. They do not prove how much persistent queue was available
  immediately before or after that trade.
- Negative PnL and `AccountBalanceNegative` are not automatically bugs. They
  can be correct consequences of fees, latency, liquidity, sizing, or expected
  losing strategies.
- Result payloads keep requested-window and loaded-window metadata separate
  through `planned_start`, `planned_end`, `loaded_start`, `loaded_end`,
  `coverage_ratio`, and `requested_coverage_ratio`.

## Vendor L2 Behavior

### PMXT

- PMXT raw files are hourly Polymarket order-book archives.
- The loader filters raw rows to market and token, decodes `book_snapshot` and
  `price_change` payloads, and emits Nautilus `OrderBookDeltas`.
- A missing PMXT hour warns and resets local book state. Subsequent
  `price_change` updates are not applied across a gap until a fresh snapshot
  rebuilds the book.
- PMXT filtered cache is enabled by default at
  `~/.cache/nautilus_trader/pmxt`.
- Public runners usually try `local:/Volumes/LaCie/pmxt_data` first, then
  `archive:r2v2.pmxt.dev`, then `archive:r2.pmxt.dev`.

### Telonex

- Telonex book runners pin `book_snapshot_full`, not shallow
  `book_snapshot_5` or `book_snapshot_25`.
- Full-depth snapshots are diffed into `OrderBookDeltas` so Nautilus receives
  L2 MBP updates rather than quote ticks.
- Real Polymarket trade ticks are interleaved with Telonex book deltas for
  matching and queue-position updates.
- After the first conversion for a market/token/day/window, the loader writes a
  materialized `OrderBookDeltas` cache under `book-deltas-v1`. Warm runs can
  load `telonex-deltas-cache::...` directly and avoid re-diffing full-book
  snapshots.
- `local:/Volumes/LaCie/telonex_data` reads the Hive-partitioned blob mirror
  through the DuckDB manifest when available, selecting only the parquet parts
  needed for the requested channel, market, outcome, and date range.
- `api:` downloads daily Telonex parquet payloads and writes both the raw
  nested cache file and a faster `.fast.parquet` sidecar for subsequent runs.

For concrete source priority and timing output, see
[Vendor Fetch Sources And Timing](vendor-fetch-sources.md).
