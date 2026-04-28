# Polymarket Account Ledger Replay

This experiment tries to replay a public Polymarket account's filled trade
ledger inside the normal backtesting framework. It was built around the
`@beffer45` profile and wallet
`0xe29aff6a6ae1e1d6a3a1c4c904f2957afa98cda0`, using a hard-coded snapshot of
public trade rows from the Polymarket data API.

The goal is not to present a profitable strategy. The goal is to ask a stricter
realism question:

```text
If we know the trades a public account took, can the backtester reproduce the
same portfolio by replaying those decisions against the historical book?
```

The answer from this experiment is no, not exactly. That failure is still useful
because it shows where a filled account ledger stops being a strategy and starts
being a record of events that already happened inside the market data.

## Runner And Notebook

The Python runner is:

```bash
uv run python backtests/polymarket_beffer45_trade_replay_telonex.py
```

The notebook wrapper is:

```text
backtests/polymarket_beffer45_trade_replay_telonex.ipynb
```

The notebook calls the same runner and stores the latest generated HTML summary
inside the notebook output. A user who downloads the repository can open the
notebook and inspect the last saved output without rerunning the backtest,
subject to the normal notebook trust and renderer behavior of their local
Jupyter or GitHub viewer.

The runner writes:

- `output/polymarket_beffer45_trade_replay_telonex_summary.html`
- `output/polymarket_beffer45_trade_replay_telonex_comparison.csv`

Those output files are generated artifacts. The committed notebook is the
portable way to keep the last HTML view attached to the example.

## What The Strategy Does

`strategies.account_trade_replay.BookAccountTradeReplayStrategy` receives the
hard-coded public ledger rows and schedules one order at each public trade
timestamp.

Important details:

- The replay uses `BookReplay` over Telonex L2 order book data.
- It submits limit IOC orders at the public ledger price.
- It does not manufacture fills.
- It does not bypass the Nautilus execution model.
- It uses the same report and portfolio machinery as the other public runners.
- Sells are treated as reduce-only attempts so the strategy cannot invent short
  exposure that the public ledger did not show.

This is intentionally strict. A forced-fill ledger simulator would reproduce the
CSV by construction, but it would not test the backtesting engine's execution
model. The point of this runner is to let the historical book accept or reject
each attempted replay order.

## Why Exact Reproduction Fails

A public filled-trade feed is not an order-management history. It omits several
facts that matter for realistic replay.

First, the original user's trades are already reflected in the historical order
book and trade stream. If the account crossed the spread, consumed depth, or
caused a book update, that effect is part of the data the backtest later
replays. Re-submitting the same account's order into that same book is not a
clean counterfactual. It can double-count the interaction with liquidity, or it
can miss the liquidity that the original order already consumed.

Second, the public row timestamp is a fill observation, not necessarily the
decision time. A copy trader only sees the trade after it is public. Even a
zero-latency replay at the fill timestamp is already an optimistic assumption
for copying. A real copier would submit later and face a different queue,
spread, and available depth.

Third, maker fills cannot be reconstructed from filled trade rows alone. A maker
fill depends on when the resting order was placed, how it was amended or
canceled, and where it sat in queue. The public account trade row may show that
the account bought or sold, but it does not provide the missing order lifecycle.

Fourth, Polymarket markets can be thin and discontinuous. Small differences in
timestamp handling, minimum size checks, trade-tick availability, queue
position, or book reconstruction can decide whether an IOC order fills at all.
That is not noise to hide. It is exactly the kind of realism gap this framework
should surface.

Finally, settlement and mark-to-market accounting are separate from order
replay. The public profile can report settled account PnL over its own
portfolio history. The backtest report only settles instruments when the replay
horizon and metadata make that outcome available to the report without leaking
future information into the strategy.

## Copy-Trading Interpretation

This experiment is a useful warning against a naive copy-trading claim:

```text
"If a public account made these trades, replaying or copying the trades should
produce the same portfolio."
```

That statement is usually false.

A public fill is an after-the-fact event. It is not a signal that existed before
the market moved, and it is not a complete description of the order that
generated the fill. Copying a public wallet from a trade feed therefore has at
least three disadvantages:

- the copied order is later than the original order
- the original order's market impact is already embedded in the observed book
- the copier does not know the original queue position, passive order history,
  cancellations, or hidden intent

This does not prove that every account-following strategy is impossible. A
strategy that models reporting delay, liquidity decay, market selection, and
post-fill price drift could still be worth studying. What this runner shows is
that a hard-coded public ledger is not itself a tradable strategy, and exact
portfolio reproduction should not be expected from L2 replay.

## External Source Check

The realism caveats above are grounded in the public mechanics documented by
Polymarket and NautilusTrader:

- Polymarket documents separate API surfaces: the public Data API covers user
  positions, trades, activity, holders, open interest, leaderboards, and builder
  analytics, while the CLOB API covers orderbook data, pricing, and trading
  operations. Trading endpoints require authentication.
  Source: [Polymarket API Introduction](https://docs.polymarket.com/api-reference/introduction).
- Polymarket describes the CLOB as an offchain matching system with onchain
  settlement, where orders are signed and matched trades settle on Polygon.
  Source: [Polymarket Trading Overview](https://docs.polymarket.com/trading/overview).
- Polymarket's order docs say orders are expressed as limit orders. GTC and GTD
  orders rest on the book, while FOK and FAK execute against resting liquidity
  immediately. The same page documents rejections for minimum size, insufficient
  balance, tick-size mismatches, and unfilled FOK orders.
  Source: [Polymarket Create Order](https://docs.polymarket.com/trading/orders/create).
- Polymarket's authenticated user WebSocket examples include order lifecycle
  fields such as `original_size`, `size_matched`, `created_at`, `status`, and
  `order_type`, plus trade fields such as `taker_order_id`, `maker_orders`,
  `trader_side`, and `timestamp`. A public filled-trade snapshot is therefore
  not equivalent to the complete private order lifecycle.
  Source: [Polymarket User Channel](https://docs.polymarket.com/api-reference/wss/user).
- Polymarket's trade history docs distinguish open order fields from trade
  fields and show that trades have match/finality statuses and maker-side
  detail. This supports treating the public ledger as a fill record, not as a
  full order-management log.
  Source: [Polymarket Cancel Order / Trade History](https://docs.polymarket.com/trading/orders/cancel).
- NautilusTrader documents that L2/L3 book data drives book state, `TradeTick`
  can trigger matching, and historical order book/trade data is immutable in
  backtesting. It also documents that order book deltas are required for
  `L2_MBP`, and that trade ticks with L2 book data provide execution evidence
  rather than automatically filling every order.
  Source: [NautilusTrader Backtesting](https://nautilustrader.io/docs/latest/concepts/backtesting/).

The copy-trading interpretation is an inference from those documented mechanics:
if the historical replay data already contains the original account's fills and
book updates, then replaying those same fills as new orders against that history
is not the same counterfactual as having submitted the original orders with the
original queue position and timing.

## Observed Result

The checked-in snapshot contains 153 public trade rows across 86 instruments.
On the Telonex replay used for this experiment, the strict replay filled 72 of
those 153 attempted orders.

The latest run reported:

- loaded instruments: `86 / 86`
- engine fills: `72 / 153`
- ledger cash PnL on loaded instruments: `-223.8793596488217363403119430 USDC`
- instruments with resolved-outcome metadata: `86`
- instruments with settlement applied by report: `44`
- ledger settlement PnL using resolved metadata:
  `50.44854735117825935968805694 USDC`
- backtest report PnL on loaded instruments:
  `-16.2740783200000076046 USDC`
- delta between report PnL and ledger settlement metadata:
  `-66.72262567117826696428805694 USDC`

These numbers should be read as diagnostic output, not as an account audit. The
important result is the divergence: many public ledger fills cannot be replayed
as fresh IOC orders against the already-realized historical book.

## Latest Terminal Output

<div class="terminal-output-scroll">
<pre><code>──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
Market                                                   Book Events  Fills        Qty   AvgPx   Notional   PnL (USDC)    Return     MaxDD   Sharpe  Sortino      PF  Coverage
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
bitcoin-up-or-down-april-26-2026-4am-et:Up                    101833      1      14.01  0.3400       4.76      -4.6995    -0.47%    -1.18%      n/a   -15.87    0.88  +100.00%
btc-updown-15m-1777222800:Up                                   61180      2      10.45  0.2400       2.51      +0.6700    +0.07%    -0.02%      n/a      n/a    2.17  +100.00%
btc-updown-15m-1777222800:Down                                 41584      1      92.87  0.0100       0.93      -0.9103    -0.09%    -0.09%      n/a   -15.87    0.74  +100.00%
btc-updown-15m-1777249800:Down                                 71869      2     101.00  0.0100       1.01      -0.9900    -0.10%    -0.10%      n/a   -15.87    0.78   +44.08%
btc-updown-5m-1776990300:Up                                    69850      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1776990600:Up                                   108375      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1776990900:Up                                    51924      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777056600:Up                                   171695      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777057500:Up                                   113222      1      52.82  0.6300      33.28     -33.0298    -3.30%    -3.38%      n/a   -15.87    0.81  +100.00%
btc-updown-5m-1777119000:Up                                   127051      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777119000:Down                                 127460      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777119300:Up                                   129809      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777119900:Up                                    93564      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777119900:Down                                  93557      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777126800:Up                                    90322      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777129200:Down                                 121662      1      31.29  0.0100       0.31      -0.3067    -0.03%    -2.98%      n/a      n/a    1.01  +100.00%
btc-updown-5m-1777129500:Up                                    99228      1       5.00  0.1400       0.70      -0.6880    -0.07%    -0.15%      n/a   -15.87    0.90  +100.00%
btc-updown-5m-1777134000:Up                                   124347      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777134300:Down                                 144982      1      21.37  0.8100      17.31      +4.1261    +0.41%    -0.35%      n/a      n/a    1.35  +100.00%
btc-updown-5m-1777135500:Up                                   170536      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777135800:Up                                   162715      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777137000:Down                                 132476      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777159800:Down                                 148439      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777170300:Up                                   112558      1      10.38  0.0800       0.83      -0.8154    -0.08%    -0.58%      n/a   -15.87    0.98  +100.00%
btc-updown-5m-1777170600:Up                                   115574      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777171200:Up                                    94273      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777171500:Down                                 118028      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777171800:Up                                   113832      1      20.89  0.4019       8.40     +12.5968    +1.26%    -0.84%      n/a      n/a    1.29  +100.00%
btc-updown-5m-1777172100:Up                                   115178      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777172100:Down                                 115159      1      35.71  0.0100       0.36      -0.3500    -0.04%    -0.14%      n/a   -15.87    0.76  +100.00%
btc-updown-5m-1777172400:Up                                    93492      1       8.51  0.1100       0.94      -0.9193    -0.09%    -0.12%      n/a   -15.87    0.63  +100.00%
btc-updown-5m-1777173000:Up                                   102545      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777173600:Up                                   117590      1       8.95  0.5400       4.83      -4.7899    -0.48%    -0.69%      n/a   -15.87    0.90  +100.00%
btc-updown-5m-1777173900:Up                                   104785      1      11.67  0.0800       0.93      -0.9166    -0.09%    -0.12%      n/a   -15.87    0.85  +100.00%
btc-updown-5m-1777173900:Down                                 104791      1      10.91  0.4376       4.77      +6.1874    +0.62%    -0.09%      n/a      n/a    1.27  +100.00%
btc-updown-5m-1777174200:Up                                   119613      1      15.45  0.0600       0.93      -0.9095    -0.09%    -0.09%      n/a   -15.87    0.56  +100.00%
btc-updown-5m-1777174500:Up                                   135045      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777174500:Down                                 135071      1      16.45  0.5500       9.05      +7.4834    +0.75%    -0.55%      n/a      n/a    1.08  +100.00%
btc-updown-5m-1777174800:Down                                 100761      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777175100:Up                                   116283      1      15.38  0.2700       4.15      -4.0913    -0.41%    -1.51%      n/a   -15.87    0.94  +100.00%
btc-updown-5m-1777175100:Down                                 116383      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777175400:Up                                   122794      1      29.36  0.1300       3.82      -3.7504    -0.38%    -2.16%      n/a   -15.87    0.97  +100.00%
btc-updown-5m-1777175700:Up                                   134559      3      30.05  0.4537      13.63     +16.5576    +1.66%    -1.92%      n/a      n/a    1.09  +100.00%
btc-updown-5m-1777175700:Down                                 134564      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777176000:Down                                 104622      1       8.51  0.0900       0.77      +7.7566    +0.78%    -0.62%      n/a      n/a    1.20  +100.00%
btc-updown-5m-1777176600:Up                                   116945      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777177800:Up                                    92877      1      33.50  0.1400       4.69      -4.6097    -0.46%    -0.64%      n/a   -15.87    0.83  +100.00%
btc-updown-5m-1777178100:Down                                  91779      1      16.66  0.0400       0.67      -0.6536    -0.07%    -0.24%      n/a   -15.87    0.91  +100.00%
btc-updown-5m-1777178400:Down                                  96229      2      14.42  0.1350       1.95      +0.2499    +0.02%    -0.04%      n/a      n/a    1.13  +100.00%
btc-updown-5m-1777178700:Down                                  91725      2      58.14  0.0240       1.40      -1.3691    -0.14%    -0.19%      n/a   -15.87    0.66  +100.00%
btc-updown-5m-1777179600:Up                                    95365      3      67.01  0.1400       9.38      +0.8309    +0.08%    -0.18%      n/a      n/a    1.26  +100.00%
btc-updown-5m-1777179900:Down                                  90094      2      62.01  0.0200       1.24      -0.5960    -0.06%    -0.06%      n/a   -15.87    0.23  +100.00%
btc-updown-5m-1777180200:Up                                    83488      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777180500:Up                                   121917      2      92.94  0.3050      28.35     +26.7309    +2.67%    -1.23%      n/a      n/a    1.33  +100.00%
btc-updown-5m-1777180800:Up                                   100572      3      90.27  0.1588      14.34     -14.1513    -1.42%    -1.50%      n/a   -15.87    0.67  +100.00%
btc-updown-5m-1777181100:Up                                    93236      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777181100:Down                                  93238      2      31.99  0.3325      10.64     -10.5400    -1.05%    -1.06%      n/a   -15.87    0.79  +100.00%
btc-updown-5m-1777181400:Up                                   115139      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777181700:Up                                    99565      1      20.46  0.4700       9.62      -9.5164    -0.95%    -0.96%      n/a   -15.87    0.85  +100.00%
btc-updown-5m-1777183500:Up                                    92191      2      12.51  0.0900       1.13      -0.4808    -0.05%    -0.05%      n/a   -15.87    0.48  +100.00%
btc-updown-5m-1777183500:Down                                  92202      2      27.21  0.7750      21.09      +1.5917    +0.16%    -0.14%      n/a      n/a    1.37  +100.00%
btc-updown-5m-1777189500:Up                                   104196      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777189800:Down                                  88289      1      58.36  0.0800       4.67      -4.5829    -0.46%    -0.52%      n/a   -15.87    0.61  +100.00%
btc-updown-5m-1777222500:Up                                    89040      1      46.47  0.0200       0.93      -0.9112    -0.09%    -0.12%      n/a   -15.87    0.61  +100.00%
btc-updown-5m-1777223700:Up                                   108963      2      20.33  0.5300      10.77      +1.7277    +0.17%    -0.29%      n/a      n/a    1.19  +100.00%
btc-updown-5m-1777224000:Up                                   114377      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777239600:Up                                    92689      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777239600:Down                                  92689      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777239900:Up                                   114596      2      44.85  0.1520       6.82      -2.0408    -0.20%    -0.24%      n/a   -15.87    0.41  +100.00%
btc-updown-5m-1777239900:Down                                 114859      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777240200:Up                                   107984      2      17.90  0.5250       9.40      +0.0010    +0.00%    -0.03%      n/a      n/a    1.00  +100.00%
btc-updown-5m-1777240200:Down                                 107981      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
btc-updown-5m-1777240500:Down                                 127097      1     116.02  0.1659      19.25     -18.9275    -1.89%    -7.09%      n/a   -15.87    0.99  +100.00%
btc-updown-5m-1777240800:Up                                   135036      2     102.22  0.0100       1.02      -1.0020    -0.10%    -0.22%      n/a   -15.87    0.55  +100.00%
btc-updown-5m-1777240800:Down                                 135000      1      23.02  0.8600      19.80      +3.2784    +0.33%    -1.00%      n/a      n/a    1.07  +100.00%
btc-updown-5m-1777241100:Up                                    67900      2      47.76  0.8400      40.12      +2.9905    +0.30%    -0.23%      n/a      n/a    1.24  +100.00%
btc-updown-5m-1777241100:Down                                  67771      1      77.69  0.0400       3.11      -3.0481    -0.30%    -0.47%      n/a   -15.87    0.76  +100.00%
btc-updown-5m-1777241400:Down                                 102409      2      15.72  0.0992       1.56      -1.5308    -0.15%    -0.55%      n/a   -15.87    0.93  +100.00%
btc-updown-5m-1777249800:Down                                 151613      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a   +55.59%
cricipl-del-pun-2026-04-25:Punjab Kings                       182587      2      14.98  0.5645       8.46      +6.5315    +0.65%    -0.29%      n/a      n/a    1.23  +100.00%
cricipl-luc-kol-2026-04-26:Lucknow Super Giants               105152      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a   +57.62%
cricipl-luc-kol-2026-04-26:Kolkata Knight Riders              151976      2      80.24  0.8950      71.82      +3.7953    +0.38%    -0.60%      n/a      n/a    1.22   +59.76%
cricipl-raj-sun-2026-04-25:Rajasthan Royals                    31606      1       8.11  0.1200       0.97      +0.0214    -0.09%    -0.14%      n/a   -15.87    0.87  +100.00%
cricipl-roy-guj-2026-04-24:Royal Challengers Bangalore        261231      2      56.51  0.7945      44.89     +11.7258    +1.17%    -0.65%      n/a      n/a    1.17  +100.00%
eth-updown-5m-1777170900:Up                                    39922      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
eth-updown-5m-1777171200:Down                                  39194      0       0.00     n/a       0.00      +0.0000    +0.00%    -0.00%      n/a      n/a     n/a  +100.00%
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
TOTAL                                                        9385899     72    1808.34  0.2556     462.27     -16.2741    -1.73%    -9.28%    -0.20    -0.27    1.00   +97.87%
──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────

Portfolio run stats: Iterations: 9,500,965 | Events: 410 | Orders: 153 | Positions: 48 | Elapsed: 260867.127s
Portfolio return stats: Sharpe Ratio (252 days): -10.87 | Sortino Ratio (252 days): -10.2 | Profit Factor: 0 | Risk Return Ratio: -0.6849 | Returns Volatility (252 days): 1.017 | Average (Return): -0.04387
Portfolio PnL stats (USDC.e): PnL (total): -148.5 | PnL% (total): -14.85 | Win Rate: 0.9167 | Expectancy: 1.164 | Avg Winner: 1.341 | Avg Loser: -0.7787

WARNING: bitcoin-up-or-down-april-26-2026-4am-et:Up: Replay selection is explicitly curated from named markets and may exclude cancelled, delisted, or zero-liquidity markets.
WARNING: bitcoin-up-or-down-april-26-2026-4am-et:Up: No portfolio-level drawdown or daily-loss circuit breaker is configured for this run.
WARNING: cricipl-del-pun-2026-04-25:Punjab Kings: Settlement outcome exists after the replay window; keeping mark-to-market PnL instead of resolved settlement because resolution was not observable by 2026-04-27T00:46:48.152000+00:00 (observable at 2026-05-02T00:00:00+00:00).
WARNING: cricipl-luc-kol-2026-04-26:Lucknow Super Giants: Settlement outcome exists after the replay window; keeping mark-to-market PnL instead of resolved settlement because resolution was not observable by 2026-04-27T00:46:48.152000+00:00 (observable at 2026-05-03T00:00:00+00:00).
WARNING: cricipl-luc-kol-2026-04-26:Kolkata Knight Riders: Settlement outcome exists after the replay window; keeping mark-to-market PnL instead of resolved settlement because resolution was not observable by 2026-04-27T00:46:48.152000+00:00 (observable at 2026-05-03T00:00:00+00:00).
WARNING: cricipl-raj-sun-2026-04-25:Rajasthan Royals: Settlement outcome exists after the replay window; keeping mark-to-market PnL instead of resolved settlement because resolution was not observable by 2026-04-27T00:46:48.152000+00:00 (observable at 2026-05-02T00:00:00+00:00).
WARNING: cricipl-roy-guj-2026-04-24:Royal Challengers Bangalore: Settlement outcome exists after the replay window; keeping mark-to-market PnL instead of resolved settlement because resolution was not observable by 2026-04-27T00:46:48.152000+00:00 (observable at 2026-05-01T00:00:00+00:00).

Summary report saved to /Users/evankolberg/prediction-market-backtesting/output/polymarket_beffer45_trade_replay_telonex_summary.html
Backtest vs hard-coded ledger comparison
  loaded instruments: 86 / 86
  engine fills: 72 / 153 ledger trades
  ledger cash PnL on loaded instruments: -223.8793596488217363403119430 USDC
  instruments with resolved-outcome metadata: 86
  instruments with settlement applied by report: 44
  ledger settlement PnL using resolved metadata: 50.44854735117825935968805694 USDC
  backtest report PnL on loaded instruments: -16.2740783200000076046 USDC
  delta report - ledger settlement metadata: -66.72262567117826696428805694 USDC
  comparison CSV: output/polymarket_beffer45_trade_replay_telonex_comparison.csv
  summary HTML: output/polymarket_beffer45_trade_replay_telonex_summary.html

Total wall time: 310.47s
(base) evankolberg@Evans-MacBook-Pro-2 prediction-market-backtesting %</code></pre>
</div>

## How To Use This Experiment

Use this runner as a regression and realism harness:

- to check that account-ledger replay orders go through the same execution path
  as other strategies
- to inspect which public fills are accepted or rejected by the historical book
- to compare account-level public ledger accounting against backtest accounting
- to test notebook output persistence for HTML summary reports
- to reason about copy-trading assumptions without changing core engine
  behavior

Do not use it as proof that the account's profile PnL is wrong, and do not use
it as proof that the backtester should force fills to match a public portfolio.
The mismatch is the point. It identifies information that a filled trade ledger
does not contain and market impact that historical data has already absorbed.
