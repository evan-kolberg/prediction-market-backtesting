# PMXT Data Fetch Sources and Timing

When running a backtest, the PMXT loader fetches historical L2 order book data one hour at a time. Each hour can come from one of six sources, tried in order:

1. **Local cache** (`~/.cache/nautilus_trader/pmxt/...`) — cached from a previous run. Sub-millisecond reads. The source column shows the full local file path.
2. **Relay prebuilt** (`https://209-209-10-83.sslip.io`) — pre-partitioned per market/token file served directly from the relay. Under 2 seconds typically.
3. **Relay raw passthrough** (`https://209-209-10-83.sslip.io/v1/raw/...`) — the hour is mirrored on the relay but not yet prebuilt for your market/token. The client downloads the raw relay parquet and filters it locally.
4. **Local raw PMXT archive** (`PMXT_LOCAL_ARCHIVE_DIR`) — a locally mirrored raw PMXT hour file. The loader filters it client-side without touching the network.
5. **Raw PMXT archive** (`https://r2.pmxt.dev`) — the hour isn't available from the relay and isn't available in your local mirror, so the loader falls back to the raw PMXT archive at `r2.pmxt.dev`. Downloads the full hour file and filters client-side.
6. **None** — the hour doesn't exist on any source (future hour, no data yet). Returns 0 rows.

If a source fails or returns nothing, the loader falls through to the next one. After a successful fetch from sources 2-5, the result is written to the local cache so subsequent runs are instant.

Caching is enabled by default at `~/.cache/nautilus_trader/pmxt/`. To disable it, set `PMXT_DISABLE_CACHE=true`. If you already have a local raw mirror, set `PMXT_LOCAL_ARCHIVE_DIR=/path/to/raw-hours` to keep that disk-local tier in the fallback chain before `r2.pmxt.dev`.

Hours print in completion order (not chronological) because they're fetched concurrently with 16 workers.

## Example output

This is real output from a fixed-window 120-hour deep value backtest on the `will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026` market, run on 2026-03-24. It shows four of the six source tiers in a single run:

```
Loading PMXT Polymarket market will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026 (token_index=0, window_start=2026-03-19T07:35:57.277659+00:00, window_end=2026-03-24T07:35:57.277659+00:00, window_hours=120.0)...
  2026-03-19T11:00:00+00:00   0.001s     214 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T11.parquet
  2026-03-19T10:00:00+00:00   0.002s     207 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T10.parquet
  2026-03-19T13:00:00+00:00   0.001s     341 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T13.parquet
  2026-03-19T12:00:00+00:00   0.002s     233 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T12.parquet
  2026-03-19T07:00:00+00:00   0.003s    1034 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T07.parquet
  2026-03-19T06:00:00+00:00   0.003s     330 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T06.parquet
  2026-03-19T09:00:00+00:00   0.002s     576 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T09.parquet
  2026-03-19T08:00:00+00:00   0.002s     830 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T08.parquet
  2026-03-19T18:00:00+00:00   0.005s      74 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T18.parquet
  2026-03-19T15:00:00+00:00   0.005s    2210 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T15.parquet
  2026-03-19T14:00:00+00:00   0.005s     138 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T14.parquet
  2026-03-19T21:00:00+00:00   0.004s     226 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T21.parquet
  2026-03-19T20:00:00+00:00   0.005s      93 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T20.parquet
  2026-03-19T19:00:00+00:00   0.005s      42 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T19.parquet
  2026-03-19T16:00:00+00:00   0.006s    1662 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T16.parquet
  2026-03-19T17:00:00+00:00   0.005s      79 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T17.parquet
  2026-03-19T23:00:00+00:00   0.010s     200 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T23.parquet
  2026-03-19T22:00:00+00:00   0.017s     942 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-19T22.parquet
  2026-03-20T00:00:00+00:00   0.017s     530 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T00.parquet
  2026-03-20T01:00:00+00:00   0.042s     363 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T01.parquet
  2026-03-20T07:00:00+00:00   0.028s     610 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T07.parquet
  2026-03-20T02:00:00+00:00   0.039s     207 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T02.parquet
  2026-03-20T06:00:00+00:00   0.038s      73 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T06.parquet
  2026-03-20T11:00:00+00:00   0.040s      71 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T11.parquet
  2026-03-20T12:00:00+00:00   0.049s     392 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T12.parquet
  2026-03-20T13:00:00+00:00   0.049s     358 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T13.parquet
  2026-03-20T08:00:00+00:00   0.042s    3201 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T08.parquet
  2026-03-20T10:00:00+00:00   0.049s     292 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T10.parquet
  2026-03-20T09:00:00+00:00   0.049s     516 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-20T09.parquet
  2026-03-20T18:00:00+00:00   0.532s      25 rows  https://209-209-10-83.sslip.io
  2026-03-20T14:00:00+00:00   0.578s     183 rows  https://209-209-10-83.sslip.io
  2026-03-20T16:00:00+00:00   0.575s     161 rows  https://209-209-10-83.sslip.io
  2026-03-20T17:00:00+00:00   0.552s     396 rows  https://209-209-10-83.sslip.io
  2026-03-20T15:00:00+00:00   0.709s     390 rows  https://209-209-10-83.sslip.io
  2026-03-20T03:00:00+00:00  41.513s    2443 rows  https://r2.pmxt.dev
  2026-03-20T19:00:00+00:00   0.646s      33 rows  https://209-209-10-83.sslip.io
  2026-03-20T05:00:00+00:00  45.111s      91 rows  https://r2.pmxt.dev
  2026-03-20T04:00:00+00:00  47.286s     148 rows  https://r2.pmxt.dev
  2026-03-21T09:00:00+00:00   1.054s     145 rows  https://209-209-10-83.sslip.io
  2026-03-21T04:00:00+00:00   1.160s     300 rows  https://209-209-10-83.sslip.io
  2026-03-21T02:00:00+00:00   1.155s       7 rows  https://209-209-10-83.sslip.io
  2026-03-21T11:00:00+00:00   1.152s      17 rows  https://209-209-10-83.sslip.io
  2026-03-21T07:00:00+00:00   1.153s     120 rows  https://209-209-10-83.sslip.io
  2026-03-21T03:00:00+00:00   1.165s     227 rows  https://209-209-10-83.sslip.io
  2026-03-21T00:00:00+00:00   1.183s     582 rows  https://209-209-10-83.sslip.io
  2026-03-21T05:00:00+00:00   1.165s      91 rows  https://209-209-10-83.sslip.io
  2026-03-21T10:00:00+00:00   1.211s     147 rows  https://209-209-10-83.sslip.io
  2026-03-21T06:00:00+00:00   1.216s     458 rows  https://209-209-10-83.sslip.io
  2026-03-21T08:00:00+00:00   1.221s     202 rows  https://209-209-10-83.sslip.io
  2026-03-20T22:00:00+00:00   1.303s     687 rows  https://209-209-10-83.sslip.io
  2026-03-20T23:00:00+00:00   1.309s    2463 rows  https://209-209-10-83.sslip.io
  2026-03-20T20:00:00+00:00   1.361s    1342 rows  https://209-209-10-83.sslip.io
  2026-03-20T21:00:00+00:00   1.412s    2961 rows  https://209-209-10-83.sslip.io
  2026-03-21T01:00:00+00:00   1.582s      17 rows  https://209-209-10-83.sslip.io
  2026-03-21T15:00:00+00:00   0.758s      23 rows  https://209-209-10-83.sslip.io
  2026-03-21T13:00:00+00:00   0.792s      60 rows  https://209-209-10-83.sslip.io
  2026-03-21T14:00:00+00:00   0.799s      48 rows  https://209-209-10-83.sslip.io
  2026-03-21T12:00:00+00:00   0.893s      29 rows  https://209-209-10-83.sslip.io
  2026-03-22T06:00:00+00:00   0.004s      56 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T06.parquet
  2026-03-22T07:00:00+00:00   0.003s      55 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T07.parquet
  2026-03-21T18:00:00+00:00   0.689s      27 rows  https://209-209-10-83.sslip.io
  2026-03-21T20:00:00+00:00   0.691s      34 rows  https://209-209-10-83.sslip.io
  2026-03-22T00:00:00+00:00   0.717s      52 rows  https://209-209-10-83.sslip.io
  2026-03-21T19:00:00+00:00   0.734s      19 rows  https://209-209-10-83.sslip.io
  2026-03-21T21:00:00+00:00   0.727s      35 rows  https://209-209-10-83.sslip.io
  2026-03-22T01:00:00+00:00   0.722s      23 rows  https://209-209-10-83.sslip.io
  2026-03-21T23:00:00+00:00   0.727s      27 rows  https://209-209-10-83.sslip.io
  2026-03-22T02:00:00+00:00   0.738s      26 rows  https://209-209-10-83.sslip.io
  2026-03-21T16:00:00+00:00   0.938s      35 rows  https://209-209-10-83.sslip.io
  2026-03-22T03:00:00+00:00   0.782s      49 rows  https://209-209-10-83.sslip.io
  2026-03-22T08:00:00+00:00   0.002s      75 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T08.parquet
  2026-03-21T17:00:00+00:00   0.799s      31 rows  https://209-209-10-83.sslip.io
  2026-03-22T10:00:00+00:00   0.002s      28 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T10.parquet
  2026-03-22T11:00:00+00:00   0.002s      38 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T11.parquet
  2026-03-22T09:00:00+00:00   0.002s      44 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T09.parquet
  2026-03-22T12:00:00+00:00   0.002s      45 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T12.parquet
  2026-03-22T13:00:00+00:00   0.002s      28 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T13.parquet
  2026-03-21T22:00:00+00:00   0.799s      27 rows  https://209-209-10-83.sslip.io
  2026-03-22T17:00:00+00:00   0.001s      31 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T17.parquet
  2026-03-22T15:00:00+00:00   0.002s      38 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T15.parquet
  2026-03-22T18:00:00+00:00   0.001s      42 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T18.parquet
  2026-03-22T14:00:00+00:00   0.002s      20 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T14.parquet
  2026-03-22T19:00:00+00:00   0.001s      20 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T19.parquet
  2026-03-22T16:00:00+00:00   0.002s      67 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T16.parquet
  2026-03-22T04:00:00+00:00   0.419s      35 rows  https://209-209-10-83.sslip.io
  2026-03-22T05:00:00+00:00   0.420s      26 rows  https://209-209-10-83.sslip.io
  2026-03-23T04:00:00+00:00   0.008s      33 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T04.parquet
  2026-03-23T03:00:00+00:00   0.009s      42 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T03.parquet
  2026-03-22T20:00:00+00:00   0.009s     128 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T20.parquet
  2026-03-23T00:00:00+00:00   0.016s      37 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T00.parquet
  2026-03-23T11:00:00+00:00   0.006s      28 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T11.parquet
  2026-03-23T07:00:00+00:00   0.009s      25 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T07.parquet
  2026-03-23T05:00:00+00:00   0.010s      27 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T05.parquet
  2026-03-23T02:00:00+00:00   0.010s      21 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T02.parquet
  2026-03-22T21:00:00+00:00   0.018s     104 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T21.parquet
  2026-03-22T22:00:00+00:00   0.018s      78 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T22.parquet
  2026-03-23T06:00:00+00:00   0.013s      12 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T06.parquet
  2026-03-23T01:00:00+00:00   0.016s      27 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T01.parquet
  2026-03-22T23:00:00+00:00   0.018s      30 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-22T23.parquet
  2026-03-23T08:00:00+00:00   0.013s      26 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T08.parquet
  2026-03-23T09:00:00+00:00   0.013s      28 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T09.parquet
  2026-03-23T10:00:00+00:00   0.014s      63 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T10.parquet
  2026-03-23T12:00:00+00:00   0.013s      44 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T12.parquet
  2026-03-23T16:00:00+00:00   0.007s      50 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T16.parquet
  2026-03-24T00:00:00+00:00   0.006s      37 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-24T00.parquet
  2026-03-23T15:00:00+00:00   0.010s      48 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T15.parquet
  2026-03-23T23:00:00+00:00   0.005s      28 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T23.parquet
  2026-03-23T14:00:00+00:00   0.008s      49 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T14.parquet
  2026-03-23T20:00:00+00:00   0.007s     211 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T20.parquet
  2026-03-23T22:00:00+00:00   0.007s      45 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T22.parquet
  2026-03-23T21:00:00+00:00   0.007s      38 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T21.parquet
  2026-03-23T17:00:00+00:00   0.008s      19 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T17.parquet
  2026-03-24T01:00:00+00:00   0.006s      40 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-24T01.parquet
  2026-03-23T18:00:00+00:00   0.008s      89 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T18.parquet
  2026-03-24T03:00:00+00:00   0.002s      65 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-24T03.parquet
  2026-03-24T02:00:00+00:00   0.003s      58 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-24T02.parquet
  2026-03-23T13:00:00+00:00   0.008s     123 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T13.parquet
  2026-03-23T19:00:00+00:00   0.008s      34 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-23T19.parquet
  2026-03-24T06:00:00+00:00   0.009s      40 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-24T06.parquet
  2026-03-24T05:00:00+00:00   0.009s      34 rows  /Users/evankolberg/.cache/nautilus_trader/pmxt/0xe9674aadfabfa7171bcee4e38b66f1a7d7e0af538d3718301e7b3cf111e9b5dd/69497259499564904327437411778598106057943098225196700538590387556685558432215/polymarket_orderbook_2026-03-24T05.parquet
  2026-03-24T07:00:00+00:00   0.404s       0 rows  none
Fetching hours: 100%|██████████████████████████████████████| 122/122 [00:50<00:00]

Market                                                                  Quotes  Fills   PnL (USDC)
will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026      32281      2      -1.4700

Total wall time: 54.36s
```

122 hours fetched in ~54 seconds. The three hours that hit `r2.pmxt.dev` (41-47s each) dominated the total time. Everything else was either cached locally (<0.05s) or served from the relay's prebuilt files (<1.6s). On a second run, every hour comes from cache and the same backtest completes in under a second.

## Timing expectations by source

| Source | Typical time | When it happens |
|---|---|---|
| Local cache | <0.05s | Second run onward (same market/token/hour) |
| Relay prebuilt | 0.4-1.6s | Hour has been fully prebuilt on the relay |
| Relay raw passthrough | network bound | Hour is mirrored on the relay but not yet prebuilt for your market/token |
| Local raw PMXT archive | local disk bound | You mirrored raw PMXT hours locally and set `PMXT_LOCAL_ARCHIVE_DIR` |
| Raw PMXT archive (r2.pmxt.dev) | 30-50s | Hour missing from relay sources, so the client falls back to the upstream raw archive |
| None | <1s | Hour doesn't exist yet |

## How to see this output

The timing instrumentation is enabled by default in `make backtest`.
Turn it off explicitly with `BACKTEST_ENABLE_TIMING=0`, or run the timing
harness standalone against any backtest file:

```bash
BACKTEST_ENABLE_TIMING=0 make backtest
```

Or:

```bash
uv run python backtests/_shared/_timing_test.py backtests/polymarket_quote_tick/polymarket_pmxt_relay_ema_crossover.py
```

The PMXT single-market relay runners are pinned in code to one known-good
historical slice:

```bash
uv run python backtests/_shared/_timing_test.py backtests/polymarket_quote_tick/polymarket_pmxt_relay_breakout.py
```

Today that slice is `will-openai-launch-a-new-consumer-hardware-product-by-march-31-2026`
from `2026-03-19T07:35:57.277659Z` through `2026-03-24T07:35:57.277659Z`
(120 hours).
