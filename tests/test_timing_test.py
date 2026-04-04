from __future__ import annotations

from backtests._shared._timing_test import _transfer_label


def test_transfer_label_identifies_local_raw_paths() -> None:
    label = _transfer_label(
        "/Volumes/LaCie/pmxt_raws/2026/02/22/polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "local raw 2026-02-22T11"


def test_transfer_label_identifies_cache_paths() -> None:
    label = _transfer_label(
        "cache::/Users/example/.cache/nautilus_trader/pmxt/cond/token/"
        "polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "cache 2026-02-22T11"


def test_transfer_label_identifies_filtered_relay_urls() -> None:
    label = _transfer_label(
        "https://209-209-10-83.sslip.io/v1/filtered/cond/token/"
        "polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "relay filtered 2026-02-22T11"


def test_transfer_label_identifies_relay_raw_urls() -> None:
    label = _transfer_label(
        "https://209-209-10-83.sslip.io/v1/raw/2026/02/22/polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "relay raw 2026-02-22T11"


def test_transfer_label_identifies_r2_raw_urls() -> None:
    label = _transfer_label(
        "https://r2.pmxt.dev/polymarket_orderbook_2026-02-22T11.parquet"
    )

    assert label == "r2 raw 2026-02-22T11"
