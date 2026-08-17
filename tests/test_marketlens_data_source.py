from __future__ import annotations

import json
import threading
import time
from email.message import Message
from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError

import pandas as pd
import pytest
from nautilus_trader.adapters.polymarket.common.parsing import parse_polymarket_instrument
from nautilus_trader.model.enums import AggressorSide, BookAction, OrderSide, RecordFlag

import prediction_market_extensions.backtesting.data_sources.marketlens as marketlens_module
from prediction_market_extensions._runtime_log import capture_loader_events
from prediction_market_extensions.backtesting.data_sources.marketlens import (
    MARKETLENS_API_KEY_ENV,
    MARKETLENS_API_WORKERS_ENV,
    MARKETLENS_BASE_URL_ENV,
    MARKETLENS_CACHE_ROOT_ENV,
    RunnerPolymarketMarketlensBookDataLoader,
    configured_marketlens_data_source,
    resolve_marketlens_loader_config,
)

_CONDITION_ID = "0x" + "1" * 64
_MARKET_UUID = "11111111-2222-3333-4444-555555555555"


def _make_marketlens_loader(*, token_index: int = 0) -> RunnerPolymarketMarketlensBookDataLoader:
    outcome = "Yes" if token_index == 0 else "No"
    instrument = parse_polymarket_instrument(
        market_info={
            "condition_id": _CONDITION_ID,
            "question": "Synthetic Marketlens market",
            "minimum_tick_size": "0.01",
            "minimum_order_size": "1",
            "end_date_iso": "2026-12-31T00:00:00Z",
            "maker_base_fee": "0",
            "taker_base_fee": "0",
        },
        token_id=str(2 + token_index) * 64,
        outcome=outcome,
        ts_init=0,
    )
    loader = RunnerPolymarketMarketlensBookDataLoader.__new__(
        RunnerPolymarketMarketlensBookDataLoader
    )
    loader._instrument = instrument
    loader._token_id = str(2 + token_index) * 64
    loader._condition_id = _CONDITION_ID
    loader._marketlens_market_slug = "synthetic-marketlens-market"
    loader._marketlens_token_index = token_index
    loader._marketlens_market_record = {"id": _MARKET_UUID, "collection_tier": "streamed"}
    loader._marketlens_day_trade_ticks = {}
    return loader


def test_marketlens_sources_resolve_from_env_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(MARKETLENS_API_KEY_ENV, "mk_test")
    monkeypatch.delenv(MARKETLENS_BASE_URL_ENV, raising=False)
    _selection, config = resolve_marketlens_loader_config()

    assert len(config.ordered_source_entries) == 1
    entry = config.ordered_source_entries[0]
    assert entry.kind == "api"
    assert entry.target == "https://api.marketlens.trade/v1"
    assert entry.api_key == "mk_test"


def test_marketlens_sources_expand_env_placeholders(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MARKETLENS_TEST_KEY", "mk_inline")
    _selection, config = resolve_marketlens_loader_config(sources=("api:${MARKETLENS_TEST_KEY}",))

    assert config.ordered_source_entries[0].api_key == "mk_inline"


def test_marketlens_sources_accept_base_url_override() -> None:
    _selection, config = resolve_marketlens_loader_config(sources=("api:https://staging.host",))

    entry = config.ordered_source_entries[0]
    assert entry.target == "https://staging.host"
    assert entry.api_key is None


def test_marketlens_sources_reject_unknown_prefix() -> None:
    with pytest.raises(ValueError, match="Use api:"):
        resolve_marketlens_loader_config(sources=("local:/tmp/data",))


def test_marketlens_sources_require_key_or_sources(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(MARKETLENS_API_KEY_ENV, raising=False)
    with pytest.raises(ValueError, match=MARKETLENS_API_KEY_ENV):
        resolve_marketlens_loader_config()


def test_marketlens_summary_masks_key_and_lists_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(MARKETLENS_CACHE_ROOT_ENV, raising=False)
    with configured_marketlens_data_source(sources=("api:mk_secret",)) as selection:
        assert "mk_secret" not in selection.summary
        assert "cache" in selection.summary
        assert "api https://api.marketlens.trade/v1 (key set)" in selection.summary


def test_marketlens_api_slot_respects_global_worker_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(MARKETLENS_API_WORKERS_ENV, "2")
    monkeypatch.setattr(marketlens_module, "_MARKETLENS_API_SEMAPHORE", None)
    active = 0
    max_active = 0
    lock = threading.Lock()

    def _worker() -> None:
        nonlocal active, max_active
        with marketlens_module._marketlens_api_slot():
            with lock:
                active += 1
                max_active = max(max_active, active)
            try:
                time.sleep(0.02)
            finally:
                with lock:
                    active -= 1

    threads = [threading.Thread(target=_worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=1)

    assert max_active == 2


_T0_MS = 1_786_708_800_000  # 2026-08-14T12:00:00Z


def _snapshot(
    t: int,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    *,
    is_reseed: bool = False,
) -> dict:
    return {
        "type": "snapshot",
        "t": t,
        "is_reseed": is_reseed,
        "bids": [{"price": price, "size": size} for price, size in bids],
        "asks": [{"price": price, "size": size} for price, size in asks],
    }


def _delta(t: int, price: float, size: float, side: str) -> dict:
    return {"type": "delta", "t": t, "price": price, "size": size, "side": side}


def _trade(t: int, price: float, size: float, side: str, trade_id: str = "01TRADE") -> dict:
    return {"type": "trade", "t": t, "id": trade_id, "price": price, "size": size, "side": side}


def _flatten(record) -> list[tuple[int, int, float, float]]:
    return [
        (
            int(delta.action),
            int(delta.order.side),
            float(delta.order.price),
            float(delta.order.size),
        )
        for delta in record.deltas
    ]


def test_marketlens_convert_emits_anchor_ladder_and_groups_deltas() -> None:
    loader = _make_marketlens_loader()
    events = [
        _snapshot(_T0_MS - 30_000, [(0.40, 10.0), (0.39, 5.0)], [(0.42, 7.0)]),
        _delta(_T0_MS + 1_000, 0.41, 3.0, "BUY"),
        _delta(_T0_MS + 1_000, 0.42, 0.0, "SELL"),
        _delta(_T0_MS + 2_000, 0.39, 0.0, "BUY"),
    ]
    records, trades = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=0,
    )

    assert trades == []
    assert len(records) == 3
    ladder = _flatten(records[0])
    assert ladder[0][0] == int(BookAction.CLEAR)
    assert (int(BookAction.ADD), int(OrderSide.BUY), 0.40, 10.0) in ladder
    assert (int(BookAction.ADD), int(OrderSide.SELL), 0.42, 7.0) in ladder
    assert int(records[0].deltas[-1].flags) == int(RecordFlag.F_LAST)
    assert all(int(delta.sequence) == 0 for delta in records[0].deltas)

    first_group = _flatten(records[1])
    assert first_group == [
        (int(BookAction.UPDATE), int(OrderSide.BUY), 0.41, 3.0),
        (int(BookAction.DELETE), int(OrderSide.SELL), 0.42, 0.0),
    ]
    assert [int(delta.sequence) for delta in records[1].deltas] == [1, 2]
    assert int(records[1].deltas[-1].flags) == int(RecordFlag.F_LAST)
    assert int(records[1].deltas[0].ts_event) == (_T0_MS + 1_000) * 1_000_000
    assert int(records[1].deltas[0].ts_init) == int(records[1].deltas[0].ts_event)

    assert _flatten(records[2]) == [(int(BookAction.DELETE), int(OrderSide.BUY), 0.39, 0.0)]


def test_marketlens_convert_skips_intact_chain_snapshots_and_heals_reseeds() -> None:
    loader = _make_marketlens_loader()
    events = [
        _snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)]),
        _delta(_T0_MS + 1_000, 0.41, 3.0, "BUY"),
        _snapshot(_T0_MS + 2_000, [(0.40, 10.0), (0.41, 3.0)], [(0.42, 7.0)]),
        _snapshot(_T0_MS + 3_000, [(0.40, 12.0)], [(0.42, 7.0)], is_reseed=True),
    ]
    records, _trades = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=0,
    )

    assert len(records) == 3
    corrections = _flatten(records[2])
    assert (int(BookAction.DELETE), int(OrderSide.BUY), 0.41, 0.0) in corrections
    assert (int(BookAction.UPDATE), int(OrderSide.BUY), 0.40, 12.0) in corrections
    assert len(corrections) == 2


def test_marketlens_convert_skips_deletes_for_absent_levels() -> None:
    loader = _make_marketlens_loader()
    events = [
        _snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)]),
        _delta(_T0_MS + 1_000, 0.55, 0.0, "BUY"),
    ]
    records, _trades = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=0,
    )

    assert len(records) == 1


def test_marketlens_convert_inverts_the_no_leg_exactly() -> None:
    loader = _make_marketlens_loader(token_index=1)
    events = [
        _snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)]),
        _delta(_T0_MS + 1_000, 0.41, 3.0, "BUY"),
        _trade(_T0_MS + 2_000, 0.42, 5.0, "BUY"),
    ]
    records, trades = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=1,
    )

    ladder = _flatten(records[0])
    assert (int(BookAction.ADD), int(OrderSide.SELL), 0.60, 10.0) in ladder
    assert (int(BookAction.ADD), int(OrderSide.BUY), 0.58, 7.0) in ladder
    assert _flatten(records[1]) == [(int(BookAction.UPDATE), int(OrderSide.SELL), 0.59, 3.0)]
    assert trades == [(0.58, 5.0, "SELL", "01TRADE", (_T0_MS + 2_000) * 1_000_000)]


def test_marketlens_trade_ticks_map_aggressor_and_window() -> None:
    loader = _make_marketlens_loader()
    events = [
        _snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)]),
        _trade(_T0_MS - 1_000, 0.42, 1.0, "BUY", trade_id="early"),
        _trade(_T0_MS + 1_000, 0.42, 5.0, "BUY", trade_id="taker-buy"),
        _trade(_T0_MS + 2_000, 0.40, 2.0, "SELL", trade_id="taker-sell"),
    ]
    _records, trade_rows = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=0,
    )
    ticks = loader._trade_ticks_from_rows(trade_rows)

    assert [str(tick.trade_id) for tick in ticks] == ["taker-buy", "taker-sell"]
    assert ticks[0].aggressor_side == AggressorSide.BUYER
    assert ticks[1].aggressor_side == AggressorSide.SELLER
    assert int(ticks[0].ts_event) == (_T0_MS + 1_000) * 1_000_000


def test_marketlens_convert_sorts_out_of_order_events() -> None:
    loader = _make_marketlens_loader()
    events = [
        _snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)]),
        _delta(_T0_MS + 2_000, 0.41, 9.0, "BUY"),
        _delta(_T0_MS + 1_000, 0.41, 3.0, "BUY"),
        _delta(_T0_MS + 1_000, 0.39, 4.0, "BUY"),
    ]
    records, _trades = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=0,
    )

    timestamps = [int(record.deltas[0].ts_event) for record in records]
    assert timestamps == sorted(timestamps)
    assert _flatten(records[1]) == [
        (int(BookAction.UPDATE), int(OrderSide.BUY), 0.41, 3.0),
        (int(BookAction.UPDATE), int(OrderSide.BUY), 0.39, 4.0),
    ]
    assert _flatten(records[2]) == [(int(BookAction.UPDATE), int(OrderSide.BUY), 0.41, 9.0)]


def test_marketlens_convert_empty_reseed_snapshot_clears_the_book() -> None:
    loader = _make_marketlens_loader()
    events = [
        _snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)]),
        _snapshot(_T0_MS + 2_000, [], [], is_reseed=True),
        _delta(_T0_MS + 3_000, 0.30, 5.0, "BUY"),
    ]
    records, _trades = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=0,
    )

    assert len(records) == 3
    corrections = _flatten(records[1])
    assert (int(BookAction.DELETE), int(OrderSide.BUY), 0.40, 0.0) in corrections
    assert (int(BookAction.DELETE), int(OrderSide.SELL), 0.42, 0.0) in corrections
    assert len(corrections) == 2
    assert _flatten(records[2]) == [(int(BookAction.UPDATE), int(OrderSide.BUY), 0.30, 5.0)]


def test_marketlens_convert_seeds_from_an_empty_anchor_snapshot() -> None:
    loader = _make_marketlens_loader()
    events = [
        _snapshot(_T0_MS - 30_000, [], []),
        _delta(_T0_MS + 1_000, 0.41, 3.0, "BUY"),
    ]
    records, _trades = loader._convert_history_events(
        events,
        start_ns=_T0_MS * 1_000_000,
        end_ns=(_T0_MS + 60_000) * 1_000_000,
        token_index=0,
    )

    assert len(records) == 1
    assert _flatten(records[0]) == [(int(BookAction.UPDATE), int(OrderSide.BUY), 0.41, 3.0)]


class _FakeJSONResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> "_FakeJSONResponse":
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._payload


def _history_page(events: list[dict], *, cursor: str | None, has_more: bool) -> dict:
    return {
        "market_id": _MARKET_UUID,
        "platform": "polymarket",
        "data": events,
        "meta": {"cursor": cursor, "has_more": has_more},
    }


def test_marketlens_fetch_history_day_follows_cursor_and_sends_bearer_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_marketlens_loader()
    pages = [
        _history_page(
            [_snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)])], cursor="c1", has_more=True
        ),
        _history_page([_delta(_T0_MS + 1_000, 0.41, 3.0, "BUY")], cursor=None, has_more=False),
    ]
    seen_urls: list[str] = []
    seen_auth: list[str | None] = []

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        seen_urls.append(request.full_url)
        seen_auth.append(request.get_header("Authorization"))
        return _FakeJSONResponse(pages[len(seen_urls) - 1])

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    entry = marketlens_module.MarketlensSourceEntry(
        kind="api", target="https://api.marketlens.trade/v1", api_key="mk_test"
    )
    events = loader._fetch_history_day(entry=entry, market_id=_MARKET_UUID, date="2026-08-14")

    assert len(events) == 2
    assert len(seen_urls) == 2
    assert "cursor=c1" in seen_urls[1]
    assert f"/markets/{_MARKET_UUID}/orderbook/history" in seen_urls[0]
    assert seen_auth == ["Bearer mk_test", "Bearer mk_test"]


def test_marketlens_fetch_history_day_refetches_pre_day_anchor_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_marketlens_loader()
    day_start_ms = int(pd.Timestamp("2026-08-14", tz="UTC").value // 1_000_000)
    anchor = _snapshot(day_start_ms - 45_000, [(0.40, 10.0)], [(0.42, 7.0)])
    gap_delta = _delta(day_start_ms - 20_000, 0.41, 3.0, "BUY")
    day_delta = _delta(day_start_ms + 1_000, 0.39, 2.0, "BUY")
    seen_urls: list[str] = []

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        seen_urls.append(request.full_url)
        if f"after={day_start_ms - 1}" in request.full_url:
            return _FakeJSONResponse(
                _history_page([anchor, day_delta], cursor=None, has_more=False)
            )
        assert f"after={day_start_ms - 45_000}" in request.full_url
        assert f"before={day_start_ms}&" in request.full_url
        return _FakeJSONResponse(_history_page([anchor, gap_delta], cursor=None, has_more=False))

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    entry = marketlens_module.MarketlensSourceEntry(
        kind="api", target="https://api.marketlens.trade/v1", api_key="mk_test"
    )
    events = loader._fetch_history_day(entry=entry, market_id=_MARKET_UUID, date="2026-08-14")

    assert len(seen_urls) == 2
    assert events == [anchor, gap_delta, day_delta]


def _http_error(
    code: int, *, body: dict | None = None, retry_after: str | None = None
) -> HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    payload = json.dumps(body or {}).encode("utf-8")
    return HTTPError("https://api.marketlens.trade/v1/x", code, "error", headers, BytesIO(payload))


def test_marketlens_get_json_retries_rate_limits_with_retry_after(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_marketlens_loader()
    attempts = 0
    sleeps: list[float] = []

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _http_error(429, body={"error": {"code": "RATE_LIMITED"}}, retry_after="7")
        return _FakeJSONResponse({"ok": True})

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    monkeypatch.setattr(marketlens_module.time, "sleep", sleeps.append)
    entry = marketlens_module.MarketlensSourceEntry(
        kind="api", target="https://api.marketlens.trade/v1", api_key="mk_test"
    )
    payload = loader._get_json(entry=entry, path="/x")

    assert payload == {"ok": True}
    assert attempts == 2
    assert sleeps == [7.0]


def test_marketlens_get_json_never_retries_budget_exhaustion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_marketlens_loader()

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        raise _http_error(
            429,
            body={"error": {"code": "DAILY_BUDGET_EXCEEDED", "message": "daily rows used"}},
        )

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    entry = marketlens_module.MarketlensSourceEntry(
        kind="api", target="https://api.marketlens.trade/v1", api_key="mk_test"
    )
    with pytest.raises(ValueError, match="DAILY_BUDGET_EXCEEDED"):
        loader._get_json(entry=entry, path="/x")


def test_marketlens_market_record_rejects_polled_tier(monkeypatch: pytest.MonkeyPatch) -> None:
    loader = _make_marketlens_loader()
    loader._marketlens_market_record = None

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        return _FakeJSONResponse({"id": _MARKET_UUID, "collection_tier": "polled"})

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    with (
        configured_marketlens_data_source(sources=("api:mk_test",)),
        pytest.raises(ValueError, match="polled tier"),
    ):
        loader._market_record()


def test_marketlens_market_record_maps_missing_market_to_value_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loader = _make_marketlens_loader()
    loader._marketlens_market_record = None

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        raise _http_error(404, body={"error": {"code": "MARKET_NOT_FOUND"}})

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    with (
        configured_marketlens_data_source(sources=("api:mk_test",)),
        pytest.raises(ValueError, match="does not track"),
    ):
        loader._market_record()


def _day_events() -> list[dict]:
    return [
        _snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)]),
        _delta(_T0_MS + 1_000, 0.41, 3.0, "BUY"),
        _trade(_T0_MS + 2_000, 0.42, 5.0, "BUY"),
    ]


def _run_window_load(loader: RunnerPolymarketMarketlensBookDataLoader, *, token_index: int = 0):
    start = pd.Timestamp(_T0_MS, unit="ms", tz="UTC")
    end = pd.Timestamp(_T0_MS + 60_000, unit="ms", tz="UTC")
    with configured_marketlens_data_source(sources=("api:mk_test",)):
        return loader.load_order_book_deltas(
            start,
            end,
            market_slug="synthetic-marketlens-market",
            token_index=token_index,
        )


def test_marketlens_window_load_reuses_raw_and_materialized_caches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(MARKETLENS_CACHE_ROOT_ENV, str(tmp_path))
    fetches = 0

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        nonlocal fetches
        fetches += 1
        return _FakeJSONResponse(_history_page(_day_events(), cursor=None, has_more=False))

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)

    yes_loader = _make_marketlens_loader()
    records = _run_window_load(yes_loader)
    assert len(records) == 2
    assert fetches == 1

    no_loader = _make_marketlens_loader(token_index=1)
    no_records = _run_window_load(no_loader, token_index=1)
    assert len(no_records) == 2
    assert fetches == 1

    def _fail_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        raise AssertionError("network must not be touched on a cache hit")

    monkeypatch.setattr(marketlens_module, "urlopen", _fail_urlopen)
    cached_loader = _make_marketlens_loader()
    cached_records = _run_window_load(cached_loader)
    assert len(cached_records) == len(records)

    with configured_marketlens_data_source(sources=("api:mk_test",)):
        ticks = cached_loader.load_marketlens_trade_ticks(
            pd.Timestamp(_T0_MS, unit="ms", tz="UTC"),
            pd.Timestamp(_T0_MS + 60_000, unit="ms", tz="UTC"),
        )
    assert [str(tick.trade_id) for tick in ticks] == ["01TRADE"]


def test_marketlens_concurrent_leg_loads_fetch_the_day_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(MARKETLENS_CACHE_ROOT_ENV, str(tmp_path))
    fetches = 0

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        nonlocal fetches
        fetches += 1
        time.sleep(0.05)
        return _FakeJSONResponse(_history_page(_day_events(), cursor=None, has_more=False))

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    results: list[int] = []
    errors: list[Exception] = []

    def _load(token_index: int) -> None:
        try:
            loader = _make_marketlens_loader(token_index=token_index)
            results.append(len(_run_window_load(loader, token_index=token_index)))
        except Exception as exc:  # noqa: BLE001 - surfaced via the errors assertion
            errors.append(exc)

    threads = [threading.Thread(target=_load, args=(token_index,)) for token_index in (0, 1)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=5)

    assert errors == []
    assert results == [2, 2]
    assert fetches == 1


def test_marketlens_trades_pass_reuses_the_book_pass_fetch_without_caches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(MARKETLENS_CACHE_ROOT_ENV, "off")
    fetches = 0

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        nonlocal fetches
        fetches += 1
        return _FakeJSONResponse(_history_page(_day_events(), cursor=None, has_more=False))

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    loader = _make_marketlens_loader()
    start = pd.Timestamp(_T0_MS, unit="ms", tz="UTC")
    end = pd.Timestamp(_T0_MS + 60_000, unit="ms", tz="UTC")
    with configured_marketlens_data_source(sources=("api:mk_test",)):
        records = loader.load_order_book_deltas(
            start,
            end,
            market_slug="synthetic-marketlens-market",
            token_index=0,
        )
        ticks = loader.load_marketlens_trade_ticks(start, end)

    assert len(records) == 2
    assert [str(tick.trade_id) for tick in ticks] == ["01TRADE"]
    assert fetches == 1


def test_marketlens_corrupt_materialized_cache_self_heals(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv(MARKETLENS_CACHE_ROOT_ENV, str(tmp_path))

    def _fake_urlopen(request, timeout=None):  # type: ignore[no-untyped-def]
        return _FakeJSONResponse(_history_page(_day_events(), cursor=None, has_more=False))

    monkeypatch.setattr(marketlens_module, "urlopen", _fake_urlopen)
    loader = _make_marketlens_loader()
    _run_window_load(loader)

    cache_files = list(tmp_path.rglob("*.parquet"))
    assert cache_files
    for cache_file in cache_files:
        cache_file.write_bytes(b"not parquet")

    with pytest.warns(UserWarning, match="ignored stale"):
        records = _run_window_load(_make_marketlens_loader())
    assert len(records) == 2


def test_marketlens_day_progress_emits_loader_event() -> None:
    loader = _make_marketlens_loader()

    with capture_loader_events() as capture:
        loader._day_progress(
            "2026-08-14",
            "complete",
            "marketlens-deltas-cache::/tmp/day.parquet",
            12,
        )

    event = next(event for event in capture.events if event.stage == "cache_read")
    assert event.vendor == "marketlens"
    assert event.status == "cache_hit"
    assert event.source_kind == "cache"
    assert event.cache_path == "/tmp/day.parquet"
    assert event.market_slug == "synthetic-marketlens-market"
    assert event.token_id == "0"
    assert event.rows == 12


def test_marketlens_cache_write_failure_warns(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bad_cache_root = tmp_path / "not-a-directory"
    bad_cache_root.write_text("occupied", encoding="utf-8")
    monkeypatch.setenv(MARKETLENS_CACHE_ROOT_ENV, str(bad_cache_root))
    loader = _make_marketlens_loader()

    with pytest.warns(UserWarning, match="failed to write raw history cache"):
        loader._write_raw_cache_day(
            base_url="https://api.marketlens.trade/v1",
            market_id=_MARKET_UUID,
            date="2026-08-14",
            events=[_snapshot(_T0_MS, [(0.40, 10.0)], [(0.42, 7.0)])],
        )
