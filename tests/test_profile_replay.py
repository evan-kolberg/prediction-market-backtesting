from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue

from prediction_market_extensions.backtesting import profile_replay
from prediction_market_extensions.backtesting.profile_replay import (
    ProfileTradeGroup,
    append_profile_replay_diagnostics,
    build_profile_replays,
    fetch_profile_trades,
    normalize_profile_trades,
    profile_actual_pnl,
    profile_replay_key,
    profile_trades_by_key,
    select_profile_trade_groups,
)
from strategies.profile_replay import BookProfileReplayConfig, BookProfileReplayStrategy

INSTRUMENT_ID = InstrumentId(Symbol("PM-TEST-YES"), Venue("POLYMARKET"))


def _payload(
    *,
    slug: str = "btc-updown-5m-1777241400",
    outcome_index: int = 1,
    side: str,
    size: float,
    price: float,
    timestamp: int,
    transaction_hash: str = "0x1",
) -> dict[str, object]:
    return {
        "side": side,
        "size": size,
        "price": price,
        "timestamp": timestamp,
        "slug": slug,
        "outcome": "Down" if outcome_index == 1 else "Up",
        "outcomeIndex": outcome_index,
        "title": "Bitcoin Up or Down",
        "transactionHash": transaction_hash,
    }


class _FakeResponse:
    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_fetch_profile_trades_sends_user_agent(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request: object, *, timeout: float) -> _FakeResponse:
        captured["request"] = request
        captured["timeout"] = timeout
        return _FakeResponse(
            b'[{"slug":"btc-updown-5m-1777241400","outcomeIndex":1,'
            b'"side":"BUY","size":1,"price":0.5,"timestamp":1777241401}]'
        )

    monkeypatch.setattr(profile_replay, "urlopen", fake_urlopen)

    trades = fetch_profile_trades(user="0xabc", limit=12, timeout_seconds=3.0)

    request = captured["request"]
    assert "user=0xabc" in request.full_url
    assert "limit=12" in request.full_url
    assert request.get_header("User-agent") == profile_replay.PROFILE_REPLAY_USER_AGENT
    assert captured["timeout"] == 3.0
    assert len(trades) == 1


def test_select_profile_trade_groups_rejects_groups_requiring_prior_inventory() -> None:
    trades = normalize_profile_trades(
        [
            _payload(side="SELL", size=5, price=0.51, timestamp=100, transaction_hash="0xsell"),
            _payload(side="BUY", size=5, price=0.49, timestamp=101, transaction_hash="0xbuy"),
        ]
    )

    groups = select_profile_trade_groups(trades, max_groups=1)

    assert groups == ()


def test_build_profile_replays_infers_btc_window_from_slug() -> None:
    trades = normalize_profile_trades(
        [_payload(side="BUY", size=10, price=0.5, timestamp=1777241444)]
    )
    group = ProfileTradeGroup("btc-updown-5m-1777241400", 1, trades)

    replays = build_profile_replays(
        [group],
        profile_user="0xabc",
        lead_time_seconds=2,
        start_buffer_seconds=10,
        end_buffer_seconds=20,
    )

    assert len(replays) == 1
    assert replays[0].market_slug == "btc-updown-5m-1777241400"
    assert replays[0].token_index == 1
    assert replays[0].start_time == pd.Timestamp(1777241400 - 12, unit="s", tz="UTC")
    assert replays[0].end_time == pd.Timestamp(1777241400 + 300 + 20, unit="s", tz="UTC")
    assert replays[0].metadata["profile_replay_key"] == "btc-updown-5m-1777241400:1"


def test_profile_actual_pnl_includes_binary_settlement_value() -> None:
    trades = normalize_profile_trades(
        [
            _payload(side="BUY", size=10, price=0.40, timestamp=100, transaction_hash="0xbuy"),
            _payload(side="SELL", size=4, price=0.60, timestamp=101, transaction_hash="0xsell"),
        ]
    )

    winning = profile_actual_pnl(trades, realized_outcome=1.0)
    losing = profile_actual_pnl(trades, realized_outcome=0.0)

    assert winning["profile_trade_cashflow"] == pytest.approx(-1.6)
    assert winning["profile_open_quantity"] == pytest.approx(6.0)
    assert winning["profile_actual_pnl"] == pytest.approx(4.4)
    assert losing["profile_actual_pnl"] == pytest.approx(-1.6)


def test_append_profile_replay_diagnostics_compares_actual_and_backtest_pnl() -> None:
    trades = normalize_profile_trades([_payload(side="BUY", size=10, price=0.40, timestamp=100)])
    group = ProfileTradeGroup("btc-updown-5m-1777241400", 1, trades)

    enriched = append_profile_replay_diagnostics(
        [
            {
                "profile_replay_key": group.key,
                "realized_outcome": 1.0,
                "pnl": 5.5,
            }
        ],
        [group],
    )

    assert enriched[0]["profile_actual_pnl"] == pytest.approx(6.0)
    assert enriched[0]["profile_pnl_error"] == pytest.approx(-0.5)


def test_profile_trades_by_key_preserves_trade_schedule_payload() -> None:
    trades = normalize_profile_trades(
        [
            _payload(
                side="BUY",
                size=1.25,
                price=0.50,
                timestamp=100,
                transaction_hash="0xbuy",
            )
        ]
    )
    group = ProfileTradeGroup("btc-updown-5m-1777241400", 1, trades)

    trades_by_key = profile_trades_by_key([group])

    assert set(trades_by_key) == {profile_replay_key(slug=group.slug, outcome_index=1)}
    assert trades_by_key[group.key] == [
        {
            "side": "BUY",
            "size": 1.25,
            "price": 0.5,
            "timestamp_ns": trades[0].timestamp_ns,
            "transaction_hash": "0xbuy",
        }
    ]


class _ScheduleHarness(BookProfileReplayStrategy):
    def __init__(self, config: BookProfileReplayConfig) -> None:
        super().__init__(config)
        self.submitted: list[object] = []

    def _submit_scheduled_order(self, scheduled_order: object) -> None:
        self.submitted.append(scheduled_order)


def test_profile_replay_strategy_submits_orders_only_after_lead_time() -> None:
    strategy = _ScheduleHarness(
        BookProfileReplayConfig(
            instrument_id=INSTRUMENT_ID,
            selection_key="profile",
            trades_by_key={
                "profile": [
                    {"side": "BUY", "size": 1, "price": 0.50, "timestamp_ns": 2_000_000_000},
                    {"side": "SELL", "size": 1, "price": 0.55, "timestamp_ns": 3_000_000_000},
                ]
            },
            lead_time_seconds=1.0,
        )
    )

    strategy._submit_due_orders(ts_event_ns=999_999_999)
    assert strategy.submitted == []

    strategy._submit_due_orders(ts_event_ns=1_000_000_000)
    assert [order.side for order in strategy.submitted] == [OrderSide.BUY]

    strategy._submit_due_orders(ts_event_ns=2_000_000_000)
    assert [order.side for order in strategy.submitted] == [OrderSide.BUY, OrderSide.SELL]


@pytest.mark.parametrize(
    ("position", "expected"),
    [
        (Decimal("3.5"), 3.5),
        (SimpleNamespace(signed_qty=Decimal("2.25")), 2.25),
        (SimpleNamespace(signed_decimal_qty=lambda: Decimal("4.75")), 4.75),
        (Decimal("-1"), 0.0),
    ],
)
def test_profile_replay_strategy_sell_cap_handles_nautilus_position_shapes(
    monkeypatch: pytest.MonkeyPatch, position: object, expected: float
) -> None:
    strategy = BookProfileReplayStrategy(
        BookProfileReplayConfig(
            instrument_id=INSTRUMENT_ID,
            selection_key="profile",
            trades_by_key={
                "profile": [
                    {"side": "SELL", "size": 1, "price": 0.50, "timestamp_ns": 2_000_000_000}
                ]
            },
        )
    )

    monkeypatch.setattr(
        BookProfileReplayStrategy,
        "portfolio",
        property(lambda self: SimpleNamespace(net_position=lambda instrument_id: position)),
        raising=False,
    )

    assert strategy._sell_quantity_cap() == pytest.approx(expected)
