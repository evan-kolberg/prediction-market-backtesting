from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import importlib
import math
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nautilus_trader.common import Environment
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.config import ImportableStrategyConfig

from live import btc_snapshot_model_sandbox
from prediction_market_extensions.live import btc_5m
from prediction_market_extensions.live.btc_features import (
    NANOSECONDS_PER_SECOND,
    LiveBtcFeatureStore,
)
from prediction_market_extensions.live import sandbox as live_sandbox
from prediction_market_extensions.live.settlement import (
    settlement_from_clob_market,
    split_polymarket_instrument_id,
)
from prediction_market_extensions.live.sandbox import (
    DEFAULT_BTC_INSTRUMENT_ID,
    PublicPolymarketLiveDataClientFactory,
    PublicPolymarketInstrumentProvider,
    build_polymarket_binance_sandbox_config,
)

try:
    private_btc_model = importlib.import_module("strategies.private.btc_snapshot_model")
except ModuleNotFoundError:
    private_btc_model = None


def test_btc_5m_slug_helpers_floor_and_skip_current_window() -> None:
    assert btc_5m.floor_to_btc_5m_start(1_778_267_742) == 1_778_267_700
    assert btc_5m.btc_5m_market_slug(1_778_267_700) == "btc-updown-5m-1778267700"
    assert btc_5m.upcoming_btc_5m_event_slugs(
        market_count=3,
        include_current=False,
        timestamp=1_778_267_742,
    ) == [
        "btc-updown-5m-1778268000",
        "btc-updown-5m-1778268300",
        "btc-updown-5m-1778268600",
    ]
    assert (
        btc_5m.upcoming_btc_5m_window_label(timestamp=1_778_267_742)
        == "2026-05-08T19:15:00+00:00 -> 2026-05-08T19:20:00+00:00"
    )


def test_configured_btc_5m_event_slugs_prefers_fixed_env(monkeypatch) -> None:
    monkeypatch.setenv(
        btc_5m.LIVE_BTC_5M_EVENT_SLUGS_ENV,
        "btc-updown-5m-1000, btc-updown-5m-1300",
    )

    assert btc_5m.configured_btc_5m_event_slugs() == [
        "btc-updown-5m-1000",
        "btc-updown-5m-1300",
    ]


def test_configured_btc_5m_event_slugs_uses_market_count_env(monkeypatch) -> None:
    monkeypatch.delenv(btc_5m.LIVE_BTC_5M_EVENT_SLUGS_ENV, raising=False)
    monkeypatch.delenv(btc_5m.LIVE_BTC_5M_INCLUDE_CURRENT_ENV, raising=False)
    monkeypatch.setenv(btc_5m.LIVE_BTC_5M_MARKET_COUNT_ENV, "2")
    monkeypatch.setattr(btc_5m.time, "time", lambda: 1_778_267_742)

    assert btc_5m.configured_btc_5m_event_slugs() == [
        "btc-updown-5m-1778267700",
        "btc-updown-5m-1778268000",
    ]


def test_configured_btc_5m_event_slugs_can_skip_current_from_env(monkeypatch) -> None:
    monkeypatch.delenv(btc_5m.LIVE_BTC_5M_EVENT_SLUGS_ENV, raising=False)
    monkeypatch.setenv(btc_5m.LIVE_BTC_5M_MARKET_COUNT_ENV, "2")
    monkeypatch.setenv(btc_5m.LIVE_BTC_5M_INCLUDE_CURRENT_ENV, "0")
    monkeypatch.setattr(btc_5m.time, "time", lambda: 1_778_267_742)

    assert btc_5m.configured_btc_5m_event_slugs() == [
        "btc-updown-5m-1778268000",
        "btc-updown-5m-1778268300",
    ]


def test_load_btc_5m_instrument_ids_loads_both_market_legs(monkeypatch) -> None:
    calls: list[tuple[str, int, object]] = []
    http_client = object()

    def fake_slugs(*, market_count: int, include_current: bool) -> list[str]:
        assert market_count == 2
        assert include_current is False
        return ["btc-updown-5m-1000", "btc-updown-5m-1300"]

    async def fake_from_market_slug(
        slug: str,
        *,
        token_index: int,
        http_client: object,
    ) -> SimpleNamespace:
        calls.append((slug, token_index, http_client))
        return SimpleNamespace(
            instrument=SimpleNamespace(id=InstrumentId.from_str(f"BTC{len(calls)}.POLYMARKET"))
        )

    monkeypatch.setattr(btc_5m, "upcoming_btc_5m_event_slugs", fake_slugs)
    monkeypatch.setattr(btc_5m.PolymarketDataLoader, "from_market_slug", fake_from_market_slug)

    instrument_ids = asyncio.run(
        btc_5m.load_btc_5m_instrument_ids(
            market_count=2,
            include_current=False,
            http_client=http_client,
        )
    )

    assert instrument_ids == tuple(
        InstrumentId.from_str(value)
        for value in (
            "BTC1.POLYMARKET",
            "BTC2.POLYMARKET",
            "BTC3.POLYMARKET",
            "BTC4.POLYMARKET",
        )
    )
    assert calls == [
        ("btc-updown-5m-1000", 0, http_client),
        ("btc-updown-5m-1000", 1, http_client),
        ("btc-updown-5m-1300", 0, http_client),
        ("btc-updown-5m-1300", 1, http_client),
    ]


def test_load_btc_5m_instrument_ids_accepts_fixed_event_slugs(monkeypatch) -> None:
    calls: list[tuple[str, int]] = []

    async def fake_from_market_slug(
        slug: str,
        *,
        token_index: int,
        http_client: object,
    ) -> SimpleNamespace:
        calls.append((slug, token_index))
        return SimpleNamespace(
            instrument=SimpleNamespace(id=InstrumentId.from_str(f"BTC{len(calls)}.POLYMARKET"))
        )

    monkeypatch.setattr(btc_5m.PolymarketDataLoader, "from_market_slug", fake_from_market_slug)

    asyncio.run(
        btc_5m.load_btc_5m_instrument_ids(
            market_count=99,
            include_current=True,
            event_slugs=["btc-updown-5m-2000"],
            http_client=object(),
        )
    )

    assert calls == [
        ("btc-updown-5m-2000", 0),
        ("btc-updown-5m-2000", 1),
    ]


def test_load_btc_5m_instrument_ids_skips_incomplete_markets(monkeypatch, caplog) -> None:
    calls: list[tuple[str, int]] = []

    async def fake_from_market_slug(
        slug: str,
        *,
        token_index: int,
        http_client: object,
    ) -> SimpleNamespace:
        calls.append((slug, token_index))
        if slug == "btc-updown-5m-missing":
            raise ValueError("not yet visible")
        return SimpleNamespace(
            instrument=SimpleNamespace(id=InstrumentId.from_str(f"BTC{len(calls)}.POLYMARKET"))
        )

    monkeypatch.setattr(btc_5m.PolymarketDataLoader, "from_market_slug", fake_from_market_slug)

    instrument_ids = asyncio.run(
        btc_5m.load_btc_5m_instrument_ids(
            event_slugs=["btc-updown-5m-missing", "btc-updown-5m-2000"],
            http_client=object(),
        )
    )

    assert instrument_ids == (
        InstrumentId.from_str("BTC2.POLYMARKET"),
        InstrumentId.from_str("BTC3.POLYMARKET"),
    )
    assert calls == [
        ("btc-updown-5m-missing", 0),
        ("btc-updown-5m-2000", 0),
        ("btc-updown-5m-2000", 1),
    ]
    assert "Skipping BTC 5m market slug btc-updown-5m-missing" in caplog.text


def test_load_btc_5m_instrument_ids_fails_when_no_complete_market_loads(monkeypatch) -> None:
    async def fake_from_market_slug(
        slug: str,
        *,
        token_index: int,
        http_client: object,
    ) -> SimpleNamespace:
        raise ValueError(f"{slug}:{token_index} unavailable")

    monkeypatch.setattr(btc_5m.PolymarketDataLoader, "from_market_slug", fake_from_market_slug)

    with pytest.raises(RuntimeError, match="Loaded 0 complete BTC 5m market"):
        asyncio.run(
            btc_5m.load_btc_5m_instrument_ids(
                event_slugs=["btc-updown-5m-missing"],
                http_client=object(),
            )
        )


def test_live_btc_feature_store_records_features_and_prunes_old_seconds() -> None:
    store = LiveBtcFeatureStore(buffer_seconds=3)
    store.record_trade(ts_ns=10 * NANOSECONDS_PER_SECOND, price=100.0, size=1.0)
    store.record_trade(ts_ns=11 * NANOSECONDS_PER_SECOND, price=101.0, size=2.0)
    store.record_trade(ts_ns=12 * NANOSECONDS_PER_SECOND, price=103.0, size=3.0)
    store.record_trade(ts_ns=12 * NANOSECONDS_PER_SECOND, price=103.5, size=0.5)
    store.record_trade(ts_ns=13 * NANOSECONDS_PER_SECOND, price=float("nan"), size=99.0)

    assert store.price_at(12) == 103.5
    assert store.price_at(11) == 101.0
    assert store.observation_second_at(12) == 12
    assert store.observation_age_seconds(13) == 1.0
    assert store.momentum(12, 2) == 3.5
    assert store.volume(12, 2) == 5.5
    assert store.volatility(12, 2) == pytest.approx(0.75)

    store.record_trade(ts_ns=14 * NANOSECONDS_PER_SECOND, price=104.0, size=4.0)

    assert math.isnan(store.price_at(10))
    assert store.observation_second_at(10) is None
    assert math.isinf(store.observation_age_seconds(10))
    assert store.price_at(11) == 101.0
    assert store.price_at(14) == 104.0


def test_split_polymarket_instrument_id_extracts_condition_and_token() -> None:
    condition_id, token_id = split_polymarket_instrument_id("0xcondition-12345.POLYMARKET")

    assert condition_id == "0xcondition"
    assert token_id == "12345"


def test_settlement_from_clob_market_reads_closed_winner() -> None:
    settlement = settlement_from_clob_market(
        {
            "condition_id": "0xcondition",
            "closed": True,
            "tokens": [
                {"token_id": "up-token", "outcome": "Up", "price": 0, "winner": False},
                {"token_id": "down-token", "outcome": "Down", "price": 1, "winner": True},
            ],
        },
        token_id="down-token",
    )

    assert settlement is not None
    assert settlement.condition_id == "0xcondition"
    assert settlement.token_id == "down-token"
    assert settlement.outcome == "Down"
    assert settlement.winner is True
    assert settlement.price == Decimal("1")


def test_settlement_from_clob_market_ignores_open_markets() -> None:
    settlement = settlement_from_clob_market(
        {
            "condition_id": "0xcondition",
            "closed": False,
            "tokens": [{"token_id": "token", "outcome": "Up", "price": 1}],
        },
        token_id="token",
    )

    assert settlement is None


def test_private_btc_strategy_recognizes_nautilus_buy_side() -> None:
    if private_btc_model is None:
        pytest.skip("private BTC strategy is intentionally not part of the public framework")
    assert private_btc_model._is_buy_order_side(OrderSide.BUY)
    assert private_btc_model._is_buy_order_side(1)
    assert private_btc_model._is_buy_order_side("BUY")
    assert not private_btc_model._is_buy_order_side(OrderSide.SELL)
    assert not private_btc_model._is_buy_order_side(2)


def test_private_btc_strategy_market_prune_due_uses_post_end_retention() -> None:
    if private_btc_model is None:
        pytest.skip("private BTC strategy is intentionally not part of the public framework")
    assert (
        private_btc_model._market_prune_due_ns(
            market_start=1_778_352_400,
            post_end_retention_seconds=600,
        )
        == (1_778_352_400 + 300 + 600) * NANOSECONDS_PER_SECOND
    )


def test_btc_snapshot_sandbox_runner_is_example_wiring_with_private_artifacts(
    monkeypatch,
) -> None:
    monkeypatch.delenv("LIVE_BTC_SNAPSHOT_MODEL_PATH", raising=False)
    monkeypatch.delenv("LIVE_BTC_SNAPSHOT_DIAGNOSTICS_PATH", raising=False)
    monkeypatch.delenv("LIVE_BTC_HEARTBEAT_LOG_SECONDS", raising=False)
    monkeypatch.delenv("LIVE_BTC_MAX_FEATURE_AGE_SECONDS", raising=False)
    monkeypatch.delenv("LIVE_BTC_DAILY_STOP_LOSS", raising=False)
    monkeypatch.delenv("LIVE_BTC_SNAPSHOT_MOMENTUM_ALIGNMENT", raising=False)
    monkeypatch.delenv("LIVE_BTC_EXPENSIVE_MIN_SIGNED_MOMENTUM_30S", raising=False)

    params = btc_snapshot_model_sandbox._strategy_parameters()

    assert btc_snapshot_model_sandbox.STRATEGY_PATH.startswith("strategies.private.")
    assert btc_snapshot_model_sandbox.CONFIG_PATH.startswith("strategies.private.")
    assert params["model_path"] == btc_snapshot_model_sandbox.DEFAULT_MODEL_PATH
    assert str(params["model_path"]).startswith("live/models/")
    assert params["diagnostics_path"] is None
    assert params["heartbeat_log_seconds"] == 300.0
    assert params["momentum_alignment"] == "pdiff_m15_m30"
    assert params["max_btc_feature_age_seconds"] == 8.0
    assert params["daily_stop_loss"] == 1.2
    assert params["expensive_min_signed_momentum_30s"] == 2.0


def test_btc_snapshot_sandbox_runner_config_injects_live_runtime_options(
    monkeypatch,
) -> None:
    monkeypatch.setenv("LIVE_BTC_SNAPSHOT_MODEL_PATH", "live/models/local-private-model.json")
    monkeypatch.setenv("LIVE_BTC_HEARTBEAT_LOG_SECONDS", "17")
    monkeypatch.setenv("LIVE_BTC_MAX_FEATURE_AGE_SECONDS", "4.5")
    monkeypatch.setenv("LIVE_BTC_DAILY_STOP_LOSS", "0.8")
    monkeypatch.setenv("LIVE_BTC_SNAPSHOT_MOMENTUM_ALIGNMENT", "momentum_vote")
    monkeypatch.setenv("LIVE_BTC_EXPENSIVE_MIN_SIGNED_MOMENTUM_30S", "1.5")
    up = InstrumentId.from_str("UP.POLYMARKET")
    down = InstrumentId.from_str("DOWN.POLYMARKET")

    config = btc_snapshot_model_sandbox._build_strategy_config(
        instrument_ids=(up, down),
        btc_instrument_id=DEFAULT_BTC_INSTRUMENT_ID,
    )

    assert config.strategy_path == btc_snapshot_model_sandbox.STRATEGY_PATH
    assert config.config_path == btc_snapshot_model_sandbox.CONFIG_PATH
    assert config.config["instrument_ids"] == [str(up), str(down)]
    assert config.config["btc_instrument_id"] == str(DEFAULT_BTC_INSTRUMENT_ID)
    assert config.config["model_path"] == "live/models/local-private-model.json"
    assert config.config["heartbeat_log_seconds"] == 17.0
    assert config.config["momentum_alignment"] == "momentum_vote"
    assert config.config["max_btc_feature_age_seconds"] == 4.5
    assert config.config["daily_stop_loss"] == 0.8
    assert config.config["expensive_min_signed_momentum_30s"] == 1.5


def test_btc_snapshot_sandbox_runner_dry_run_allows_missing_private_model(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setenv("LIVE_BTC_SNAPSHOT_MODEL_PATH", "/tmp/pmbt-missing-private-model.json")
    monkeypatch.setattr(
        btc_snapshot_model_sandbox,
        "upcoming_btc_5m_event_slugs",
        lambda **_kwargs: ["btc-updown-5m-2000"],
    )
    monkeypatch.setattr(
        btc_snapshot_model_sandbox,
        "upcoming_btc_5m_window_label",
        lambda: "window-label",
    )

    async def fake_load_btc_5m_instrument_ids(**_kwargs: object) -> tuple[InstrumentId, ...]:
        return (
            InstrumentId.from_str("UP.POLYMARKET"),
            InstrumentId.from_str("DOWN.POLYMARKET"),
        )

    monkeypatch.setattr(
        btc_snapshot_model_sandbox,
        "load_btc_5m_instrument_ids",
        fake_load_btc_5m_instrument_ids,
    )

    asyncio.run(btc_snapshot_model_sandbox._main([]))

    output = capsys.readouterr().out
    assert "Model profile: /tmp/pmbt-missing-private-model.json (missing; dry-run only)" in output
    assert "Dry run only. Pass --run to start the Nautilus sandbox node." in output


def test_btc_snapshot_sandbox_runner_build_requires_private_model(monkeypatch) -> None:
    monkeypatch.setenv("LIVE_BTC_SNAPSHOT_MODEL_PATH", "/tmp/pmbt-missing-private-model.json")

    with pytest.raises(FileNotFoundError):
        asyncio.run(btc_snapshot_model_sandbox._main(["--build-only"]))


def test_build_polymarket_binance_sandbox_config_uses_sandbox_execution() -> None:
    strategy = ImportableStrategyConfig(
        strategy_path="strategies:DemoStrategy",
        config_path="strategies:DemoConfig",
        config={"parameter_name": 1},
    )

    config = build_polymarket_binance_sandbox_config(
        strategies=[strategy],
        event_slug_builder="tests.fake:slugs",
        starting_balance=Decimal("20"),
        trader_id="SANDBOX-TEST-001",
        log_level="DEBUG",
        polymarket_update_interval_mins=2,
        risk_submit_rate="7/00:00:01",
    )

    assert config.environment == Environment.SANDBOX
    assert str(config.trader_id) == "SANDBOX-TEST-001"
    assert config.strategies == [strategy]
    assert set(config.data_clients) == {"POLYMARKET", "BINANCE"}
    assert set(config.exec_clients) == {"POLYMARKET"}

    polymarket = config.data_clients["POLYMARKET"]
    assert polymarket.instrument_config.event_slug_builder == "tests.fake:slugs"
    assert polymarket.update_instruments_interval_mins == 2
    assert polymarket.compute_effective_deltas is False

    binance = config.data_clients["BINANCE"]
    assert binance.instrument_provider.load_ids == frozenset({DEFAULT_BTC_INSTRUMENT_ID})
    assert binance.us is True

    execution = config.exec_clients["POLYMARKET"]
    assert execution.starting_balances == ["20 pUSD"]
    assert execution.base_currency == "pUSD"
    assert execution.oms_type == "NETTING"
    assert execution.account_type == "CASH"
    assert execution.book_type == "L2_MBP"
    assert execution.trade_execution is True

    assert config.risk_engine.max_order_submit_rate == "7/00:00:01"
    assert config.exec_engine.reconciliation is False
    assert config.logging.log_level == "DEBUG"


def test_build_polymarket_binance_sandbox_config_disables_polymarket_refresh_by_default() -> None:
    strategy = ImportableStrategyConfig(
        strategy_path="strategies:DemoStrategy",
        config_path="strategies:DemoConfig",
        config={"parameter_name": 1},
    )

    config = build_polymarket_binance_sandbox_config(
        strategies=[strategy],
        event_slug_builder="tests.fake:slugs",
    )

    assert config.data_clients["POLYMARKET"].update_instruments_interval_mins is None


def test_build_polymarket_binance_sandbox_config_can_use_global_binance() -> None:
    strategy = ImportableStrategyConfig(
        strategy_path="strategies:DemoStrategy",
        config_path="strategies:DemoConfig",
        config={"parameter_name": 1},
    )

    config = build_polymarket_binance_sandbox_config(
        strategies=[strategy],
        event_slug_builder="tests.fake:slugs",
        binance_us=False,
    )

    assert config.data_clients["BINANCE"].us is False


def test_public_polymarket_data_factory_does_not_require_credentials(monkeypatch) -> None:
    calls: dict[str, object] = {}

    class FakeClobClient:
        def __init__(self, host: str, *, chain_id: int) -> None:
            calls["clob"] = {"host": host, "chain_id": chain_id}

    class FakeProvider:
        def __init__(self, *, client: object, clock: object, config: object) -> None:
            calls["provider"] = {"client": client, "clock": clock, "config": config}

    class FakeDataClient:
        def __init__(self, **kwargs: object) -> None:
            calls["data_client"] = kwargs

    monkeypatch.setattr(live_sandbox, "ClobClient", FakeClobClient)
    monkeypatch.setattr(live_sandbox, "PublicPolymarketInstrumentProvider", FakeProvider)
    monkeypatch.setattr(live_sandbox, "PublicPolymarketDataClient", FakeDataClient)

    config = live_sandbox.PolymarketDataClientConfig(
        instrument_config=live_sandbox.PolymarketInstrumentProviderConfig(
            event_slug_builder="tests.fake:slugs",
        ),
        base_url_http="https://example.test",
    )
    client = PublicPolymarketLiveDataClientFactory.create(
        loop=object(),
        name="POLYMARKET",
        config=config,
        msgbus=object(),
        cache=object(),
        clock=object(),
    )

    assert isinstance(client, FakeDataClient)
    assert calls["clob"] == {"host": "https://example.test", "chain_id": live_sandbox.POLYGON}
    assert calls["provider"]["config"] is config.instrument_config
    assert calls["data_client"]["http_client"] is calls["provider"]["client"]
    assert calls["data_client"]["config"] is config


def test_public_polymarket_provider_overlays_clob_trading_constraints() -> None:
    provider = PublicPolymarketInstrumentProvider.__new__(PublicPolymarketInstrumentProvider)

    class FakeClient:
        def get_market(self, condition_id: str) -> dict[str, object]:
            assert condition_id == "0xcondition"
            return {
                "condition_id": condition_id,
                "minimum_tick_size": 0.01,
                "minimum_order_size": 5,
                "accepting_orders": True,
                "active": True,
                "closed": False,
                "tokens": [
                    {
                        "token_id": "1",
                        "outcome": "Up",
                        "price": 0.55,
                    },
                ],
            }

    provider._client = FakeClient()
    provider._log_warnings = True
    provider._log = SimpleNamespace(warning=lambda *_args, **_kwargs: None)
    market_info = {
        "condition_id": "0xcondition",
        "minimum_tick_size": 0.001,
        "minimum_order_size": 1,
        "accepting_orders": False,
        "active": False,
        "closed": True,
        "tokens": [
            {
                "token_id": "1",
                "outcome": "Up",
                "price": 0.45,
            },
        ],
    }

    asyncio.run(provider._overlay_clob_trading_constraints(market_info))

    assert market_info["minimum_tick_size"] == 0.01
    assert market_info["minimum_order_size"] == 5
    assert market_info["accepting_orders"] is True
    assert market_info["active"] is True
    assert market_info["closed"] is False
    assert market_info["tokens"][0]["price"] == 0.55


def test_public_polymarket_provider_prunes_stale_event_slug_instruments() -> None:
    provider = PublicPolymarketInstrumentProvider.__new__(PublicPolymarketInstrumentProvider)
    current = InstrumentId.from_str("CURRENT.POLYMARKET")
    stale = InstrumentId.from_str("STALE.POLYMARKET")
    provider._instruments = {
        current: SimpleNamespace(info={"market_slug": "btc-updown-5m-2000"}),
        stale: SimpleNamespace(info={"market_slug": "btc-updown-5m-1700"}),
    }

    pruned = provider._prune_loaded_event_slug_instruments(["btc-updown-5m-2000"])

    assert pruned == 1
    assert list(provider._instruments) == [current]


def test_is_duplicate_tick_size_change() -> None:
    instrument = SimpleNamespace(
        id=InstrumentId.from_str("TOKEN.POLYMARKET"),
        price_increment=live_sandbox.Price.from_str("0.01"),
    )
    change = live_sandbox.PolymarketTickSizeChange(
        market="0xcondition",
        asset_id="TOKEN",
        new_tick_size="0.01",
        old_tick_size="0.001",
        timestamp="1778288340031",
    )

    assert live_sandbox.is_duplicate_tick_size_change(instrument, change) is True


def test_is_post_expiry_tick_size_change_uses_precise_gamma_end_date() -> None:
    instrument = SimpleNamespace(
        info={"_gamma_original": {"endDate": "2026-05-09T01:45:00Z"}},
    )
    after_expiry = live_sandbox.PolymarketTickSizeChange(
        market="0xcondition",
        asset_id="TOKEN",
        new_tick_size="0.001",
        old_tick_size="0.01",
        timestamp=str(int(datetime(2026, 5, 9, 1, 46, 28, tzinfo=UTC).timestamp() * 1000)),
    )
    before_expiry = live_sandbox.PolymarketTickSizeChange(
        market="0xcondition",
        asset_id="TOKEN",
        new_tick_size="0.001",
        old_tick_size="0.01",
        timestamp=str(int(datetime(2026, 5, 9, 1, 44, 59, tzinfo=UTC).timestamp() * 1000)),
    )

    assert live_sandbox.is_post_expiry_tick_size_change(instrument, after_expiry) is True
    assert live_sandbox.is_post_expiry_tick_size_change(instrument, before_expiry) is False


def test_is_post_expiry_tick_size_change_ignores_date_only_end_date() -> None:
    instrument = SimpleNamespace(info={"_gamma_original": {"endDate": "2026-05-09"}})
    change = live_sandbox.PolymarketTickSizeChange(
        market="0xcondition",
        asset_id="TOKEN",
        new_tick_size="0.001",
        old_tick_size="0.01",
        timestamp=str(int(datetime(2026, 5, 9, 1, 46, 28, tzinfo=UTC).timestamp() * 1000)),
    )

    assert live_sandbox.is_post_expiry_tick_size_change(instrument, change) is False
