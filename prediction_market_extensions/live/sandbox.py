from __future__ import annotations

import asyncio
import traceback
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Sequence

from py_clob_client_v2.client import ClobClient
from py_clob_client_v2.constants import POLYGON
from nautilus_trader.adapters.binance.config import (
    BinanceDataClientConfig,
    BinanceInstrumentProviderConfig,
)
from nautilus_trader.adapters.binance.factories import BinanceLiveDataClientFactory
from nautilus_trader.adapters.polymarket.config import PolymarketDataClientConfig
from nautilus_trader.adapters.polymarket.common.gamma_markets import (
    normalize_gamma_market_to_clob_format,
)
from nautilus_trader.adapters.polymarket.common.parsing import update_instrument
from nautilus_trader.adapters.polymarket.data import PolymarketDataClient
from nautilus_trader.adapters.polymarket.http.errors import check_clob_response
from nautilus_trader.adapters.polymarket.loaders import PolymarketDataLoader
from nautilus_trader.adapters.polymarket.providers import (
    PolymarketInstrumentProvider,
    PolymarketInstrumentProviderConfig,
)
from nautilus_trader.adapters.polymarket.schemas.book import PolymarketTickSizeChange
from nautilus_trader.adapters.sandbox.config import SandboxExecutionClientConfig
from nautilus_trader.adapters.sandbox.factory import SandboxLiveExecClientFactory
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common import Environment
from nautilus_trader.common.component import LiveClock, MessageBus
from nautilus_trader.common.config import LoggingConfig
from nautilus_trader.common.config import resolve_path
from nautilus_trader.live.config import (
    LiveExecEngineConfig,
    LiveRiskEngineConfig,
    RoutingConfig,
    TradingNodeConfig,
)
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.identifiers import InstrumentId, TraderId
from nautilus_trader.model.instruments import BinaryOption
from nautilus_trader.model.objects import Price
from nautilus_trader.trading.config import ImportableStrategyConfig

DEFAULT_BTC_INSTRUMENT_ID = InstrumentId.from_str("BTCUSDT.BINANCE")


def is_duplicate_tick_size_change(
    instrument: BinaryOption,
    ws_message: PolymarketTickSizeChange,
) -> bool:
    return instrument.price_increment == Price.from_str(ws_message.new_tick_size)


def _parse_iso8601_ns(value: object) -> int | None:
    if not isinstance(value, str) or "T" not in value:
        return None
    raw = value.strip()
    if raw.endswith("Z"):
        raw = f"{raw[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1_000_000_000)


def _tick_size_change_ts_ns(ws_message: PolymarketTickSizeChange) -> int | None:
    try:
        return int(float(ws_message.timestamp) * 1_000_000)
    except (TypeError, ValueError):
        return None


def is_post_expiry_tick_size_change(
    instrument: BinaryOption,
    ws_message: PolymarketTickSizeChange,
) -> bool:
    info = dict(getattr(instrument, "info", None) or {})
    gamma_original = info.get("_gamma_original") or {}
    if not isinstance(gamma_original, dict):
        gamma_original = {}
    end_ns = _parse_iso8601_ns(gamma_original.get("endDate"))
    if end_ns is None:
        return False
    event_ns = _tick_size_change_ts_ns(ws_message)
    return event_ns is not None and event_ns >= end_ns


class PublicPolymarketInstrumentProvider(PolymarketInstrumentProvider):
    """Instrument provider that uses Gamma discovery and CLOB trading constraints."""

    async def _load_from_event_slugs(self) -> None:
        if (
            not isinstance(self._config, PolymarketInstrumentProviderConfig)
            or not self._config.event_slug_builder
        ):
            return

        slug_builder = resolve_path(self._config.event_slug_builder)
        event_slugs: list[str] = slug_builder()

        self._log.info(f"Loading instruments from {len(event_slugs)} event slugs")

        instruments_loaded = 0
        events_loaded = 0

        for slug in event_slugs:
            try:
                event = await PolymarketDataLoader._fetch_event_by_slug(
                    slug=slug,
                    http_client=self._http_client,
                )
                events_loaded += 1
                instruments_loaded += await self._load_event_instruments_with_clob_constraints(
                    event,
                )
            except ValueError as e:
                if self._log_warnings:
                    self._log.warning(f"Event slug '{slug}' not found: {e}")
            except Exception:
                self._log.error(
                    f"Failed to load event slug '{slug}':\n{traceback.format_exc()}",
                )

        self._log.info(
            f"Loaded {instruments_loaded} instruments from {events_loaded} events",
        )
        pruned = self._prune_loaded_event_slug_instruments(event_slugs)
        if pruned:
            self._log.info(
                f"Pruned {pruned} stale instruments outside the current event slug set",
            )

    def _prune_loaded_event_slug_instruments(self, event_slugs: Sequence[str]) -> int:
        retained_slugs = set(event_slugs)
        if not retained_slugs:
            return 0

        before = len(self._instruments)
        self._instruments = {
            instrument_id: instrument
            for instrument_id, instrument in self._instruments.items()
            if self._instrument_market_slug(instrument) in retained_slugs
        }
        return before - len(self._instruments)

    @staticmethod
    def _instrument_market_slug(instrument: object) -> str:
        info = getattr(instrument, "info", None) or {}
        if not isinstance(info, dict):
            return ""
        return str(info.get("market_slug") or "")

    async def _load_event_instruments_with_clob_constraints(self, event: dict[str, Any]) -> int:
        count = 0

        for market in event.get("markets", []):
            condition_id = market.get("conditionId")
            if not condition_id:
                continue

            normalized_market = normalize_gamma_market_to_clob_format(market)
            await self._overlay_clob_trading_constraints(normalized_market)

            for token_info in normalized_market.get("tokens", []):
                token_id = token_info["token_id"]
                if not token_id:
                    if self._log_warnings:
                        self._log.warning(f"Market {condition_id} had an empty token")
                    continue

                outcome = token_info["outcome"]
                self._load_instrument(normalized_market, token_id, outcome)
                count += 1

        return count

    async def _overlay_clob_trading_constraints(self, market_info: dict[str, Any]) -> None:
        condition_id = market_info.get("condition_id")
        if not condition_id:
            return

        try:
            clob_market = await asyncio.to_thread(self._client.get_market, condition_id)
            clob_market = check_clob_response(clob_market)
        except Exception as e:
            if self._log_warnings:
                self._log.warning(
                    f"Could not load CLOB trading constraints for {condition_id}: {e}",
                )
            return

        for key in (
            "minimum_tick_size",
            "minimum_order_size",
            "accepting_orders",
            "active",
            "closed",
        ):
            value = clob_market.get(key)
            if value is not None:
                market_info[key] = value

        clob_tokens = {
            token["token_id"]: token
            for token in clob_market.get("tokens", [])
            if token.get("token_id")
        }
        for token in market_info.get("tokens", []):
            clob_token = clob_tokens.get(token.get("token_id"))
            if clob_token is None:
                continue
            token["price"] = clob_token.get("price", token.get("price"))
            token["outcome"] = clob_token.get("outcome", token.get("outcome"))


class PublicPolymarketDataClient(PolymarketDataClient):
    """Polymarket data client tuned for public sandbox market-data subscriptions."""

    async def _unsubscribe_order_book_deltas(self, command) -> None:  # type: ignore[no-untyped-def]
        await super()._unsubscribe_order_book_deltas(command)
        instrument_id = command.instrument_id
        self._local_books.pop(instrument_id, None)
        self._last_quotes.pop(instrument_id, None)

    def _handle_instrument_update(
        self,
        instrument: BinaryOption,
        ws_message: PolymarketTickSizeChange,
    ) -> None:
        if is_duplicate_tick_size_change(instrument, ws_message):
            self._log.debug(
                f"Ignoring duplicate tick size change for {instrument.id}: "
                f"{ws_message.old_tick_size} -> {ws_message.new_tick_size}",
            )
            return
        if is_post_expiry_tick_size_change(instrument, ws_message):
            self._apply_tick_size_change(
                instrument=instrument,
                ws_message=ws_message,
                post_expiry=True,
            )
            return

        self._apply_tick_size_change(
            instrument=instrument,
            ws_message=ws_message,
            post_expiry=False,
        )

    def _apply_tick_size_change(
        self,
        instrument: BinaryOption,
        ws_message: PolymarketTickSizeChange,
        *,
        post_expiry: bool,
    ) -> None:
        now_ns = self._clock.timestamp_ns()
        old_book = self._local_books.get(instrument.id)
        old_quote = self._last_quotes.get(instrument.id)
        instrument = update_instrument(instrument, change=ws_message, ts_init=now_ns)

        self._instrument_provider.add(instrument)
        self._cache.add_instrument(instrument)
        self._handle_data(instrument)
        if post_expiry:
            self._log.debug(
                f"Applied post-expiry Polymarket tick size change for {instrument.id}: "
                f"{ws_message.old_tick_size} -> {ws_message.new_tick_size}",
            )
        else:
            self._log.info(
                f"Applied Polymarket tick size change for {instrument.id}: "
                f"{ws_message.old_tick_size} -> {ws_message.new_tick_size}",
            )

        if old_book is not None:
            self._reset_local_book_after_tick_size_change(
                instrument=instrument,
                change=ws_message,
                old_book=old_book,
                old_quote=old_quote,
                ts_init=now_ns,
            )


class PublicPolymarketLiveDataClientFactory(LiveDataClientFactory):
    """Polymarket market-data factory using unauthenticated public CLOB access."""

    @staticmethod
    def create(
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: PolymarketDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> PolymarketDataClient:
        http_client = ClobClient(
            config.base_url_http or "https://clob.polymarket.com",
            chain_id=POLYGON,
        )
        provider = PublicPolymarketInstrumentProvider(
            client=http_client,
            clock=clock,
            config=config.instrument_config,
        )
        return PublicPolymarketDataClient(
            loop=loop,
            http_client=http_client,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


def build_polymarket_binance_sandbox_config(
    *,
    strategies: Sequence[ImportableStrategyConfig],
    event_slug_builder: str,
    binance_instrument_ids: frozenset[InstrumentId] | None = None,
    starting_balance: Decimal | str = Decimal("20"),
    trader_id: str = "SANDBOX-001",
    log_level: str = "INFO",
    polymarket_update_interval_mins: int | None = None,
    binance_us: bool = True,
    risk_submit_rate: str = "20/00:00:01",
) -> TradingNodeConfig:
    """Build a Nautilus sandbox node config."""

    polymarket_venues = frozenset({"POLYMARKET"})
    binance_venues = frozenset({"BINANCE"})
    btc_ids = binance_instrument_ids or frozenset({DEFAULT_BTC_INSTRUMENT_ID})
    balance = Decimal(str(starting_balance))

    return TradingNodeConfig(
        environment=Environment.SANDBOX,
        trader_id=TraderId(trader_id),
        data_clients={
            "POLYMARKET": PolymarketDataClientConfig(
                instrument_config=PolymarketInstrumentProviderConfig(
                    event_slug_builder=event_slug_builder,
                ),
                routing=RoutingConfig(venues=polymarket_venues),
                update_instruments_interval_mins=polymarket_update_interval_mins,
                compute_effective_deltas=False,
            ),
            "BINANCE": BinanceDataClientConfig(
                instrument_provider=BinanceInstrumentProviderConfig(load_ids=btc_ids),
                routing=RoutingConfig(venues=binance_venues),
                us=binance_us,
            ),
        },
        exec_clients={
            "POLYMARKET": SandboxExecutionClientConfig(
                routing=RoutingConfig(venues=polymarket_venues),
                venue="POLYMARKET",
                starting_balances=[f"{balance} pUSD"],
                base_currency="pUSD",
                oms_type="NETTING",
                account_type="CASH",
                book_type="L2_MBP",
            ),
        },
        strategies=list(strategies),
        risk_engine=LiveRiskEngineConfig(max_order_submit_rate=risk_submit_rate),
        exec_engine=LiveExecEngineConfig(reconciliation=False),
        logging=LoggingConfig(log_level=log_level),
    )


def build_polymarket_binance_sandbox_node(*, config: TradingNodeConfig) -> TradingNode:
    node = TradingNode(config=config)
    node.add_data_client_factory("POLYMARKET", PublicPolymarketLiveDataClientFactory)
    node.add_data_client_factory("BINANCE", BinanceLiveDataClientFactory)
    node.add_exec_client_factory("POLYMARKET", SandboxLiveExecClientFactory)
    return node
