# Codebase UML Inventory

This file is generated from Python AST metadata and excludes `tests/`.
Generated: 2026-04-23T11:45:13+00:00
Modules: 105 | Classes: 167 | Functions/methods: 1047

## Backtesting Data Flow

```mermaid
flowchart TD
    Main[main.py / runner scripts] --> Experiment[ReplayExperiment or ParameterSearchExperiment]
    Experiment --> Backtest[PredictionMarketBacktest]
    Backtest --> Registry[data_sources.registry]
    Registry --> Adapter[HistoricalReplayAdapter]
    Adapter --> Loader[Vendor loader: Kalshi / Polymarket / PMXT / Telonex]
    Loader --> Records[LoadedReplay records + instrument]
    Records --> Engine[Nautilus BacktestEngine]
    Engine --> Strategy[Strategy configs / LongOnlyPredictionMarketStrategy]
    Engine --> Artifacts[Artifacts, reports, summary series]
    Artifacts --> Optimizer[Optimizer score and leaderboard]
```

## Module Inventory

### `backtests/__init__.py`
- Imports: none

### `backtests/_script_helpers.py`
- Imports: `__future__, importlib, pathlib, sys`
- Function L8: `ensure_repo_root(script_path: str | Path) -> Path`
- Function L23: `parse_csv_env(raw: str) -> list[str]`
- Function L27: `parse_bool_env(raw: str, *, default: bool = True) -> bool`

### `backtests/kalshi_trade_tick_breakout.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L128: `run() -> None`

### `backtests/kalshi_trade_tick_independent_multi_replay_runner.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L157: `run() -> None`

### `backtests/kalshi_trade_tick_joint_portfolio_runner.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L155: `run() -> None`

### `backtests/polymarket_quote_tick_ema_crossover.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L127: `run() -> None`

### `backtests/polymarket_quote_tick_ema_optimizer.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L148: `run() -> None`

### `backtests/polymarket_quote_tick_independent_25_replay_runner.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L316: `run() -> None`

### `backtests/polymarket_quote_tick_independent_multi_replay_runner.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L196: `run() -> None`

### `backtests/polymarket_quote_tick_joint_portfolio_runner.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L180: `run() -> None`

### `backtests/polymarket_telonex_quote_tick_joint_portfolio_runner.py`
- Imports: `__future__, decimal, dotenv, os, prediction_market_extensions`
- Function L178: `run() -> None`

### `backtests/polymarket_trade_tick_independent_multi_replay_runner.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L181: `run() -> None`

### `backtests/polymarket_trade_tick_joint_portfolio_runner.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L182: `run() -> None`

### `backtests/polymarket_trade_tick_vwap_reversion.py`
- Imports: `__future__, decimal, prediction_market_extensions`
- Function L130: `run() -> None`

### `backtests/sitecustomize.py`
- Imports: `__future__, importlib, pathlib, sys`

### `main.py`
- Imports: `__future__, ast, asyncio, functools, importlib, inspect, os, pathlib, prediction_market_extensions, re, string, subprocess, sys, time, typing`
- Function L69: `_env_flag_enabled(name: str) -> bool`
- Function L76: `_discoverable_backtest_paths(backtests_root: Path) -> list[Path]`
- Function L94: `_warn(message: str) -> None`
- Function L98: `_literal_string(node: ast.AST | None) -> str | None`
- Function L108: `_assignment_targets(node: ast.Assign | ast.AnnAssign) -> list[str]`
- Function L116: `_has_assignment(module_ast: ast.Module, target_name: str) -> bool`
- Function L125: `_experiment_constructor_kwargs(module_ast: ast.Module) -> dict[str, str] | None`
- Function L144: `_has_run_entrypoint(module_ast: ast.Module) -> bool`
- Function L151: `_load_runner_metadata(path: Path) -> dict[str, Any] | None`
- Function L193: `discover() -> list[dict]`
- Function L206: `_relative_parts(backtest: dict[str, Any]) -> tuple[str, ...]`
- Function L215: `_relative_runner_path(backtest: dict[str, Any]) -> Path`
- Function L219: `_runner_stem(backtest: dict[str, Any]) -> str`
- Function L223: `_menu_label(backtest: dict[str, Any]) -> str`
- Function L227: `_textual_menu_label(backtest: dict[str, Any], shortcut: str | None) -> str`
- Function L234: `_runner_search_text(backtest: dict[str, Any]) -> str`
- Function L246: `_filter_backtests(backtests: list[dict[str, Any]], query: str) -> list[int]`
- Function L257: `_shortcut_candidates(backtest: dict[str, Any]) -> list[str]`
- Function L288: `_assign_shortcuts(backtests: list[dict[str, Any]]) -> dict[str, str | None]`
- Function L306: `_runner_file_preview(path: Path) -> str`
- Function L313: `_runner_preview(backtest: dict[str, Any]) -> str`
- Function L561: `_load_runner(backtest: dict[str, Any]) -> Any`
- Function L612: `_supports_textual_menu() -> bool`
- Function L633: `_show_basic_menu(backtests: list[dict[str, Any]]) -> int`
- Function L661: `_show_textual_menu(backtests: list[dict[str, Any]]) -> int`
- Function L671: `_build_menu_tree(backtests: list[dict[str, Any]]) -> dict[str, Any]`
- Function L682: `_render_menu_tree(node: dict[str, Any], *, prefix: str = '') -> list[str]`
- Function L709: `show_menu(backtests: list[dict]) -> int`
- Function L719: `main() -> None`

### `prediction_market_extensions/__init__.py`
- Imports: `__future__`
- Function L6: `install_commission_patch() -> None`

### `prediction_market_extensions/adapters/__init__.py`
- Imports: none

### `prediction_market_extensions/adapters/kalshi/__init__.py`
- Imports: none

### `prediction_market_extensions/adapters/kalshi/config.py`
- Imports: `__future__, nautilus_trader, os`
- Class L22: `KalshiDataClientConfig(LiveDataClientConfig)`
  - Method L58: `resolved_api_key_id(self) -> str | None`
  - Method L62: `resolved_private_key_pem(self) -> str | None`
  - Method L66: `has_credentials(self) -> bool`

### `prediction_market_extensions/adapters/kalshi/data.py`
- Imports: `__future__, asyncio, nautilus_trader, prediction_market_extensions, typing`
- Class L55: `KalshiDataClient(LiveMarketDataClient)`
  - Method L84: `__init__(self, loop: asyncio.AbstractEventLoop, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: KalshiInstrumentProvider, config: KalshiDataClientConfig, name: str | None) -> None`
  - Method L105: `async _connect(self) -> None`
  - Method L109: `async _disconnect(self) -> None`
  - Method L112: `_send_all_instruments_to_data_engine(self) -> None`
  - Method L119: `_log_unsupported(self, action: str) -> None`
  - Method L124: `async _subscribe_order_book_deltas(self, command: SubscribeOrderBook) -> None`
  - Method L127: `async _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None`
  - Method L130: `async _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None`
  - Method L133: `async _subscribe_bars(self, command: SubscribeBars) -> None`
  - Method L136: `async _subscribe_instrument_status(self, command: SubscribeInstrumentStatus) -> None`
  - Method L139: `async _subscribe_instrument_close(self, command: SubscribeInstrumentClose) -> None`
  - Method L142: `async _unsubscribe_order_book_deltas(self, command: UnsubscribeOrderBook) -> None`
  - Method L145: `async _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None`
  - Method L148: `async _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None`
  - Method L151: `async _unsubscribe_bars(self, command: UnsubscribeBars) -> None`
  - Method L154: `async _unsubscribe_instrument_status(self, command: UnsubscribeInstrumentStatus) -> None`
  - Method L157: `async _unsubscribe_instrument_close(self, command: UnsubscribeInstrumentClose) -> None`
  - Method L160: `async _request_instrument(self, request: RequestInstrument) -> None`
  - Method L168: `async _request_instruments(self, request: RequestInstruments) -> None`
  - Method L178: `async _request_quote_ticks(self, request: RequestQuoteTicks) -> None`
  - Method L181: `async _request_trade_ticks(self, request: RequestTradeTicks) -> None`
  - Method L184: `async _request_bars(self, request: RequestBars) -> None`

### `prediction_market_extensions/adapters/kalshi/factories.py`
- Imports: `__future__, asyncio, nautilus_trader, prediction_market_extensions, typing`
- Class L31: `KalshiLiveDataClientFactory(LiveDataClientFactory)`
  - Method L37: `create(loop: asyncio.AbstractEventLoop, name: str, config: KalshiDataClientConfig, msgbus: MessageBus, cache: Cache, clock: LiveClock) -> KalshiDataClient`

### `prediction_market_extensions/adapters/kalshi/fee_model.py`
- Imports: `__future__, decimal, nautilus_trader, prediction_market_extensions`
- Class L30: `KalshiProportionalFeeModelConfig(FeeModelConfig)`
- Class L44: `KalshiProportionalFeeModel(FeeModel)`
  - Method L96: `__init__(self, fee_rate: Decimal = KALSHI_TAKER_FEE_RATE, config: KalshiProportionalFeeModelConfig | None = None) -> None`
  - Method L105: `get_commission(self, order, fill_qty, fill_px, instrument) -> Money`

### `prediction_market_extensions/adapters/kalshi/loaders.py`
- Imports: `__future__, hashlib, msgspec, nautilus_trader, pandas, prediction_market_extensions, typing`
- Class L43: `KalshiDataLoader`
  - Method L78: `_normalize_price(raw: float | str) -> float`
  - Method L103: `_trade_timestamp_ns(trade: dict[str, Any]) -> int`
  - Method L110: `_trade_timestamp_seconds(cls, trade: dict[str, Any]) -> int`
  - Method L114: `_trade_sort_key(cls, trade: dict[str, Any]) -> tuple[int, str, str, str, str, str]`
  - Method L125: `_extract_yes_price(cls, trade: dict[str, Any]) -> float`
  - Method L135: `_extract_quantity(payload: dict[str, Any], *, fp_key: str, raw_key: str) -> str | int | float`
  - Method L143: `_extract_candle_price(price_payload: dict[str, Any], field: str) -> float | None`
  - Method L155: `_fallback_trade_id(ticker: str, trade: dict[str, Any], occurrence: int) -> TradeId`
  - Method L163: `__init__(self, instrument: BinaryOption, series_ticker: str, http_client: nautilus_pyo3.HttpClient | None = None) -> None`
  - Method L174: `_create_http_client() -> nautilus_pyo3.HttpClient`
  - Method L180: `instrument(self) -> BinaryOption`
  - Method L185: `async from_market_ticker(cls, ticker: str, http_client: nautilus_pyo3.HttpClient | None = None) -> KalshiDataLoader`
  - Method L239: `async fetch_trades(self, min_ts: int | None = None, max_ts: int | None = None, limit: int = 1000) -> list[dict[str, Any]]`
  - Method L293: `async fetch_candlesticks(self, start_ts: int | None = None, end_ts: int | None = None, interval: str = 'Minutes1') -> list[dict[str, Any]]`
  - Method L345: `parse_trades(self, trades_data: list[dict[str, Any]]) -> list[TradeTick]`
  - Method L412: `parse_candlesticks(self, candlesticks_data: list[dict[str, Any]], interval: str = 'Minutes1') -> list[Bar]`
  - Method L482: `async load_bars(self, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None, interval: str = 'Minutes1') -> list[Bar]`
  - Method L515: `async load_trades(self, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None) -> list[TradeTick]`

### `prediction_market_extensions/adapters/kalshi/market_selection.py`
- Imports: `__future__, collections, datetime, re, typing`
- Function L29: `_parse_datetime(raw) -> datetime | None`
- Function L39: `volume_24h(market: Mapping[str, Any]) -> float`
- Function L56: `yes_price(market: Mapping[str, Any]) -> float | None`
- Function L75: `end_date_utc(market: Mapping[str, Any]) -> datetime | None`
- Function L82: `market_close_time_ns(raw) -> int`
- Function L92: `days_since_close(raw, now: datetime) -> float | None`
- Function L103: `market_duration_days(market: Mapping[str, Any]) -> float | None`
- Function L119: `is_game_market(market: Mapping[str, Any]) -> bool`
- Function L132: `is_sports_market(market: Mapping[str, Any], *, now: datetime, max_hours_to_close: float, max_market_duration_days: float | None = None) -> bool`
- Function L161: `is_resolved_sports_market(market: Mapping[str, Any], *, now: datetime, max_days_since_close: float, max_market_duration_days: float | None = None) -> bool`

### `prediction_market_extensions/adapters/kalshi/providers.py`
- Imports: `__future__, datetime, decimal, logging, math, nautilus_trader, prediction_market_extensions`
- Function L49: `calculate_kalshi_commission(quantity: decimal.Decimal, price: decimal.Decimal, fee_rate: decimal.Decimal = KALSHI_TAKER_FEE_RATE) -> decimal.Decimal`
- Function L98: `_market_dict_to_instrument(market: dict) -> BinaryOption`
- Class L149: `_KalshiHttpClient`
  - Method L164: `__init__(self, base_url: str) -> None`
  - Method L175: `async get_markets(self, series_tickers: tuple[str, ...] = (), event_tickers: tuple[str, ...] = ()) -> list[dict]`
- Class L223: `KalshiInstrumentProvider(InstrumentProvider)`
  - Method L236: `__init__(self, config: KalshiDataClientConfig) -> None`
  - Method L242: `async load_all_async(self, filters: dict | None = None) -> None`
  - Method L252: `async _fetch_markets(self) -> list[dict]`
  - Method L258: `_market_to_instrument(self, market: dict) -> BinaryOption`

### `prediction_market_extensions/adapters/kalshi/research.py`
- Imports: `__future__, asyncio, collections, datetime, msgspec, nautilus_trader, pandas, prediction_market_extensions, typing`
- Function L49: `_passes_filters(market: Mapping[str, Any], *, min_volume_24h: float, yes_price_min: float | None, yes_price_max: float | None, min_expiry_dt: datetime | None, predicate: MarketPredicate | None) -> bool`
- Function L78: `_extend_with_event_markets(all_markets: list[dict[str, Any]], events: list[dict[str, Any]], *, exclude_ticker_prefixes: tuple[str, ...]) -> None`
- Function L98: `_default_http_client(*, quota_rate_per_second: int) -> nautilus_pyo3.HttpClient`
- Function L104: `async fetch_market_by_ticker(ticker: str, *, http_client: nautilus_pyo3.HttpClient | None = None, quota_rate_per_second: int = 10) -> dict[str, Any]`
- Function L128: `async discover_markets(*, http_client: nautilus_pyo3.HttpClient, candidate_limit: int, status: str = 'open', page_limit: int = 200, max_pages: int | None = None, include_nested_markets: bool = True, exclude_ticker_prefixes: tuple[str, ...] = ('KXMVE',), min_volume_24h: float = 0.0, yes_price_min: float | None = None, yes_price_max: float | None = None, min_days_to_expiry: int | None = None, predicate: MarketPredicate | None = None, sort_key: MarketSortKey = volume_24h, descending: bool = True) -> list[dict[str, Any]]`
- Function L202: `async discover_live_sports_markets(*, candidate_limit: int, http_client: nautilus_pyo3.HttpClient | None = None, quota_rate_per_second: int = 10, max_pages: int | None = None, page_limit: int = 200, min_volume: float = 0.0, max_hours_to_close: float, max_market_duration_days: float | None = None, games_only: bool = False) -> list[dict[str, Any]]`
- Function L238: `async discover_resolved_sports_markets(*, candidate_limit: int, http_client: nautilus_pyo3.HttpClient | None = None, quota_rate_per_second: int = 10, max_pages: int | None = None, page_limit: int = 200, min_volume: float = 0.0, max_days_since_close: float, max_market_duration_days: float | None = None, games_only: bool = False) -> list[dict[str, Any]]`
- Function L274: `_analysis_window_end(*, market: Mapping[str, Any], now: datetime) -> datetime | None`
- Function L281: `async analyze_market_trade_window(*, market: Mapping[str, Any], lookback_days: int, entry_price: float, now: datetime | None = None) -> dict[str, Any] | None`
- Function L332: `async select_breakout_markets_per_game(*, markets: list[dict[str, Any]], lookback_days: int, entry_price: float, now: datetime | None = None, max_results: int | None = None) -> list[dict[str, Any]]`
- Function L407: `async load_market_bars(*, market: Mapping[str, Any], start: pd.Timestamp, end: pd.Timestamp, http_client: nautilus_pyo3.HttpClient, interval: str = 'Minutes1', chunk_minutes: int = 5000, min_bars: int = 0, min_price_range: float = 0.0, max_retries: int = 4, retry_base_delay: float = 2.0) -> tuple[KalshiDataLoader, list[Bar]] | None`

### `prediction_market_extensions/adapters/polymarket/__init__.py`
- Imports: none

### `prediction_market_extensions/adapters/polymarket/execution.py`
- Imports: `asyncio, collections, json, msgspec, nautilus_trader, prediction_market_extensions, py_clob_client, typing`
- Class L124: `PolymarketExecutionClient(LiveExecutionClient)`
  - Method L151: `__init__(self, loop: asyncio.AbstractEventLoop, http_client: ClobClient, msgbus: MessageBus, cache: Cache, clock: LiveClock, instrument_provider: PolymarketInstrumentProvider, ws_auth: PolymarketWebSocketAuth, config: PolymarketExecClientConfig, name: str | None) -> None`
  - Method L249: `async _connect(self) -> None`
  - Method L271: `async _disconnect(self) -> None`
  - Method L274: `_stop(self) -> None`
  - Method L277: `async _maintain_active_market(self, instrument_id: InstrumentId) -> None`
  - Method L281: `async _update_account_state(self) -> None`
  - Method L302: `async _fetch_user_positions(self, *, limit: int = 100, size_threshold: int = 0) -> list[dict[str, Any]]`
  - Method L350: `async generate_order_status_reports(self, command: GenerateOrderStatusReports) -> list[OrderStatusReport]`
  - Method L521: `async generate_order_status_report(self, command: GenerateOrderStatusReport) -> OrderStatusReport | None`
  - Method L576: `async generate_fill_reports(self, command: GenerateFillReports) -> list[FillReport]`
  - Method L625: `async generate_position_status_reports(self, command: GeneratePositionStatusReports) -> list[PositionStatusReport]`
  - Method L664: `_parse_trades_response_object(self, command: GenerateFillReports, json_obj: JSON, parsed_fill_keys: set[tuple[TradeId, VenueOrderId]], reports: list[FillReport]) -> None`
  - Method L719: `async _fetch_quantities_from_gamma_api(self, instrument_ids: list[InstrumentId]) -> dict[InstrumentId, Quantity]`
  - Method L757: `async _fetch_quantities_from_clob_api(self, instrument_ids: list[InstrumentId]) -> dict[InstrumentId, Quantity]`
  - Method L785: `_generate_cancel_event(self, strategy_id, instrument_id, client_order_id, venue_order_id, reason: str, ts_event: int) -> None`
  - Method L811: `_get_neg_risk_for_instrument(self, instrument) -> bool`
  - Method L816: `async _query_account(self, _command: QueryAccount) -> None`
  - Method L820: `async _cancel_order(self, command: CancelOrder) -> None`
  - Method L867: `async _batch_cancel_orders(self, command: BatchCancelOrders) -> None`
  - Method L920: `async _cancel_all_orders(self, command: CancelAllOrders) -> None`
  - Method L968: `async _cancel_all_global(self) -> None`
  - Method L1002: `async _cancel_market_orders(self, instrument_id: InstrumentId | None = None, asset_id: str = '') -> None`
  - Method L1056: `async _submit_order(self, command: SubmitOrder) -> None`
  - Method L1130: `_validate_order_for_batch(self, order: Order) -> str | None`
  - Method L1154: `async _submit_order_list(self, command: SubmitOrderList) -> None`
  - Method L1224: `async _sign_orders_for_batch(self, orders: list[Order]) -> tuple[list[Order], list[PostOrdersArgs]]`
  - Method L1285: `async _post_signed_orders_batch(self, orders: list[Order], signed_orders_args: list[PostOrdersArgs]) -> None`
  - Method L1314: `_reject_all_orders(self, orders: list[Order], reason: str) -> None`
  - Method L1327: `_process_batch_response(self, orders: list[Order], response: list) -> None`
  - Method L1378: `_deny_market_order_quantity(self, order: Order, reason: str) -> None`
  - Method L1390: `async _submit_market_order(self, command: SubmitOrder, instrument) -> None`
  - Method L1439: `async _submit_limit_order(self, command: SubmitOrder, instrument) -> None`
  - Method L1485: `async _post_signed_order(self, order: Order, signed_order, post_only: bool = False) -> None`
  - Method L1521: `_handle_ws_message(self, raw: bytes) -> None`
  - Method L1543: `_add_trade_to_cache(self, msg: PolymarketUserTrade, raw: bytes) -> None`
  - Method L1553: `async _wait_for_ack_order(self, msg: PolymarketUserOrder, venue_order_id: VenueOrderId) -> None`
  - Method L1576: `async _wait_for_ack_trade(self, msg: PolymarketUserTrade, venue_order_id: VenueOrderId) -> None`
  - Method L1603: `_handle_ws_order_msg(self, msg: PolymarketUserOrder, wait_for_ack: bool) -> Any`
  - Method L1675: `_truncate_ordered_dict(self, store: OrderedDict[Any, Any]) -> None`
  - Method L1679: `_record_processed_trade(self, trade_id: TradeId, status: PolymarketTradeStatus) -> None`
  - Method L1693: `_record_processed_fill(self, trade_id: TradeId, venue_order_id: VenueOrderId) -> None`
  - Method L1699: `_handle_ws_trade_msg(self, msg: PolymarketUserTrade, wait_for_ack: bool) -> Any`
  - Method L1741: `_handle_user_trade_in_ws_trade_msg(self, msg: PolymarketUserTrade, trade_id: TradeId, wait_for_ack: bool, order_id: str) -> Any`

### `prediction_market_extensions/adapters/polymarket/fee_model.py`
- Imports: `__future__, decimal, nautilus_trader, prediction_market_extensions`
- Class L28: `PolymarketFeeModel(FeeModel)`
  - Method L49: `get_commission(self, order, fill_qty, fill_px, instrument) -> Money`

### `prediction_market_extensions/adapters/polymarket/gamma_markets.py`
- Imports: `__future__, collections, math, msgspec, nautilus_trader, os, typing`
- Function L43: `_normalize_base_url(base_url: str | None) -> str`
- Function L48: `build_markets_query(filters: dict[str, Any] | None = None) -> dict[str, Any]`
- Function L105: `async _request_markets_page(http_client: HttpClient, base_url: str, params: dict[str, Any], offset: int, limit: int, timeout: float) -> list[dict[str, Any]]`
- Function L142: `async iter_markets(http_client: HttpClient, filters: dict[str, Any] | None = None, base_url: str | None = None, timeout: float = 10.0) -> AsyncGenerator[dict[str, Any]]`
- Function L174: `_decode_gamma_list(raw) -> list[Any]`
- Function L182: `_truthy_gamma_value(raw) -> bool`
- Function L192: `_gamma_market_allows_price_winner_inference(gamma_market: dict[str, Any]) -> bool`
- Function L217: `infer_gamma_token_winners(gamma_market: dict[str, Any]) -> tuple[dict[str, bool], bool]`
- Function L249: `normalize_gamma_market_to_clob_format(gamma_market: dict[str, Any]) -> dict[str, Any]`
- Function L339: `async list_markets(http_client: HttpClient, filters: dict[str, Any] | None = None, base_url: str | None = None, timeout: float = 10.0, max_results: int | None = None) -> list[dict[str, Any]]`

### `prediction_market_extensions/adapters/polymarket/loaders.py`
- Imports: `__future__, decimal, msgspec, nautilus_trader, pandas, prediction_market_extensions, typing, warnings`
- Function L43: `_trade_sort_key(trade: dict[str, Any]) -> tuple[int, str, str, str, str, str]`
- Class L54: `PolymarketDataLoader`
  - Method L82: `__init__(self, instrument: BinaryOption, token_id: str | None = None, condition_id: str | None = None, http_client: nautilus_pyo3.HttpClient | None = None) -> None`
  - Method L95: `_create_http_client() -> nautilus_pyo3.HttpClient`
  - Method L101: `async _fetch_market_by_slug(slug: str, http_client: nautilus_pyo3.HttpClient) -> dict[str, Any]`
  - Method L135: `async _fetch_market_details(condition_id: str, http_client: nautilus_pyo3.HttpClient) -> dict[str, Any]`
  - Method L150: `_coerce_fee_rate_bps(value) -> Decimal | None`
  - Method L160: `async _fetch_market_fee_rate_bps(cls, token_id: str, http_client: nautilus_pyo3.HttpClient) -> Decimal | None`
  - Method L180: `async _enrich_market_details_with_fee_rate(cls, market_details: dict[str, Any], token_id: str, http_client: nautilus_pyo3.HttpClient) -> dict[str, Any]`
  - Method L200: `async _fetch_event_by_slug(slug: str, http_client: nautilus_pyo3.HttpClient) -> dict[str, Any]`
  - Method L225: `async from_market_slug(cls, slug: str, token_index: int = 0, http_client: nautilus_pyo3.HttpClient | None = None) -> PolymarketDataLoader`
  - Method L302: `async from_event_slug(cls, slug: str, token_index: int = 0, http_client: nautilus_pyo3.HttpClient | None = None) -> list[PolymarketDataLoader]`
  - Method L379: `async query_market_by_slug(slug: str, http_client: nautilus_pyo3.HttpClient | None = None) -> dict[str, Any]`
  - Method L409: `async query_market_details(condition_id: str, http_client: nautilus_pyo3.HttpClient | None = None) -> dict[str, Any]`
  - Method L437: `async query_event_by_slug(slug: str, http_client: nautilus_pyo3.HttpClient | None = None) -> dict[str, Any]`
  - Method L467: `instrument(self) -> BinaryOption`
  - Method L474: `token_id(self) -> str | None`
  - Method L481: `condition_id(self) -> str | None`
  - Method L487: `async load_trades(self, start: pd.Timestamp | None = None, end: pd.Timestamp | None = None) -> list[TradeTick]`
  - Method L546: `async fetch_event_by_slug(self, slug: str) -> dict[str, Any]`
  - Method L573: `async fetch_events(self, active: bool = True, closed: bool = False, archived: bool = False, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]`
  - Method L621: `async get_event_markets(self, slug: str) -> list[dict[str, Any]]`
  - Method L646: `async fetch_markets(self, active: bool = True, closed: bool = False, archived: bool = False, limit: int = 100, offset: int = 0) -> list[dict]`
  - Method L694: `async fetch_market_by_slug(self, slug: str) -> dict[str, Any]`
  - Method L718: `async find_market_by_slug(self, slug: str) -> dict[str, Any]`
  - Method L740: `async fetch_market_details(self, condition_id: str) -> dict[str, Any]`
  - Method L757: `async fetch_trades(self, condition_id: str, limit: int = _TRADES_PAGE_LIMIT, start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]`
  - Method L844: `parse_trades(self, trades_data: list[dict]) -> list[TradeTick]`

### `prediction_market_extensions/adapters/polymarket/market_selection.py`
- Imports: `__future__, collections, datetime, msgspec, re, typing`
- Function L57: `_parse_datetime(raw) -> datetime | None`
- Function L67: `_event_payload(market: Mapping[str, Any]) -> Mapping[str, Any]`
- Function L77: `volume_24h(market: Mapping[str, Any]) -> float`
- Function L91: `yes_price(market: Mapping[str, Any]) -> float | None`
- Function L108: `end_date_utc(market: Mapping[str, Any]) -> datetime | None`
- Function L116: `event_start_utc(market: Mapping[str, Any]) -> datetime | None`
- Function L135: `closed_time_utc(market: Mapping[str, Any]) -> datetime | None`
- Function L155: `market_close_time_ns(raw) -> int`
- Function L165: `is_game_market(market: Mapping[str, Any]) -> bool`
- Function L191: `is_sports_market(market: Mapping[str, Any], *, now: datetime, max_hours_to_close: float) -> bool`
- Function L215: `is_resolved_sports_market(market: Mapping[str, Any], *, now: datetime, max_days_since_close: float) -> bool`

### `prediction_market_extensions/adapters/polymarket/parsing.py`
- Imports: `__future__, decimal`
- Function L34: `basis_points_as_decimal(basis_points: Decimal) -> Decimal`
- Function L52: `infer_fee_exponent(fee_rate_bps: Decimal) -> int`
- Function L75: `calculate_commission(quantity: Decimal, price: Decimal, fee_rate_bps: Decimal, fee_exponent: int = 1, **_kwargs: object) -> float`

### `prediction_market_extensions/adapters/polymarket/pmxt.py`
- Imports: `__future__, collections, concurrent, contextlib, datetime, fsspec, msgspec, nautilus_trader, os, pandas, pathlib, pyarrow, re, shutil, tempfile, time, typing, urllib`
- Class L42: `_PMXTBookSnapshotPayload(msgspec.Struct)`
- Class L54: `_PMXTPriceChangePayload(msgspec.Struct)`
- Class L71: `PolymarketPMXTDataLoader(PolymarketDataLoader)`
  - Method L98: `__init__(self, *args, **kwargs) -> None`
  - Method L117: `_normalize_timestamp(value: pd.Timestamp | str | None) -> pd.Timestamp | None`
  - Method L126: `_archive_hours(start: pd.Timestamp, end: pd.Timestamp) -> list[pd.Timestamp]`
  - Method L136: `_archive_filename_for_hour(cls, hour: pd.Timestamp) -> str`
  - Method L141: `_archive_url_for_hour(cls, hour: pd.Timestamp) -> str`
  - Method L145: `_archive_relative_path_for_hour(cls, hour: pd.Timestamp) -> str`
  - Method L153: `_env_flag_enabled(value: str | None) -> bool`
  - Method L159: `_default_cache_dir(cls) -> Path`
  - Method L165: `_resolve_cache_dir(cls) -> Path | None`
  - Method L181: `_resolve_local_archive_dir(cls) -> Path | None`
  - Method L192: `_resolve_prefetch_workers(cls) -> int`
  - Method L207: `_resolve_http_block_size(cls) -> int`
  - Method L222: `_resolve_http_cache_type(cls) -> str`
  - Method L230: `_reset_http_filesystem(self) -> None`
  - Method L237: `_market_cache_path_for_hour(cls, cache_dir: Path, condition_id: str, token_id: str, hour: pd.Timestamp) -> Path`
  - Method L242: `_cache_path_for_hour(self, hour: pd.Timestamp) -> Path | None`
  - Method L251: `_local_archive_candidate_paths_for_hour(cls, archive_dir: Path, hour: pd.Timestamp) -> tuple[Path, ...]`
  - Method L258: `_local_archive_paths_for_hour(self, hour: pd.Timestamp) -> tuple[Path, ...]`
  - Method L263: `_market_filter(self) -> Any`
  - Method L270: `_empty_market_table(cls) -> pa.Table`
  - Method L276: `_to_market_batch(cls, batch: pa.RecordBatch) -> pa.RecordBatch`
  - Method L283: `_filter_batch_to_token(self, batch: pa.RecordBatch) -> pa.RecordBatch`
  - Method L293: `_filter_raw_batch(self, batch: pa.RecordBatch) -> pa.RecordBatch`
  - Method L310: `_load_cached_market_table(self, hour: pd.Timestamp) -> pa.Table | None`
  - Method L322: `_load_cached_market_batches(self, hour: pd.Timestamp) -> list[pa.RecordBatch] | None`
  - Method L335: `_write_market_cache(self, hour: pd.Timestamp, table: pa.Table) -> None`
  - Method L348: `_scan_raw_market_batches(self, dataset: ds.Dataset, *, batch_size: int, source: str | None = None, total_bytes: int | None = None) -> list[pa.RecordBatch]`
  - Method L403: `_load_remote_market_table(self, hour: pd.Timestamp, *, batch_size: int) -> pa.Table | None`
  - Method L411: `_load_remote_market_batches(self, hour: pd.Timestamp, *, batch_size: int) -> list[pa.RecordBatch] | None`
  - Method L417: `_load_raw_market_batches_via_download(self, archive_url: str, *, batch_size: int) -> list[pa.RecordBatch] | None`
  - Method L440: `_load_local_archive_market_batches(self, hour: pd.Timestamp, *, batch_size: int) -> list[pa.RecordBatch] | None`
  - Method L464: `_filter_table_to_token(self, table: pa.Table) -> pa.Table`
  - Method L474: `_load_market_table(self, hour: pd.Timestamp, *, batch_size: int) -> pa.Table | None`
  - Method L501: `_load_market_batches(self, hour: pd.Timestamp, *, batch_size: int) -> list[pa.RecordBatch] | None`
  - Method L526: `_emit_download_progress(self, url: str, *, downloaded_bytes: int, total_bytes: int | None, finished: bool) -> None`
  - Method L534: `_emit_scan_progress(self, source: str, *, scanned_batches: int, scanned_rows: int, matched_rows: int, total_bytes: int | None, finished: bool) -> None`
  - Method L550: `_content_length_from_response(response: object) -> int | None`
  - Method L562: `_progress_total_bytes(self, source: str) -> int | None`
  - Method L589: `_download_to_file_with_progress(self, url: str, destination: Path) -> int | None`
  - Method L638: `_download_payload_with_progress(self, url: str) -> bytes | None`
  - Method L677: `_load_raw_market_batches_from_local_file(self, parquet_path: Path, *, batch_size: int, progress_source: str, total_bytes: int | None) -> list[pa.RecordBatch] | None`
  - Method L689: `_temporary_download_filename(url: str) -> str`
  - Method L694: `_pid_is_active(pid: int) -> bool`
  - Method L706: `_temporary_download_path(self, url: str) -> Iterator[Path]`
  - Method L718: `_cleanup_stale_temp_downloads(self) -> None`
  - Method L747: `_iter_market_tables(self, hours: list[pd.Timestamp], *, batch_size: int) -> Iterator[tuple[pd.Timestamp, pa.Table | None]]`
  - Method L778: `_iter_market_batches(self, hours: list[pd.Timestamp], *, batch_size: int) -> Iterator[tuple[pd.Timestamp, list[pa.RecordBatch] | None]]`
  - Method L810: `_timestamp_to_ms_string(timestamp_secs: float) -> str`
  - Method L814: `_quote_from_book(*, instrument, local_book: OrderBook, ts_event_ns: int) -> QuoteTick | None`
  - Method L836: `_decode_book_snapshot(payload_text: str) -> _PMXTBookSnapshotPayload`
  - Method L840: `_decode_price_change(payload_text: str) -> _PMXTPriceChangePayload`
  - Method L844: `_to_book_snapshot(payload: _PMXTBookSnapshotPayload) -> PolymarketBookSnapshot`
  - Method L854: `_to_price_change(payload: _PMXTPriceChangePayload) -> PolymarketQuotes`
  - Method L876: `_event_sort_key(record: OrderBookDeltas | QuoteTick) -> tuple[int, int, int]`
  - Method L887: `_payload_sort_key(self, update_type: str, payload_text: str) -> tuple[int, int]`
  - Method L899: `_process_book_snapshot(self, payload_text: str, *, token_id: str, instrument, local_book: OrderBook, has_snapshot: bool, events: list[OrderBookDeltas | QuoteTick], start_ns: int, end_ns: int, include_order_book: bool, include_quotes: bool) -> tuple[OrderBook, bool]`
  - Method L940: `_process_price_change(self, payload_text: str, *, token_id: str, instrument, local_book: OrderBook, has_snapshot: bool, events: list[OrderBookDeltas | QuoteTick], start_ns: int, end_ns: int, include_order_book: bool, include_quotes: bool) -> OrderBook`
  - Method L982: `load_order_book_and_quotes(self, start: pd.Timestamp, end: pd.Timestamp, *, batch_size: int = 25000, include_order_book: bool = True, include_quotes: bool = True) -> list[OrderBookDeltas | QuoteTick]`

### `prediction_market_extensions/adapters/polymarket/research.py`
- Imports: `__future__, collections, datetime, msgspec, nautilus_trader, pandas, prediction_market_extensions, typing`
- Function L49: `_default_http_client(*, quota_rate_per_second: int) -> nautilus_pyo3.HttpClient`
- Function L55: `_passes_filters(market: Mapping[str, Any], *, min_volume_24h: float, yes_price_min: float | None, yes_price_max: float | None, min_expiry_dt: datetime | None, predicate: MarketPredicate | None) -> bool`
- Function L84: `_event_volume(market: Mapping[str, Any]) -> float`
- Function L88: `_main_market_from_event(event: Mapping[str, Any]) -> dict[str, Any] | None`
- Function L116: `async _discover_resolved_game_markets_from_events(*, candidate_limit: int, http_client: nautilus_pyo3.HttpClient | None = None, max_results: int, quota_rate_per_second: int, min_volume_24h: float, max_days_since_close: float) -> list[dict[str, Any]]`
- Function L199: `async discover_markets(*, candidate_limit: int, http_client: nautilus_pyo3.HttpClient | None = None, api_filters: dict[str, Any] | None = None, max_results: int = 200, quota_rate_per_second: int = 20, min_volume_24h: float = 0.0, yes_price_min: float | None = None, yes_price_max: float | None = None, min_days_to_expiry: int | None = None, predicate: MarketPredicate | None = None, sort_key: MarketSortKey = volume_24h, descending: bool = True) -> list[dict[str, Any]]`
- Function L258: `async fetch_market_by_slug(slug: str, *, http_client: nautilus_pyo3.HttpClient | None = None, quota_rate_per_second: int = 10) -> dict[str, Any]`
- Function L282: `async discover_live_sports_markets(*, candidate_limit: int, http_client: nautilus_pyo3.HttpClient | None = None, max_results: int = 200, quota_rate_per_second: int = 20, min_volume_24h: float = 0.0, max_hours_to_close: float, games_only: bool = False) -> list[dict[str, Any]]`
- Function L309: `async discover_resolved_sports_markets(*, candidate_limit: int, http_client: nautilus_pyo3.HttpClient | None = None, max_results: int = 200, quota_rate_per_second: int = 20, min_volume_24h: float = 0.0, max_days_since_close: float, games_only: bool = False) -> list[dict[str, Any]]`
- Function L351: `market_trade_window_bounds(market: Mapping[str, Any], *, active_window_hours: float, now: datetime | None = None) -> tuple[datetime | None, datetime | None]`
- Function L382: `async analyze_market_trade_window(*, market: Mapping[str, Any], lookback_days: int, entry_price: float, active_window_hours: float, now: datetime | None = None, http_client: nautilus_pyo3.HttpClient | None = None) -> dict[str, Any] | None`
- Function L489: `async load_market_trades(*, slug: str, start: pd.Timestamp, end: pd.Timestamp, min_trades: int = 0, min_price_range: float = 0.0) -> tuple[PolymarketDataLoader, list[TradeTick]] | None`

### `prediction_market_extensions/adapters/prediction_market/__init__.py`
- Imports: `prediction_market_extensions`

### `prediction_market_extensions/adapters/prediction_market/backtest_utils.py`
- Imports: `__future__, collections, datetime, pandas`
- Function L29: `_parse_numeric(value: object, default: float = 0.0) -> float`
- Function L49: `extract_realized_pnl(pos_report: pd.DataFrame) -> float`
- Function L59: `_timestamp_to_naive_utc_datetime(ts: pd.Timestamp) -> datetime`
- Function L70: `to_naive_utc(value: object) -> datetime | None`
- Function L100: `extract_price_points(records: Sequence[object], *, price_attr: str, ts_attrs: tuple[str, ...] = _DEFAULT_TS_ATTRS) -> list[PricePoint]`
- Function L137: `downsample_price_points(points: list[PricePoint], max_points: int = 5000) -> list[PricePoint]`
- Function L168: `_probability_frame(points: Sequence[PricePoint]) -> pd.DataFrame`
- Function L207: `_resolved_outcome_from_result(info: Mapping[object, object], outcome_name: str) -> float | None`
- Function L220: `_resolved_outcome_from_numeric_fields(info: Mapping[object, object]) -> float | None`
- Function L239: `_resolved_outcome_from_tokens(info: Mapping[object, object], outcome_name: str) -> float | None`
- Function L257: `infer_realized_outcome(source: object | None) -> float | None`
- Function L288: `compute_binary_settlement_pnl(fill_events: Sequence[Mapping[object, object]], resolved_outcome: float | None) -> float | None`
- Function L320: `build_brier_inputs(points: Sequence[PricePoint], window: int, realized_outcome: float | None = None) -> tuple[pd.Series, pd.Series, pd.Series]`
- Function L352: `build_market_prices(points: Sequence[PricePoint], *, resample_rule: str | None = None) -> list[tuple[datetime, float]]`

### `prediction_market_extensions/adapters/prediction_market/fill_model.py`
- Imports: `__future__, decimal, nautilus_trader`
- Function L21: `effective_prediction_market_slippage_tick(instrument) -> float`
- Class L38: `PredictionMarketTakerFillModel(FillModel)`
  - Method L71: `__init__(self, *, slippage_ticks: int = 1, entry_slippage_pct: float = 0.0, exit_slippage_pct: float = 0.0) -> None`
  - Method L91: `get_orderbook_for_fill_simulation(self, instrument, order, best_bid, best_ask) -> Any`

### `prediction_market_extensions/adapters/prediction_market/replay.py`
- Imports: `__future__, abc, collections, contextlib, dataclasses, nautilus_trader, typing`
- Class L31: `ReplayAdapterKey`
- Class L38: `ReplayWindow`
- Class L44: `ReplayCoverageStats`
- Class L53: `ReplayLoadRequest`
- Class L63: `ReplayEngineProfile`
- Class L77: `LoadedReplay`
  - Method L91: `spec(self) -> Any`
  - Method L95: `count(self) -> int`
  - Method L99: `count_key(self) -> str`
  - Method L103: `market_key(self) -> str`
  - Method L107: `market_id(self) -> str`
  - Method L111: `prices(self) -> tuple[float, ...]`
- Class L115: `HistoricalReplayAdapter(ABC)`
  - Method L118: `key(self) -> ReplayAdapterKey`
  - Method L123: `replay_spec_type(self) -> type[Any]`
  - Method L126: `build_single_market_replay(self, *, field_values: Mapping[str, Any]) -> Any`
  - Method L132: `configure_sources(self, *, sources: Sequence[str]) -> AbstractContextManager[Any]`
  - Method L137: `engine_profile(self) -> ReplayEngineProfile`
  - Method L141: `async load_replay(self, replay, *, request: ReplayLoadRequest) -> LoadedReplay | None`

### `prediction_market_extensions/adapters/prediction_market/research.py`
- Imports: `__future__, collections, datetime, nautilus_trader, os, pandas, pathlib, prediction_market_extensions, re, typing`
- Function L69: `_extract_account_pnl_series(engine: BacktestEngine) -> pd.Series`
- Function L92: `_dense_account_series_from_engine(*, engine: BacktestEngine, market_id: str, market_prices: Sequence[tuple[datetime, float]], initial_cash: float) -> tuple[pd.Series, pd.Series]`
- Function L104: `_dense_account_series_from_engine_for_markets(*, engine: BacktestEngine, market_prices: Mapping[str, Sequence[tuple[datetime, float]]], initial_cash: float) -> tuple[pd.Series, pd.Series]`
- Function L143: `_dense_market_account_series_from_fill_events(*, market_id: str, market_prices: Sequence[tuple[datetime, float]], fill_events: Sequence[dict[str, Any]], initial_cash: float) -> tuple[pd.Series, pd.Series]`
- Function L220: `_pairs_to_series(pairs: Sequence[tuple[str, float]] | Sequence[tuple[Any, float]]) -> pd.Series`
- Function L235: `_to_legacy_datetime(timestamp: pd.Timestamp) -> datetime`
- Function L239: `_series_to_iso_pairs(series: pd.Series) -> list[tuple[str, float]]`
- Function L246: `_align_series_to_timeline(series: pd.Series, timeline: pd.DatetimeIndex, *, before: float, after: float) -> pd.Series`
- Function L258: `_parse_float_like(value, default: float = 0.0) -> float`
- Function L278: `_serialize_fill_events(*, market_id: str, fills_report: pd.DataFrame) -> list[dict[str, Any]]`
- Function L325: `_deserialize_fill_events(*, market_id: str, fill_events: Sequence[dict[str, Any]], models_module) -> list[Any]`
- Function L361: `_aggregate_brier_frames(results: Sequence[dict[str, Any]]) -> dict[str, pd.DataFrame]`
- Function L389: `_aggregate_brier_unavailable_reason(results: Sequence[dict[str, Any]]) -> str | None`
- Function L411: `_summary_panels_need_market_prices(plot_panels: Sequence[str]) -> bool`
- Function L415: `_summary_panels_need_fill_events(plot_panels: Sequence[str]) -> bool`
- Function L421: `_summary_panels_need_overlay_series(plot_panels: Sequence[str]) -> bool`
- Function L428: `_yes_price_fill_marker_budget(max_points: int) -> int`
- Function L434: `_summary_yes_price_fill_marker_limit(fill_count: int, max_points: int) -> int | None`
- Function L445: `_configure_summary_report_downsampling(plotting_module, *, adaptive: bool = True, max_points: int = 5000) -> None`
- Function L492: `_build_summary_brier_panel(brier_frames: dict[str, pd.DataFrame], *, axis_label: str, max_points_per_market: int) -> Any | None`
- Function L506: `_build_total_summary_brier_frame(brier_frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame`
- Function L527: `_build_summary_brier_extra_panels(*, results: Sequence[dict[str, Any]], resolved_plot_panels: Sequence[str], max_points_per_market: int) -> dict[str, Any]`
- Function L572: `_apply_summary_layout_overrides(layout, *, initial_cash: float, max_yes_price_fill_markers: int | None) -> Any`
- Function L586: `run_market_backtest(*, market_id: str, instrument, data: Sequence[object], strategy: Strategy, strategy_name: str, output_prefix: str, platform: str, venue: Venue, base_currency: Currency, fee_model, fill_model: Any | None = None, apply_default_fill_model: bool = True, initial_cash: float, probability_window: int, price_attr: str, count_key: str, data_count: int | None = None, chart_resample_rule: str | None = None, market_key: str = 'market', open_browser: bool = False, emit_html: bool = True, return_chart_layout: bool = False, return_summary_series: bool = False, chart_output_path: str | Path | None = None, book_type: BookType = BookType.L1_MBP, liquidity_consumption: bool = False, queue_position: bool = False, latency_model: Any | None = None) -> dict[str, Any]`
- Function L753: `save_combined_backtest_report(*, results: Sequence[dict[str, Any]], output_path: str | Path, title: str, market_key: str, pnl_label: str) -> str | None`
- Function L807: `save_aggregate_backtest_report(*, results: Sequence[dict[str, Any]], output_path: str | Path, title: str, market_key: str, pnl_label: str, max_points_per_market: int = 400, plot_panels: Sequence[str] | None = None) -> str | None`
- Function L1050: `save_joint_portfolio_backtest_report(*, results: Sequence[dict[str, Any]], output_path: str | Path, title: str, market_key: str, pnl_label: str, max_points_per_market: int = 400, plot_panels: Sequence[str] | None = None) -> str | None`
- Function L1286: `print_backtest_summary(*, results: list[dict[str, Any]], market_key: str, count_key: str, count_label: str, pnl_label: str, empty_message: str = 'No markets had sufficient data.') -> None`

### `prediction_market_extensions/analysis/__init__.py`
- Imports: none

### `prediction_market_extensions/analysis/config.py`
- Imports: `__future__, nautilus_trader`
- Class L29: `TearsheetPnLChart(TearsheetChart)`
  - Method L31: `name(self) -> str`
- Class L35: `TearsheetAllocationChart(TearsheetChart)`
  - Method L37: `name(self) -> str`
- Class L41: `TearsheetCumulativeBrierAdvantageChart(TearsheetChart)`
  - Method L43: `name(self) -> str`

### `prediction_market_extensions/analysis/legacy_backtesting/__init__.py`
- Imports: none

### `prediction_market_extensions/analysis/legacy_backtesting/models.py`
- Imports: `__future__, collections, dataclasses, datetime, enum, typing, uuid`
- Function L110: `normalize_plot_panels(panels: Sequence[str] | None, *, default: Sequence[str]) -> tuple[str, ...]`
- Class L22: `Platform(str, Enum)`
- Class L27: `Side(str, Enum)`
- Class L32: `OrderAction(str, Enum)`
- Class L37: `OrderStatus(str, Enum)`
- Class L43: `MarketStatus(str, Enum)`
- Class L134: `MarketInfo`
- Class L149: `TradeEvent`
- Class L163: `Order`
- Class L180: `Fill`
- Class L194: `Position`
- Class L208: `PortfolioSnapshot`
- Class L219: `BacktestResult`
  - Method L245: `plot(self, **kwargs) -> Any`

### `prediction_market_extensions/analysis/legacy_backtesting/plotting.py`
- Imports: `__future__, bokeh, collections, colorsys, functools, itertools, numpy, os, pandas, prediction_market_extensions, random, sys, typing`
- Function L94: `_is_notebook() -> bool`
- Function L99: `set_bokeh_output(notebook: bool = False) -> None`
- Function L131: `_bokeh_reset(filename: str | None = None) -> None`
- Function L142: `colorgen() -> Any`
- Function L147: `lightness(color, light: float = 0.94) -> str`
- Function L155: `_series_from_pairs(values: pd.Series | Sequence[tuple[Any, float]] | None) -> pd.Series`
- Function L184: `_normalize_overlay_mapping(values: Mapping[str, pd.Series | Sequence[tuple[Any, float]]]) -> dict[str, pd.Series]`
- Function L196: `_align_overlay_series(series: pd.Series, datetimes: pd.Series | pd.DatetimeIndex) -> np.ndarray`
- Function L207: `_drawdown_array(values: np.ndarray) -> np.ndarray`
- Function L221: `_estimate_ticks_per_year(datetimes: pd.DatetimeIndex | None = None) -> float`
- Function L239: `_rolling_sharpe_array(values: np.ndarray, annualize: bool = True, annualization_factor: float | None = None, datetimes: pd.DatetimeIndex | None = None) -> tuple[np.ndarray, int | None]`
- Function L267: `_build_dataframes(result: BacktestResult, bar: PinnedProgress[None] | None = None, max_markets: int = 10) -> Any`
- Function L396: `_downsample(eq: pd.DataFrame, fills_df: pd.DataFrame, market_df: pd.DataFrame, max_points: int = 5000, alloc_df: pd.DataFrame | None = None, keep_indices: set[int] | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame | None]`
- Function L470: `_build_allocation_data(eq: pd.DataFrame, fills_df: pd.DataFrame, market_prices: dict[str, list[tuple]], top_n: int | None = None) -> pd.DataFrame`
- Function L621: `plot(result: BacktestResult, *, filename: str = '', plot_width: int | None = None, plot_equity: bool = True, plot_drawdown: bool = True, plot_pl: bool = True, plot_cash: bool = True, plot_market_prices: bool = True, plot_allocation: bool = True, show_legend: bool = True, open_browser: bool = True, relative_equity: bool = True, plot_monthly_returns: bool | None = None, max_markets: int = 30, progress: bool = True, plot_panels: Sequence[str] | None = None, extra_panels: Mapping[str, Any] | None = None) -> Any`

### `prediction_market_extensions/analysis/legacy_backtesting/progress.py`
- Imports: `__future__, collections, os, sys, time, typing`
- Function L30: `_term_width() -> int`
- Function L37: `_term_height() -> int`
- Class L44: `PinnedProgress(Generic[T])`
  - Method L52: `__init__(self, iterable: Iterable[T], total: int, desc: str = '', unit: str = ' it', refresh_interval: float = 0.05) -> Any`
  - Method L74: `_setup(self) -> None`
  - Method L85: `_teardown(self) -> None`
  - Method L97: `_refresh_bar(self) -> None`
  - Method L149: `_strip_ansi(s: str) -> str`
  - Method L155: `_fmt_time(seconds: float) -> str`
  - Method L164: `write(self, msg: str) -> None`
  - Method L174: `advance(self, n: int = 1) -> None`
  - Method L181: `set_desc(self, desc: str) -> None`
  - Method L188: `__enter__(self) -> PinnedProgress[T]`
  - Method L192: `__exit__(self, *exc: object) -> None`
  - Method L198: `__iter__(self) -> Iterator[T]`

### `prediction_market_extensions/analysis/legacy_plot_adapter.py`
- Imports: `__future__, collections, datetime, importlib, nautilus_trader, numpy, pandas, pathlib, prediction_market_extensions, re, typing`
- Function L46: `_parse_float(value, default: float = 0.0) -> float`
- Function L71: `_to_naive_utc(value) -> datetime | None`
- Function L102: `_timestamp_to_naive_utc_datetime(ts: pd.Timestamp) -> datetime`
- Function L113: `_first_value(row: pd.Series, *keys: str) -> Any`
- Function L122: `prepare_cumulative_brier_advantage(user_probabilities: pd.Series | None = None, market_probabilities: pd.Series | None = None, outcomes: pd.Series | None = None) -> pd.DataFrame`
- Function L169: `_load_legacy_modules(repo_path: Path | None = None) -> tuple[Any, Any]`
- Function L181: `_extract_account_report(engine) -> pd.DataFrame`
- Function L214: `_infer_market_side(models_module, market_id: str) -> Any`
- Function L221: `_signed_quantity(action: str, side: str, qty: float) -> float`
- Function L233: `_convert_fills(fills_report: pd.DataFrame, models_module) -> list[Any]`
- Function L292: `_position_count_by_snapshot(snapshot_times: list[datetime], fills: list[Any]) -> list[int]`
- Function L317: `_build_portfolio_snapshots(models_module, account_report: pd.DataFrame, fills: list[Any]) -> list[Any]`
- Function L343: `_build_dense_timeline(fills: list[Any], market_prices: Mapping[str, Sequence[tuple[datetime, float]]]) -> pd.DatetimeIndex`
- Function L353: `_dense_cash_series(sparse_snapshots: list[Any], dense_dt: pd.DatetimeIndex, initial_cash: float) -> np.ndarray`
- Function L370: `_replay_fill_position_deltas(fills: list[Any], dense_dts: np.ndarray) -> tuple[dict[str, np.ndarray], dict[str, float]]`
- Function L394: `_aligned_market_prices(market_id: str, market_prices: Mapping[str, Sequence[tuple[datetime, float]]], dense_dts: np.ndarray, n_bars: int, fallback_price: float) -> tuple[np.ndarray, np.datetime64 | None]`
- Function L421: `_apply_resolution_cutoffs(pos_qty: dict[str, np.ndarray], pos_changes: Mapping[str, np.ndarray], market_last_ts: Mapping[str, np.datetime64 | None], dense_dts: np.ndarray) -> None`
- Function L443: `_mark_to_market(pos_qty: Mapping[str, np.ndarray], price_on_bar: Mapping[str, np.ndarray]) -> tuple[np.ndarray, np.ndarray]`
- Function L463: `_build_dense_portfolio_snapshots(models_module, sparse_snapshots: list[Any], fills: list[Any], market_prices: Mapping[str, Sequence[tuple[datetime, float]]], initial_cash: float) -> list[Any]`
- Function L532: `_normalize_market_prices(market_prices: Mapping[str, Sequence[tuple[Any, float]]] | None) -> dict[str, list[tuple[datetime, float]]]`
- Function L563: `_market_prices_from_fills(fills: list[Any]) -> dict[str, list[tuple[datetime, float]]]`
- Function L572: `_merge_market_price_sources(primary: Mapping[str, Sequence[tuple[Any, float]]] | None, secondary: Mapping[str, Sequence[tuple[Any, float]]] | None) -> dict[str, list[tuple[datetime, float]]]`
- Function L596: `_market_prices_with_fill_points(market_prices: Mapping[str, Sequence[tuple[Any, float]]] | None, fills: list[Any]) -> dict[str, list[tuple[datetime, float]]]`
- Function L607: `_build_metrics(snapshots: list[Any], initial_cash: float) -> dict[str, float]`
- Function L625: `_platform_enum(models_module, platform: str) -> Any`
- Function L632: `_mark_panel_figure(fig, panel_id: str) -> Any`
- Function L640: `_brier_unavailable_reason(*, user_probabilities: pd.Series | None, market_probabilities: pd.Series | None, outcomes: pd.Series | None) -> str | None`
- Function L659: `_build_brier_placeholder_panel(message: str) -> Any`
- Function L700: `_style_panel_legend(fig) -> None`
- Function L712: `_build_brier_timeseries_panel(brier_frame: pd.DataFrame, *, panel_id: str, axis_label: str, legend_label: str, line_color: str = '#2ca0f0') -> Any | None`
- Function L813: `_build_brier_panel(brier_frame: pd.DataFrame) -> Any | None`
- Function L822: `_build_total_brier_panel(brier_frame: pd.DataFrame) -> Any | None`
- Function L832: `_iter_layout_nodes(node) -> Any`
- Function L844: `_iter_figures(layout) -> Any`
- Function L850: `_field_name(spec) -> str | None`
- Function L861: `_filter_tool_container(container, tools_to_remove: set[Any]) -> None`
- Function L895: `_remove_tools_from_layout(layout, tools_to_remove: set[Any]) -> None`
- Function L904: `_remove_hover_tools(fig, *, layout: Any | None = None) -> set[Any]`
- Function L916: `_format_period_label(start, end) -> str`
- Function L929: `_find_figure_with_yaxis_label(layout, predicate) -> Any | None`
- Function L937: `_periodic_pnl_panel_source(target) -> tuple[dict[str, Any] | None, float | None]`
- Function L955: `_build_periodic_pnl_panel_source_data(source_data: dict[str, Any]) -> dict[str, Any] | None`
- Function L975: `_resolve_periodic_pnl_bar_width(x_values: np.ndarray, bar_width: float | None) -> float`
- Function L983: `_yes_price_line_renderers(target) -> list[Any]`
- Function L1005: `_remove_data_banner(layout) -> Any`
- Function L1024: `_legend_item_label_text(item) -> str`
- Function L1034: `_remove_yes_price_profitability_legend_items(fig) -> set[Any]`
- Function L1050: `_remove_yes_price_profitability_connectors(layout) -> None`
- Function L1076: `_standardize_periodic_pnl_panel(layout) -> None`
- Function L1124: `_relabel_market_pnl_panel(layout, axis_label: str = 'Market P&L') -> None`
- Function L1151: `_build_multi_market_brier_panel(brier_frames: Mapping[str, pd.DataFrame], *, axis_label: str = 'Cumulative Brier Advantage', color_by_market: Mapping[str, Any] | None = None) -> Any | None`
- Function L1285: `_standardize_yes_price_hover(layout) -> None`
- Function L1316: `_focus_allocation_panel(layout) -> None`
- Function L1353: `_apply_layout_overrides(layout, initial_cash: float, *, relabel_market_pnl: bool = False) -> Any`
- Function L1370: `_save_layout(layout, output_path: Path, title: str) -> None`
- Function L1383: `save_legacy_backtest_layout(layout, output_path: str | Path, title: str) -> str`
- Function L1393: `build_legacy_backtest_layout(engine, output_path: str | Path, strategy_name: str, platform: str, initial_cash: float, market_prices: Mapping[str, Sequence[tuple[Any, float]]] | None = None, user_probabilities: pd.Series | None = None, market_probabilities: pd.Series | None = None, outcomes: pd.Series | None = None, legacy_repo_path: str | Path | None = None, open_browser: bool = False, max_markets: int = 30, progress: bool = False, plot_panels: Sequence[str] | None = None) -> tuple[Any, str]`

### `prediction_market_extensions/analysis/tearsheet.py`
- Imports: `__future__, collections, difflib, nautilus_trader, numbers, pandas, typing`
- Function L63: `_hex_to_rgba(hex_color: str, alpha: float = 1.0) -> str`
- Function L87: `_normalize_theme_config(theme_config: dict[str, Any]) -> dict[str, Any]`
- Function L123: `_calculate_drawdown(returns: pd.Series) -> pd.Series`
- Function L156: `_clone_config_with_charts(config, charts: list[TearsheetChart]) -> Any`
- Function L174: `_prepare_brier_advantage_data(user_probabilities: pd.Series | None = None, market_probabilities: pd.Series | None = None, outcomes: pd.Series | None = None) -> pd.DataFrame`
- Function L221: `_extract_account_equity_series(engine: BacktestEngine | None) -> tuple[pd.Series, str | None]`
- Function L271: `_build_allocation_from_fills(fills_df: pd.DataFrame) -> pd.DataFrame`
- Function L322: `register_chart(name: str, func: Callable | None = None) -> Callable | None`
- Function L384: `get_chart(name: str) -> Callable`
- Function L422: `list_charts() -> list[str]`
- Function L435: `create_tearsheet(engine: BacktestEngine, output_path: str | None = 'tearsheet.html', title: str = 'NautilusTrader Backtest Results', currency = None, config = None, benchmark_returns: pd.Series | None = None, benchmark_name: str = 'Benchmark', user_probabilities: pd.Series | None = None, market_probabilities: pd.Series | None = None, outcomes: pd.Series | None = None) -> str | None`
- Function L581: `create_tearsheet_from_stats(stats_pnls: dict[str, Any] | dict[str, dict[str, Any]], stats_returns: dict[str, Any], stats_general: dict[str, Any], returns: pd.Series, output_path: str | None = 'tearsheet.html', title: str = 'NautilusTrader Backtest Results', config = None, benchmark_returns: pd.Series | None = None, benchmark_name: str = 'Benchmark', run_info: dict[str, Any] | None = None, account_info: dict[str, Any] | None = None, user_probabilities: pd.Series | None = None, market_probabilities: pd.Series | None = None, outcomes: pd.Series | None = None, engine = None) -> str | None`
- Function L723: `create_equity_curve(returns: pd.Series, output_path: str | None = None, title: str = 'Equity Curve', benchmark_returns: pd.Series | None = None, benchmark_name: str = 'Benchmark') -> go.Figure`
- Function L807: `create_drawdown_chart(returns: pd.Series, output_path: str | None = None, title: str = 'Drawdown', theme: str = 'plotly_white') -> go.Figure`
- Function L876: `create_monthly_returns_heatmap(returns: pd.Series, output_path: str | None = None, title: str = 'Monthly Returns (%)') -> go.Figure`
- Function L965: `create_returns_distribution(returns: pd.Series, output_path: str | None = None, title: str = 'Returns Distribution') -> go.Figure`
- Function L1021: `create_rolling_sharpe(returns: pd.Series, window: int = 60, output_path: str | None = None, title: str = 'Rolling Sharpe Ratio (60-day)') -> go.Figure`
- Function L1103: `create_yearly_returns(returns: pd.Series, output_path: str | None = None, title: str = 'Yearly Returns') -> go.Figure`
- Function L1173: `create_pnl_chart(returns: pd.Series, output_path: str | None = None, title: str = 'PnL Over Time', theme: str = 'plotly_white') -> go.Figure`
- Function L1218: `create_cumulative_brier_advantage_chart(user_probabilities: pd.Series, market_probabilities: pd.Series, outcomes: pd.Series, output_path: str | None = None, title: str = 'Cumulative Brier Advantage', theme: str = 'plotly_white') -> go.Figure`
- Function L1276: `_create_tearsheet_figure(stats_returns: dict[str, Any], stats_general: dict[str, Any], stats_pnls: dict[str, Any] | dict[str, dict[str, Any]], returns: pd.Series, title: str, config = None, benchmark_returns: pd.Series | None = None, benchmark_name: str = 'Benchmark', run_info: dict[str, Any] | None = None, account_info: dict[str, Any] | None = None, brier_data: pd.DataFrame | None = None, engine = None) -> go.Figure`
- Function L1418: `_create_stats_table(stats_pnls: dict[str, Any] | dict[str, dict[str, Any]], stats_returns: dict[str, Any], stats_general: dict[str, Any], theme_config: dict[str, Any] | None = None, run_info: dict[str, Any] | None = None, account_info: dict[str, Any] | None = None) -> go.Table`
- Function L1529: `_render_run_info(fig: go.Figure, row: int, col: int, theme_config: dict[str, Any], run_info: dict[str, Any] | None = None, account_info: dict[str, Any] | None = None, **kwargs) -> None`
- Function L1594: `_render_stats_table(fig: go.Figure, row: int, col: int, stats_pnls: dict[str, dict[str, Any]], stats_returns: dict[str, Any], stats_general: dict[str, Any], theme_config: dict[str, Any], **kwargs) -> None`
- Function L1618: `_render_equity(fig: go.Figure, row: int, col: int, returns: pd.Series, theme_config: dict[str, Any], benchmark_returns: pd.Series | None = None, benchmark_name: str = 'Benchmark', **kwargs) -> None`
- Function L1676: `_render_pnl(fig: go.Figure, row: int, col: int, returns: pd.Series, theme_config: dict[str, Any], engine = None, **kwargs) -> None`
- Function L1748: `_render_allocation(fig: go.Figure, row: int, col: int, theme_config: dict[str, Any], engine = None, **kwargs) -> None`
- Function L1805: `_render_cumulative_brier_advantage(fig: go.Figure, row: int, col: int, brier_data: pd.DataFrame, theme_config: dict[str, Any], **kwargs) -> None`
- Function L1849: `_render_drawdown(fig: go.Figure, row: int, col: int, returns: pd.Series, theme_config: dict[str, Any], **kwargs) -> None`
- Function L1891: `_render_monthly_returns(fig: go.Figure, row: int, col: int, returns: pd.Series, **kwargs) -> None`
- Function L1954: `_render_distribution(fig: go.Figure, row: int, col: int, returns: pd.Series, theme_config: dict[str, Any], **kwargs) -> None`
- Function L1993: `_estimate_ticks_per_year(returns: pd.Series) -> float`
- Function L2007: `_render_rolling_sharpe(fig: go.Figure, row: int, col: int, returns: pd.Series, theme_config: dict[str, Any], window: int = 60, **kwargs) -> None`
- Function L2064: `_render_yearly_returns(fig: go.Figure, row: int, col: int, returns: pd.Series, theme_config: dict[str, Any], **kwargs) -> None`
- Function L2105: `create_bars_with_fills(engine: BacktestEngine, bar_type: BarType, title: str | None = None, theme: str = 'plotly_white', output_path: str | None = None) -> go.Figure`
- Function L2202: `_render_bars_with_fills(fig: go.Figure, row: int, col: int, engine = None, bar_type = None, title: str | None = None, theme_config: dict[str, Any] | None = None, show_rangeslider: bool = False, **kwargs) -> None`
- Function L2489: `_add_fill_scatter_trace(fig: go.Figure, fills_df: pd.DataFrame, row: int, col: int, marker_symbol: str, marker_color: str, name: str) -> None`
- Function L2538: `_register_tearsheet_chart(name: str, subplot_type: str, title: str, renderer: Callable) -> None`
- Function L2558: `_calculate_grid_layout(charts: list[TearsheetChart], custom_layout = None) -> tuple[int, int, list, list[str], list[float], float, float]`

### `prediction_market_extensions/backtesting/__init__.py`
- Imports: none

### `prediction_market_extensions/backtesting/_artifact_paths.py`
- Imports: `__future__, pathlib, re`
- Function L7: `sanitize_chart_label(value: object, *, default: str) -> str`
- Function L16: `resolve_independent_replay_detail_chart_output_path(*, backtest_name: str, configured_path: str | Path | None, emit_html: bool, market_id: str, sim_label: str, default_filename_label: str, configured_suffix_label: str) -> str | Path | None`

### `prediction_market_extensions/backtesting/_backtest_runtime.py`
- Imports: `__future__, collections, nautilus_trader, os, pandas, pathlib, prediction_market_extensions, typing`
- Function L39: `_record_timestamp_ns(record: object) -> int | None`
- Function L53: `_iso_from_nanos(timestamp_ns: int | None) -> str | None`
- Function L59: `_data_window_ns(data: Sequence[object]) -> tuple[int | None, int | None]`
- Function L73: `_coverage_ratio_for_window(*, start_ns: int | None, end_ns: int | None, simulated_through_ns: int | None) -> float | None`
- Function L87: `build_backtest_run_state(*, data: Sequence[object], backtest_end_ns: int | None, forced_stop: bool, requested_start_ns: int | None = None, requested_end_ns: int | None = None) -> dict[str, Any]`
- Function L133: `apply_backtest_run_state(*, result: dict[str, Any], run_state: dict[str, Any]) -> dict[str, Any]`
- Function L140: `print_backtest_result_warnings(*, results: Sequence[dict[str, Any]], market_key: str) -> None`
- Function L179: `run_market_backtest(*, market_id: str, instrument, data: Sequence[object], strategy: Strategy, strategy_name: str, output_prefix: str, platform: str, venue: Venue, base_currency: Currency, fee_model, fill_model: Any | None = None, apply_default_fill_model: bool = True, slippage_ticks: int = 1, entry_slippage_pct: float = 0.0, exit_slippage_pct: float = 0.0, initial_cash: float, probability_window: int, price_attr: str, count_key: str, data_count: int | None = None, chart_resample_rule: str | None = None, market_key: str = 'market', open_browser: bool = False, emit_html: bool = True, return_chart_layout: bool = False, return_summary_series: bool = False, chart_output_path: str | Path | None = None, book_type: BookType = BookType.L1_MBP, liquidity_consumption: bool = False, queue_position: bool = False, latency_model: Any | None = None, nautilus_log_level: str = 'INFO', requested_start_ns: int | None = None, requested_end_ns: int | None = None) -> dict[str, Any]`

### `prediction_market_extensions/backtesting/_execution_config.py`
- Imports: `__future__, dataclasses, math, nautilus_trader`
- Function L11: `_validate_milliseconds(*, name: str, value: float) -> None`
- Function L18: `_milliseconds_to_nanos(value: float) -> int`
- Class L23: `StaticLatencyConfig`
  - Method L29: `__post_init__(self) -> None`
  - Method L35: `build_latency_model(self) -> LatencyModel | None`
- Class L53: `ExecutionModelConfig`
  - Method L60: `__post_init__(self) -> None`
  - Method L68: `build_latency_model(self) -> LatencyModel | None`
  - Method L73: `build_fill_model_kwargs(self) -> dict[str, int | float]`

### `prediction_market_extensions/backtesting/_experiments.py`
- Imports: `__future__, asyncio, collections, dataclasses, datetime, pandas, pathlib, prediction_market_extensions, typing`
- Function L80: `build_backtest_for_experiment(experiment: ReplayExperiment) -> PredictionMarketBacktest`
- Function L107: `build_replay_experiment(*, name: str, description: str, data: MarketDataConfig, replays: Sequence[ReplaySpec], strategy_configs: Sequence[StrategyConfigSpec] = (), strategy_factory: Callable[..., Any] | None = None, initial_cash: float = 100.0, probability_window: int = 30, min_trades: int = 0, min_quotes: int = 0, min_price_range: float = 0.0, default_lookback_days: int | None = None, default_lookback_hours: float | None = None, default_start_time: pd.Timestamp | datetime | str | None = None, default_end_time: pd.Timestamp | datetime | str | None = None, nautilus_log_level: str = 'INFO', execution: ExecutionModelConfig | None = None, chart_resample_rule: str | None = None, emit_html: bool = True, chart_output_path: str | Path | None = None, return_chart_layout: bool = False, return_summary_series: bool = False, detail_plot_panels: Sequence[str] | None = None, multi_replay_mode: MultiReplayMode = 'joint_portfolio', report: MarketReportConfig | None = None, empty_message: str | None = None, partial_message: str | None = None, result_policy: ResultPolicy | None = None) -> ReplayExperiment`
- Function L170: `replay_experiment_from_backtest(*, backtest: PredictionMarketBacktest, description: str, report: MarketReportConfig | None = None, empty_message: str | None = None, partial_message: str | None = None, result_policy: ResultPolicy | None = None) -> ReplayExperiment`
- Function L211: `async run_replay_experiment_async(experiment: ReplayExperiment) -> list[dict[str, Any]]`
- Function L241: `_dispatch_multi_replay_runner(backtest: PredictionMarketBacktest) -> list[dict[str, Any]]`
- Function L249: `async _dispatch_multi_replay_runner_async(backtest: PredictionMarketBacktest) -> list[dict[str, Any]]`
- Function L259: `_run_replay_backtest(backtest: PredictionMarketBacktest, *, multi_replay_mode: MultiReplayMode) -> list[dict[str, Any]]`
- Function L267: `async _run_replay_backtest_async(*, backtest: PredictionMarketBacktest, multi_replay_mode: MultiReplayMode) -> list[dict[str, Any]]`
- Function L275: `_finalize_replay_results(experiment: ReplayExperiment, results: list[dict[str, Any]]) -> list[dict[str, Any]]`
- Function L305: `run_experiment(experiment: Experiment) -> list[dict[str, Any]] | ParameterSearchSummary`
- Function L323: `async run_experiment_async(experiment: Experiment) -> list[dict[str, Any]] | ParameterSearchSummary`
- Class L35: `ReplayExperiment`
- Class L67: `ParameterSearchExperiment`
  - Method L73: `optimization(self) -> ParameterSearchConfig`

### `prediction_market_extensions/backtesting/_independent_multi_replay_runner.py`
- Imports: `__future__, asyncio, prediction_market_extensions, typing`
- Function L20: `_resolve_independent_replay_chart_output_path(*, backtest: PredictionMarketBacktest, sim, sim_index: int) -> str | None`
- Function L41: `_single_replay_backtest_kwargs(*, backtest: PredictionMarketBacktest, sim, sim_index: int) -> dict[str, Any]`
- Function L72: `async run_independent_multi_replay_backtest_async(*, backtest: PredictionMarketBacktest) -> list[dict[str, Any]]`

### `prediction_market_extensions/backtesting/_isolated_replay_runner.py`
- Imports: `__future__, asyncio, contextlib, multiprocessing, pathlib, pickle, tempfile, traceback, typing`
- Function L13: `_single_replay_worker(backtest_kwargs: dict[str, Any], result_path: str, send_conn) -> None`
- Function L39: `run_single_replay_backtest_in_subprocess(*, backtest_kwargs: dict[str, Any]) -> dict[str, Any] | None`

### `prediction_market_extensions/backtesting/_market_data_config.py`
- Imports: `__future__, dataclasses, prediction_market_extensions`
- Function L28: `_normalize_name(value: str | MarketPlatform | MarketDataType | MarketDataVendor) -> str`
- Class L13: `MarketDataConfig`
  - Method L19: `__post_init__(self) -> None`

### `prediction_market_extensions/backtesting/_market_data_support.py`
- Imports: `prediction_market_extensions`

### `prediction_market_extensions/backtesting/_notebook_runner.py`
- Imports: `__future__, pathlib, prediction_market_extensions, typing`
- Function L18: `load_notebook_metadata(notebook_path: Path, *, project_root: Path) -> dict[str, Any] | None`
- Function L51: `execute_notebook_runner(notebook_path: Path, *, project_root: Path) -> None`
- Function L92: `_import_nbclient() -> Any`
- Function L100: `_import_nbformat() -> Any`
- Function L108: `_notebook_description(notebook) -> str`
- Function L124: `_replace_auto_embed_cell(*, notebook, notebook_path: Path, html_artifacts: list[Path], nbformat) -> None`
- Function L140: `_auto_embed_cell_source(*, notebook_path: Path, html_artifacts: list[Path]) -> str`
- Function L170: `_relative_html_path(*, notebook_path: Path, html_path: Path) -> str`
- Function L174: `_write_notebook(*, notebook_path: Path, notebook, nbformat) -> None`

### `prediction_market_extensions/backtesting/_notebook_support.py`
- Imports: `__future__, collections, contextlib, dataclasses, importlib, os, pathlib, prediction_market_extensions, sys, typing`
- Function L15: `find_repo_root(start_path: str | Path | None = None) -> Path`
- Function L26: `ensure_notebook_repo_context(start_path: str | Path | None = None) -> Path`
- Function L38: `suppress_notebook_cell_output() -> Any`
- Function L76: `resolve_optimizer_config(module) -> Any`
- Function L86: `load_optimizer_handle(module_name: str) -> tuple[Any, Any]`
- Function L91: `build_research_parameter_search(optimizer_config, *, max_trials: int, holdout_top_k: int, name_suffix: str = '_research') -> Any`
- Function L102: `select_parameter_search_window(parameter_search) -> Any`
- Function L108: `snapshot_html_artifacts(output_root: Path) -> dict[Path, tuple[int, int]]`
- Function L122: `find_updated_html_artifacts(output_root: Path, before: Mapping[Path, tuple[int, int]]) -> list[Path]`
- Function L143: `partition_html_artifacts(html_artifacts: Sequence[Path]) -> tuple[list[Path], list[Path]]`
- Function L159: `_embed_html_as_iframe(html_text: str, *, height: int = 820) -> str`
- Function L170: `display_html_artifacts(html_artifacts: Sequence[Path], *, repo_root: Path, iframe_height: int = 820) -> None`

### `prediction_market_extensions/backtesting/_optimizer.py`
- Imports: `__future__, collections, contextlib, csv, dataclasses, datetime, itertools, json, multiprocessing, pathlib, pickle, prediction_market_extensions, random, statistics, tempfile, traceback, types, typing`
- Function L251: `_validate_parameter_spec(name: str, spec) -> ParameterSpec`
- Function L286: `_collect_search_placeholders(value) -> set[str]`
- Function L299: `_replace_search_placeholders(value, params: Mapping[str, Any]) -> Any`
- Function L315: `_parameter_candidates(parameter_grid: Mapping[str, Sequence[Any]]) -> list[ParameterValues]`
- Function L330: `_sample_parameter_sets(config: ParameterSearchConfig) -> list[ParameterValues]`
- Function L340: `_windowed_replay(*, base_replay: ReplaySpec, window: ParameterSearchWindow) -> ReplaySpec`
- Function L356: `_windowed_replays(*, base_replays: Sequence[ReplaySpec], window: ParameterSearchWindow) -> tuple[ReplaySpec, ...]`
- Function L362: `_build_backtest(*, config: ParameterSearchConfig, trial_id: int, window: ParameterSearchWindow, params: ParameterValues) -> PredictionMarketBacktest`
- Function L383: `_coerce_parameter_values(*, config: ParameterSearchConfig, params: ParameterValues | Mapping[str, Any]) -> ParameterValues`
- Function L391: `build_parameter_search_window_backtest(*, config: ParameterSearchConfig, window: ParameterSearchWindow, params: ParameterValues | Mapping[str, Any], trial_id: int = 1, name: str | None = None, emit_html: bool | None = None, chart_output_path: str | Path | None = None, return_summary_series: bool | None = None) -> PredictionMarketBacktest`
- Function L421: `_build_backtest_kwargs(*, config: ParameterSearchConfig, trial_id: int, window: ParameterSearchWindow, params: ParameterValues) -> dict[str, Any]`
- Function L450: `_default_evaluation_worker(worker_kwargs: dict[str, Any], result_path: str, send_conn) -> None`
- Function L474: `_run_default_evaluator_in_subprocess(*, worker_kwargs: dict[str, Any]) -> object`
- Function L524: `_coerce_results(value: object) -> list[dict[str, Any]]`
- Function L539: `_series_values(series: object) -> list[float]`
- Function L554: `_max_drawdown_currency(equity_series: object) -> float`
- Function L567: `_joint_portfolio_drawdown(equity_series_list: Sequence[object]) -> float`
- Function L629: `_as_float(value: object, *, default: float = 0.0) -> float`
- Function L635: `_as_int(value: object, *, default: int = 0) -> int`
- Function L643: `_score_result(*, pnl: float, max_drawdown_currency: float, fills: int, requested_coverage_ratio: float, terminated_early: bool, initial_cash: float, min_fills_per_window: int) -> float`
- Function L661: `_evaluate_window(*, config: ParameterSearchConfig, evaluator: BacktestEvaluator | None, trial_id: int, params: ParameterValues, window: ParameterSearchWindow) -> _WindowEvaluation`
- Function L756: `_median_metric(values: Sequence[float]) -> float`
- Function L760: `_build_leaderboard_row(*, trial_id: int, params: ParameterValues, train_evaluations: Sequence[_WindowEvaluation], holdout_evaluations: Sequence[_WindowEvaluation] = ()) -> ParameterSearchLeaderboardRow`
- Function L811: `_train_row_sort_key(row: ParameterSearchLeaderboardRow) -> tuple[float, int]`
- Function L815: `_final_row_sort_key(row: ParameterSearchLeaderboardRow) -> tuple[int, float, float, int]`
- Function L823: `_params_dict(params: ParameterValues) -> dict[str, Any]`
- Function L827: `_json_safe(value) -> Any`
- Function L841: `_write_leaderboard_csv(*, rows: Sequence[ParameterSearchLeaderboardRow], output_path: Path) -> str`
- Function L904: `_summary_payload(*, config: ParameterSearchConfig, summary: ParameterSearchSummary) -> dict[str, Any]`
- Function L943: `_write_summary_json(*, config: ParameterSearchConfig, summary: ParameterSearchSummary, output_path: Path) -> str`
- Function L956: `_format_score(value: float | None) -> str`
- Function L962: `_print_top_candidates(*, rows: Sequence[ParameterSearchLeaderboardRow], holdout_enabled: bool) -> None`
- Function L984: `_evaluate_train_windows(*, config: ParameterSearchConfig, evaluator: BacktestEvaluator | None, trial_id: int, params: ParameterValues) -> tuple[_WindowEvaluation, ...]`
- Function L999: `_run_random_trials(config: ParameterSearchConfig, *, evaluator: BacktestEvaluator | None) -> tuple[dict[int, tuple[_WindowEvaluation, ...]], dict[int, ParameterSearchLeaderboardRow], int, int]`
- Function L1024: `_suggest_params_from_trial(trial, parameter_space: Mapping[str, ParameterSpec]) -> ParameterValues`
- Function L1059: `_run_tpe_trials(config: ParameterSearchConfig, *, evaluator: BacktestEvaluator | None) -> tuple[dict[int, tuple[_WindowEvaluation, ...]], dict[int, ParameterSearchLeaderboardRow], int, int]`
- Function L1099: `run_parameter_search(config: ParameterSearchConfig, *, evaluator: BacktestEvaluator | None = None) -> ParameterSearchSummary`
- Class L51: `ParameterSearchWindow`
- Class L58: `ParameterSearchConfig`
  - Method L87: `optimizer_type(self) -> str`
  - Method L90: `__post_init__(self) -> None`
- Class L202: `ParameterSearchLeaderboardRow`
- Class L220: `ParameterSearchSummary`
  - Method L234: `optimizer_type(self) -> str`
- Class L239: `_WindowEvaluation`

### `prediction_market_extensions/backtesting/_prediction_market_backtest.py`
- Imports: `__future__, asyncio, collections, datetime, nautilus_trader, pandas, pathlib, prediction_market_extensions, typing, warnings`
- Function L72: `_record_ts_event(record) -> int | None`
- Function L82: `_largest_record_gap_ns(records: Sequence[Any]) -> int | None`
- Function L512: `_LoadedMarketSim(*, spec: ReplaySpec | MarketSimConfig, instrument, records: Sequence[Any], count: int, count_key: str, market_key: str, market_id: str, outcome: str, realized_outcome: float | None, prices: Sequence[float], metadata: Mapping[str, Any] | None, requested_start_ns: int | None, requested_end_ns: int | None) -> LoadedReplay`
- Class L96: `PredictionMarketBacktest`
  - Method L97: `__init__(self, *, name: str, data: MarketDataConfig, replays: Sequence[ReplaySpec] | None = None, sims: Sequence[ReplaySpec | MarketSimConfig] | None = None, strategy_configs: Sequence[StrategyConfigSpec] = (), strategy_factory: StrategyFactory | None = None, initial_cash: float, probability_window: int, min_trades: int = 0, min_quotes: int = 0, min_price_range: float = 0.0, default_lookback_days: int | None = None, default_lookback_hours: float | None = None, default_start_time: pd.Timestamp | datetime | str | None = None, default_end_time: pd.Timestamp | datetime | str | None = None, nautilus_log_level: str = 'INFO', execution: ExecutionModelConfig | None = None, chart_resample_rule: str | None = None, emit_html: bool = True, chart_output_path: str | Path | None = None, return_chart_layout: bool = False, return_summary_series: bool = False, detail_plot_panels: Sequence[str] | None = None) -> None`
  - Method L163: `sims(self) -> tuple[ReplaySpec | MarketSimConfig, ...]`
  - Method L166: `_strategy_summary_label(self) -> str`
  - Method L173: `run(self) -> list[dict[str, Any]]`
  - Method L183: `run_backtest(self) -> list[dict[str, Any]]`
  - Method L186: `async run_async(self) -> list[dict[str, Any]]`
  - Method L243: `async run_backtest_async(self) -> list[dict[str, Any]]`
  - Method L246: `_create_artifact_builder(self) -> PredictionMarketArtifactBuilder`
  - Method L262: `_build_result(self, *, loaded_sim: LoadedReplay, fills_report: pd.DataFrame, positions_report: pd.DataFrame, market_artifacts: Mapping[str, Any] | None = None, joint_portfolio_artifacts: Mapping[str, Any] | None = None, run_state: dict[str, Any] | None = None) -> dict[str, Any]`
  - Method L281: `_build_market_artifacts(self, *, engine: BacktestEngine, loaded_sims: Sequence[LoadedReplay], fills_report: pd.DataFrame) -> dict[str, dict[str, Any]]`
  - Method L292: `_build_joint_portfolio_artifacts(self, *, engine: BacktestEngine, loaded_sims: Sequence[LoadedReplay]) -> dict[str, Any]`
  - Method L299: `_resolve_chart_output_path(self, *, market_id: str) -> Path`
  - Method L302: `_normalize_replays(self, replays: Sequence[ReplaySpec | MarketSimConfig]) -> tuple[ReplaySpec, ...]`
  - Method L326: `_load_request(self) -> ReplayLoadRequest`
  - Method L339: `async _load_sims_async(self) -> list[LoadedReplay]`
  - Method L363: `_build_engine(self) -> BacktestEngine`
  - Method L396: `_build_importable_strategy_configs(self, loaded_sims: Sequence[LoadedReplay]) -> list[Any]`
  - Method L418: `_is_batch_strategy_config(self, strategy_spec: StrategyConfigSpec) -> bool`
  - Method L427: `_contains_value(self, value, target: str) -> bool`
  - Method L436: `_bind_strategy_spec(self, *, strategy_spec: StrategyConfigSpec, loaded_sim: LoadedReplay, all_instrument_ids: Sequence[InstrumentId]) -> StrategyConfigSpec`
  - Method L464: `_bind_value(self, value, *, instrument_id: InstrumentId, all_instrument_ids: Sequence[InstrumentId], metadata: Mapping[str, Any]) -> Any`

### `prediction_market_extensions/backtesting/_prediction_market_runner.py`
- Imports: `__future__, collections, datetime, nautilus_trader, pandas, pathlib, prediction_market_extensions, typing`
- Function L29: `async run_single_market_backtest(*, name: str, data: MarketDataConfig, probability_window: int, strategy_factory: StrategyFactory | None = None, strategy_configs: Sequence[StrategyConfigSpec] | None = None, market_slug: str | None = None, market_ticker: str | None = None, token_index: int = 0, lookback_days: int | None = None, lookback_hours: float | None = None, min_trades: int = 0, min_quotes: int = 0, min_price_range: float = 0.0, initial_cash: float = 100.0, nautilus_log_level: str = 'INFO', chart_resample_rule: str | None = None, emit_summary: bool = True, emit_html: bool = True, chart_output_path: str | Path | None = None, return_chart_layout: bool = False, return_summary_series: bool = False, detail_plot_panels: Sequence[str] | None = None, report: MarketReportConfig | None = None, empty_message: str | None = None, partial_message: str | None = None, result_policy: ResultPolicy | None = None, start_time: pd.Timestamp | datetime | str | None = None, end_time: pd.Timestamp | datetime | str | None = None, execution: ExecutionModelConfig | None = None) -> dict[str, Any] | None`

### `prediction_market_extensions/backtesting/_replay_specs.py`
- Imports: `__future__, collections, dataclasses, pandas, typing`
- Function L51: `coerce_legacy_market_sim_config(*, platform: str, data_type: str, vendor: str, sim: MarketSimConfig) -> ReplaySpec`
- Class L13: `TradeReplay`
- Class L25: `QuoteReplay`
- Class L39: `MarketSimConfig`

### `prediction_market_extensions/backtesting/_result_policies.py`
- Imports: `__future__, collections, dataclasses, prediction_market_extensions, typing`
- Class L15: `ResultPolicy(Protocol)`
  - Method L16: `apply(self, results: Results) -> Results | None`
- Class L20: `BinarySettlementPnlPolicy`
  - Method L27: `apply(self, results: Results) -> Results`

### `prediction_market_extensions/backtesting/_strategy_configs.py`
- Imports: `__future__, collections, copy, nautilus_trader, typing`
- Function L17: `_normalized_config(*, raw_config: Mapping[str, Any], instrument_id: InstrumentId) -> dict[str, Any]`
- Function L33: `build_importable_strategy_configs(*, strategy_configs: Sequence[StrategyConfigSpec], instrument_id: InstrumentId) -> list[ImportableStrategyConfig]`
- Function L55: `build_strategies_from_configs(*, strategy_configs: Sequence[StrategyConfigSpec], instrument_id: InstrumentId) -> list[Strategy]`

### `prediction_market_extensions/backtesting/_timing_harness.py`
- Imports: `__future__, collections, functools, inspect, os, typing`
- Function L15: `_timing_enabled() -> bool`
- Function L22: `install_timing_harness() -> None`
- Function L31: `timing_harness(func: Callable[P, T] | Callable[P, Awaitable[T]] | None = None) -> Callable[[Callable[P, T] | Callable[P, Awaitable[T]]], Callable[P, T] | Callable[P, Awaitable[T]]] | Callable[P, T] | Callable[P, Awaitable[T]]`

### `prediction_market_extensions/backtesting/_timing_test.py`
- Imports: `__future__, asyncio, concurrent, datetime, importlib, os, pathlib, sys, threading, time, urllib`
- Function L42: `_hour_label(source: str) -> str`
- Function L51: `_format_bytes(size: int | None) -> str`
- Function L64: `_transfer_label(source: str) -> str`
- Function L86: `_progress_bar_description(*, total_hours: int, started_hours: int, completed_hours: int, active_hours: int | None = None, item_label: str = 'hours') -> str`
- Function L115: `_hour_progress_key(hour) -> str`
- Function L122: `_progress_bar_total(total_hours: int) -> int`
- Function L126: `_progress_bar_position(*, total_hours: int, completed_hours: int, active_hours_progress: float = 0.0) -> float`
- Function L136: `_format_completed_hour_line(hour, *, elapsed: float, rows: int, source: str, timestamp_width: int = _COMPLETED_HOUR_TIMESTAMP_WIDTH) -> str`
- Function L151: `_hour_label_from_hour(hour) -> str`
- Function L158: `_is_local_scan_source(source: str | None) -> bool`
- Function L167: `_transfer_progress_fraction(*, mode: str | None, source: str | None = None, downloaded_bytes: int, total_bytes: int | None, scanned_batches: int) -> float`
- Function L195: `_active_transfer_progress(downloads: dict[str, dict[str, object]]) -> tuple[int, float]`
- Function L216: `install_timing() -> None`
- Function L1066: `_load_backtest_module(path_str: str) -> Any`

### `prediction_market_extensions/backtesting/data_sources/__init__.py`
- Imports: `prediction_market_extensions`

### `prediction_market_extensions/backtesting/data_sources/_common.py`
- Imports: `__future__, pathlib, re, urllib`
- Function L12: `env_value(raw: str | None) -> str | None`
- Function L19: `is_disabled(raw: str | None) -> bool`
- Function L26: `looks_like_local_path(value: str) -> bool`
- Function L39: `normalize_local_path(value: str) -> str`
- Function L43: `normalize_urlish(value: str) -> str`
- Function L55: `trim_url_suffix(url: str, suffixes: tuple[str, ...]) -> str`

### `prediction_market_extensions/backtesting/data_sources/data_types.py`
- Imports: `__future__, dataclasses`
- Class L7: `MarketDataType`
  - Method L10: `__post_init__(self) -> None`
  - Method L13: `__str__(self) -> str`

### `prediction_market_extensions/backtesting/data_sources/kalshi_native.py`
- Imports: `__future__, collections, contextlib, contextvars, dataclasses, msgspec, os, prediction_market_extensions, typing`
- Function L41: `_current_loader_config() -> KalshiNativeLoaderConfig | None`
- Function L157: `_summary_from_rest_base_url(rest_base_url: str | None) -> str`
- Function L165: `_parse_named_source(raw_source: str) -> str | None`
- Function L180: `_resolve_explicit_sources(sources: Sequence[str]) -> tuple[KalshiNativeDataSourceSelection, KalshiNativeLoaderConfig]`
- Function L208: `resolve_kalshi_native_loader_config(sources: Sequence[str] | None = None) -> tuple[KalshiNativeDataSourceSelection, KalshiNativeLoaderConfig]`
- Function L226: `resolve_kalshi_native_data_source_selection(sources: Sequence[str] | None = None) -> tuple[KalshiNativeDataSourceSelection, dict[str, str | None]]`
- Function L236: `configured_kalshi_native_data_source(*, sources: Sequence[str] | None = None) -> Iterator[KalshiNativeDataSourceSelection]`
- Class L27: `KalshiNativeDataSourceSelection`
- Class L32: `KalshiNativeLoaderConfig`
- Class L45: `RunnerKalshiDataLoader(KalshiDataLoader)`
  - Method L47: `_configured_rest_base_url(cls) -> str`
  - Method L60: `async from_market_ticker(cls, ticker: str, http_client = None) -> RunnerKalshiDataLoader`
  - Method L92: `async fetch_trades(self, min_ts: int | None = None, max_ts: int | None = None, limit: int = 1000) -> list[dict[str, Any]]`
  - Method L128: `async fetch_candlesticks(self, start_ts: int | None = None, end_ts: int | None = None, interval: str = 'Minutes1') -> list[dict[str, Any]]`

### `prediction_market_extensions/backtesting/data_sources/platforms.py`
- Imports: `__future__, dataclasses`
- Class L7: `MarketPlatform`
  - Method L10: `__post_init__(self) -> None`
  - Method L13: `__str__(self) -> str`

### `prediction_market_extensions/backtesting/data_sources/pmxt.py`
- Imports: `__future__, collections, contextlib, contextvars, dataclasses, os, pathlib, prediction_market_extensions, pyarrow, time, urllib`
- Function L79: `_current_loader_config() -> PMXTLoaderConfig | None`
- Function L521: `_normalize_mode(value: str | None) -> str`
- Function L535: `_env_value(name: str) -> str | None`
- Function L543: `_env_enabled(name: str) -> bool`
- Function L550: `_resolve_prefetch_workers_override(*, default_when_unset: int | None) -> int | None`
- Function L560: `_resolve_source_priority_override() -> tuple[str, ...]`
- Function L580: `_resolve_existing_remote_url() -> str | None`
- Function L585: `_resolve_existing_remote_urls() -> tuple[str, ...]`
- Function L601: `_resolve_required_directory(env_name: str, *, label: str) -> Path`
- Function L614: `_strip_prefixed_local_source(source: str, *, prefixes: Sequence[str]) -> str | None`
- Function L624: `_strip_prefixed_remote_source(source: str, *, prefixes: Sequence[str]) -> str | None`
- Function L634: `_classify_explicit_pmxt_sources(sources: Sequence[str]) -> tuple[str | None, tuple[str, ...], tuple[str, ...], tuple[str, ...], tuple[tuple[str, str], ...]]`
- Function L710: `_explicit_source_summary(*, ordered_sources: Sequence[str], ordered_entries: Sequence[tuple[str, str]] = ()) -> str`
- Function L728: `resolve_pmxt_loader_config(*, sources: Sequence[str] | None = None) -> tuple[PMXTDataSourceSelection, PMXTLoaderConfig]`
- Function L866: `_loader_config_to_env_updates(config: PMXTLoaderConfig) -> dict[str, str | None]`
- Function L880: `resolve_pmxt_data_source_selection(*, sources: Sequence[str] | None = None) -> tuple[PMXTDataSourceSelection, dict[str, str | None]]`
- Function L890: `configured_pmxt_data_source(*, sources: Sequence[str] | None = None) -> Iterator[PMXTDataSourceSelection]`
- Class L60: `PMXTLoaderConfig`
  - Method L70: `remote_base_url(self) -> str | None`
- Class L83: `RunnerPolymarketPMXTDataLoader(PolymarketPMXTDataLoader)`
  - Method L90: `__init__(self, *args, **kwargs) -> None`
  - Method L109: `_resolve_raw_root(cls) -> Path | None`
  - Method L125: `_resolve_remote_base_url(cls) -> str | None`
  - Method L130: `_resolve_remote_base_urls(cls) -> tuple[str, ...]`
  - Method L150: `_archive_url_for_hour(self, hour) -> Any`
  - Method L161: `_archive_urls_for_hour(self, hour) -> Any`
  - Method L169: `_raw_path_for_hour(self, hour) -> Path | None`
  - Method L182: `_load_local_raw_market_batches(self, hour, *, batch_size: int) -> Any`
  - Method L195: `_load_local_archive_market_batches(self, hour, *, batch_size: int) -> Any`
  - Method L201: `_load_remote_market_batches(self, hour, *, batch_size: int) -> Any`
  - Method L223: `_resolve_source_priority(cls) -> tuple[str, ...]`
  - Method L247: `_resolve_prefetch_workers(cls) -> int`
  - Method L254: `_scoped_source_entry(self, kind: str, target: str) -> Any`
  - Method L275: `_load_entry_batches(self, kind: str, hour, *, batch_size: int) -> Any`
  - Method L282: `_write_cache_if_enabled(self, hour, table) -> None`
  - Method L287: `_load_market_table(self, hour, *, batch_size: int) -> Any`
  - Method L341: `_load_market_batches(self, hour, *, batch_size: int) -> Any`
  - Method L391: `_download_to_file_with_progress(self, url: str, destination: Path) -> int | None`
  - Method L444: `_download_payload_with_progress(self, url: str) -> bytes | None`
  - Method L484: `_progress_total_bytes(self, source: str) -> int | None`
- Class L516: `PMXTDataSourceSelection`

### `prediction_market_extensions/backtesting/data_sources/polymarket_native.py`
- Imports: `__future__, collections, contextlib, contextvars, dataclasses, msgspec, os, prediction_market_extensions, typing, urllib, warnings`
- Function L56: `_current_loader_config() -> PolymarketNativeLoaderConfig | None`
- Function L269: `_summary_from_overrides(*, gamma_base_url: str | None, clob_base_url: str | None, trade_api_base_url: str | None) -> str`
- Function L287: `_normalized_override(value: str | None, *, env_name: str, suffixes: tuple[str, ...]) -> str | None`
- Function L296: `_parse_named_source(raw_source: str) -> tuple[str | None, str]`
- Function L313: `_infer_env_name_from_url(url: str) -> str`
- Function L338: `_normalized_env_updates(*, gamma_base_url: str | None, clob_base_url: str | None, trade_api_base_url: str | None) -> dict[str, str | None]`
- Function L360: `_resolve_explicit_sources(sources: Sequence[str]) -> tuple[PolymarketNativeDataSourceSelection, PolymarketNativeLoaderConfig]`
- Function L399: `resolve_polymarket_native_loader_config(sources: Sequence[str] | None = None) -> tuple[PolymarketNativeDataSourceSelection, PolymarketNativeLoaderConfig]`
- Function L436: `resolve_polymarket_native_data_source_selection(sources: Sequence[str] | None = None) -> tuple[PolymarketNativeDataSourceSelection, dict[str, str | None]]`
- Function L453: `configured_polymarket_native_data_source(*, sources: Sequence[str] | None = None) -> Iterator[PolymarketNativeDataSourceSelection]`
- Class L40: `PolymarketNativeDataSourceSelection`
- Class L45: `PolymarketNativeLoaderConfig`
- Class L60: `RunnerPolymarketDataLoader(PolymarketDataLoader)`
  - Method L62: `_configured_gamma_base_url(cls) -> str`
  - Method L75: `_configured_clob_base_url(cls) -> str`
  - Method L88: `_configured_trade_api_base_url(cls) -> str`
  - Method L102: `async _fetch_market_by_slug(cls, slug: str, http_client) -> dict[str, Any]`
  - Method L127: `async _fetch_market_details(cls, condition_id: str, http_client) -> dict[str, Any]`
  - Method L137: `async _fetch_market_fee_rate_bps(cls, token_id: str, http_client) -> Any`
  - Method L155: `async _fetch_event_by_slug(cls, slug: str, http_client) -> dict[str, Any]`
  - Method L170: `async fetch_events(self, active: bool = True, closed: bool = False, archived: bool = False, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]`
  - Method L193: `async fetch_markets(self, active: bool = True, closed: bool = False, archived: bool = False, limit: int = 100, offset: int = 0) -> list[dict]`
  - Method L216: `async fetch_trades(self, condition_id: str, limit: int = PolymarketDataLoader._TRADES_PAGE_LIMIT, start_ts: int | None = None, end_ts: int | None = None) -> list[dict[str, Any]]`

### `prediction_market_extensions/backtesting/data_sources/registry.py`
- Imports: `__future__, dataclasses, prediction_market_extensions, typing`
- Function L21: `_normalize_key_part(value: object) -> str`
- Function L31: `_normalize_lookup_key(*, platform: object, data_type: object, vendor: object) -> MarketDataKey`
- Function L39: `_support_from_adapter(adapter: HistoricalReplayAdapter) -> MarketDataSupport`
- Function L53: `register_market_data_support(support: MarketDataSupport) -> None`
- Function L57: `unregister_market_data_support(key: MarketDataKey) -> MarketDataSupport | None`
- Function L61: `resolve_market_data_support(*, platform: object, data_type: object, vendor: object) -> MarketDataSupport`
- Function L78: `resolve_replay_adapter(*, platform: object, data_type: object, vendor: object) -> HistoricalReplayAdapter`
- Function L86: `supported_market_data_keys() -> tuple[MarketDataKey, ...]`
- Function L90: `build_single_market_replay(*, support: MarketDataSupport, field_values: dict[str, Any]) -> ReplaySpec`
- Class L16: `MarketDataSupport`

### `prediction_market_extensions/backtesting/data_sources/replay_adapters.py`
- Imports: `__future__, collections, contextlib, dataclasses, datetime, importlib, nautilus_trader, pandas, prediction_market_extensions, typing`
- Function L58: `_resolve_backtest_compat_symbol(name: str, default) -> Any`
- Function L68: `_normalize_timestamp(value: object | None, *, default_now: bool = False) -> pd.Timestamp`
- Function L84: `_loaded_window(records: tuple[object, ...]) -> ReplayWindow | None`
- Function L100: `_requested_window(start: pd.Timestamp, end: pd.Timestamp) -> ReplayWindow`
- Function L104: `_price_range(prices: tuple[float, ...]) -> float`
- Function L110: `_validate_replay_window(*, market_label: str, count_label: str, count: int, min_record_count: int, prices: tuple[float, ...], min_price_range: float) -> bool`
- Class L131: `_BaseReplayAdapter(HistoricalReplayAdapter)`
  - Method L141: `key(self) -> ReplayAdapterKey`
  - Method L145: `replay_spec_type(self) -> type[Any]`
  - Method L148: `configure_sources(self, *, sources: tuple[str, ...] | list[str]) -> AbstractContextManager[Any]`
  - Method L154: `engine_profile(self) -> ReplayEngineProfile`
  - Method L157: `build_single_market_replay(self, *, field_values: Mapping[str, Any]) -> Any`
  - Method L169: `_build_loaded_replay(self, *, replay, instrument, records: tuple[Any, ...], count: int, count_key: str, market_key: str, market_id: str, prices: tuple[float, ...], outcome: str, realized_outcome: float | None, metadata: dict[str, Any], requested_window: ReplayWindow) -> LoadedReplay`
- Class L205: `KalshiTradeTickReplayAdapter(_BaseReplayAdapter)`
  - Method L206: `__init__(self) -> None`
  - Method L237: `async load_replay(self, replay: TradeReplay, *, request: ReplayLoadRequest) -> LoadedReplay | None`
- Class L304: `PolymarketTradeTickReplayAdapter(_BaseReplayAdapter)`
  - Method L305: `__init__(self) -> None`
  - Method L338: `async load_replay(self, replay: TradeReplay, *, request: ReplayLoadRequest) -> LoadedReplay | None`
- Class L412: `PolymarketPMXTQuoteReplayAdapter(_BaseReplayAdapter)`
  - Method L413: `__init__(self) -> None`
  - Method L449: `async load_replay(self, replay: QuoteReplay, *, request: ReplayLoadRequest) -> LoadedReplay | None`
- Class L532: `PolymarketTelonexQuoteReplayAdapter(_BaseReplayAdapter)`
  - Method L533: `__init__(self) -> None`
  - Method L572: `async load_replay(self, replay: QuoteReplay, *, request: ReplayLoadRequest) -> LoadedReplay | None`

### `prediction_market_extensions/backtesting/data_sources/telonex.py`
- Imports: `__future__, collections, contextlib, contextvars, dataclasses, datetime, hashlib, io, nautilus_trader, numpy, os, pandas, pathlib, prediction_market_extensions, pyarrow, re, urllib, warnings`
- Function L86: `_current_loader_config() -> TelonexLoaderConfig | None`
- Function L90: `_env_value(name: str) -> str | None`
- Function L100: `_resolve_channel(channel: str | None = None) -> str`
- Function L104: `_default_cache_root() -> Path`
- Function L110: `_resolve_api_cache_root() -> Path | None`
- Function L120: `_normalize_api_base_url(value: str | None) -> str`
- Function L129: `_expand_source_vars(source: str) -> str`
- Function L139: `_classify_telonex_sources(sources: Sequence[str]) -> tuple[TelonexSourceEntry, ...]`
- Function L182: `_default_telonex_sources_from_env() -> tuple[TelonexSourceEntry, ...]`
- Function L203: `_source_summary(entries: Sequence[TelonexSourceEntry]) -> str`
- Function L214: `resolve_telonex_loader_config(*, sources: Sequence[str] | None = None, channel: str | None = None) -> tuple[TelonexDataSourceSelection, TelonexLoaderConfig]`
- Function L234: `resolve_telonex_data_source_selection(*, sources: Sequence[str] | None = None) -> tuple[TelonexDataSourceSelection, dict[str, str | None]]`
- Function L242: `configured_telonex_data_source(*, sources: Sequence[str] | None = None, channel: str | None = None) -> Iterator[TelonexDataSourceSelection]`
- Class L63: `TelonexSourceEntry`
- Class L70: `TelonexLoaderConfig`
- Class L76: `TelonexDataSourceSelection`
- Class L253: `RunnerPolymarketTelonexQuoteDataLoader(PolymarketDataLoader)`
  - Method L254: `__init__(self, *args, **kwargs) -> None`
  - Method L258: `_ensure_blob_scan_caches(self) -> None`
  - Method L266: `_download_progress(self, url: str, downloaded_bytes: int, total_bytes: int | None, finished: bool) -> None`
  - Method L274: `_resolve_api_cache_root(cls) -> Path | None`
  - Method L277: `_config(self) -> TelonexLoaderConfig`
  - Method L284: `_date_range(start: pd.Timestamp, end: pd.Timestamp) -> list[str]`
  - Method L295: `_outcome_segments(*, token_index: int, outcome: str | None) -> tuple[str, ...]`
  - Method L302: `_local_blob_root(root: Path) -> Path | None`
  - Method L316: `_outcome_segment_candidates(*, token_index: int, outcome: str | None) -> tuple[str, ...]`
  - Method L323: `_month_partition_dirs(*, channel_dir: Path, start: pd.Timestamp, end: pd.Timestamp) -> tuple[Path, ...]`
  - Method L334: `_readable_blob_part_paths(self, *, channel_dir: Path, start: pd.Timestamp, end: pd.Timestamp) -> tuple[list[str], bool]`
  - Method L363: `_scan_readable_blob_part_paths(self, partition_dir: Path) -> tuple[tuple[str, ...], bool]`
  - Method L385: `_load_blob_range(self, *, store_root: Path, channel: str, market_slug: str, token_index: int, outcome: str | None, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None`
  - Method L461: `_local_consolidated_candidates(cls, *, root: Path, channel: str, market_slug: str, token_index: int, outcome: str | None) -> tuple[Path, ...]`
  - Method L486: `_local_daily_candidates(cls, *, root: Path, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> tuple[Path, ...]`
  - Method L519: `_local_consolidated_path(self, *, root: Path, channel: str, market_slug: str, token_index: int, outcome: str | None) -> Path | None`
  - Method L539: `_local_path_for_day(self, *, root: Path, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> Path | None`
  - Method L562: `_safe_read_parquet(path: Path) -> pd.DataFrame | None`
  - Method L572: `_load_local_range(self, *, root: Path, channel: str, market_slug: str, token_index: int, outcome: str | None) -> pd.DataFrame | None`
  - Method L592: `_load_local_day(self, *, root: Path, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> pd.DataFrame | None`
  - Method L615: `_api_url(*, base_url: str, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> str`
  - Method L635: `_api_cache_path(cls, *, base_url: str, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> Path | None`
  - Method L664: `_load_api_cache_day(self, *, base_url: str, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> pd.DataFrame | None`
  - Method L693: `_write_api_cache_day(self, *, payload: bytes, base_url: str, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> None`
  - Method L730: `_resolve_presigned_url(*, url: str, api_key: str) -> str`
  - Method L757: `_load_api_day(self, *, base_url: str, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None, api_key: str | None = None) -> pd.DataFrame | None`
  - Method L846: `_column_to_ns(column: pd.Series, column_name: str) -> np.ndarray`
  - Method L857: `_normalize_to_utc(value: pd.Timestamp) -> pd.Timestamp`
  - Method L862: `_day_window(self, date: str, *, start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.Timestamp, pd.Timestamp] | None`
  - Method L876: `_first_present_column(frame: pd.DataFrame, names: Sequence[str], *, label: str) -> str`
  - Method L882: `_quote_ticks_from_frame(self, frame: pd.DataFrame, *, start: pd.Timestamp, end: pd.Timestamp) -> list[QuoteTick]`
  - Method L944: `_book_levels_from_value(value: object, *, side: str) -> tuple[PolymarketBookLevel, ...]`
  - Method L979: `_book_side_map(levels: Sequence[PolymarketBookLevel]) -> dict[str, str]`
  - Method L982: `_snapshot_to_deltas(self, *, bids: Sequence[PolymarketBookLevel], asks: Sequence[PolymarketBookLevel], ts_event: int) -> OrderBookDeltas | None`
  - Method L998: `_diff_to_deltas(self, *, previous_bids: dict[str, str], previous_asks: dict[str, str], current_bids: dict[str, str], current_asks: dict[str, str], ts_event: int) -> OrderBookDeltas | None`
  - Method L1047: `_quote_from_levels(self, *, bids: Sequence[PolymarketBookLevel], asks: Sequence[PolymarketBookLevel], ts_event: int) -> QuoteTick | None`
  - Method L1063: `_book_events_from_frame(self, frame: pd.DataFrame, *, start: pd.Timestamp, end: pd.Timestamp, include_order_book: bool = True, include_quotes: bool = True) -> list[OrderBookDeltas | QuoteTick]`
  - Method L1143: `_try_load_range_from_local(self, *, entry: TelonexSourceEntry, channel: str, market_slug: str, token_index: int, outcome: str | None, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame | None`
  - Method L1191: `_try_load_day_from_local(self, *, entry: TelonexSourceEntry, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None, start: pd.Timestamp, end: pd.Timestamp, range_cache: dict[Path, pd.DataFrame | None]) -> pd.DataFrame | None`
  - Method L1258: `_try_load_day_from_entry(self, *, entry: TelonexSourceEntry, channel: str, date: str, market_slug: str, token_index: int, outcome: str | None) -> pd.DataFrame | None`
  - Method L1298: `load_quotes(self, start: pd.Timestamp, end: pd.Timestamp, *, market_slug: str, token_index: int, outcome: str | None) -> list[QuoteTick]`
  - Method L1365: `load_order_book_and_quotes(self, start: pd.Timestamp, end: pd.Timestamp, *, market_slug: str, token_index: int, outcome: str | None, include_order_book: bool = True, include_quotes: bool = True) -> list[OrderBookDeltas | QuoteTick]`

### `prediction_market_extensions/backtesting/data_sources/vendors.py`
- Imports: `__future__, dataclasses`
- Class L7: `MarketDataVendor`
  - Method L10: `__post_init__(self) -> None`
  - Method L13: `__str__(self) -> str`

### `prediction_market_extensions/backtesting/optimizers/__init__.py`
- Imports: `prediction_market_extensions`

### `prediction_market_extensions/backtesting/prediction_market/__init__.py`
- Imports: `prediction_market_extensions`

### `prediction_market_extensions/backtesting/prediction_market/artifacts.py`
- Imports: `__future__, collections, dataclasses, datetime, nautilus_trader, pandas, pathlib, prediction_market_extensions, typing`
- Function L32: `resolve_repo_relative_path(path_like: str | Path) -> Path`
- Class L40: `PredictionMarketArtifactBuilder`
  - Method L54: `build_result(self, *, loaded_sim: LoadedReplay, fills_report: pd.DataFrame, positions_report: pd.DataFrame, market_artifacts: Mapping[str, Any] | None = None, joint_portfolio_artifacts: Mapping[str, Any] | None = None, run_state: dict[str, Any] | None = None) -> dict[str, Any]`
  - Method L101: `build_market_artifacts(self, *, engine: BacktestEngine, loaded_sims: Sequence[LoadedReplay], fills_report: pd.DataFrame) -> dict[str, dict[str, Any]]`
  - Method L121: `build_joint_portfolio_artifacts(self, *, engine: BacktestEngine, loaded_sims: Sequence[LoadedReplay]) -> dict[str, Any]`
  - Method L168: `_build_market_artifacts_for_loaded_sim(self, *, engine: BacktestEngine, loaded_sim: LoadedReplay, fills_report: pd.DataFrame, include_portfolio_series: bool) -> dict[str, Any]`
  - Method L240: `resolve_chart_output_path(self, *, market_id: str) -> Path`
  - Method L268: `_build_market_summary_series(self, *, engine: BacktestEngine, loaded_sim: LoadedReplay, fills_report: pd.DataFrame, market_prices, user_probabilities: pd.Series, market_probabilities: pd.Series, outcomes: pd.Series, include_portfolio_series: bool) -> dict[str, Any]`
  - Method L355: `_filter_report_rows(report: pd.DataFrame, *, instrument_id: str) -> pd.DataFrame`

### `prediction_market_extensions/backtesting/prediction_market/reporting.py`
- Imports: `__future__, collections, dataclasses, prediction_market_extensions, typing`
- Function L39: `finalize_market_results(*, name: str, results: Sequence[dict[str, object]], report: MarketReportConfig, multi_replay_mode: str = 'joint_portfolio') -> None`
- Function L91: `run_reported_backtest(*, backtest: PredictionMarketBacktest, report: MarketReportConfig, empty_message: str | None = None, multi_replay_mode: str = 'joint_portfolio') -> list[dict[str, object]]`
- Function L110: `_resolve_report_market_key(*, results: Sequence[dict[str, object]], configured_key: str) -> str`
- Class L29: `MarketReportConfig`

### `scripts/__init__.py`
- Imports: none

### `scripts/_cache_clear_guard.py`
- Imports: `__future__, argparse, pathlib, sys`
- Function L8: `_resolved(path: str) -> Path | None`
- Function L15: `_is_same_or_nested(a: Path, b: Path) -> bool`
- Function L19: `main() -> int`

### `scripts/_pmxt_raw_download.py`
- Imports: `__future__, collections, dataclasses, datetime, os, pathlib, pyarrow, re, time, tqdm, urllib`
- Function L66: `extract_archive_filenames(html: str) -> list[str]`
- Function L77: `fetch_archive_page(archive_listing_url: str, page: int, timeout_secs: int) -> str`
- Function L86: `floor_utc_hour(value: datetime) -> datetime`
- Function L90: `archive_filename_for_hour(hour: datetime) -> str`
- Function L95: `parse_archive_hour(filename: str) -> datetime`
- Function L102: `raw_relative_path(filename: str) -> Path`
- Function L107: `_parse_hour_bound(value: str | None) -> datetime | None`
- Function L125: `discover_archive_filenames(*, archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL, timeout_secs: int = 60, stale_pages: int = 1, max_pages: int | None = None) -> list[str]`
- Function L159: `discover_archive_hours(*, archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL, timeout_secs: int = 60, stale_pages: int = 1, max_pages: int | None = None) -> list[datetime]`
- Function L177: `_filter_filenames_to_window(filenames: list[str], *, start_hour: datetime | None, end_hour: datetime | None) -> list[str]`
- Function L191: `_sort_filenames_newest_first(filenames: list[str]) -> list[str]`
- Function L195: `_filename_for_hour(hour: datetime) -> str`
- Function L199: `_hour_range_filenames(*, start_hour: datetime, end_hour: datetime) -> list[str]`
- Function L208: `_archive_url(base_url: str, filename: str) -> str`
- Function L212: `_archive_sources_from_args(*, archive_sources: list[tuple[str, str]] | None, archive_listing_url: str, archive_base_url: str) -> list[ArchiveSource]`
- Function L242: `_archive_candidate_urls(*, filename: str, archive_sources: list[ArchiveSource], discovered_archive_base_urls: dict[str, str]) -> list[tuple[str, str]]`
- Function L262: `_ranked_archive_candidate_urls(*, filename: str, archive_sources: list[ArchiveSource], discovered_archive_base_urls: dict[str, str], timeout_secs: int) -> list[tuple[str, str]]`
- Function L289: `_candidate_urls(*, source: str, filename: str, archive_sources: list[ArchiveSource], discovered_archive_base_urls: dict[str, str], timeout_secs: int | None = None) -> list[tuple[str, str]]`
- Function L313: `_content_length_from_headers(headers) -> int | None`
- Function L333: `_remote_content_length(*, url: str, timeout_secs: int) -> int | None`
- Function L356: `_hour_label_for_filename(filename: str) -> str`
- Function L362: `_progress_bar_description(*, total_hours: int, completed_hours: int, active_hours: int) -> str`
- Function L375: `_format_mib(size_bytes: int) -> str`
- Function L379: `_active_status_text(*, source: str, hour_label: str, written_bytes: int, total_bytes: int | None, elapsed_secs: float) -> str`
- Function L394: `_hour_result_text(*, hour_label: str, elapsed_secs: float, detail: str, source: str) -> str`
- Function L398: `_format_download_error(exc: Exception) -> str`
- Function L406: `_source_priority_summary(*, source_sequence: list[str], archive_sources: list[ArchiveSource]) -> str`
- Function L417: `_window_label_from_filenames(filenames: list[str]) -> tuple[str | None, str | None]`
- Function L424: `_read_parquet_row_count(path: Path) -> int | None`
- Function L431: `_local_raw_is_empty(path: Path) -> bool`
- Function L439: `_existing_refresh_reason(*, path: Path, source_urls: list[str], timeout_secs: int) -> str | None`
- Function L460: `_validate_local_raw_hours(*, destination: Path, filenames: list[str]) -> tuple[list[str], list[str], list[str], list[str]]`
- Function L472: `_pid_is_active(pid: int) -> bool`
- Function L484: `_stale_tmp_download_paths(destination: Path) -> list[Path]`
- Function L496: `_is_stale_tmp_download_path(tmp_path: Path, *, destination_exists: bool) -> bool`
- Function L512: `_cleanup_stale_tmp_downloads(destination: Path) -> int`
- Function L528: `_set_status(progress_bar: tqdm | None, *, total_hours: int, completed_hours: int, active_hours: int, status: str, force: bool = False) -> None`
- Function L561: `_write_progress_line(progress_bar: tqdm | None, line: str) -> None`
- Function L567: `_download_one(*, url: str, destination: Path, timeout_secs: int, progress_bar: tqdm | None, total_hours: int, completed_hours: int, source: str, hour_label: str) -> int`
- Function L628: `download_raw_hours(*, destination: Path, archive_listing_url: str = _DEFAULT_ARCHIVE_LISTING_URL, archive_base_url: str = _DEFAULT_ARCHIVE_BASE_URL, archive_sources: list[tuple[str, str]] | None = None, source_order: list[str] | None = None, start_time: str | None = None, end_time: str | None = None, overwrite: bool = False, timeout_secs: int = 60, show_progress: bool = True, discovery_stale_pages: int = 1, discovery_max_pages: int | None = None) -> RawDownloadSummary`
- Class L37: `ArchiveSource`
- Class L43: `RawDownloadSummary`
  - Method L62: `as_dict(self) -> dict[str, object]`

### `scripts/_profile_telonex.py`
- Imports: `__future__, concurrent, datetime, dotenv, httpx, io, os, pandas, pathlib, time, urllib`
- Function L42: `_parse_d(v) -> Any`
- Function L71: `build_url(slug: str, date: str, channel: str = CHANNEL) -> str`
- Function L76: `urllib_fetch(slug: str, date: str) -> tuple[float, float, int]`
- Function L113: `httpx_fetch(slug: str, date: str) -> tuple[float, float, int]`
- Function L125: `bench(label: str, fn, workers: int) -> Any`
- Function L166: `httpx_fetch_and_parse(slug: str, date: str) -> tuple[float, float, int]`

### `scripts/_script_helpers.py`
- Imports: `__future__, pathlib, sys`
- Function L12: `ensure_repo_root(script_path: str | Path) -> Path`

### `scripts/_telonex_data_download.py`
- Imports: `__future__, asyncio, collections, concurrent, dataclasses, datetime, duckdb, httpx, io, os, pandas, pathlib, pyarrow, queue, random, signal, socket, sys, threading, time, tqdm, urllib`
- Function L99: `_format_bytes(size: int | None) -> str`
- Function L112: `_parse_date_bound(value: str | None) -> date | None`
- Function L130: `_date_range(start: date, end: date) -> list[date]`
- Function L139: `_api_url(*, base_url: str, channel: str, market_slug: str, outcome: str | None, outcome_id: int | None, day: date) -> str`
- Function L252: `_is_nullish_type(value_type: pa.DataType) -> bool`
- Function L260: `_normalize_telonex_table(table: pa.Table) -> pa.Table`
- Function L299: `_merge_promotable_schema(base: pa.Schema, incoming: pa.Schema) -> pa.Schema | None`
- Function L326: `_align_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table | None`
- Function L846: `_fetch_markets_dataset(base_url: str, timeout_secs: int, *, show_progress: bool = False) -> pd.DataFrame`
- Function L880: `_iter_days_for_market_tuple(row, *, from_idx: int, to_idx: int, window_start: date | None, window_end: date | None) -> list[date]`
- Function L913: `_iter_jobs_from_catalog(*, markets: pd.DataFrame, channels: list[str], outcomes: list[int], window_start: date | None, window_end: date | None, status_filter: str | None, slug_filter: set[str] | None, show_progress: bool) -> _CatalogJobIterable`
- Function L1002: `_build_jobs_from_explicit(*, channels: list[str], market_slugs: list[str], outcome: str | None, outcome_id: int | None, start: date, end: date) -> list[_Job]`
- Function L1039: `_is_transient(exc: BaseException) -> bool`
- Function L1060: `_resolve_parse_worker_count(value: int | None) -> int`
- Function L1072: `async _download_day_bytes_with_retry_async(*, client: httpx.AsyncClient, timeout_secs: int, url: str, api_key: str, stop_event: asyncio.Event, progress_cb, max_retries: int, total_timeout_secs: float | None = None) -> bytes`
- Function L1139: `async _download_day_bytes_async(*, client: httpx.AsyncClient, timeout_secs: int, url: str, api_key: str, stop_event: asyncio.Event, progress_cb) -> bytes`
- Function L1228: `_postfix_text(*, downloaded_days: int, missing: int, failed: int, bytes_total: int, active: list[_ActiveDownload]) -> str`
- Function L1259: `_prune_jobs_against_manifest(*, jobs: Iterable[_Job], store: _TelonexParquetStore, overwrite: bool, show_progress: bool, channels_hint: set[str] | None = None, recheck_empty_after_days: int | None = _DEFAULT_EMPTY_RECHECK_AFTER_DAYS) -> tuple[Iterator[_Job], list[int]]`
- Function L1321: `_run_jobs(jobs: Iterable[_Job], *, store: _TelonexParquetStore, api_key: str, base_url: str, timeout_secs: int, workers: int, show_progress: bool, total_jobs: int | None = None, commit_batch_rows: int | None = None, commit_batch_secs: float | None = None, parse_workers: int | None = None) -> tuple[int, int, int, int, int, bool, list[str]]`
- Function L1888: `download_telonex_days(*, destination: Path, market_slugs: list[str] | None = None, outcome: str | None = None, outcome_id: int | None = None, channel: str | None = None, channels: list[str] | None = None, base_url: str = _DEFAULT_API_BASE_URL, start_date: str | None = None, end_date: str | None = None, all_markets: bool = False, status_filter: str | None = None, outcomes_for_all: list[int] | None = None, overwrite: bool = False, timeout_secs: int = 60, workers: int = 16, show_progress: bool = True, db_filename: str = _MANIFEST_FILENAME, recheck_empty_after_days: int | None = _DEFAULT_EMPTY_RECHECK_AFTER_DAYS, parse_workers: int | None = None) -> TelonexDownloadSummary`
- Class L76: `TelonexDownloadSummary`
  - Method L95: `as_dict(self) -> dict[str, object]`
- Class L161: `_Job`
- Class L171: `_CatalogJobIterable`
  - Method L179: `__iter__(self) -> Iterator[_Job]`
- Class L217: `_DownloadResult`
- Class L229: `_CancelledError(Exception)`
- Class L234: `_OpenPart`
- Class L352: `_TelonexParquetStore`
  - Method L370: `__init__(self, root: Path, *, manifest_name: str = _MANIFEST_FILENAME) -> None`
  - Method L387: `manifest_path(self) -> Path`
  - Method L391: `data_root(self) -> Path`
  - Method L394: `close(self) -> None`
  - Method L405: `_init_schema(self) -> None`
  - Method L436: `completed_keys(self, channel: str) -> set[tuple[str, str, date]]`
  - Method L444: `empty_keys(self, channel: str, *, recheck_after_days: int | None = None) -> set[tuple[str, str, date]]`
  - Method L460: `mark_empty(self, job: _Job, *, status: str) -> None`
  - Method L463: `mark_empty_batch(self, entries: list[tuple[_Job, str]]) -> None`
  - Method L486: `_partition_dir(self, channel: str, year: int, month: int) -> Path`
  - Method L492: `_next_part_number(partition_dir: Path) -> int`
  - Method L503: `_open_part(self, key: tuple[str, int, int], schema: pa.Schema) -> _OpenPart`
  - Method L524: `_flush_open_part_locked(self, key: tuple[str, int, int]) -> None`
  - Method L586: `_append_to_partition(self, key: tuple[str, int, int], entries: list[_DownloadResult]) -> int`
  - Method L628: `_write_partition_table_locked(self, key: tuple[str, int, int], table: pa.Table, pending: list[tuple[_DownloadResult, int]]) -> int`
  - Method L694: `ingest_batch(self, results: list[_DownloadResult]) -> int`
  - Method L793: `flush_all(self) -> None`
  - Method L800: `size_bytes(self) -> int`
  - Method L813: `_remove_orphan_parts(self) -> int`
- Class L1030: `_FakeHTTPError(Exception)`
  - Method L1034: `__init__(self, code: int, message: str) -> None`
- Class L1185: `_ActiveDownload`
- Class L1192: `_ActiveRegistry`
  - Method L1193: `__init__(self) -> None`
  - Method L1198: `start(self, job: _Job) -> int`
  - Method L1210: `update(self, token: int, downloaded: int, total: int | None) -> None`
  - Method L1219: `finish(self, token: int) -> None`
  - Method L1223: `snapshot(self) -> list[_ActiveDownload]`

### `scripts/generate_codebase_uml.py`
- Imports: `__future__, ast, dataclasses, datetime, pathlib`
- Function L47: `_is_included_python_file(path: Path) -> bool`
- Function L54: `_unparse(node: ast.AST | None) -> str`
- Function L63: `_format_arg(arg: ast.arg, default: ast.AST | None = None) -> str`
- Function L71: `_callable_info(node: ast.FunctionDef | ast.AsyncFunctionDef) -> CallableInfo`
- Function L97: `_imports(tree: ast.Module) -> list[str]`
- Function L107: `_module_info(path: Path) -> ModuleInfo`
- Function L132: `_mermaid_overview() -> str`
- Function L148: `_render_module(module: ModuleInfo) -> list[str]`
- Function L169: `build_document() -> str`
- Function L199: `main() -> int`
- Class L23: `CallableInfo`
- Class L32: `ClassInfo`
- Class L40: `ModuleInfo`

### `scripts/pmxt_download_raws.py`
- Imports: `__future__, argparse, json, pathlib, scripts`
- Function L17: `_parse_archive_source(value: str) -> tuple[str, str]`
- Function L30: `main() -> int`

### `scripts/run_all_backtests.py`
- Imports: `__future__, argparse, dataclasses, pathlib, subprocess, sys, time, tqdm`
- Function L33: `discover_runner_paths() -> list[Path]`
- Function L41: `_resolve_selected_runners(raw_values: list[str] | None) -> list[Path]`
- Function L72: `_run_runner(relative_path: Path, *, python_executable: str) -> RunnerResult`
- Function L83: `main() -> int`
- Class L23: `RunnerResult`
  - Method L29: `ok(self) -> bool`

### `scripts/telonex_download_data.py`
- Imports: `__future__, argparse, dotenv, json, pathlib, scripts`
- Function L23: `main() -> int`

### `strategies/__init__.py`
- Imports: `strategies`

### `strategies/breakout.py`
- Imports: `__future__, collections, decimal, math, nautilus_trader, strategies, typing`
- Class L34: `_BreakoutConfig(Protocol)`
- Class L48: `BarBreakoutConfig(StrategyConfig)`
  - Method L62: `__post_init__(self) -> None`
- Class L75: `TradeTickBreakoutConfig(StrategyConfig)`
  - Method L88: `__post_init__(self) -> None`
- Class L101: `QuoteTickBreakoutConfig(StrategyConfig)`
  - Method L114: `__post_init__(self) -> None`
- Class L127: `_BreakoutBase(LongOnlyPredictionMarketStrategy)`
  - Method L132: `__init__(self, config: _BreakoutConfig) -> None`
  - Method L139: `_append_price(self, price: float) -> None`
  - Method L143: `_breakout_buffer(self) -> float`
  - Method L146: `_mean_reversion_buffer(self) -> float`
  - Method L149: `_min_holding_periods(self) -> int`
  - Method L152: `_reentry_cooldown(self) -> int`
  - Method L155: `_requires_fresh_breakout_cross(self) -> bool`
  - Method L163: `_on_price(self, price: float, *, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L209: `on_order_filled(self, event) -> None`
  - Method L218: `on_reset(self) -> None`
- Class L226: `BarBreakoutStrategy(_BreakoutBase)`
  - Method L227: `_subscribe(self) -> None`
  - Method L230: `on_bar(self, bar: Bar) -> None`
- Class L235: `TradeTickBreakoutStrategy(_BreakoutBase)`
  - Method L236: `_subscribe(self) -> None`
  - Method L239: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L244: `QuoteTickBreakoutStrategy(_BreakoutBase)`
  - Method L245: `_subscribe(self) -> None`
  - Method L248: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/core.py`
- Imports: `__future__, decimal, nautilus_trader, typing`
- Function L36: `_decimal_or_none(value: object) -> Decimal | None`
- Function L45: `_estimate_entry_unit_cost(*, reference_price: Decimal, taker_fee: Decimal) -> Decimal`
- Function L50: `_cap_entry_size_to_free_balance(*, desired_size: Decimal, reference_price: Decimal | None, taker_fee: Decimal, free_balance: Decimal | None) -> Decimal`
- Function L74: `_cap_entry_size_to_visible_liquidity(*, desired_size: Decimal, visible_size: Decimal | None) -> Decimal`
- Function L86: `_effective_entry_reference_price(*, reference_price: Decimal | None, visible_size: Decimal | None) -> Decimal`
- Class L31: `LongOnlyConfig(Protocol)`
- Class L99: `LongOnlyPredictionMarketStrategy(Strategy)`
  - Method L104: `__init__(self, config: LongOnlyConfig) -> None`
  - Method L112: `_subscribe(self) -> None`
  - Method L115: `on_start(self) -> None`
  - Method L123: `_in_position(self) -> bool`
  - Method L126: `_free_quote_balance(self) -> Decimal | None`
  - Method L136: `_entry_quantity(self, *, reference_price: float | None = None, visible_size: float | None = None) -> Any`
  - Method L180: `_submit_entry(self, *, reference_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L195: `_submit_exit(self) -> None`
  - Method L199: `_risk_exit(self, *, price: float, take_profit: float, stop_loss: float) -> bool`
  - Method L210: `on_order_filled(self, event) -> None`
  - Method L232: `on_order_rejected(self, event) -> None`
  - Method L235: `on_order_canceled(self, event) -> None`
  - Method L238: `on_order_expired(self, event) -> None`
  - Method L241: `on_stop(self) -> None`
  - Method L245: `on_reset(self) -> None`

### `strategies/deep_value.py`
- Imports: `__future__, decimal, nautilus_trader, strategies`
- Class L31: `TradeTickDeepValueHoldConfig(StrategyConfig)`
- Class L38: `QuoteTickDeepValueHoldConfig(StrategyConfig)`
- Class L45: `_DeepValueHoldBase(LongOnlyPredictionMarketStrategy)`
  - Method L50: `__init__(self, config: TradeTickDeepValueHoldConfig | QuoteTickDeepValueHoldConfig) -> None`
  - Method L54: `_on_price(self, price: float, *, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L72: `on_order_filled(self, event) -> None`
  - Method L77: `on_reset(self) -> None`
- Class L82: `TradeTickDeepValueHoldStrategy(_DeepValueHoldBase)`
  - Method L83: `_subscribe(self) -> None`
  - Method L86: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L91: `QuoteTickDeepValueHoldStrategy(_DeepValueHoldBase)`
  - Method L92: `_subscribe(self) -> None`
  - Method L95: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/ema_crossover.py`
- Imports: `__future__, decimal, nautilus_trader, strategies, typing`
- Class L31: `_EMACrossoverConfig(Protocol)`
- Class L41: `BarEMACrossoverConfig(StrategyConfig)`
- Class L52: `TradeTickEMACrossoverConfig(StrategyConfig)`
- Class L62: `QuoteTickEMACrossoverConfig(StrategyConfig)`
- Class L72: `_EMACrossoverBase(LongOnlyPredictionMarketStrategy)`
  - Method L77: `__init__(self, config: _EMACrossoverConfig) -> None`
  - Method L86: `_on_price(self, price: float, *, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L119: `on_reset(self) -> None`
- Class L126: `BarEMACrossoverStrategy(_EMACrossoverBase)`
  - Method L127: `_subscribe(self) -> None`
  - Method L130: `on_bar(self, bar: Bar) -> None`
- Class L135: `TradeTickEMACrossoverStrategy(_EMACrossoverBase)`
  - Method L136: `_subscribe(self) -> None`
  - Method L139: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L144: `QuoteTickEMACrossoverStrategy(_EMACrossoverBase)`
  - Method L145: `_subscribe(self) -> None`
  - Method L148: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/final_period_momentum.py`
- Imports: `__future__, decimal, nautilus_trader, strategies, typing`
- Class L19: `_FinalPeriodMomentumConfig(Protocol)`
- Class L29: `BarFinalPeriodMomentumConfig(StrategyConfig)`
- Class L40: `TradeTickFinalPeriodMomentumConfig(StrategyConfig)`
- Class L50: `QuoteTickFinalPeriodMomentumConfig(StrategyConfig)`
- Class L60: `_FinalPeriodMomentumBase(LongOnlyPredictionMarketStrategy)`
  - Method L65: `__init__(self, config: _FinalPeriodMomentumConfig) -> None`
  - Method L70: `_final_period_start_ns(self) -> int`
  - Method L78: `_is_in_final_period(self, ts_event_ns: int) -> bool`
  - Method L84: `_crossed_above_entry(self, previous_price: float | None, price: float) -> bool`
  - Method L89: `_on_price(self, *, price: float, ts_event_ns: int, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L124: `on_reset(self) -> None`
  - Method L129: `on_order_filled(self, event) -> None`
- Class L135: `BarFinalPeriodMomentumStrategy(_FinalPeriodMomentumBase)`
  - Method L136: `_subscribe(self) -> None`
  - Method L139: `on_bar(self, bar: Bar) -> None`
- Class L148: `TradeTickFinalPeriodMomentumStrategy(_FinalPeriodMomentumBase)`
  - Method L149: `_subscribe(self) -> None`
  - Method L152: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L161: `QuoteTickFinalPeriodMomentumStrategy(_FinalPeriodMomentumBase)`
  - Method L162: `_subscribe(self) -> None`
  - Method L165: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/late_favorite_limit_hold.py`
- Imports: `__future__, decimal, nautilus_trader, strategies`
- Function L18: `_validate_late_favorite_config(*, trade_size: Decimal, entry_price: float, activation_start_time_ns: int, market_close_time_ns: int) -> None`
- Class L43: `TradeTickLateFavoriteLimitHoldConfig(StrategyConfig)`
  - Method L50: `__post_init__(self) -> None`
- Class L59: `QuoteTickLateFavoriteLimitHoldConfig(StrategyConfig)`
  - Method L66: `__post_init__(self) -> None`
- Class L75: `_LateFavoriteLimitHoldBase(LongOnlyPredictionMarketStrategy)`
  - Method L83: `__init__(self, config: TradeTickLateFavoriteLimitHoldConfig | QuoteTickLateFavoriteLimitHoldConfig) -> None`
  - Method L89: `_on_price(self, *, signal_price: float, order_price: float, ts_event_ns: int, visible_size: float | None = None) -> None`
  - Method L126: `on_order_filled(self, event) -> None`
  - Method L131: `on_order_expired(self, event) -> None`
  - Method L134: `on_stop(self) -> None`
  - Method L138: `on_reset(self) -> None`
- Class L143: `TradeTickLateFavoriteLimitHoldStrategy(_LateFavoriteLimitHoldBase)`
  - Method L144: `_subscribe(self) -> None`
  - Method L147: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L156: `QuoteTickLateFavoriteLimitHoldStrategy(_LateFavoriteLimitHoldBase)`
  - Method L157: `_subscribe(self) -> None`
  - Method L160: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/mean_reversion.py`
- Imports: `__future__, collections, decimal, nautilus_trader, strategies, typing`
- Class L32: `_MeanReversionConfig(Protocol)`
- Class L41: `BarMeanReversionConfig(StrategyConfig)`
  - Method L51: `__post_init__(self) -> None`
- Class L66: `TradeTickMeanReversionConfig(StrategyConfig)`
  - Method L75: `__post_init__(self) -> None`
- Class L90: `QuoteTickMeanReversionConfig(StrategyConfig)`
  - Method L99: `__post_init__(self) -> None`
- Class L114: `_MeanReversionBase(LongOnlyPredictionMarketStrategy)`
  - Method L121: `__init__(self, config: _MeanReversionConfig) -> None`
  - Method L125: `_window(self) -> int`
  - Method L128: `_on_price(self, price: float, *, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L152: `on_reset(self) -> None`
- Class L157: `BarMeanReversionStrategy(_MeanReversionBase)`
  - Method L158: `_subscribe(self) -> None`
  - Method L161: `on_bar(self, bar: Bar) -> None`
- Class L166: `TradeTickMeanReversionStrategy(_MeanReversionBase)`
  - Method L169: `_subscribe(self) -> None`
  - Method L172: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L177: `QuoteTickMeanReversionStrategy(_MeanReversionBase)`
  - Method L178: `_subscribe(self) -> None`
  - Method L181: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/panic_fade.py`
- Imports: `__future__, collections, decimal, nautilus_trader, strategies, typing`
- Class L33: `_PanicFadeConfig(Protocol)`
- Class L45: `BarPanicFadeConfig(StrategyConfig)`
- Class L58: `TradeTickPanicFadeConfig(StrategyConfig)`
- Class L70: `QuoteTickPanicFadeConfig(StrategyConfig)`
- Class L82: `_PanicFadeBase(LongOnlyPredictionMarketStrategy)`
  - Method L87: `__init__(self, config: _PanicFadeConfig) -> None`
  - Method L92: `_on_price(self, price: float, *, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L122: `on_order_filled(self, event) -> None`
  - Method L127: `on_reset(self) -> None`
- Class L133: `BarPanicFadeStrategy(_PanicFadeBase)`
  - Method L134: `_subscribe(self) -> None`
  - Method L137: `on_bar(self, bar: Bar) -> None`
- Class L142: `TradeTickPanicFadeStrategy(_PanicFadeBase)`
  - Method L143: `_subscribe(self) -> None`
  - Method L146: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L151: `QuoteTickPanicFadeStrategy(_PanicFadeBase)`
  - Method L152: `_subscribe(self) -> None`
  - Method L155: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/rsi_reversion.py`
- Imports: `__future__, collections, decimal, nautilus_trader, strategies, typing`
- Class L32: `_RSIReversionConfig(Protocol)`
- Class L42: `BarRSIReversionConfig(StrategyConfig)`
- Class L53: `TradeTickRSIReversionConfig(StrategyConfig)`
- Class L63: `QuoteTickRSIReversionConfig(StrategyConfig)`
- Class L73: `_RSIReversionBase(LongOnlyPredictionMarketStrategy)`
  - Method L78: `__init__(self, config: _RSIReversionConfig) -> None`
  - Method L82: `_compute_rsi(self) -> float | None`
  - Method L108: `_on_price(self, price: float, *, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L135: `on_reset(self) -> None`
- Class L140: `BarRSIReversionStrategy(_RSIReversionBase)`
  - Method L141: `_subscribe(self) -> None`
  - Method L144: `on_bar(self, bar: Bar) -> None`
- Class L149: `TradeTickRSIReversionStrategy(_RSIReversionBase)`
  - Method L150: `_subscribe(self) -> None`
  - Method L153: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L158: `QuoteTickRSIReversionStrategy(_RSIReversionBase)`
  - Method L159: `_subscribe(self) -> None`
  - Method L162: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/threshold_momentum.py`
- Imports: `__future__, decimal, nautilus_trader, strategies, typing`
- Class L19: `_ThresholdMomentumConfig(Protocol)`
- Class L29: `BarThresholdMomentumConfig(StrategyConfig)`
- Class L40: `TradeTickThresholdMomentumConfig(StrategyConfig)`
- Class L50: `QuoteTickThresholdMomentumConfig(StrategyConfig)`
- Class L60: `_ThresholdMomentumBase(LongOnlyPredictionMarketStrategy)`
  - Method L65: `__init__(self, config: _ThresholdMomentumConfig) -> None`
  - Method L70: `_crossed_above_entry(self, previous_price: float | None, price: float) -> bool`
  - Method L75: `_entry_window_is_open(self, ts_event_ns: int) -> bool`
  - Method L86: `_on_price(self, *, price: float, ts_event_ns: int, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L121: `on_reset(self) -> None`
  - Method L126: `on_order_filled(self, event) -> None`
- Class L132: `BarThresholdMomentumStrategy(_ThresholdMomentumBase)`
  - Method L133: `_subscribe(self) -> None`
  - Method L136: `on_bar(self, bar: Bar) -> None`
- Class L145: `TradeTickThresholdMomentumStrategy(_ThresholdMomentumBase)`
  - Method L146: `_subscribe(self) -> None`
  - Method L149: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L158: `QuoteTickThresholdMomentumStrategy(_ThresholdMomentumBase)`
  - Method L159: `_subscribe(self) -> None`
  - Method L162: `on_quote_tick(self, tick: QuoteTick) -> None`

### `strategies/vwap_reversion.py`
- Imports: `__future__, collections, decimal, nautilus_trader, strategies`
- Class L31: `TradeTickVWAPReversionConfig(StrategyConfig)`
  - Method L41: `__post_init__(self) -> None`
- Class L58: `QuoteTickVWAPReversionConfig(StrategyConfig)`
  - Method L68: `__post_init__(self) -> None`
- Class L85: `_VWAPReversionBase(LongOnlyPredictionMarketStrategy)`
  - Method L90: `__init__(self, config: TradeTickVWAPReversionConfig | QuoteTickVWAPReversionConfig) -> None`
  - Method L96: `_append_point(self, *, price: float, size: float) -> None`
  - Method L110: `_recompute_sums(self) -> None`
  - Method L114: `_on_price_size(self, *, price: float, size: float, entry_price: float | None = None, visible_size: float | None = None) -> None`
  - Method L149: `on_reset(self) -> None`
- Class L156: `TradeTickVWAPReversionStrategy(_VWAPReversionBase)`
  - Method L157: `_subscribe(self) -> None`
  - Method L160: `on_trade_tick(self, tick: TradeTick) -> None`
- Class L169: `QuoteTickVWAPReversionStrategy(_VWAPReversionBase)`
  - Method L174: `_subscribe(self) -> None`
  - Method L177: `on_quote_tick(self, tick: QuoteTick) -> None`
