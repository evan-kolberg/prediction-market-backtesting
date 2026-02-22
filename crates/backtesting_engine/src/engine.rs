/// PyO3 engine — the compiled hot loop with Python FFI.
///
/// RustEngine is a #[pyclass] that owns broker + portfolio via RefCell.
/// Strategy callbacks and trade iteration go through Python.
/// The broker/portfolio/lifecycle logic runs as compiled Rust.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Local};
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::broker::Broker;
use crate::models::*;
use crate::portfolio::Portfolio;

// ---------------------------------------------------------------------------
// ANSI color codes
// ---------------------------------------------------------------------------

const GREEN: &str = "\x1b[32m";
const RED: &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const BOLD: &str = "\x1b[1m";
const DIM: &str = "\x1b[2m";
const RESET: &str = "\x1b[0m";

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

fn format_market_ts(ts: f64) -> String {
    DateTime::from_timestamp(ts as i64, ((ts.fract()) * 1_000_000_000.0) as u32)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "????-??-?? ??:??:??".to_string())
}

fn wall_clock_ts() -> String {
    Local::now().format("%H:%M:%S").to_string()
}

fn log_line(market_ts: f64, component: &str, message: &str, color: &str) -> String {
    let wall = wall_clock_ts();
    let sim = format_market_ts(market_ts);
    format!(
        "{DIM}{wall}  {sim}{RESET}  {BOLD}{component}{RESET}  {color}{message}{RESET}"
    )
}

fn emit_log(write_fn: Option<&Bound<'_, PyAny>>, line: &str) -> PyResult<()> {
    if let Some(wfn) = write_fn {
        wfn.call1((line,))?;
    }
    Ok(())
}

fn fmt_pnl(value: f64) -> String {
    if value >= 0.0 {
        format!("+${:.2}", value)
    } else {
        format!("-${:.2}", value.abs())
    }
}

// ---------------------------------------------------------------------------
// Python ↔ Rust conversion helpers
// ---------------------------------------------------------------------------

/// Cache of Python class/enum objects to avoid repeated imports.
struct PyClasses {
    fill_cls: PyObject,
    order_cls: PyObject,
    snap_cls: PyObject,
    timedelta_cls: PyObject,
    buy_action: PyObject,
    sell_action: PyObject,
    yes_side: PyObject,
    no_side: PyObject,
    pending_status: PyObject,
    epoch: PyObject,
}

impl PyClasses {
    fn load(py: Python<'_>) -> PyResult<Self> {
        let models = py.import_bound("src.backtesting.models")?;
        let dt_mod = py.import_bound("datetime")?;

        let order_action = models.getattr("OrderAction")?;
        let side_cls = models.getattr("Side")?;
        let order_status = models.getattr("OrderStatus")?;
        let datetime_cls = dt_mod.getattr("datetime")?;
        let timedelta_cls = dt_mod.getattr("timedelta")?;

        let epoch = datetime_cls.call1((1970, 1, 1))?;

        Ok(Self {
            fill_cls: models.getattr("Fill")?.unbind(),
            order_cls: models.getattr("Order")?.unbind(),
            snap_cls: models.getattr("PortfolioSnapshot")?.unbind(),
            timedelta_cls: timedelta_cls.unbind(),
            buy_action: order_action.call1(("buy",))?.unbind(),
            sell_action: order_action.call1(("sell",))?.unbind(),
            yes_side: side_cls.call1(("yes",))?.unbind(),
            no_side: side_cls.call1(("no",))?.unbind(),
            pending_status: order_status.call1(("pending",))?.unbind(),
            epoch: epoch.unbind(),
        })
    }

    fn action_py<'py>(&self, py: Python<'py>, action: OrderAction) -> &Bound<'py, PyAny> {
        match action {
            OrderAction::Buy => self.buy_action.bind(py),
            OrderAction::Sell => self.sell_action.bind(py),
        }
    }

    fn side_py<'py>(&self, py: Python<'py>, side: Side) -> &Bound<'py, PyAny> {
        match side {
            Side::Yes => self.yes_side.bind(py),
            Side::No => self.no_side.bind(py),
        }
    }

    fn ts_to_py(&self, py: Python<'_>, ts: f64) -> PyResult<PyObject> {
        let delta = self.timedelta_cls.bind(py).call1((0, ts))?;
        let dt = self.epoch.bind(py).call_method1("__add__", (delta,))?;
        Ok(dt.unbind())
    }
}

fn extract_timestamp(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<f64> {
    obj.call_method0(intern!(py, "timestamp"))?.extract()
}

fn extract_optional_ts(py: Python<'_>, obj: &Bound<'_, PyAny>) -> PyResult<Option<f64>> {
    if obj.is_none() {
        Ok(None)
    } else {
        Ok(Some(extract_timestamp(py, obj)?))
    }
}

fn extract_trade(py: Python<'_>, py_trade: &Bound<'_, PyAny>) -> PyResult<Trade> {
    let ts_obj = py_trade.getattr(intern!(py, "timestamp"))?;
    let taker = py_trade.getattr(intern!(py, "taker_side"))?;
    let taker_val: String = taker.getattr(intern!(py, "value"))?.extract()?;

    Ok(Trade {
        timestamp: extract_timestamp(py, &ts_obj)?,
        market_id: py_trade.getattr(intern!(py, "market_id"))?.extract()?,
        yes_price: py_trade.getattr(intern!(py, "yes_price"))?.extract()?,
        no_price: py_trade.getattr(intern!(py, "no_price"))?.extract()?,
        quantity: py_trade.getattr(intern!(py, "quantity"))?.extract()?,
        taker_side: if taker_val == "yes" { Side::Yes } else { Side::No },
    })
}

fn extract_markets(
    py: Python<'_>,
    py_markets: &Bound<'_, PyDict>,
) -> PyResult<HashMap<String, MarketData>> {
    let mut out = HashMap::with_capacity(py_markets.len());
    for (key, value) in py_markets {
        let mid: String = key.extract()?;
        let title: String = value.getattr(intern!(py, "title"))?.extract()?;
        let open_time = extract_optional_ts(py, &value.getattr(intern!(py, "open_time"))?)?;
        let close_time = extract_optional_ts(py, &value.getattr(intern!(py, "close_time"))?)?;

        let result_attr = value.getattr(intern!(py, "result"))?;
        let result = if result_attr.is_none() {
            None
        } else {
            let val: String = result_attr.getattr(intern!(py, "value"))?.extract()?;
            Some(if val == "yes" { Side::Yes } else { Side::No })
        };

        out.insert(mid.clone(), MarketData { market_id: mid, title, open_time, close_time, result });
    }
    Ok(out)
}

fn build_lifecycle_events(
    markets: &HashMap<String, MarketData>,
    get_time: impl Fn(&MarketData) -> Option<f64>,
) -> Vec<(f64, String)> {
    let mut events: Vec<(f64, String)> = markets
        .values()
        .filter_map(|m| get_time(m).map(|t| (t, m.market_id.clone())))
        .collect();
    events.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    events
}

// ---------------------------------------------------------------------------
// RustEngine
// ---------------------------------------------------------------------------

#[pyclass]
pub struct RustEngine {
    initial_cash: f64,
    commission_rate: f64,
    slippage: f64,
    liquidity_cap: bool,
    snapshot_interval: u64,
    ema_decay: f64,
    broker: RefCell<Broker>,
    portfolio: RefCell<Portfolio>,
    current_ts: Cell<f64>,
}

#[pymethods]
impl RustEngine {
    #[new]
    #[pyo3(signature = (initial_cash=10_000.0, commission_rate=0.01, slippage=0.005, liquidity_cap=true, snapshot_interval=1000, ema_decay=0.1))]
    fn new(
        initial_cash: f64,
        commission_rate: f64,
        slippage: f64,
        liquidity_cap: bool,
        snapshot_interval: u64,
        ema_decay: f64,
    ) -> Self {
        Self {
            initial_cash,
            commission_rate,
            slippage,
            liquidity_cap,
            snapshot_interval,
            ema_decay,
            broker: RefCell::new(Broker::new(commission_rate, slippage, liquidity_cap, ema_decay)),
            portfolio: RefCell::new(Portfolio::new(initial_cash)),
            current_ts: Cell::new(0.0),
        }
    }

    fn place_order(
        &self,
        py: Python<'_>,
        market_id: String,
        action: String,
        side: String,
        price: f64,
        quantity: f64,
    ) -> PyResult<PyObject> {
        let act = if action == "buy" { OrderAction::Buy } else { OrderAction::Sell };
        let s = if side == "yes" { Side::Yes } else { Side::No };
        let order = self.broker.borrow_mut().place_order(
            &market_id, act, s, price, quantity, self.current_ts.get(),
        );
        let cls = PyClasses::load(py)?;
        order_to_python(py, &order, &cls)
    }

    fn cancel_order(&self, order_id: String) -> bool {
        self.broker.borrow_mut().cancel_order(&order_id)
    }

    #[pyo3(signature = (market_id=None))]
    fn cancel_all(&self, market_id: Option<String>) -> i32 {
        self.broker.borrow_mut().cancel_all(market_id.as_deref()) as i32
    }

    fn get_portfolio_snapshot(&self, py: Python<'_>) -> PyResult<PyObject> {
        let portfolio = self.portfolio.borrow();
        let snap = portfolio.compute_snapshot(self.current_ts.get());
        let cls = PyClasses::load(py)?;
        snapshot_to_python(py, &snap, &cls)
    }

    fn get_pending_orders(&self, py: Python<'_>) -> PyResult<PyObject> {
        let broker = self.broker.borrow();
        let orders = broker.all_pending();
        let cls = PyClasses::load(py)?;
        let py_list = PyList::empty_bound(py);
        for order in orders {
            py_list.append(order_to_python(py, order, &cls)?)?;
        }
        Ok(py_list.into())
    }

    #[getter]
    fn current_time(&self, py: Python<'_>) -> PyResult<PyObject> {
        let cls = PyClasses::load(py)?;
        cls.ts_to_py(py, self.current_ts.get())
    }

    /// Run the full simulation. Returns a Python dict with all result data.
    #[pyo3(signature = (trade_iter, strategy, py_markets, write_fn=None))]
    fn run(
        &self,
        py: Python<'_>,
        trade_iter: &Bound<'_, PyAny>,
        strategy: &Bound<'_, PyAny>,
        py_markets: &Bound<'_, PyDict>,
        write_fn: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        // Reset state
        *self.broker.borrow_mut() =
            Broker::new(self.commission_rate, self.slippage, self.liquidity_cap, self.ema_decay);
        *self.portfolio.borrow_mut() = Portfolio::new(self.initial_cash);
        self.current_ts.set(0.0);

        let cls = PyClasses::load(py)?;

        // Extract market data
        let rust_markets = extract_markets(py, py_markets)?;
        let mut py_market_objs: HashMap<String, PyObject> = HashMap::new();
        for (key, value) in py_markets {
            let mid: String = key.extract()?;
            py_market_objs.insert(mid, value.unbind());
        }

        // Lifecycle events
        let open_events = build_lifecycle_events(&rust_markets, |m| m.open_time);
        let close_events = build_lifecycle_events(&rust_markets, |m| m.close_time);

        let mut opened: HashSet<String> = HashSet::new();
        let mut closed: HashSet<String> = HashSet::new();
        let mut resolved: HashSet<String> = HashSet::new();
        let mut open_idx: usize = 0;
        let mut close_idx: usize = 0;
        let mut trade_count: u64 = 0;
        let mut fill_count: u64 = 0;
        let mut first_time: Option<f64> = None;
        let mut last_time: Option<f64> = None;

        let mut all_fills: Vec<Fill> = Vec::new();
        let mut filled_market_ids: HashSet<String> = HashSet::new();
        let mut price_history: HashMap<String, Vec<(f64, f64)>> = HashMap::new();
        let mut event_log: Vec<String> = Vec::new();

        // Initialize strategy
        strategy.call_method0(intern!(py, "initialize"))?;
        event_log.push("ENGINE: Backtest started".to_string());

        // ===== HOT LOOP =====
        let iter = trade_iter.iter()?;
        for item_result in iter {
            let py_trade = item_result?;
            let trade = extract_trade(py, &py_trade)?;
            let now = trade.timestamp;
            self.current_ts.set(now);

            if first_time.is_none() {
                first_time = Some(now);
            }
            last_time = Some(now);

            // 1. Fire market open events
            while open_idx < open_events.len() && open_events[open_idx].0 <= now {
                let mid = &open_events[open_idx].1;
                open_idx += 1;
                if !opened.contains(mid) {
                    if let Some(py_mkt) = py_market_objs.get(mid) {
                        opened.insert(mid.clone());
                        let title = rust_markets.get(mid).map(|m| m.title.as_str()).unwrap_or("");
                        let msg = format!("{} \"{}\"", mid, title);
                        emit_log(write_fn, &log_line(now, "Market.OPEN", &msg, ""))?;
                        event_log.push(format!("OPEN: {} \"{}\"", mid, title));
                        strategy.call_method1(
                            intern!(py, "on_market_open"),
                            (py_mkt.bind(py),),
                        )?;
                    }
                }
            }

            // 2. Fire market close + resolve events
            while close_idx < close_events.len() && close_events[close_idx].0 <= now {
                let mid = close_events[close_idx].1.clone();
                close_idx += 1;
                if !closed.contains(&mid) {
                    if let Some(py_mkt) = py_market_objs.get(&mid) {
                        let title = rust_markets.get(&mid).map(|m| m.title.as_str()).unwrap_or("");
                        closed.insert(mid.clone());
                        let close_msg = format!("{} \"{}\"", mid, title);
                        emit_log(write_fn, &log_line(now, "Market.CLOSE", &close_msg, ""))?;
                        event_log.push(format!("CLOSE: {} \"{}\"", mid, title));
                        strategy.call_method1(
                            intern!(py, "on_market_close"),
                            (py_mkt.bind(py),),
                        )?;
                        if let Some(mdata) = rust_markets.get(&mid) {
                            if let Some(result_side) = mdata.result {
                                if !resolved.contains(&mid) {
                                    self.broker.borrow_mut().cancel_all(Some(&mid));
                                    let pnl = self.portfolio.borrow_mut()
                                        .resolve_market(&mid, result_side);
                                    let color = if pnl > 0.0 { GREEN } else if pnl < 0.0 { RED } else { "" };
                                    let msg = format!(
                                        "{} \"{}\" -> {} | P&L={}",
                                        mid, title, result_side.as_str().to_uppercase(), fmt_pnl(pnl)
                                    );
                                    emit_log(write_fn, &log_line(now, "Market.RESOLVE", &msg, color))?;
                                    event_log.push(format!(
                                        "RESOLVE: {} -> {} (pnl={:.2})",
                                        mid, result_side.as_str(), pnl
                                    ));

                                    // Check if position was closed by resolution
                                    if let Some(pos) = self.portfolio.borrow().positions.get(&mid) {
                                        if pos.quantity == 0.0 {
                                            let rpnl = pos.realized_pnl;
                                            let pc = if rpnl > 0.0 { GREEN } else if rpnl < 0.0 { RED } else { "" };
                                            let pmsg = format!("{} | realized P&L={}", mid, fmt_pnl(rpnl));
                                            emit_log(write_fn, &log_line(now, "Position.CLOSED", &pmsg, pc))?;
                                        }
                                    }

                                    let py_side = cls.side_py(py, result_side);
                                    strategy.call_method1(
                                        intern!(py, "on_market_resolve"),
                                        (py_mkt.bind(py), py_side),
                                    )?;
                                    resolved.insert(mid.clone());
                                }
                            }
                        }
                    }
                }
            }

            // 3. Update mark-to-market price
            self.portfolio.borrow_mut().update_price(&trade.market_id, trade.yes_price);

            // 4. Update EMA trade size (used by market impact model), then check fills
            self.broker.borrow_mut().update_trade_size(&trade.market_id, trade.quantity);
            let cash = self.portfolio.borrow().cash;
            let fills = self.broker.borrow_mut().check_fills(&trade, cash);

            for fill in &fills {
                // Capture position state before fill
                let qty_before = self.portfolio.borrow()
                    .positions.get(&fill.market_id)
                    .map(|p| p.quantity).unwrap_or(0.0);

                self.portfolio.borrow_mut().apply_fill(fill);
                all_fills.push(fill.clone());
                filled_market_ids.insert(fill.market_id.clone());
                fill_count += 1;

                // Capture position state after fill
                let (qty_after, realized_pnl, cash_after, avg_entry) = {
                    let p = self.portfolio.borrow();
                    let pos = p.positions.get(&fill.market_id);
                    (
                        pos.map(|p| p.quantity).unwrap_or(0.0),
                        pos.map(|p| p.realized_pnl).unwrap_or(0.0),
                        p.cash,
                        pos.map(|p| p.avg_entry_price).unwrap_or(0.0),
                    )
                };

                // Log fill — always yellow to match front-testing ORDER color
                let fill_color = YELLOW;
                let fill_msg = format!(
                    "{} {:.0} {} @ {:.2} {} | comm=${:.2}",
                    fill.action.as_str().to_uppercase(),
                    fill.quantity,
                    fill.side.as_str().to_uppercase(),
                    fill.price,
                    fill.market_id,
                    fill.commission,
                );
                emit_log(write_fn, &log_line(fill.timestamp, "Order.FILLED", &fill_msg, fill_color))?;
                event_log.push(format!(
                    "FILL: {} {} {} @ {:.4} x{} (comm={:.4})",
                    fill.market_id, fill.action.as_str(), fill.side.as_str(),
                    fill.price, fill.quantity, fill.commission
                ));

                // Position opened
                if qty_before == 0.0 && qty_after != 0.0 {
                    let side_label = if qty_after > 0.0 { "YES" } else { "NO" };
                    let msg = format!(
                        "{} {:.0} {} @ {:.2} | cash=${:.2}",
                        fill.market_id, qty_after.abs(), side_label, avg_entry, cash_after
                    );
                    emit_log(write_fn, &log_line(fill.timestamp, "Position.OPENED", &msg, ""))?;
                }

                // Position closed
                if qty_before != 0.0 && qty_after == 0.0 {
                    let color = if realized_pnl > 0.0 { GREEN } else if realized_pnl < 0.0 { RED } else { "" };
                    let msg = format!("{} | realized P&L={}", fill.market_id, fmt_pnl(realized_pnl));
                    emit_log(write_fn, &log_line(fill.timestamp, "Position.CLOSED", &msg, color))?;
                }

                let py_fill = fill_to_python(py, fill, &cls)?;
                strategy.call_method1(intern!(py, "on_fill"), (py_fill,))?;

                // Periodic portfolio update
                if fill_count % 10 == 0 {
                    let snap = self.portfolio.borrow().compute_snapshot(now);
                    let msg = format!(
                        "cash=${:.2}, equity=${:.2}, positions={}",
                        snap.cash, snap.total_equity, snap.num_positions
                    );
                    emit_log(write_fn, &log_line(now, "Portfolio", &msg, ""))?;
                }
            }

            // 5. Strategy on_trade
            strategy.call_method1(intern!(py, "on_trade"), (&py_trade,))?;

            // 6. Periodic snapshot
            trade_count += 1;
            if trade_count % self.snapshot_interval == 0 {
                self.portfolio.borrow_mut().snapshot(now);
                let portfolio = self.portfolio.borrow();
                for mid in &filled_market_ids {
                    if !resolved.contains(mid) {
                        if let Some(&price) = portfolio.last_prices.get(mid) {
                            price_history.entry(mid.clone()).or_default().push((now, price));
                        }
                    }
                }
            }
        }

        // Resolve remaining markets
        let final_ts = last_time.unwrap_or(0.0);
        for (mid, mdata) in &rust_markets {
            if let Some(result_side) = mdata.result {
                if !resolved.contains(mid) {
                    if !closed.contains(mid) {
                        closed.insert(mid.clone());
                        let close_msg = format!("{} \"{}\"", mid, mdata.title);
                        emit_log(write_fn, &log_line(final_ts, "Market.CLOSE", &close_msg, ""))?;
                    }
                    self.broker.borrow_mut().cancel_all(Some(mid));
                    let pnl = self.portfolio.borrow_mut().resolve_market(mid, result_side);

                    let color = if pnl > 0.0 { GREEN } else if pnl < 0.0 { RED } else { "" };
                    let msg = format!(
                        "{} \"{}\" -> {} | P&L={}",
                        mid, mdata.title, result_side.as_str().to_uppercase(), fmt_pnl(pnl)
                    );
                    emit_log(write_fn, &log_line(final_ts, "Market.RESOLVE", &msg, color))?;
                    event_log.push(format!(
                        "RESOLVE(end): {} -> {} (pnl={:.2})",
                        mid, result_side.as_str(), pnl
                    ));

                    // Check if position was closed by resolution
                    if let Some(pos) = self.portfolio.borrow().positions.get(mid) {
                        if pos.quantity == 0.0 {
                            let rpnl = pos.realized_pnl;
                            let pc = if rpnl > 0.0 { GREEN } else if rpnl < 0.0 { RED } else { "" };
                            let pmsg = format!("{} | realized P&L={}", mid, fmt_pnl(rpnl));
                            emit_log(write_fn, &log_line(final_ts, "Position.CLOSED", &pmsg, pc))?;
                        }
                    }

                    if let Some(py_mkt) = py_market_objs.get(mid) {
                        let py_side = cls.side_py(py, result_side);
                        strategy.call_method1(
                            intern!(py, "on_market_resolve"),
                            (py_mkt.bind(py), py_side),
                        )?;
                    }
                    resolved.insert(mid.clone());
                }
            }
        }

        // Final snapshot
        if let Some(ts) = last_time {
            self.portfolio.borrow_mut().snapshot(ts);
            let portfolio = self.portfolio.borrow();
            for mid in &filled_market_ids {
                if !resolved.contains(mid) {
                    if let Some(&price) = portfolio.last_prices.get(mid) {
                        price_history.entry(mid.clone()).or_default().push((ts, price));
                    }
                }
            }
        }

        // Finalize
        strategy.call_method0(intern!(py, "finalize"))?;
        event_log.push("ENGINE: Backtest complete".to_string());

        self.build_result_dict(py, &cls, &all_fills, &filled_market_ids, &resolved,
            &price_history, &event_log, first_time, last_time)
    }
}

impl RustEngine {
    fn build_result_dict(
        &self,
        py: Python<'_>,
        cls: &PyClasses,
        all_fills: &[Fill],
        _filled_market_ids: &HashSet<String>,
        resolved: &HashSet<String>,
        price_history: &HashMap<String, Vec<(f64, f64)>>,
        event_log: &[String],
        first_time: Option<f64>,
        last_time: Option<f64>,
    ) -> PyResult<PyObject> {
        let portfolio = self.portfolio.borrow();
        let dict = PyDict::new_bound(py);

        // equity_curve
        let eq_list = PyList::empty_bound(py);
        for snap in &portfolio.snapshots {
            eq_list.append(snapshot_to_python(py, snap, cls)?)?;
        }
        dict.set_item("equity_curve", eq_list)?;

        // fills
        let fills_list = PyList::empty_bound(py);
        for fill in all_fills {
            fills_list.append(fill_to_python(py, fill, cls)?)?;
        }
        dict.set_item("fills", fills_list)?;

        // timestamps
        match first_time {
            Some(t) => dict.set_item("start_time", cls.ts_to_py(py, t)?)?,
            None => dict.set_item("start_time", py.None())?,
        }
        match last_time {
            Some(t) => dict.set_item("end_time", cls.ts_to_py(py, t)?)?,
            None => dict.set_item("end_time", py.None())?,
        }

        // final_equity
        let final_equity = portfolio.snapshots.last()
            .map(|s| s.total_equity)
            .unwrap_or(self.initial_cash);
        dict.set_item("final_equity", final_equity)?;

        // market counts
        let markets_traded: HashSet<&String> = all_fills.iter().map(|f| &f.market_id).collect();
        let resolved_and_traded = resolved.iter().filter(|mid| markets_traded.contains(mid)).count();
        dict.set_item("num_markets_traded", markets_traded.len())?;
        dict.set_item("num_markets_resolved", resolved_and_traded)?;

        // event_log
        let log_list = PyList::new_bound(py, event_log);
        dict.set_item("event_log", log_list)?;

        // market_prices
        let prices_dict = PyDict::new_bound(py);
        for (mid, entries) in price_history {
            let entry_list = PyList::empty_bound(py);
            for &(ts, price) in entries {
                let dt = cls.ts_to_py(py, ts)?;
                let tup = PyTuple::new_bound(py, &[dt, price.to_object(py)]);
                entry_list.append(tup)?;
            }
            prices_dict.set_item(mid, entry_list)?;
        }
        dict.set_item("market_prices", prices_dict)?;

        // market_pnls
        let pnls_dict = PyDict::new_bound(py);
        for mid in &markets_traded {
            if let Some(pos) = portfolio.positions.get(*mid) {
                pnls_dict.set_item(*mid, pos.realized_pnl)?;
            }
        }
        dict.set_item("market_pnls", pnls_dict)?;

        Ok(dict.into())
    }
}

fn fill_to_python(py: Python<'_>, fill: &Fill, cls: &PyClasses) -> PyResult<PyObject> {
    let dt = cls.ts_to_py(py, fill.timestamp)?;
    cls.fill_cls.bind(py).call1((
        &fill.order_id, &fill.market_id,
        cls.action_py(py, fill.action), cls.side_py(py, fill.side),
        fill.price, fill.quantity, dt, fill.commission,
    )).map(|o| o.unbind())
}

fn order_to_python(py: Python<'_>, order: &Order, cls: &PyClasses) -> PyResult<PyObject> {
    let dt = cls.ts_to_py(py, order.created_at)?;
    cls.order_cls.bind(py).call1((
        &order.order_id, &order.market_id,
        cls.action_py(py, order.action), cls.side_py(py, order.side),
        order.price, order.quantity, cls.pending_status.bind(py), dt,
    )).map(|o| o.unbind())
}

fn snapshot_to_python(py: Python<'_>, snap: &Snapshot, cls: &PyClasses) -> PyResult<PyObject> {
    let dt = cls.ts_to_py(py, snap.timestamp)?;
    cls.snap_cls.bind(py).call1((
        dt, snap.cash, snap.total_equity, snap.unrealized_pnl, snap.num_positions,
    )).map(|o| o.unbind())
}
