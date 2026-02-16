/// Core data types for the backtesting engine.
///
/// Internal Rust representations — not exposed to Python directly.
/// Conversion to/from Python objects happens in engine.rs at FFI boundaries.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    Yes,
    No,
}

impl Side {
    pub fn as_str(&self) -> &'static str {
        match self {
            Side::Yes => "yes",
            Side::No => "no",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderAction {
    Buy,
    Sell,
}

impl OrderAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            OrderAction::Buy => "buy",
            OrderAction::Sell => "sell",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderStatus {
    Pending,
    Filled,
    Cancelled,
}

/// Trade event extracted from a Python TradeEvent.
#[derive(Clone, Debug)]
pub struct Trade {
    pub timestamp: f64, // epoch seconds
    pub market_id: String,
    pub yes_price: f64,
    pub no_price: f64,
    pub quantity: f64,
    pub taker_side: Side,
}

/// Limit order managed by the broker.
#[derive(Clone, Debug)]
pub struct Order {
    pub order_id: String,
    pub market_id: String,
    pub action: OrderAction,
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub status: OrderStatus,
    pub created_at: f64,
    pub filled_at: Option<f64>,
    pub fill_price: Option<f64>,
    pub filled_quantity: f64,
}

/// Record of a filled order.
#[derive(Clone, Debug)]
pub struct Fill {
    pub order_id: String,
    pub market_id: String,
    pub action: OrderAction,
    pub side: Side,
    pub price: f64,
    pub quantity: f64,
    pub timestamp: f64,
    pub commission: f64,
}

/// Position in a single market.
#[derive(Clone, Debug)]
pub struct Position {
    pub market_id: String,
    pub quantity: f64,
    pub avg_entry_price: f64,
    pub realized_pnl: f64,
}

impl Position {
    pub fn new(market_id: String) -> Self {
        Self {
            market_id,
            quantity: 0.0,
            avg_entry_price: 0.0,
            realized_pnl: 0.0,
        }
    }
}

/// Point-in-time portfolio snapshot.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub timestamp: f64,
    pub cash: f64,
    pub total_equity: f64,
    pub unrealized_pnl: f64,
    pub num_positions: i32,
}

/// Extracted market metadata for lifecycle event processing.
#[derive(Clone, Debug)]
pub struct MarketData {
    pub market_id: String,
    pub title: String,
    pub open_time: Option<f64>,
    pub close_time: Option<f64>,
    pub result: Option<Side>,
}
