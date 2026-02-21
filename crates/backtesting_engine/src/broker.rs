/// Order management and fill matching.
///
/// Key difference from Python version: orders are indexed by market_id
/// for O(1) lookup instead of scanning all pending orders per trade.

use std::collections::HashMap;

use crate::models::{Fill, Order, OrderAction, OrderStatus, Side, Trade};

pub struct Broker {
    /// Orders indexed by market_id for fast lookup.
    pending: HashMap<String, Vec<Order>>,
    commission_rate: f64,
    slippage: f64,
    liquidity_cap: bool,
    next_id: u64,
    /// Exponential moving average of trade size per market.
    /// Used by the square-root market impact model to scale slippage.
    ema_trade_size: HashMap<String, f64>,
    ema_decay: f64,
}

/// Check if an order should fill against a trade. Returns fill price if matched.
///
/// Taker-side-aware: a resting limit order only fills when the trade taker is on
/// the opposite side. This correctly models CLOB maker/taker semantics.
///   YES bid fills on NO taker  (someone selling YES hits our bid)
///   YES ask fills on YES taker (someone buying YES hits our ask)
///   NO  bid fills on YES taker (someone selling NO  hits our bid)
///   NO  ask fills on NO taker  (someone buying NO  hits our ask)
fn match_order(order: &Order, trade: &Trade) -> Option<f64> {
    match (order.action, order.side) {
        (OrderAction::Buy, Side::Yes) => {
            if trade.taker_side == Side::No && trade.yes_price <= order.price {
                Some(trade.yes_price)
            } else {
                None
            }
        }
        (OrderAction::Sell, Side::Yes) => {
            if trade.taker_side == Side::Yes && trade.yes_price >= order.price {
                Some(trade.yes_price)
            } else {
                None
            }
        }
        (OrderAction::Buy, Side::No) => {
            if trade.taker_side == Side::Yes && trade.no_price <= order.price {
                Some(trade.no_price)
            } else {
                None
            }
        }
        (OrderAction::Sell, Side::No) => {
            if trade.taker_side == Side::No && trade.no_price >= order.price {
                Some(trade.no_price)
            } else {
                None
            }
        }
    }
}

/// Apply market impact to a fill price.
///
/// Combines two effects:
///   1. Price-proportional spread: spread widens at extreme prices (p near 0 or 1).
///      At p=0.50 the multiplier is 1×; at p=0.15 it is ~2×; at p=0.05 it is ~5×.
///      This reflects real prediction-market microstructure where thin books at
///      extreme prices carry much wider bid-ask spreads.
///   2. Square-root size impact: large orders relative to the market's average
///      trade size pay more (standard Almgren-Chriss / Kyle-lambda approach).
///
/// Both effects are at least 1× so the minimum cost is always base_slippage.
/// Result is clamped to [0.01, 0.99].
fn apply_market_impact(
    base_slippage: f64,
    price: f64,
    action: OrderAction,
    order_qty: f64,
    avg_trade_size: f64,
) -> f64 {
    if base_slippage == 0.0 {
        return price;
    }
    // Spread factor: 1 / (4 * p * (1-p)), floored so the max multiplier is ~25×.
    let variance = (price * (1.0 - price)).max(0.01);
    let spread_factor = (0.25 / variance).max(1.0);

    // Size factor: sqrt(order / avg_trade), at least 1×.
    let size_ratio = order_qty / avg_trade_size.max(0.01);
    let size_factor = size_ratio.sqrt().max(1.0);

    let impact = base_slippage * spread_factor * size_factor;
    match action {
        OrderAction::Buy => (price + impact).min(0.99),
        OrderAction::Sell => (price - impact).max(0.01),
    }
}

impl Broker {
    pub fn new(commission_rate: f64, slippage: f64, liquidity_cap: bool, ema_decay: f64) -> Self {
        Self {
            pending: HashMap::new(),
            commission_rate,
            slippage,
            liquidity_cap,
            next_id: 1,
            ema_trade_size: HashMap::new(),
            ema_decay,
        }
    }

    /// Update the EMA of trade size for a market. Call on every trade before check_fills.
    pub fn update_trade_size(&mut self, market_id: &str, trade_qty: f64) {
        let entry = self.ema_trade_size.entry(market_id.to_string()).or_insert(trade_qty);
        *entry = *entry * (1.0 - self.ema_decay) + trade_qty * self.ema_decay;
    }

    pub fn place_order(
        &mut self,
        market_id: &str,
        action: OrderAction,
        side: Side,
        price: f64,
        quantity: f64,
        timestamp: f64,
    ) -> Order {
        let order = Order {
            order_id: self.next_id.to_string(),
            market_id: market_id.to_string(),
            action,
            side,
            price,
            quantity,
            status: OrderStatus::Pending,
            created_at: timestamp,
            filled_at: None,
            fill_price: None,
            filled_quantity: 0.0,
        };
        self.next_id += 1;
        self.pending
            .entry(market_id.to_string())
            .or_default()
            .push(order.clone());
        order
    }

    pub fn cancel_order(&mut self, order_id: &str) -> bool {
        for orders in self.pending.values_mut() {
            if let Some(pos) = orders.iter().position(|o| o.order_id == order_id) {
                orders.remove(pos);
                return true;
            }
        }
        false
    }

    pub fn cancel_all(&mut self, market_id: Option<&str>) -> usize {
        match market_id {
            Some(mid) => {
                if let Some(orders) = self.pending.remove(mid) {
                    orders.len()
                } else {
                    0
                }
            }
            None => {
                let count: usize = self.pending.values().map(|v| v.len()).sum();
                self.pending.clear();
                count
            }
        }
    }

    /// Check all pending orders for this trade's market. O(orders_for_market).
    pub fn check_fills(&mut self, trade: &Trade, available_cash: f64) -> Vec<Fill> {
        let commission_rate = self.commission_rate;
        let slippage = self.slippage;
        let liquidity_cap = self.liquidity_cap;
        let avg_trade_size = self
            .ema_trade_size
            .get(&trade.market_id)
            .copied()
            .unwrap_or(trade.quantity);

        let orders = match self.pending.get_mut(&trade.market_id) {
            Some(orders) if !orders.is_empty() => orders,
            _ => return vec![],
        };

        let mut fills: Vec<Fill> = Vec::new();
        let mut cash = available_cash;
        let mut remaining_liq = if liquidity_cap {
            trade.quantity
        } else {
            f64::INFINITY
        };
        let mut to_remove: Vec<usize> = Vec::new();

        for (idx, order) in orders.iter_mut().enumerate() {
            let fill_price = match match_order(order, trade) {
                Some(p) => p,
                None => continue,
            };

            let fill_price =
                apply_market_impact(slippage, fill_price, order.action, order.quantity, avg_trade_size);

            let mut fill_qty = if liquidity_cap {
                order.quantity.min(remaining_liq)
            } else {
                order.quantity
            };
            if fill_qty <= 0.0 {
                continue;
            }

            let mut cost = fill_price * fill_qty;
            let mut commission = cost * commission_rate;

            if order.action == OrderAction::Buy && cost + commission > cash {
                if liquidity_cap {
                    let max_qty = cash / (fill_price * (1.0 + commission_rate));
                    fill_qty = fill_qty.min(max_qty);
                    if fill_qty < 1.0 {
                        continue;
                    }
                    fill_qty = fill_qty.floor();
                    cost = fill_price * fill_qty;
                    commission = cost * commission_rate;
                } else {
                    continue;
                }
            }

            if order.action == OrderAction::Buy {
                cash -= cost + commission;
            }
            remaining_liq -= fill_qty;

            fills.push(Fill {
                order_id: order.order_id.clone(),
                market_id: order.market_id.clone(),
                action: order.action,
                side: order.side,
                price: fill_price,
                quantity: fill_qty,
                timestamp: trade.timestamp,
                commission,
            });

            order.status = OrderStatus::Filled;
            order.filled_at = Some(trade.timestamp);
            order.fill_price = Some(fill_price);
            order.filled_quantity = fill_qty;
            to_remove.push(idx);
        }

        // Remove filled orders (reverse order to preserve indices)
        for &idx in to_remove.iter().rev() {
            orders.remove(idx);
        }

        fills
    }

    /// Return references to all pending orders (flattened).
    pub fn all_pending(&self) -> Vec<&Order> {
        self.pending.values().flat_map(|v| v.iter()).collect()
    }
}
