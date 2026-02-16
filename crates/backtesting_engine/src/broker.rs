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
}

/// Check if an order should fill against a trade. Returns fill price if matched.
fn match_order(order: &Order, trade: &Trade) -> Option<f64> {
    match (order.action, order.side) {
        (OrderAction::Buy, Side::Yes) => {
            if trade.yes_price <= order.price {
                Some(trade.yes_price)
            } else {
                None
            }
        }
        (OrderAction::Sell, Side::Yes) => {
            if trade.yes_price >= order.price {
                Some(trade.yes_price)
            } else {
                None
            }
        }
        (OrderAction::Buy, Side::No) => {
            if trade.no_price <= order.price {
                Some(trade.no_price)
            } else {
                None
            }
        }
        (OrderAction::Sell, Side::No) => {
            if trade.no_price >= order.price {
                Some(trade.no_price)
            } else {
                None
            }
        }
    }
}

/// Apply slippage to a fill price, clamping to [0.01, 0.99].
fn apply_slippage(slippage: f64, price: f64, action: OrderAction) -> f64 {
    if slippage == 0.0 {
        return price;
    }
    match action {
        OrderAction::Buy => (price + slippage).min(0.99),
        OrderAction::Sell => (price - slippage).max(0.01),
    }
}

impl Broker {
    pub fn new(commission_rate: f64, slippage: f64, liquidity_cap: bool) -> Self {
        Self {
            pending: HashMap::new(),
            commission_rate,
            slippage,
            liquidity_cap,
            next_id: 1,
        }
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

            let fill_price = apply_slippage(slippage, fill_price, order.action);

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
