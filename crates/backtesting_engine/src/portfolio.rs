/// Portfolio tracking — positions, cash, equity snapshots.
///
/// Direct port of portfolio.py with identical accounting logic.
/// Positions stored as yes-contract quantity: +qty = long YES, -qty = long NO.

use std::collections::{HashMap, HashSet};

use crate::models::{Fill, OrderAction, Position, Side, Snapshot};

pub struct Portfolio {
    pub cash: f64,
    pub initial_cash: f64,
    pub positions: HashMap<String, Position>,
    pub last_prices: HashMap<String, f64>,
    pub snapshots: Vec<Snapshot>,
    resolved_markets: HashSet<String>,
}

impl Portfolio {
    pub fn new(initial_cash: f64) -> Self {
        Self {
            cash: initial_cash,
            initial_cash,
            positions: HashMap::new(),
            last_prices: HashMap::new(),
            snapshots: Vec::new(),
            resolved_markets: HashSet::new(),
        }
    }

    pub fn apply_fill(&mut self, fill: &Fill) {
        let pos = self
            .positions
            .entry(fill.market_id.clone())
            .or_insert_with(|| Position::new(fill.market_id.clone()));

        match (fill.action, fill.side) {
            (OrderAction::Buy, Side::Yes) => {
                Self::add_to_position(pos, fill.quantity, fill.price);
                self.cash -= fill.price * fill.quantity;
            }
            (OrderAction::Sell, Side::Yes) => {
                Self::reduce_position(pos, fill.quantity, fill.price);
                self.cash += fill.price * fill.quantity;
            }
            (OrderAction::Buy, Side::No) => {
                let yes_equiv = 1.0 - fill.price;
                Self::add_to_position(pos, -fill.quantity, yes_equiv);
                self.cash -= fill.price * fill.quantity;
            }
            (OrderAction::Sell, Side::No) => {
                let yes_equiv = 1.0 - fill.price;
                Self::reduce_position(pos, -fill.quantity, yes_equiv);
                self.cash += fill.price * fill.quantity;
            }
        }

        self.cash -= fill.commission;
    }

    pub fn resolve_market(&mut self, market_id: &str, result: Side) -> f64 {
        if self.resolved_markets.contains(market_id) {
            return 0.0;
        }
        let pos = match self.positions.get_mut(market_id) {
            Some(p) => p,
            None => {
                self.resolved_markets.insert(market_id.to_string());
                return 0.0;
            }
        };
        if pos.quantity == 0.0 {
            self.resolved_markets.insert(market_id.to_string());
            return 0.0;
        }

        let settlement = if result == Side::Yes { 1.0 } else { 0.0 };

        // Long YES (qty > 0): get qty * settlement ($1 if YES, $0 if NO)
        // Long NO  (qty < 0): get |qty| * (1 - settlement) ($1 if NO, $0 if YES)
        let payout = if pos.quantity > 0.0 {
            pos.quantity * settlement
        } else {
            pos.quantity.abs() * (1.0 - settlement)
        };
        self.cash += payout;

        let cost_basis = if pos.quantity > 0.0 {
            pos.quantity * pos.avg_entry_price
        } else {
            pos.quantity.abs() * (1.0 - pos.avg_entry_price)
        };
        let resolution_pnl = payout - cost_basis;
        pos.realized_pnl += resolution_pnl;

        pos.quantity = 0.0;
        pos.avg_entry_price = 0.0;
        self.resolved_markets.insert(market_id.to_string());

        resolution_pnl
    }

    pub fn update_price(&mut self, market_id: &str, yes_price: f64) {
        self.last_prices.insert(market_id.to_string(), yes_price);
    }

    /// Compute and store a snapshot.
    pub fn snapshot(&mut self, timestamp: f64) -> Snapshot {
        let snap = self.compute_snapshot(timestamp);
        self.snapshots.push(snap.clone());
        snap
    }

    /// Compute a snapshot without storing it.
    pub fn compute_snapshot(&self, timestamp: f64) -> Snapshot {
        let mut unrealized = 0.0;
        let mut num_positions = 0i32;

        for (mid, pos) in &self.positions {
            if pos.quantity == 0.0 || self.resolved_markets.contains(mid) {
                continue;
            }
            num_positions += 1;
            let last_price = self
                .last_prices
                .get(mid)
                .copied()
                .unwrap_or(pos.avg_entry_price);
            if pos.quantity > 0.0 {
                unrealized += pos.quantity * (last_price - pos.avg_entry_price);
            } else {
                unrealized += pos.quantity.abs() * (pos.avg_entry_price - last_price);
            }
        }

        Snapshot {
            timestamp,
            cash: self.cash,
            total_equity: self.cash + unrealized,
            unrealized_pnl: unrealized,
            num_positions,
        }
    }

    pub fn is_resolved(&self, market_id: &str) -> bool {
        self.resolved_markets.contains(market_id)
    }

    fn add_to_position(pos: &mut Position, delta: f64, price: f64) {
        if pos.quantity == 0.0 {
            pos.quantity = delta;
            pos.avg_entry_price = price;
            return;
        }

        let same_direction = (pos.quantity > 0.0) == (delta > 0.0);
        if same_direction {
            let total_cost = pos.quantity.abs() * pos.avg_entry_price + delta.abs() * price;
            pos.quantity += delta;
            if pos.quantity != 0.0 {
                pos.avg_entry_price = total_cost / pos.quantity.abs();
            }
        } else {
            Self::close_partial(pos, delta, price);
        }
    }

    fn reduce_position(pos: &mut Position, delta: f64, price: f64) {
        Self::close_partial(pos, -delta, price);
    }

    fn close_partial(pos: &mut Position, delta: f64, price: f64) {
        let closing_qty = delta.abs().min(pos.quantity.abs());
        if closing_qty == 0.0 {
            pos.quantity += delta;
            pos.avg_entry_price = price;
            return;
        }

        let pnl = if pos.quantity > 0.0 {
            closing_qty * (price - pos.avg_entry_price)
        } else {
            closing_qty * (pos.avg_entry_price - price)
        };
        pos.realized_pnl += pnl;

        let remaining = delta.abs() - closing_qty;
        pos.quantity += delta;

        if pos.quantity.abs() < 1e-10 {
            pos.quantity = 0.0;
            pos.avg_entry_price = 0.0;
        } else if remaining > 0.0 {
            pos.avg_entry_price = price;
        }
    }
}
