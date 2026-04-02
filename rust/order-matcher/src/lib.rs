use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use serde::{Deserialize, Serialize};
use parking_lot::RwLock;
use smallvec::SmallVec;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Market,
    Limit,
    StopMarket,
    StopLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TimeInForce {
    GTC,
    IOC,
    FOK,
    GTD,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub order_type: OrderType,
    pub tif: TimeInForce,
    pub price: f64,
    pub stop_price: Option<f64>,
    pub quantity: f64,
    pub remaining: f64,
    pub timestamp: i64,
    pub client_order_id: Option<String>,
    pub user_id: Option<u64>,
}

impl Order {
    pub fn new_limit(
        id: u64,
        side: Side,
        price: f64,
        quantity: f64,
        timestamp: i64,
        tif: TimeInForce,
    ) -> Self {
        Self {
            id,
            side,
            order_type: OrderType::Limit,
            tif,
            price,
            stop_price: None,
            quantity,
            remaining: quantity,
            timestamp,
            client_order_id: None,
            user_id: None,
        }
    }

    pub fn new_market(id: u64, side: Side, quantity: f64, timestamp: i64) -> Self {
        Self {
            id,
            side,
            order_type: OrderType::Market,
            tif: TimeInForce::IOC,
            price: if side == Side::Buy { f64::MAX } else { 0.0 },
            stop_price: None,
            quantity,
            remaining: quantity,
            timestamp,
            client_order_id: None,
            user_id: None,
        }
    }

    pub fn with_client_id(mut self, client_id: String) -> Self {
        self.client_order_id = Some(client_id);
        self
    }

    pub fn with_user(mut self, user_id: u64) -> Self {
        self.user_id = Some(user_id);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fill {
    pub maker_id: u64,
    pub taker_id: u64,
    pub price: f64,
    pub quantity: f64,
    pub side: Side,
    pub timestamp: i64,
    pub maker_user_id: Option<u64>,
    pub taker_user_id: Option<u64>,
    pub trade_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchResult {
    pub fills: Vec<Fill>,
    pub remaining: f64,
    pub filled_qty: f64,
    pub avg_fill_price: f64,
}

#[derive(Debug, Error)]
pub enum OrderError {
    #[error("Order not found: {0}")]
    OrderNotFound(u64),
    #[error("Invalid order parameters")]
    InvalidParameters,
    #[error("Self-trade prevention triggered")]
    SelfTradePrevention,
    #[error("Insufficient liquidity")]
    InsufficientLiquidity,
}

pub type Result<T> = std::result::Result<T, OrderError>;

#[derive(Debug, Clone)]
struct PriceLevel {
    orders: Vec<Order>,
}

impl PriceLevel {
    fn new(_price: u64) -> Self {
        Self {
            orders: Vec::with_capacity(16),
        }
    }

    fn total_quantity(&self) -> f64 {
        self.orders.iter().map(|o| o.remaining).sum()
    }

}

#[derive(Debug)]
struct ShardedOrderBook {
    bids: BTreeMap<u64, PriceLevel>,
    asks: BTreeMap<u64, PriceLevel>,
    max_depth: usize,
    trade_counter: AtomicU64,
    self_trade_prevention: bool,
}

impl ShardedOrderBook {
    fn new(_shard_id: usize, max_depth: usize) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            max_depth,
            trade_counter: AtomicU64::new(0),
            self_trade_prevention: true,
        }
    }

    fn best_bid(&self) -> Option<f64> {
        self.bids.iter().next_back().map(|(k, _)| *k as f64 / 1_000_000.0)
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.iter().next().map(|(k, _)| *k as f64 / 1_000_000.0)
    }

    fn spread(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) if b < a => Some(a - b),
            _ => None,
        }
    }

    fn mid_price(&self) -> Option<f64> {
        match (self.best_bid(), self.best_ask()) {
            (Some(b), Some(a)) if b < a => Some((b + a) / 2.0),
            _ => None,
        }
    }

    fn add_order(&mut self, order: Order) -> MatchResult {
        match order.order_type {
            OrderType::Market => self.match_market(order),
            OrderType::Limit => self.match_limit(order),
            OrderType::StopMarket => self.handle_stop_market(order),
            OrderType::StopLimit => self.handle_stop_limit(order),
        }
    }

    fn match_market(&mut self, mut taker: Order) -> MatchResult {
        let mut fills: SmallVec<[Fill; 16]> = SmallVec::new();
        let mut filled_qty = 0.0;
        let mut fill_value = 0.0;

        let book = if taker.side == Side::Buy { &mut self.asks } else { &mut self.bids };
        let keys: Vec<u64> = if taker.side == Side::Buy {
            book.keys().copied().collect()
        } else {
            book.keys().rev().copied().collect()
        };

        for key in keys {
            if taker.remaining <= f64::EPSILON {
                break;
            }

            if let Some(level) = book.get_mut(&key) {
                let fill_price = key as f64 / 1_000_000.0;

                if self.self_trade_prevention {
                    if let Some(user_id) = taker.user_id {
                        if level.orders.iter().any(|o| o.user_id == Some(user_id)) {
                            continue;
                        }
                    }
                }

                let mut to_remove = false;
                for maker in level.orders.iter_mut() {
                    if taker.remaining <= f64::EPSILON {
                        break;
                    }

                    if self.self_trade_prevention && maker.user_id == taker.user_id {
                        continue;
                    }

                    let fill_qty = taker.remaining.min(maker.remaining);
                    maker.remaining -= fill_qty;
                    taker.remaining -= fill_qty;

                    fills.push(Fill {
                        maker_id: maker.id,
                        taker_id: taker.id,
                        price: fill_price,
                        quantity: fill_qty,
                        side: taker.side,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as i64,
                        maker_user_id: maker.user_id,
                        taker_user_id: taker.user_id,
                        trade_id: self.trade_counter.fetch_add(1, Ordering::Relaxed),
                    });

                    filled_qty += fill_qty;
                    fill_value += fill_qty * fill_price;
                }

                level.orders.retain(|o| o.remaining > f64::EPSILON);
                if level.orders.is_empty() {
                    to_remove = true;
                }

                if to_remove {
                    book.remove(&key);
                }
            }
        }

        let avg_price = if filled_qty > 0.0 { fill_value / filled_qty } else { 0.0 };

        MatchResult {
            fills: fills.into_vec(),
            remaining: taker.remaining,
            filled_qty,
            avg_fill_price: avg_price,
        }
    }

    fn match_limit(&mut self, mut taker: Order) -> MatchResult {
        let mut fills: SmallVec<[Fill; 16]> = SmallVec::new();
        let mut filled_qty = 0.0;
        let mut fill_value = 0.0;
        let max_price = (taker.price * 1_000_000.0) as u64;
        let min_price = max_price;

        let book: &mut BTreeMap<u64, PriceLevel> = if taker.side == Side::Buy {
            &mut self.asks
        } else {
            &mut self.bids
        };

        let keys: Vec<u64> = if taker.side == Side::Buy {
            book.keys().take_while(|&k| *k <= max_price).copied().collect()
        } else {
            book.keys().rev().take_while(|&k| *k >= min_price).copied().collect()
        };

        for key in keys {
            if taker.remaining <= f64::EPSILON {
                break;
            }

            if let Some(level) = book.get_mut(&key) {
                let fill_price = key as f64 / 1_000_000.0;

                if self.self_trade_prevention && level.orders.iter().any(|o| o.user_id == taker.user_id) {
                    continue;
                }

                let mut to_remove = false;
                for maker in level.orders.iter_mut() {
                    if taker.remaining <= f64::EPSILON {
                        break;
                    }

                    if self.self_trade_prevention && maker.user_id == taker.user_id {
                        continue;
                    }

                    let fill_qty = taker.remaining.min(maker.remaining);
                    maker.remaining -= fill_qty;
                    taker.remaining -= fill_qty;

                    fills.push(Fill {
                        maker_id: maker.id,
                        taker_id: taker.id,
                        price: fill_price,
                        quantity: fill_qty,
                        side: taker.side,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as i64,
                        maker_user_id: maker.user_id,
                        taker_user_id: taker.user_id,
                        trade_id: self.trade_counter.fetch_add(1, Ordering::Relaxed),
                    });

                    filled_qty += fill_qty;
                    fill_value += fill_qty * fill_price;
                }

                level.orders.retain(|o| o.remaining > f64::EPSILON);
                if level.orders.is_empty() {
                    to_remove = true;
                }

                if to_remove {
                    book.remove(&key);
                }
            }
        }

        if taker.remaining > f64::EPSILON && taker.tif == TimeInForce::GTC {
            let resting = if taker.side == Side::Buy { &mut self.bids } else { &mut self.asks };
            let key = max_price;

            if let Some(level) = resting.get_mut(&key) {
                if level.orders.len() < self.max_depth {
                    level.orders.push(taker.clone());
                }
            } else {
                let mut level = PriceLevel::new(key);
                level.orders.push(taker.clone());
                resting.insert(key, level);
            }
        } else if taker.tif == TimeInForce::FOK && taker.remaining > f64::EPSILON {
            fills.clear();
            filled_qty = 0.0;
            taker.remaining = taker.quantity;
        }

        let avg_price = if filled_qty > 0.0 { fill_value / filled_qty } else { 0.0 };

        MatchResult {
            fills: fills.into_vec(),
            remaining: taker.remaining,
            filled_qty,
            avg_fill_price: avg_price,
        }
    }

    fn handle_stop_market(&mut self, order: Order) -> MatchResult {
        let stop_price = order.stop_price.unwrap_or(order.price);
        let triggered = if order.side == Side::Buy {
            self.best_ask().map_or(false, |ask| ask >= stop_price)
        } else {
            self.best_bid().map_or(false, |bid| bid <= stop_price)
        };

        if triggered {
            self.match_market(order)
        } else {
            MatchResult {
                fills: Vec::new(),
                remaining: order.quantity,
                filled_qty: 0.0,
                avg_fill_price: 0.0,
            }
        }
    }

    fn handle_stop_limit(&mut self, order: Order) -> MatchResult {
        let stop_price = order.stop_price.unwrap_or(order.price);
        let triggered = if order.side == Side::Buy {
            self.best_ask().map_or(false, |ask| ask >= stop_price)
        } else {
            self.best_bid().map_or(false, |bid| bid <= stop_price)
        };

        if triggered {
            self.match_limit(order)
        } else {
            MatchResult {
                fills: Vec::new(),
                remaining: order.quantity,
                filled_qty: 0.0,
                avg_fill_price: 0.0,
            }
        }
    }

    fn cancel_order(&mut self, order_id: u64, side: Side) -> bool {
        let book = if side == Side::Buy { &mut self.bids } else { &mut self.asks };

        let mut remove_key: Option<u64> = None;
        for (key, level) in book.iter_mut() {
            if let Some(pos) = level.orders.iter().position(|o| o.id == order_id) {
                level.orders.remove(pos);
                if level.orders.is_empty() {
                    remove_key = Some(*key);
                }
                break;
            }
        }
        if let Some(k) = remove_key {
            book.remove(&k);
            return true;
        }
        false
    }

    fn get_depth(&self, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        let mut bids = Vec::with_capacity(depth);
        let mut asks = Vec::with_capacity(depth);

        for (key, level) in self.bids.iter().rev().take(depth) {
            bids.push((*key as f64 / 1_000_000.0, level.total_quantity()));
        }

        for (key, level) in self.asks.iter().take(depth) {
            asks.push((*key as f64 / 1_000_000.0, level.total_quantity()));
        }

        (bids, asks)
    }
}

#[derive(Debug)]
pub struct OrderBookManager {
    shards: Vec<RwLock<ShardedOrderBook>>,
    num_shards: usize,
    self_trade_prevention: bool,
}

impl OrderBookManager {
    pub fn new(num_shards: usize, max_depth: usize) -> Self {
        let shards = (0..num_shards)
            .map(|shard_id| RwLock::new(ShardedOrderBook::new(shard_id, max_depth)))
            .collect();

        Self {
            shards,
            num_shards,
            self_trade_prevention: true,
        }
    }

    fn shard_for(&self, symbol: &str) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        symbol.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    pub fn best_bid(&self, _symbol: &str) -> Option<f64> {
        self.shards[0].read().best_bid()
    }

    pub fn best_ask(&self, _symbol: &str) -> Option<f64> {
        self.shards[0].read().best_ask()
    }

    pub fn mid_price(&self, symbol: &str) -> Option<f64> {
        self.shards[self.shard_for(symbol)].read().mid_price()
    }

    pub fn spread(&self, symbol: &str) -> Option<f64> {
        self.shards[self.shard_for(symbol)].read().spread()
    }

    pub fn add_order(&self, order: Order, symbol: &str) -> MatchResult {
        self.shards[self.shard_for(symbol)].write().add_order(order)
    }

    pub fn cancel_order(&self, order_id: u64, side: Side, symbol: &str) -> bool {
        self.shards[self.shard_for(symbol)].write().cancel_order(order_id, side)
    }

    pub fn get_depth(&self, symbol: &str, depth: usize) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
        self.shards[self.shard_for(symbol)].read().get_depth(depth)
    }

    pub fn set_self_trade_prevention(&mut self, enabled: bool) {
        self.self_trade_prevention = enabled;
        for shard in &self.shards {
            shard.write().self_trade_prevention = enabled;
        }
    }
}

#[derive(Debug)]
pub struct OrderBook {
    inner: OrderBookManager,
}

impl OrderBook {
    pub fn new(max_depth: usize) -> Self {
        Self {
            inner: OrderBookManager::new(1, max_depth),
        }
    }

    pub fn best_bid(&self) -> Option<f64> {
        self.inner.best_bid("")
    }

    pub fn best_ask(&self) -> Option<f64> {
        self.inner.best_ask("")
    }

    pub fn mid_price(&self) -> Option<f64> {
        self.inner.mid_price("")
    }

    pub fn spread(&self) -> Option<f64> {
        self.inner.spread("")
    }

    pub fn add_order(&mut self, order: Order) -> MatchResult {
        self.inner.add_order(order, "")
    }

    pub fn cancel_order(&mut self, order_id: u64, side: Side) -> bool {
        self.inner.cancel_order(order_id, side, "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_limit_order_match() {
        let mut book = OrderBook::new(100);
        let ask = Order::new_limit(1, Side::Sell, 50000.0, 1.0, 0, TimeInForce::GTC);
        book.add_order(ask);

        let bid = Order::new_limit(2, Side::Buy, 50000.0, 1.0, 1, TimeInForce::GTC);
        let result = book.add_order(bid);

        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].quantity, 1.0);
        assert_eq!(result.fills[0].price, 50000.0);
        assert_eq!(result.remaining, 0.0);
    }

    #[test]
    fn test_market_order() {
        let mut book = OrderBook::new(100);
        book.add_order(Order::new_limit(1, Side::Sell, 50100.0, 2.0, 0, TimeInForce::GTC));
        book.add_order(Order::new_limit(2, Side::Sell, 50200.0, 1.0, 0, TimeInForce::GTC));

        let market = Order::new_market(3, Side::Buy, 1.5, 2);
        let result = book.add_order(market);

        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.fills[0].price, 50100.0);
        assert_eq!(result.fills[0].quantity, 1.5);
        assert_eq!(result.remaining, 0.0);
    }

    #[test]
    fn test_cancel_order() {
        let mut book = OrderBook::new(100);
        book.add_order(Order::new_limit(1, Side::Buy, 49000.0, 1.0, 0, TimeInForce::GTC));
        assert!(book.cancel_order(1, Side::Buy));
        assert!(book.best_bid().is_none());
    }

    #[test]
    fn test_spread() {
        let mut book = OrderBook::new(100);
        book.add_order(Order::new_limit(1, Side::Buy, 49900.0, 1.0, 0, TimeInForce::GTC));
        book.add_order(Order::new_limit(2, Side::Sell, 50100.0, 1.0, 0, TimeInForce::GTC));
        assert_eq!(book.spread().unwrap(), 200.0);
    }

    #[test]
    fn test_self_trade_prevention() {
        let mut book = OrderBook::new(100);
        let ask = Order::new_limit(1, Side::Sell, 50000.0, 1.0, 0, TimeInForce::GTC)
            .with_user(100);
        book.add_order(ask);

        let bid = Order::new_limit(2, Side::Buy, 50000.0, 1.0, 1, TimeInForce::GTC)
            .with_user(100);
        let result = book.add_order(bid);

        assert_eq!(result.fills.len(), 0);
        assert_eq!(result.remaining, 1.0);
    }

    #[test]
    fn test_ioc_order() {
        let mut book = OrderBook::new(100);
        book.add_order(Order::new_limit(1, Side::Sell, 50000.0, 0.5, 0, TimeInForce::GTC));

        let ioc = Order::new_limit(2, Side::Buy, 49900.0, 1.0, 1, TimeInForce::IOC);
        let result = book.add_order(ioc);

        assert_eq!(result.fills.len(), 1);
        assert_eq!(result.filled_qty, 0.5);
        assert_eq!(result.remaining, 0.5);
    }
}
