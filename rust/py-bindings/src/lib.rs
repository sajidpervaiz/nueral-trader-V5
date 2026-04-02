use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyBytes};
use std::collections::HashMap;
use std::sync::Arc;

use tick_parser::{TickParser as RustTickParser, MultiSymbolParser, RawTick, Candle, RingBuffer};
use order_matcher::{Order, OrderBook as RustOrderBook, Side as OMSide, TimeInForce, MatchResult};
use risk_engine::{RiskEngine, RiskLimits, Side as RiskSide};

#[pyclass]
struct TickParser {
    inner: HashMap<String, RustTickParser>,
}

#[pymethods]
impl TickParser {
    #[new]
    fn new() -> Self {
        Self {
            inner: HashMap::new(),
        }
    }

    fn add_tick(
        &mut self,
        py: Python<'_>,
        exchange: String,
        symbol: String,
        timeframe_seconds: i64,
        timestamp_ns: i64,
        price: f64,
        volume: f64,
    ) -> PyResult<Option<PyObject>> {
        let key = format!("{}:{}:{}", exchange, symbol, timeframe_seconds);
        let parser = self.inner
            .entry(key)
            .or_insert_with(|| RustTickParser::new(timeframe_seconds, 10000, 1000));

        match parser.add_tick(&exchange, &symbol, timestamp_ns, price, volume) {
            Some(candle) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("exchange", &candle.exchange)?;
                dict.set_item("symbol", &candle.symbol)?;
                dict.set_item("timeframe", &candle.timeframe)?;
                dict.set_item("start_time_ns", candle.start_time_ns)?;
                dict.set_item("end_time_ns", candle.end_time_ns)?;
                dict.set_item("open", candle.open)?;
                dict.set_item("high", candle.high)?;
                dict.set_item("low", candle.low)?;
                dict.set_item("close", candle.close)?;
                dict.set_item("volume", candle.volume)?;
                dict.set_item("num_trades", candle.num_trades)?;
                dict.set_item("vwap", candle.vwap)?;
                Ok(Some(dict.into()))
            }
            None => Ok(None),
        }
    }

    #[staticmethod]
    fn parse_batch(py: Python<'_>, exchange: String, raw_json: String) -> PyResult<PyObject> {
        let mut parser = RustTickParser::new(60, 10000, 1000);
        let ticks = parser.parse_batch(&exchange, &raw_json);
        let list = PyList::empty_bound(py);
        for tick in ticks {
            let dict = PyDict::new_bound(py);
            dict.set_item("exchange", &tick.exchange)?;
            dict.set_item("symbol", &tick.symbol)?;
            dict.set_item("timestamp_ns", tick.timestamp_ns)?;
            dict.set_item("price", tick.price)?;
            dict.set_item("volume", tick.volume)?;
            dict.set_item("side", &tick.side)?;
            dict.set_item("trade_id", &tick.trade_id)?;
            list.append(dict)?;
        }
        Ok(list.into())
    }

    fn parse_batch_zero_copy(&self, py: Python<'_>, exchange: String, json_str: String) -> PyResult<PyObject> {
        let parser = RustTickParser::new(60, 10000, 1000);
        let ticks = parser.parse_batch_zero_copy(&json_str);
        let list = PyList::empty_bound(py);
        for tick in ticks {
            let dict = PyDict::new_bound(py);
            dict.set_item("exchange", exchange.as_str())?;
            dict.set_item("symbol", &tick.symbol)?;
            dict.set_item("timestamp_ns", tick.timestamp_ns)?;
            dict.set_item("price", tick.price)?;
            dict.set_item("volume", tick.volume)?;
            list.append(dict)?;
        }
        Ok(list.into())
    }

    fn flush_buffer(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let mut candles = Vec::new();
        for parser in self.inner.values_mut() {
            candles.extend(parser.flush_buffer());
        }

        let list = PyList::empty_bound(py);
        for candle in candles {
            let dict = PyDict::new_bound(py);
            dict.set_item("exchange", &candle.exchange)?;
            dict.set_item("symbol", &candle.symbol)?;
            dict.set_item("timeframe", &candle.timeframe)?;
            dict.set_item("open", candle.open)?;
            dict.set_item("high", candle.high)?;
            dict.set_item("low", candle.low)?;
            dict.set_item("close", candle.close)?;
            dict.set_item("volume", candle.volume)?;
            dict.set_item("num_trades", candle.num_trades)?;
            dict.set_item("vwap", candle.vwap)?;
            list.append(dict)?;
        }
        Ok(list.into())
    }
}

#[pyclass]
struct OrderBook {
    inner: RustOrderBook,
}

#[pymethods]
impl OrderBook {
    #[new]
    #[pyo3(signature = (max_depth = 500))]
    fn new(max_depth: usize) -> Self {
        Self {
            inner: RustOrderBook::new(max_depth),
        }
    }

    fn add_limit_order(
        &mut self,
        py: Python<'_>,
        order_id: u64,
        side: &str,
        price: f64,
        quantity: f64,
        timestamp: i64,
    ) -> PyResult<PyObject> {
        let order_side = if side == "buy" { OMSide::Buy } else { OMSide::Sell };
        let order = Order::new_limit(order_id, order_side, price, quantity, timestamp, TimeInForce::GTC);
        let result = self.inner.add_order(order);
        fills_to_pylist(py, &result)
    }

    fn add_market_order(
        &mut self,
        py: Python<'_>,
        order_id: u64,
        side: &str,
        quantity: f64,
        timestamp: i64,
    ) -> PyResult<PyObject> {
        let order_side = if side == "buy" { OMSide::Buy } else { OMSide::Sell };
        let order = Order::new_market(order_id, order_side, quantity, timestamp);
        let result = self.inner.add_order(order);
        fills_to_pylist(py, &result)
    }

    fn cancel_order(&mut self, order_id: u64, side: &str) -> bool {
        let order_side = if side == "buy" { OMSide::Buy } else { OMSide::Sell };
        self.inner.cancel_order(order_id, order_side)
    }

    fn best_bid(&self) -> Option<f64> {
        self.inner.best_bid()
    }

    fn best_ask(&self) -> Option<f64> {
        self.inner.best_ask()
    }

    fn mid_price(&self) -> Option<f64> {
        self.inner.mid_price()
    }

    fn spread(&self) -> Option<f64> {
        self.inner.spread()
    }

    fn get_depth(&self, py: Python<'_>, depth: usize) -> PyResult<(PyObject, PyObject)> {
        let mut bids = PyList::empty_bound(py);
        let mut asks = PyList::empty_bound(py);
        for i in 0..depth {
            if let Some(bid) = self.inner.best_bid() {
                bids.append((bid - (i as f64 * 0.1), 10.0))?;
            }
            if let Some(ask) = self.inner.best_ask() {
                asks.append((ask + (i as f64 * 0.1), 10.0))?;
            }
        }
        Ok((bids.into(), asks.into()))
    }
}

fn fills_to_pylist(py: Python<'_>, result: &MatchResult) -> PyResult<PyObject> {
    let list = PyList::empty_bound(py);
    for fill in &result.fills {
        let dict = PyDict::new_bound(py);
        dict.set_item("maker_id", fill.maker_id)?;
        dict.set_item("taker_id", fill.taker_id)?;
        dict.set_item("price", fill.price)?;
        dict.set_item("quantity", fill.quantity)?;
        dict.set_item("side", if fill.side == OMSide::Buy { "buy" } else { "sell" })?;
        dict.set_item("timestamp", fill.timestamp)?;
        dict.set_item("trade_id", fill.trade_id)?;
        list.append(dict)?;
    }
    let result_dict = PyDict::new_bound(py);
    result_dict.set_item("fills", list)?;
    result_dict.set_item("remaining", result.remaining)?;
    result_dict.set_item("filled_qty", result.filled_qty)?;
    result_dict.set_item("avg_fill_price", result.avg_fill_price)?;
    Ok(result_dict.into())
}

#[pyclass]
struct RiskManager {
    inner: RiskEngine,
}

#[pymethods]
impl RiskManager {
    #[new]
    #[pyo3(signature = (max_position_value=1_000_000.0, account_balance=1_000_000.0))]
    fn new(max_position_value: f64, account_balance: f64) -> Self {
        let limits = RiskLimits {
            max_position_value,
            max_order_size: max_position_value * 0.1,
            max_orders_per_sec: 100,
            max_concentration: 0.3,
            leverage_limit: 10.0,
            stop_loss_pct: 0.05,
        };
        Self {
            inner: RiskEngine::new(limits, account_balance),
        }
    }

    fn pre_trade_check(
        &self,
        py: Python<'_>,
        symbol: String,
        side: String,
        quantity: f64,
        price: f64,
    ) -> PyResult<PyObject> {
        let order_side = if side == "buy" { RiskSide::Buy } else { RiskSide::Sell };

        match self.inner.pre_trade_check(&symbol, order_side, quantity, price) {
            Ok(()) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("passed", true)?;
                dict.set_item("reason", "")?;
                Ok(dict.into())
            }
            Err(e) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("passed", false)?;
                dict.set_item("reason", e.to_string())?;
                Ok(dict.into())
            }
        }
    }

    fn update_position(&self, symbol: String, side: String, quantity: f64, price: f64) {
        let order_side = if side == "buy" { RiskSide::Buy } else { RiskSide::Sell };
        self.inner.update_position(&symbol, order_side, quantity, price);
    }

    fn get_position(&self, py: Python<'_>, symbol: String) -> PyResult<Option<PyObject>> {
        match self.inner.get_position(&symbol) {
            Some(pos) => {
                let dict = PyDict::new_bound(py);
                dict.set_item("symbol", &pos.symbol)?;
                dict.set_item("side", if pos.side == RiskSide::Buy { "buy" } else { "sell" })?;
                dict.set_item("quantity", pos.quantity)?;
                dict.set_item("avg_price", pos.avg_price)?;
                dict.set_item("unrealized_pnl", pos.unrealized_pnl)?;
                Ok(Some(dict.into()))
            }
            None => Ok(None),
        }
    }

    fn get_margin_available(&self) -> f64 {
        self.inner.get_margin_available()
    }

    fn get_total_exposure(&self) -> f64 {
        self.inner.get_total_exposure()
    }

    fn reset_order_counts(&self) {
        self.inner.reset_order_counts();
    }

    fn register_order_book(&self, symbol: String) {
        self.inner.register_order_book(symbol);
    }

    fn update_order_book_price(&self, symbol: String, side: String, price: f64, quantity: f64) {
        let order_side = if side == "buy" { RiskSide::Buy } else { RiskSide::Sell };
        self.inner.update_order_book_price(&symbol, order_side, price, quantity);
    }

    fn get_market_price(&self, symbol: String) -> Option<f64> {
        self.inner.get_market_price(&symbol)
    }
}

#[pyclass]
struct FastBacktester;

#[pymethods]
impl FastBacktester {
    #[staticmethod]
    fn run(
        py: Python<'_>,
        prices: Vec<f64>,
        signals: Vec<f64>,
        initial_capital: f64,
        commission_pct: f64,
        slippage_pct: f64,
    ) -> PyResult<PyObject> {
        let result = fast_backtest_inner(
            &prices,
            &signals,
            initial_capital,
            commission_pct,
            slippage_pct,
        );
        let dict = PyDict::new_bound(py);
        dict.set_item("total_return", result.0)?;
        dict.set_item("sharpe_ratio", result.1)?;
        dict.set_item("sortino_ratio", result.2)?;
        dict.set_item("max_drawdown", result.3)?;
        dict.set_item("win_rate", result.4)?;
        dict.set_item("profit_factor", result.5)?;
        dict.set_item("num_trades", result.6)?;
        dict.set_item("avg_pnl", result.7)?;
        Ok(dict.into())
    }
}

fn fast_backtest_inner(
    prices: &[f64],
    signals: &[f64],
    initial_capital: f64,
    commission_pct: f64,
    slippage_pct: f64,
) -> (f64, f64, f64, f64, f64, f64, u64, f64) {
    let mut equity = initial_capital;
    let mut peak_equity = equity;
    let mut max_dd = 0.0f64;
    let mut pnls: Vec<f64> = Vec::new();

    #[derive(Clone)]
    struct Position {
        direction: i8,
        entry_price: f64,
    }

    let mut position: Option<Position> = None;

    for i in 0..prices.len().min(signals.len()) {
        let price = prices[i];
        let sig = signals[i];

        if let Some(ref pos) = position.clone() {
            let should_exit = (pos.direction == 1 && sig < 0.0)
                || (pos.direction == -1 && sig > 0.0);
            if should_exit {
                let exit_price = if pos.direction == 1 {
                    price * (1.0 - slippage_pct)
                } else {
                    price * (1.0 + slippage_pct)
                };
                let pnl_pct = if pos.direction == 1 {
                    (exit_price - pos.entry_price) / pos.entry_price
                } else {
                    (pos.entry_price - exit_price) / pos.entry_price
                } - 2.0 * commission_pct;
                let trade_pnl = equity * 0.02 * pnl_pct;
                equity += trade_pnl;
                peak_equity = peak_equity.max(equity);
                let dd = (peak_equity - equity) / peak_equity;
                max_dd = max_dd.max(dd);
                pnls.push(trade_pnl);
                position = None;
            }
        }

        if position.is_none() && sig.abs() > 0.5 {
            let direction: i8 = if sig > 0.0 { 1 } else { -1 };
            let entry_price = if direction == 1 {
                price * (1.0 + slippage_pct)
            } else {
                price * (1.0 - slippage_pct)
            };
            position = Some(Position { direction, entry_price });
        }
    }

    if pnls.is_empty() {
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0);
    }

    let num_trades = pnls.len() as u64;
    let total_return = (equity - initial_capital) / initial_capital;
    let avg_pnl = pnls.iter().sum::<f64>() / num_trades as f64;

    let wins: Vec<f64> = pnls.iter().copied().filter(|&p| p > 0.0).collect();
    let losses: Vec<f64> = pnls.iter().copied().filter(|&p| p <= 0.0).collect();
    let win_rate = wins.len() as f64 / num_trades as f64;
    let sum_wins: f64 = wins.iter().sum();
    let sum_losses: f64 = losses.iter().map(|l| l.abs()).sum();
    let profit_factor = if sum_losses > 0.0 {
        sum_wins / sum_losses
    } else {
        f64::INFINITY
    };

    let daily_returns: Vec<f64> = pnls.iter().map(|p| p / initial_capital).collect();
    let mean_ret = daily_returns.iter().sum::<f64>() / daily_returns.len() as f64;
    let variance = daily_returns.iter().map(|r| (r - mean_ret).powi(2)).sum::<f64>()
        / daily_returns.len() as f64;
    let std_dev = variance.sqrt();
    let sharpe = if std_dev > 0.0 {
        mean_ret / std_dev * 252f64.sqrt()
    } else {
        0.0
    };

    let downside_var = daily_returns
        .iter()
        .filter(|&&r| r < 0.0)
        .map(|r| r.powi(2))
        .sum::<f64>()
        / daily_returns.len() as f64;
    let downside_std = downside_var.sqrt();
    let sortino = if downside_std > 0.0 {
        mean_ret / downside_std * 252f64.sqrt()
    } else {
        0.0
    };

    (total_return, sharpe, sortino, max_dd, win_rate, profit_factor, num_trades, avg_pnl)
}

#[pymodule]
fn neural_trader_rust(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<TickParser>()?;
    m.add_class::<OrderBook>()?;
    m.add_class::<RiskManager>()?;
    m.add_class::<FastBacktester>()?;
    Ok(())
}
