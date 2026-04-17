use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Instant;
use crossbeam_channel::{bounded, Receiver, Sender};
use dashmap::DashMap;
use rayon::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawTick {
    pub exchange: String,
    pub symbol: String,
    pub timestamp_ns: i64,
    pub price: f64,
    pub volume: f64,
    pub side: String,
    pub trade_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candle {
    pub exchange: String,
    pub symbol: String,
    pub timeframe: String,
    pub start_time_ns: i64,
    pub end_time_ns: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub num_trades: u64,
    pub vwap: f64,
}

#[derive(Debug, Clone)]
struct CandleAccumulator {
    start_time_ns: i64,
    end_time_ns: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    num_trades: u64,
    vwap_accum: f64,
    volume_accum: f64,
}

impl CandleAccumulator {
    fn new(start_time_ns: i64, duration_ns: i64, price: f64, volume: f64) -> Self {
        Self {
            start_time_ns,
            end_time_ns: start_time_ns + duration_ns,
            open: price,
            high: price,
            low: price,
            close: price,
            volume,
            num_trades: 1,
            vwap_accum: price * volume,
            volume_accum: volume,
        }
    }

    fn update(&mut self, price: f64, volume: f64) {
        self.high = self.high.max(price);
        self.low = self.low.min(price);
        self.close = price;
        self.volume += volume;
        self.num_trades += 1;
        self.vwap_accum += price * volume;
        self.volume_accum += volume;
    }

    fn finalize(&self, timeframe_name: String) -> Candle {
        Candle {
            exchange: String::new(),
            symbol: String::new(),
            timeframe: timeframe_name,
            start_time_ns: self.start_time_ns,
            end_time_ns: self.end_time_ns,
            open: self.open,
            high: self.high,
            low: self.low,
            close: self.close,
            volume: self.volume,
            num_trades: self.num_trades,
            vwap: if self.volume_accum > 0.0 {
                self.vwap_accum / self.volume_accum
            } else {
                0.0
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct TickBatch {
    pub ticks: Vec<RawTick>,
    pub timestamp: i64,
    pub size: usize,
}

pub struct RingBuffer {
    buffer: Vec<Option<RawTick>>,
    head: usize,
    tail: usize,
    len: usize,
    capacity: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            buffer: vec![None; capacity],
            head: 0,
            tail: 0,
            len: 0,
            capacity,
        }
    }

    pub fn push(&mut self, tick: RawTick) -> bool {
        if self.len >= self.capacity {
            return false;
        }

        self.buffer[self.head] = Some(tick);
        self.head = (self.head + 1) % self.capacity;
        self.len += 1;
        true
    }

    pub fn drain(&mut self) -> Vec<RawTick> {
        if self.len == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(self.len);
        while self.len > 0 {
            if let Some(tick) = self.buffer[self.tail].take() {
                result.push(tick);
            }
            self.tail = (self.tail + 1) % self.capacity;
            self.len -= 1;
        }
        result
    }
}

pub struct TickParser {
    timeframe_ns: i64,
    accumulators: DashMap<String, CandleAccumulator>,
    ring_buffer: RingBuffer,
    _batch_sender: Sender<TickBatch>,
    batch_receiver: Receiver<TickBatch>,
    _batch_size: usize,
}

impl TickParser {
    pub fn new(timeframe_seconds: i64, ring_capacity: usize, batch_size: usize) -> Self {
        let (batch_sender, batch_receiver) = bounded(1000);
        Self {
            timeframe_ns: timeframe_seconds * 1_000_000_000,
            accumulators: DashMap::new(),
            ring_buffer: RingBuffer::new(ring_capacity),
            _batch_sender: batch_sender,
            batch_receiver,
            _batch_size: batch_size,
        }
    }

    pub fn with_timeframe_ns(timeframe_ns: i64) -> Self {
        let normalized_timeframe_ns = if timeframe_ns <= 0 {
            1_000_000_000
        } else if timeframe_ns < 1_000_000_000 {
            timeframe_ns * 1_000_000_000
        } else {
            timeframe_ns
        };

        let mut parser = Self::new(1, 10000, 1000);
        parser.timeframe_ns = normalized_timeframe_ns;
        parser
    }

    pub fn add_tick(
        &mut self,
        exchange: &str,
        symbol: &str,
        timestamp_ns: i64,
        price: f64,
        volume: f64,
    ) -> Option<Candle> {
        let key = format!("{}:{}", exchange, symbol);

        match self.accumulators.get_mut(&key) {
            Some(mut acc) if timestamp_ns < acc.end_time_ns => {
                acc.update(price, volume);
                None
            }
            Some(mut acc) => {
                let timeframe_name = format!("{}s", self.timeframe_ns / 1_000_000_000);
                let mut completed = acc.finalize(timeframe_name);
                completed.exchange = exchange.to_string();
                completed.symbol = symbol.to_string();

                let elapsed = timestamp_ns.saturating_sub(acc.start_time_ns);
                let step_count = (elapsed / self.timeframe_ns).max(1);
                let new_start_ns = acc.start_time_ns + (step_count * self.timeframe_ns);
                *acc = CandleAccumulator::new(new_start_ns, self.timeframe_ns, price, volume);
                Some(completed)
            }
            None => {
                self.accumulators.insert(
                    key,
                    CandleAccumulator::new(timestamp_ns, self.timeframe_ns, price, volume),
                );
                None
            }
        }
    }

    pub fn add_tick_to_buffer(&mut self, tick: RawTick) -> bool {
        self.ring_buffer.push(tick)
    }

    pub fn flush_buffer(&mut self) -> Vec<Candle> {
        let ticks = self.ring_buffer.drain();
        let mut completed_candles = Vec::new();

        for tick in ticks {
            if let Some(candle) = self.add_tick(&tick.exchange, &tick.symbol, tick.timestamp_ns, tick.price, tick.volume) {
                completed_candles.push(candle);
            }
        }

        completed_candles
    }

    pub fn parse_batch(&mut self, exchange: &str, raw_json: &str) -> Vec<RawTick> {
        let start = Instant::now();
        let values: Vec<serde_json::Value> = match serde_json::from_str(raw_json) {
            Ok(v) => v,
            Err(_) => return Vec::new(),
        };

        let ticks: Vec<RawTick> = values
            .par_iter()
            .filter_map(|v| {
                let price = v["price"].as_f64()?;
                let symbol = v["symbol"].as_str()?.to_string();
                let timestamp_us = v["timestamp_us"].as_i64().unwrap_or(0);
                let timestamp_ns = timestamp_us * 1000;
                let volume = v["volume"].as_f64().unwrap_or(0.0);
                Some(RawTick {
                    exchange: exchange.to_string(),
                    symbol,
                    timestamp_ns,
                    price,
                    volume,
                    side: v["side"].as_str().unwrap_or("").to_string(),
                    trade_id: v["trade_id"].as_str().unwrap_or("").to_string(),
                })
            })
            .collect();

        log::debug!("Parsed {} ticks in {:?}", ticks.len(), start.elapsed());
        ticks
    }

    pub fn parse_batch_zero_copy(&self, json_str: &str) -> Vec<RawTick> {
        let mut ticks = Vec::new();
        let mut pos = 0;

        while pos < json_str.len() {
            if let Some(start) = json_str[pos..].find('{') {
                pos += start;
                if let Some(end) = find_matching_brace(&json_str[pos..]) {
                    let json_obj = &json_str[pos..pos + end];
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(json_obj) {
                        if let Some(tick) = parse_tick_from_value(&v, "exchange") {
                            ticks.push(tick);
                        }
                    }
                    pos += end;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        ticks
    }

    pub fn get_batch_receiver(&self) -> &Receiver<TickBatch> {
        &self.batch_receiver
    }

    pub fn start_batch_processor(&self) {
        let receiver = self.batch_receiver.clone();
        std::thread::spawn(move || {
            for batch in receiver {
                log::debug!("Processing batch of {} ticks", batch.size);
            }
        });
    }
}

fn find_matching_brace(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i + 1);
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_tick_from_value(v: &serde_json::Value, exchange: &str) -> Option<RawTick> {
    let price = v["price"].as_f64()?;
    let symbol = v["symbol"].as_str()?.to_string();
    let timestamp_us = v["timestamp_us"].as_i64().unwrap_or(0);
    let timestamp_ns = timestamp_us * 1000;
    let volume = v["volume"].as_f64().unwrap_or(0.0);

    Some(RawTick {
        exchange: exchange.to_string(),
        symbol,
        timestamp_ns,
        price,
        volume,
        side: v["side"].as_str().unwrap_or("").to_string(),
        trade_id: v["trade_id"].as_str().unwrap_or("").to_string(),
    })
}

pub struct MultiSymbolParser {
    parsers: HashMap<String, TickParser>,
    default_timeframe: i64,
}

impl MultiSymbolParser {
    pub fn new(default_timeframe: i64) -> Self {
        Self {
            parsers: HashMap::new(),
            default_timeframe,
        }
    }

    pub fn add_symbol(&mut self, symbol: String) {
        if !self.parsers.contains_key(&symbol) {
            self.parsers.insert(
                symbol.clone(),
                TickParser::new(self.default_timeframe, 10000, 1000),
            );
        }
    }

    pub fn parse_tick_for_symbol(
        &mut self,
        symbol: &str,
        exchange: &str,
        timestamp_ns: i64,
        price: f64,
        volume: f64,
    ) -> Option<Candle> {
        if let Some(parser) = self.parsers.get_mut(symbol) {
            parser.add_tick(exchange, symbol, timestamp_ns, price, volume)
        } else {
            None
        }
    }

    pub fn get_all_candles(&mut self) -> Vec<Candle> {
        let mut candles = Vec::new();
        for parser in self.parsers.values_mut() {
            let completed = parser.flush_buffer();
            candles.extend(completed);
        }
        candles
    }
}

pub struct TickNormalizer {
    exchange_offsets: HashMap<String, i64>,
}

impl TickNormalizer {
    pub fn new() -> Self {
        Self {
            exchange_offsets: HashMap::new(),
        }
    }

    pub fn set_exchange_offset(&mut self, exchange: String, offset_us: i64) {
        self.exchange_offsets.insert(exchange, offset_us * 1_000);
    }

    pub fn normalize_timestamp(&self, exchange: &str, timestamp_ns: i64) -> i64 {
        let offset = *self.exchange_offsets.get(exchange).unwrap_or(&0);
        timestamp_ns + offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_candle_aggregation() {
        let mut parser = TickParser::with_timeframe_ns(60);
        let result = parser.add_tick("binance", "BTC/USDT", 1_000_000_000_000, 50000.0, 1.0);
        assert!(result.is_none());

        let result2 = parser.add_tick("binance", "BTC/USDT", 1_000_000_000_000 + 30_000_000_000, 50100.0, 0.5);
        assert!(result2.is_none());

        let result3 = parser.add_tick("binance", "BTC/USDT", 1_000_060_000_000_000, 50200.0, 0.3);
        assert!(result3.is_some());

        let candle = result3.unwrap();
        assert_eq!(candle.open, 50000.0);
        assert_eq!(candle.high, 50100.0);
        assert_eq!(candle.low, 50000.0);
        assert_eq!(candle.close, 50100.0);
        assert_eq!(candle.num_trades, 2);
    }

    #[test]
    fn test_parse_batch_valid() {
        let mut parser = TickParser::with_timeframe_ns(60);
        let json = r#"[{"symbol":"BTC/USDT","price":50000.0,"volume":1.0,"timestamp_us":1000000,"side":"buy","trade_id":"1"}]"#;
        let ticks = parser.parse_batch("binance", json);
        assert_eq!(ticks.len(), 1);
        assert_eq!(ticks[0].price, 50000.0);
        assert_eq!(ticks[0].timestamp_ns, 1000000000);
    }

    #[test]
    fn test_ring_buffer() {
        let mut buffer = RingBuffer::new(10);
        let tick = RawTick {
            exchange: "test".to_string(),
            symbol: "BTC/USDT".to_string(),
            timestamp_ns: 1000,
            price: 50000.0,
            volume: 1.0,
            side: "buy".to_string(),
            trade_id: "1".to_string(),
        };

        for _ in 0..10 {
            assert!(buffer.push(tick.clone()));
        }

        assert!(!buffer.push(tick));

        let drained = buffer.drain();
        assert_eq!(drained.len(), 10);
    }

    #[test]
    fn test_tick_normalizer() {
        let mut normalizer = TickNormalizer::new();
        normalizer.set_exchange_offset("binance".to_string(), 100);

        let normalized = normalizer.normalize_timestamp("binance", 1_000_000_000_000);
        assert_eq!(normalized, 1_000_000_100_000);
    }

    #[test]
    fn test_vwap_calculation() {
        let mut parser = TickParser::with_timeframe_ns(60);
        parser.add_tick("binance", "BTC/USDT", 1_000_000_000_000, 50000.0, 1.0);
        parser.add_tick("binance", "BTC/USDT", 1_000_000_000_000 + 10_000_000_000, 50100.0, 2.0);

        let candle = parser.add_tick("binance", "BTC/USDT", 1_000_060_000_000_000, 50200.0, 0.5).unwrap();
        let expected_vwap = (50000.0 * 1.0 + 50100.0 * 2.0) / 3.0;
        assert!((candle.vwap - expected_vwap).abs() < f64::EPSILON);
    }
}
