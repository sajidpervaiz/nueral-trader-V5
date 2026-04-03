CREATE DATABASE IF NOT EXISTS neural_trader;

CREATE TABLE IF NOT EXISTS neural_trader.tool_selection_events (
  event_id UUID,
  request_id String,
  model String,
  selected_tool String,
  latency_ms UInt32,
  status LowCardinality(String),
  created_at DateTime64(3)
)
ENGINE = MergeTree
ORDER BY (created_at, request_id)
TTL toDateTime(created_at) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192;
