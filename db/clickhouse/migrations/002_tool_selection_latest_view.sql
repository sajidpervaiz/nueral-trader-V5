CREATE VIEW IF NOT EXISTS neural_trader.tool_selection_latest AS
SELECT
  request_id,
  model,
  selected_tool,
  latency_ms,
  status,
  created_at
FROM neural_trader.tool_selection_events
ORDER BY created_at DESC
LIMIT 1;
