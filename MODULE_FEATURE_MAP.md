# Module Feature Map

This document maps runtime feature modules to their backend endpoints and current data sources.

## Sentiment

- Endpoint: `GET /api/feargreed`
- Handler: `buildFearGreedPayload` in `services/bridge-api/src/index.ts`
- Source order:
  1. Binance 24h market breadth and average change
  2. Neutral fallback if external market feed unavailable

## News

- Endpoint: `GET /api/news`
- Handler: `buildNewsPayload` in `services/bridge-api/src/index.ts`
- Source order:
  1. CryptoCompare news API
  2. CoinGecko news API fallback
  3. Empty set (`provider=unavailable`) if both sources unavailable

## Backtest Summary

- Endpoint: `GET /api/backtest/summary`
- Source: Rolling last 500 records from ClickHouse `execution_orders`
- Notes:
  - Win rate from completed/fill statuses
  - Drawdown and Sharpe derived from order-level proxy returns

## Logs

- Endpoint: `GET /api/logs/recent`
- Source:
  - Runtime health + uptime
  - Auto mode state
  - Recent config persistence events from `ui_config_state`
  - Recent trade events from `execution_orders`

## Auto Trading

- Endpoints:
  - `GET /api/auto/status`
  - `POST /api/auto/toggle`
- Persistence:
  - ClickHouse table `auto_trading_state`
  - Restored at bridge-api startup

## Signals

- Endpoint: `GET /api/signals/recent`
- Source:
  - ClickHouse `execution_orders`
  - Confidence derived from status + mark/entry edge using live marks

## Unified Data Source Status

- Endpoint: `GET /api/system/data-sources`
- Purpose:
  - Single map of module source + last update timestamps for:
    - sentiment
    - news
    - backtest
    - logs
    - auto
    - signals
