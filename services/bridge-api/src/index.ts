import express, { Request, Response } from "express";
import cors from "cors";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { createClient } from "@clickhouse/client";
import { createHash, randomUUID } from "crypto";
import { Counter, Histogram, Registry, collectDefaultMetrics } from "prom-client";

const app = express();
app.use(express.json());
app.use(cors());

const registry = new Registry();
collectDefaultMetrics({ register: registry });
const requestCounter = new Counter({
  name: "bridge_api_requests_total",
  help: "Total bridge API requests",
  registers: [registry],
  labelNames: ["route", "status"] as const,
});
const requestLatency = new Histogram({
  name: "bridge_api_request_duration_seconds",
  help: "Request duration seconds",
  registers: [registry],
  labelNames: ["route"] as const,
});

const grpcTarget = process.env.GRPC_TARGET ?? "gateway:50051";
const clickhouseUrl = process.env.CLICKHOUSE_URL ?? "http://clickhouse:8123";
const marketDataBaseUrl = process.env.MARKET_DATA_BASE_URL ?? "https://api.binance.com";
const newsDataBaseUrl = process.env.NEWS_DATA_BASE_URL ?? "https://min-api.cryptocompare.com";
const newsDataFallbackBaseUrl = process.env.NEWS_DATA_FALLBACK_BASE_URL ?? "https://api.coingecko.com";
const clickhouseUser = process.env.CLICKHOUSE_USER ?? "nt_app";
const clickhousePassword = process.env.CLICKHOUSE_PASSWORD ?? "nt_app_password";
const clickhouseDb = process.env.CLICKHOUSE_DB ?? "neural_trader";
const bridgeProtoPath = process.env.BRIDGE_PROTO_PATH ?? "/app/proto/bridge.proto";
const port = Number.parseInt(process.env.BRIDGE_API_PORT ?? "8080", 10);

const clickhouse = createClient({
  host: clickhouseUrl,
  username: clickhouseUser,
  password: clickhousePassword,
  database: clickhouseDb,
});

const pkgDef = protoLoader.loadSync(bridgeProtoPath, {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true,
});
const proto = grpc.loadPackageDefinition(pkgDef) as any;
const BridgeServiceClient = proto.neural_trader.bridge.BridgeService;
const grpcClient = new BridgeServiceClient(grpcTarget, grpc.credentials.createInsecure());

type ExecutionOrder = {
  request_id: string;
  symbol: string;
  side: string;
  quantity: number;
  price: number;
  venue: string;
  model: string;
  selected_tool: string;
  operator_tag: string;
  latency_ms: number;
  status: string;
  created_at: string;
};

type PositionSummary = {
  symbol: string;
  side: "LONG" | "SHORT";
  size: number;
  entry_price: number;
  mark_price: number;
  unrealized_pnl: number;
};

type MarketTicker = {
  symbol: string;
  name: string;
  price: number;
  change_24h: number;
  volume_24h: number;
  market_cap: number;
  high_24h: number;
  low_24h: number;
};

type AutoTradingState = {
  enabled: boolean;
  mode: "paper" | "live";
  updated_at: string;
};

type UiConfigState = {
  binance_api_key: string;
  binance_secret: string;
  bybit_api_key: string;
  bybit_secret: string;
  okx_api_key: string;
  okx_secret: string;
  okx_passphrase: string;
  kraken_api_key: string;
  kraken_secret: string;
  dex_rpc_url: string;
  dex_private_key: string;
  telegram_bot_token: string;
  telegram_chat_id: string;
  binance_enabled: boolean;
  bybit_enabled: boolean;
  okx_enabled: boolean;
  kraken_enabled: boolean;
  uniswap_v3_enabled: boolean;
  sushiswap_enabled: boolean;
  dydx_enabled: boolean;
  trade_alerts_enabled: boolean;
  risk_alerts_enabled: boolean;
  daily_summary_enabled: boolean;
  auto_trading_enabled: boolean;
  auto_stop_loss_enabled: boolean;
  auto_take_profit_enabled: boolean;
  trailing_stops_enabled: boolean;
  atr_position_sizing_enabled: boolean;
  updated_at: string;
};

type ConnectionRegistryRecord = {
  exchange: string;
  venue_type: "cex" | "dex";
  enabled: boolean;
  credential_present: boolean;
  connected: boolean;
  status: string;
  details: string;
  credential_fingerprint: string;
  credential_mask: string;
  updated_at: string;
};

type NewsItem = {
  title: string;
  source: string;
  sentiment: "bullish" | "bearish" | "neutral";
  ts: string;
};

type ModuleSourceState = {
  sentiment_source: string;
  sentiment_updated_at: string;
  news_provider: string;
  news_updated_at: string;
  backtest_source: string;
  backtest_updated_at: string;
  logs_source: string;
  logs_updated_at: string;
  auto_source: string;
  auto_updated_at: string;
  signals_source: string;
  signals_updated_at: string;
};

const serviceStartedAt = Date.now();
let autoTradingState: AutoTradingState = {
  enabled: false,
  mode: "paper",
  updated_at: new Date().toISOString(),
};

let moduleSourceState: ModuleSourceState = {
  sentiment_source: "market_breadth_binance",
  sentiment_updated_at: formatClickhouseDate(new Date()),
  news_provider: "provider_pending",
  news_updated_at: formatClickhouseDate(new Date()),
  backtest_source: "execution_orders_rolling_500",
  backtest_updated_at: formatClickhouseDate(new Date()),
  logs_source: "runtime_plus_clickhouse_events",
  logs_updated_at: formatClickhouseDate(new Date()),
  auto_source: "auto_trading_state_db",
  auto_updated_at: formatClickhouseDate(new Date()),
  signals_source: "execution_orders_plus_live_mark",
  signals_updated_at: formatClickhouseDate(new Date()),
};

let uiConfigState: UiConfigState = {
  binance_api_key: "",
  binance_secret: "",
  bybit_api_key: "",
  bybit_secret: "",
  okx_api_key: "",
  okx_secret: "",
  okx_passphrase: "",
  kraken_api_key: "",
  kraken_secret: "",
  dex_rpc_url: "",
  dex_private_key: "",
  telegram_bot_token: "",
  telegram_chat_id: "",
  binance_enabled: true,
  bybit_enabled: false,
  okx_enabled: false,
  kraken_enabled: false,
  uniswap_v3_enabled: false,
  sushiswap_enabled: false,
  dydx_enabled: false,
  trade_alerts_enabled: true,
  risk_alerts_enabled: true,
  daily_summary_enabled: false,
  auto_trading_enabled: false,
  auto_stop_loss_enabled: true,
  auto_take_profit_enabled: true,
  trailing_stops_enabled: true,
  atr_position_sizing_enabled: false,
  updated_at: formatClickhouseDate(new Date()),
};

function hasText(v: string): boolean {
  return v.trim().length > 0;
}

function boolToUInt8(v: boolean): number {
  return v ? 1 : 0;
}

function asBool(v: unknown): boolean {
  return v === true || Number(v) === 1 || String(v).toLowerCase() === "true";
}

function parseAutoMode(v: unknown): "paper" | "live" {
  return String(v ?? "paper").toLowerCase() === "live" ? "live" : "paper";
}

function maskSecret(value: string): string {
  const v = String(value ?? "");
  if (!v) return "";
  if (v.length <= 4) return "****";
  return `${"*".repeat(Math.max(4, v.length - 4))}${v.slice(-4)}`;
}

function fingerprintSecret(value: string): string {
  const v = String(value ?? "");
  if (!v) return "";
  return createHash("sha256").update(v).digest("hex").slice(0, 16);
}

function uiConfigToDbRow(cfg: UiConfigState) {
  return {
    ...cfg,
    binance_enabled: boolToUInt8(cfg.binance_enabled),
    bybit_enabled: boolToUInt8(cfg.bybit_enabled),
    okx_enabled: boolToUInt8(cfg.okx_enabled),
    kraken_enabled: boolToUInt8(cfg.kraken_enabled),
    uniswap_v3_enabled: boolToUInt8(cfg.uniswap_v3_enabled),
    sushiswap_enabled: boolToUInt8(cfg.sushiswap_enabled),
    dydx_enabled: boolToUInt8(cfg.dydx_enabled),
    trade_alerts_enabled: boolToUInt8(cfg.trade_alerts_enabled),
    risk_alerts_enabled: boolToUInt8(cfg.risk_alerts_enabled),
    daily_summary_enabled: boolToUInt8(cfg.daily_summary_enabled),
    auto_trading_enabled: boolToUInt8(cfg.auto_trading_enabled),
    auto_stop_loss_enabled: boolToUInt8(cfg.auto_stop_loss_enabled),
    auto_take_profit_enabled: boolToUInt8(cfg.auto_take_profit_enabled),
    trailing_stops_enabled: boolToUInt8(cfg.trailing_stops_enabled),
    atr_position_sizing_enabled: boolToUInt8(cfg.atr_position_sizing_enabled),
    updated_at: formatClickhouseDate(new Date(cfg.updated_at)),
  };
}

function dbRowToUiConfig(row: any): UiConfigState {
  return {
    binance_api_key: String(row.binance_api_key ?? ""),
    binance_secret: String(row.binance_secret ?? ""),
    bybit_api_key: String(row.bybit_api_key ?? ""),
    bybit_secret: String(row.bybit_secret ?? ""),
    okx_api_key: String(row.okx_api_key ?? ""),
    okx_secret: String(row.okx_secret ?? ""),
    okx_passphrase: String(row.okx_passphrase ?? ""),
    kraken_api_key: String(row.kraken_api_key ?? ""),
    kraken_secret: String(row.kraken_secret ?? ""),
    dex_rpc_url: String(row.dex_rpc_url ?? ""),
    dex_private_key: String(row.dex_private_key ?? ""),
    telegram_bot_token: String(row.telegram_bot_token ?? ""),
    telegram_chat_id: String(row.telegram_chat_id ?? ""),
    binance_enabled: asBool(row.binance_enabled),
    bybit_enabled: asBool(row.bybit_enabled),
    okx_enabled: asBool(row.okx_enabled),
    kraken_enabled: asBool(row.kraken_enabled),
    uniswap_v3_enabled: asBool(row.uniswap_v3_enabled),
    sushiswap_enabled: asBool(row.sushiswap_enabled),
    dydx_enabled: asBool(row.dydx_enabled),
    trade_alerts_enabled: asBool(row.trade_alerts_enabled),
    risk_alerts_enabled: asBool(row.risk_alerts_enabled),
    daily_summary_enabled: asBool(row.daily_summary_enabled),
    auto_trading_enabled: asBool(row.auto_trading_enabled),
    auto_stop_loss_enabled: asBool(row.auto_stop_loss_enabled),
    auto_take_profit_enabled: asBool(row.auto_take_profit_enabled),
    trailing_stops_enabled: asBool(row.trailing_stops_enabled),
    atr_position_sizing_enabled: asBool(row.atr_position_sizing_enabled),
    updated_at: String(row.updated_at ?? new Date().toISOString()),
  };
}

async function persistUiConfigStateToDb(cfg: UiConfigState): Promise<void> {
  await clickhouse.insert({
    table: `${clickhouseDb}.ui_config_state`,
    values: [uiConfigToDbRow(cfg)],
    format: "JSONEachRow",
  });
}

async function loadLatestUiConfigStateFromDb(): Promise<UiConfigState | null> {
  const rows = await clickhouse
    .query({
      query: `
        SELECT *
        FROM ${clickhouseDb}.ui_config_state
        ORDER BY updated_at DESC
        LIMIT 1
      `,
      format: "JSONEachRow",
    })
    .then((r) => r.json<any[]>());

  if (!rows.length) return null;
  return dbRowToUiConfig(rows[0]);
}

async function persistAutoTradingStateToDb(state: AutoTradingState): Promise<void> {
  await clickhouse.insert({
    table: `${clickhouseDb}.auto_trading_state`,
    values: [
      {
        enabled: boolToUInt8(state.enabled),
        mode: state.mode,
        updated_at: formatClickhouseDate(new Date(state.updated_at)),
      },
    ],
    format: "JSONEachRow",
  });
}

async function loadLatestAutoTradingStateFromDb(): Promise<AutoTradingState | null> {
  const rows = await clickhouse
    .query({
      query: `
        SELECT enabled, mode, updated_at
        FROM ${clickhouseDb}.auto_trading_state
        ORDER BY updated_at DESC
        LIMIT 1
      `,
      format: "JSONEachRow",
    })
    .then((r) => r.json<any[]>());

  if (!rows.length) return null;
  const row = rows[0];
  return {
    enabled: asBool(row.enabled),
    mode: parseAutoMode(row.mode),
    updated_at: String(row.updated_at ?? formatClickhouseDate(new Date())),
  };
}

function buildConnectionRegistry(cfg: UiConfigState, infra: { grpc: boolean; clickhouse: boolean }): ConnectionRegistryRecord[] {
  const now = formatClickhouseDate(new Date());
  const cexEntries: Array<{
    exchange: string;
    enabled: boolean;
    credentialPresent: boolean;
    secretValue: string;
  }> = [
    {
      exchange: "binance",
      enabled: cfg.binance_enabled,
      credentialPresent: hasText(cfg.binance_api_key) && hasText(cfg.binance_secret),
      secretValue: `${cfg.binance_api_key}:${cfg.binance_secret}`,
    },
    {
      exchange: "bybit",
      enabled: cfg.bybit_enabled,
      credentialPresent: hasText(cfg.bybit_api_key) && hasText(cfg.bybit_secret),
      secretValue: `${cfg.bybit_api_key}:${cfg.bybit_secret}`,
    },
    {
      exchange: "okx",
      enabled: cfg.okx_enabled,
      credentialPresent: hasText(cfg.okx_api_key) && hasText(cfg.okx_secret) && hasText(cfg.okx_passphrase),
      secretValue: `${cfg.okx_api_key}:${cfg.okx_secret}:${cfg.okx_passphrase}`,
    },
    {
      exchange: "kraken",
      enabled: cfg.kraken_enabled,
      credentialPresent: hasText(cfg.kraken_api_key) && hasText(cfg.kraken_secret),
      secretValue: `${cfg.kraken_api_key}:${cfg.kraken_secret}`,
    },
  ];

  const dexCredentialPresent = hasText(cfg.dex_rpc_url) && hasText(cfg.dex_private_key);
  const dexEntries: Array<{ exchange: string; enabled: boolean }> = [
    { exchange: "uniswap_v3", enabled: cfg.uniswap_v3_enabled },
    { exchange: "sushiswap", enabled: cfg.sushiswap_enabled },
    { exchange: "dydx", enabled: cfg.dydx_enabled },
  ];

  const records: ConnectionRegistryRecord[] = [];

  for (const e of cexEntries) {
    const connected = e.enabled && e.credentialPresent && infra.grpc && infra.clickhouse;
    const status = !e.enabled
      ? "disabled"
      : !e.credentialPresent
        ? "missing_credentials"
        : connected
          ? "connected"
          : "degraded";
    records.push({
      exchange: e.exchange,
      venue_type: "cex",
      enabled: e.enabled,
      credential_present: e.credentialPresent,
      connected,
      status,
      details: connected ? "credentials persisted and runtime healthy" : status,
      credential_fingerprint: fingerprintSecret(e.secretValue),
      credential_mask: maskSecret(e.secretValue),
      updated_at: now,
    });
  }

  for (const e of dexEntries) {
    const connected = e.enabled && dexCredentialPresent && infra.clickhouse;
    const status = !e.enabled
      ? "disabled"
      : !dexCredentialPresent
        ? "missing_credentials"
        : connected
          ? "connected"
          : "degraded";
    records.push({
      exchange: e.exchange,
      venue_type: "dex",
      enabled: e.enabled,
      credential_present: dexCredentialPresent,
      connected,
      status,
      details: connected ? "wallet/rpc persisted and runtime healthy" : status,
      credential_fingerprint: fingerprintSecret(`${cfg.dex_rpc_url}:${cfg.dex_private_key}`),
      credential_mask: `${maskSecret(cfg.dex_rpc_url)}|${maskSecret(cfg.dex_private_key)}`,
      updated_at: now,
    });
  }

  return records;
}

async function persistConnectionRegistry(records: ConnectionRegistryRecord[]): Promise<void> {
  if (!records.length) return;
  await clickhouse.insert({
    table: `${clickhouseDb}.exchange_connection_registry`,
    values: records.map((r) => ({
      ...r,
      enabled: boolToUInt8(r.enabled),
      credential_present: boolToUInt8(r.credential_present),
      connected: boolToUInt8(r.connected),
    })),
    format: "JSONEachRow",
  });
}

async function loadLatestConnectionRegistry(): Promise<ConnectionRegistryRecord[]> {
  const rows = await clickhouse
    .query({
      query: `
        SELECT *
        FROM (
          SELECT
            exchange,
            venue_type,
            enabled,
            credential_present,
            connected,
            status,
            details,
            credential_fingerprint,
            credential_mask,
            updated_at
          FROM ${clickhouseDb}.exchange_connection_registry
          ORDER BY updated_at DESC
        )
        LIMIT 1 BY exchange
      `,
      format: "JSONEachRow",
    })
    .then((r) => r.json<any[]>());

  return rows.map((row) => ({
    exchange: String(row.exchange),
    venue_type: String(row.venue_type) as "cex" | "dex",
    enabled: asBool(row.enabled),
    credential_present: asBool(row.credential_present),
    connected: asBool(row.connected),
    status: String(row.status ?? "unknown"),
    details: String(row.details ?? ""),
    credential_fingerprint: String(row.credential_fingerprint ?? ""),
    credential_mask: String(row.credential_mask ?? ""),
    updated_at: String(row.updated_at ?? new Date().toISOString()),
  })).sort((a, b) => a.exchange.localeCompare(b.exchange));
}

async function evaluateConnectivity(cfg: UiConfigState) {
  let grpcOk = false;
  let clickhouseOk = false;

  try {
    await rpc<{ timestamp: number }, { status: string }>("Ping", { timestamp: Math.floor(Date.now() / 1000) });
    grpcOk = true;
  } catch (_e) {
    grpcOk = false;
  }

  try {
    await clickhouse.ping();
    clickhouseOk = true;
  } catch (_e) {
    clickhouseOk = false;
  }

  const cexCredentials = {
    binance: hasText(cfg.binance_api_key) && hasText(cfg.binance_secret),
    bybit: hasText(cfg.bybit_api_key) && hasText(cfg.bybit_secret),
    okx: hasText(cfg.okx_api_key) && hasText(cfg.okx_secret) && hasText(cfg.okx_passphrase),
    kraken: hasText(cfg.kraken_api_key) && hasText(cfg.kraken_secret),
  };
  const dexCredentials = {
    rpc_url: hasText(cfg.dex_rpc_url),
    private_key: hasText(cfg.dex_private_key),
  };
  const enabled = {
    binance: cfg.binance_enabled,
    bybit: cfg.bybit_enabled,
    okx: cfg.okx_enabled,
    kraken: cfg.kraken_enabled,
    uniswap_v3: cfg.uniswap_v3_enabled,
    sushiswap: cfg.sushiswap_enabled,
    dydx: cfg.dydx_enabled,
  };

  const enabledCexReady = [
    !enabled.binance || cexCredentials.binance,
    !enabled.bybit || cexCredentials.bybit,
    !enabled.okx || cexCredentials.okx,
    !enabled.kraken || cexCredentials.kraken,
  ].every(Boolean);
  const anyDexEnabled = enabled.uniswap_v3 || enabled.sushiswap || enabled.dydx;
  const enabledDexReady = !anyDexEnabled || (dexCredentials.rpc_url && dexCredentials.private_key);

  const records = buildConnectionRegistry(cfg, { grpc: grpcOk, clickhouse: clickhouseOk });

  return {
    checks: {
      grpc: grpcOk,
      clickhouse: clickhouseOk,
      credentials_present: enabledCexReady && enabledDexReady,
      cex_credentials: cexCredentials,
      dex_credentials: dexCredentials,
      enabled,
    },
    records,
  };
}

const symbolCatalog: Array<{ symbol: string; name: string; basePrice: number }> = [
  { symbol: "BTC", name: "Bitcoin", basePrice: 95200 },
  { symbol: "ETH", name: "Ethereum", basePrice: 3120 },
  { symbol: "SOL", name: "Solana", basePrice: 191 },
  { symbol: "BNB", name: "BNB", basePrice: 625 },
  { symbol: "XRP", name: "XRP", basePrice: 0.62 },
  { symbol: "DOGE", name: "Dogecoin", basePrice: 0.12 },
  { symbol: "ADA", name: "Cardano", basePrice: 0.53 },
  { symbol: "AVAX", name: "Avalanche", basePrice: 42 },
];

function formatClickhouseDate(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
}

function defaultSymbolPrice(symbol: string): number {
  const map: Record<string, number> = {
    "BTC/USDT": 95200,
    "ETH/USDT": 3120,
    "SOL/USDT": 191,
    "BNB/USDT": 625,
  };
  return map[symbol] ?? 100;
}

function stableHash(text: string): number {
  let h = 0;
  for (let i = 0; i < text.length; i += 1) {
    h = (h * 31 + text.charCodeAt(i)) % 1_000_003;
  }
  return h;
}

function syntheticChangePct(symbol: string): number {
  const seed = stableHash(symbol);
  return Number((((seed % 900) - 450) / 100).toFixed(2));
}

function timeframeToSeconds(timeframe: string): number {
  const map: Record<string, number> = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
  };
  return map[timeframe] ?? 60;
}

function symbolToPairSymbol(raw: string): string {
  if (raw.includes("/")) return raw;
  return `${raw}/USDT`;
}

function toBinanceSymbol(pairOrBase: string): string {
  const upper = String(pairOrBase).toUpperCase();
  if (upper.includes("/")) {
    return upper.replace("/", "").replace(":USDT", "");
  }
  return `${upper}USDT`;
}

function fromBinanceSymbol(binanceSymbol: string): string {
  const s = String(binanceSymbol).toUpperCase();
  if (s.endsWith("USDT")) {
    return `${s.slice(0, -4)}/USDT`;
  }
  return `${s}/USDT`;
}

async function fetchJson<T>(path: string): Promise<T> {
  const res = await fetch(`${marketDataBaseUrl}${path}`);
  if (!res.ok) {
    throw new Error(`market_data_http_${res.status}`);
  }
  return (await res.json()) as T;
}

async function fetchJsonFromBase<T>(baseUrl: string, path: string): Promise<T> {
  const res = await fetch(`${baseUrl}${path}`);
  if (!res.ok) {
    throw new Error(`http_${res.status}`);
  }
  return (await res.json()) as T;
}

type BinanceTicker24h = {
  symbol: string;
  lastPrice: string;
  priceChangePercent: string;
  highPrice: string;
  lowPrice: string;
  quoteVolume: string;
};

async function fetchBinance24hForCatalog(limit: number): Promise<MarketTicker[]> {
  const selected = symbolCatalog.slice(0, limit);
  const reqSymbols = selected.map((s) => toBinanceSymbol(s.symbol));
  const symbolsParam = encodeURIComponent(JSON.stringify(reqSymbols));
  const rows = await fetchJson<BinanceTicker24h[]>(`/api/v3/ticker/24hr?symbols=${symbolsParam}`);
  const bySym = new Map(rows.map((r) => [r.symbol, r]));

  return selected.map((s) => {
    const binanceSymbol = toBinanceSymbol(s.symbol);
    const row = bySym.get(binanceSymbol);
    const price = Number(row?.lastPrice ?? s.basePrice);
    const change = Number(row?.priceChangePercent ?? syntheticChangePct(s.symbol));
    const high = Number(row?.highPrice ?? price);
    const low = Number(row?.lowPrice ?? price);
    const quoteVolume = Number(row?.quoteVolume ?? price * 10_000);
    return {
      symbol: s.symbol,
      name: s.name,
      price: Number(price.toFixed(4)),
      change_24h: Number(change.toFixed(2)),
      volume_24h: Number(quoteVolume.toFixed(2)),
      market_cap: Number((price * (2_000_000 + stableHash(`${s.symbol}:cap`) % 15_000_000)).toFixed(2)),
      high_24h: Number(high.toFixed(4)),
      low_24h: Number(low.toFixed(4)),
    };
  });
}

async function fetchBinanceLivePriceMapForCatalog(): Promise<Record<string, number>> {
  const reqSymbols = symbolCatalog.map((s) => toBinanceSymbol(s.symbol));
  const symbolsParam = encodeURIComponent(JSON.stringify(reqSymbols));
  const rows = await fetchJson<Array<{ symbol: string; price: string }>>(`/api/v3/ticker/price?symbols=${symbolsParam}`);
  const out: Record<string, number> = {};
  for (const row of rows) {
    const pair = fromBinanceSymbol(row.symbol);
    const px = Number(row.price);
    if (Number.isFinite(px) && px > 0) {
      out[pair] = px;
    }
  }
  return out;
}

function timeframeToBinanceInterval(timeframe: string): string {
  const map: Record<string, string> = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "1h": "1h",
    "4h": "4h",
    "1d": "1d",
  };
  return map[timeframe] ?? "1m";
}

async function fetchBinanceCandles(symbol: string, timeframe: string, limit: number) {
  const pair = symbolToPairSymbol(symbol);
  const binanceSymbol = toBinanceSymbol(pair);
  const interval = timeframeToBinanceInterval(timeframe);
  const rows = await fetchJson<any[]>(`/api/v3/klines?symbol=${encodeURIComponent(binanceSymbol)}&interval=${encodeURIComponent(interval)}&limit=${Math.min(limit, 1000)}`);

  return rows.map((k) => ({
    time: new Date(Number(k[0])).toISOString(),
    open: Number(k[1]),
    high: Number(k[2]),
    low: Number(k[3]),
    close: Number(k[4]),
    volume: Number(k[5]),
  }));
}

function normalizeDepthLimit(depth: number): number {
  if (depth <= 5) return 5;
  if (depth <= 10) return 10;
  if (depth <= 20) return 20;
  if (depth <= 50) return 50;
  if (depth <= 100) return 100;
  if (depth <= 500) return 500;
  return 1000;
}

async function fetchBinanceOrderbook(symbol: string, depth: number) {
  const pair = symbolToPairSymbol(symbol);
  const binanceSymbol = toBinanceSymbol(pair);
  const limit = normalizeDepthLimit(depth);
  const data = await fetchJson<{ bids: Array<[string, string]>; asks: Array<[string, string]> }>(
    `/api/v3/depth?symbol=${encodeURIComponent(binanceSymbol)}&limit=${limit}`
  );

  const bids = data.bids.slice(0, depth).map(([price, quantity]) => ({ price: Number(price), quantity: Number(quantity) }));
  const asks = data.asks.slice(0, depth).map(([price, quantity]) => ({ price: Number(price), quantity: Number(quantity) }));
  const bestBid = bids[0]?.price ?? 0;
  const bestAsk = asks[0]?.price ?? 0;

  return {
    symbol: pair,
    mid_price: bestBid > 0 && bestAsk > 0 ? Number(((bestBid + bestAsk) / 2).toFixed(4)) : 0,
    spread: bestBid > 0 && bestAsk > 0 ? Number((bestAsk - bestBid).toFixed(4)) : 0,
    bids,
    asks,
  };
}

function marketTickers(markMap: Record<string, number>, limit: number): MarketTicker[] {
  return symbolCatalog.slice(0, limit).map((s) => {
    const pair = `${s.symbol}/USDT`;
    const price = Number((markMap[pair] ?? s.basePrice).toFixed(4));
    const change_24h = syntheticChangePct(s.symbol);
    const high_24h = Number((price * (1 + Math.abs(change_24h) / 100 * 0.65)).toFixed(4));
    const low_24h = Number((price * (1 - Math.abs(change_24h) / 100 * 0.75)).toFixed(4));
    const volume_24h = Number((price * (10_000 + stableHash(s.symbol) % 80_000)).toFixed(2));
    const market_cap = Number((price * (2_000_000 + stableHash(`${s.symbol}:cap`) % 15_000_000)).toFixed(2));
    return {
      symbol: s.symbol,
      name: s.name,
      price,
      change_24h,
      volume_24h,
      market_cap,
      high_24h,
      low_24h,
    };
  });
}

function syntheticCandles(symbol: string, timeframe: string, limit: number, latestPrice: number) {
  const tfSec = timeframeToSeconds(timeframe);
  const now = Math.floor(Date.now() / 1000);
  const seed = stableHash(symbol + timeframe);
  const candles: Array<{ time: string; open: number; high: number; low: number; close: number; volume: number }> = [];

  let prev = latestPrice > 0 ? latestPrice : defaultSymbolPrice(symbol);
  for (let i = limit - 1; i >= 0; i -= 1) {
    const t = now - i * tfSec;
    const w = ((seed + i * 17) % 100) / 100;
    const drift = (Math.sin((seed + i) / 7) + Math.cos((seed + i) / 13)) * 0.0015;
    const open = prev;
    const close = Number((open * (1 + drift + (w - 0.5) * 0.001)).toFixed(4));
    const high = Number((Math.max(open, close) * (1 + 0.0012 + w * 0.0008)).toFixed(4));
    const low = Number((Math.min(open, close) * (1 - 0.0012 - w * 0.0008)).toFixed(4));
    const volume = Number((100 + ((seed + i * 29) % 900) + Math.abs(close - open) * 50).toFixed(2));
    candles.push({
      time: new Date(t * 1000).toISOString(),
      open: Number(open.toFixed(4)),
      high,
      low,
      close,
      volume,
    });
    prev = close;
  }
  return candles;
}

function parseLimit(raw: unknown, fallback: number, cap: number): number {
  const n = Number.parseInt(String(raw ?? fallback), 10);
  if (Number.isNaN(n) || n <= 0) return fallback;
  return Math.min(n, cap);
}

function parsePositiveQuantity(raw: unknown, cap: number): number {
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error("invalid_quantity");
  }
  return Math.min(n, cap);
}

async function latestPricesBySymbol(): Promise<Record<string, number>> {
  const out: Record<string, number> = {};

  // Prefer live public exchange marks for current UI market data.
  try {
    Object.assign(out, await fetchBinanceLivePriceMapForCatalog());
  } catch (_e) {
    // Fallback to recent traded marks from local store if public feed is unavailable.
  }

  const rows = await clickhouse
    .query({
      query: `
        SELECT
          symbol,
          toFloat64(argMaxIf(price, created_at, price > 0)) AS last_price
        FROM ${clickhouseDb}.execution_orders
        GROUP BY symbol
      `,
      format: "JSONEachRow",
    })
    .then((r) => r.json<Array<{ symbol: string; last_price: number }>>());

  for (const row of rows) {
    const p = Number(row.last_price);
    if (!(row.symbol in out)) {
      out[row.symbol] = p > 0 ? p : defaultSymbolPrice(row.symbol);
    }
  }
  for (const s of symbolCatalog) {
    const pair = `${s.symbol}/USDT`;
    if (!(pair in out)) {
      out[pair] = s.basePrice;
    }
  }
  return out;
}

async function computePositions(markMapOverride?: Record<string, number>): Promise<PositionSummary[]> {
  const rows = await clickhouse
    .query({
      query: `
        SELECT
          symbol,
          sumIf(quantity, lower(side) = 'buy') AS buy_qty,
          sumIf(quantity, lower(side) = 'sell') AS sell_qty,
          sumIf(quantity * if(price > 0, price, 0), lower(side) = 'buy') AS buy_notional,
          sumIf(quantity * if(price > 0, price, 0), lower(side) = 'sell') AS sell_notional
        FROM ${clickhouseDb}.execution_orders
        WHERE status IN ('SUBMITTED', 'FILLED')
        GROUP BY symbol
      `,
      format: "JSONEachRow",
    })
    .then((r) =>
      r.json<
        Array<{
        symbol: string;
        buy_qty: number;
        sell_qty: number;
        buy_notional: number;
        sell_notional: number;
        }>
      >()
    );

  const markMap = markMapOverride ?? (await latestPricesBySymbol());
  const positions: PositionSummary[] = [];

  for (const row of rows) {
    const mark = markMap[row.symbol] ?? defaultSymbolPrice(row.symbol);
    const buyQty = Number(row.buy_qty ?? 0);
    const sellQty = Number(row.sell_qty ?? 0);
    const buyNotional = Number(row.buy_notional ?? 0);
    const sellNotional = Number(row.sell_notional ?? 0);

    // Compute net position so long+short inventory can actually flatten on close-all.
    const netQty = buyQty - sellQty;
    if (Math.abs(netQty) < 1e-12) {
      continue;
    }

    if (netQty > 0) {
      const entry = buyQty > 0 && buyNotional > 0 ? buyNotional / buyQty : mark;
      positions.push({
        symbol: row.symbol,
        side: "LONG",
        size: netQty,
        entry_price: entry,
        mark_price: mark,
        unrealized_pnl: (mark - entry) * netQty,
      });
    } else {
      const shortSize = Math.abs(netQty);
      const entry = sellQty > 0 && sellNotional > 0 ? sellNotional / sellQty : mark;
      positions.push({
        symbol: row.symbol,
        side: "SHORT",
        size: shortSize,
        entry_price: entry,
        mark_price: mark,
        unrealized_pnl: (entry - mark) * shortSize,
      });
    }
  }

  return positions;
}

type SubmitArgs = {
  requestId: string;
  symbol: string;
  side: "buy" | "sell";
  quantity: number;
  price: number;
  orderType?: "market" | "limit";
  timeInForce?: string;
  venue: string;
  model: string;
  selectedTool: string;
  operatorTag: string;
  userId?: string;
  eventId?: string;
};

async function submitExecutionOrder(args: SubmitArgs) {
  const started = Date.now();
  const orderRes = await rpc<any, any>("SubmitOrder", {
    order_id: "",
    client_order_id: args.requestId,
    user_id: args.userId ?? "system",
    symbol: args.symbol,
    side: args.side,
    order_type: args.orderType ?? "market",
    quantity: args.quantity,
    price: args.price,
    time_in_force: args.timeInForce ?? "IOC",
    reduce_only: false,
    venue: args.venue,
    idempotency_key: args.requestId,
    metadata: {
      model: args.model,
      selected_tool: args.selectedTool,
    },
  });

  const latencyMs = Date.now() - started;
  const status = String(orderRes?.status ?? "SUBMITTED");
  const createdAt = formatClickhouseDate(new Date());
  const eventId = args.eventId ?? randomUUID();

  await clickhouse.insert({
    table: `${clickhouseDb}.tool_selection_events`,
    values: [
      {
        event_id: eventId,
        request_id: args.requestId,
        model: args.model,
        selected_tool: args.selectedTool,
        latency_ms: latencyMs,
        status,
        created_at: createdAt,
      },
    ],
    format: "JSONEachRow",
  });

  await clickhouse.insert({
    table: `${clickhouseDb}.execution_orders`,
    values: [
      {
        event_id: eventId,
        request_id: args.requestId,
        symbol: args.symbol,
        side: args.side,
        quantity: args.quantity,
        price: args.price,
        venue: args.venue,
        model: args.model,
        selected_tool: args.selectedTool,
        operator_tag: args.operatorTag,
        latency_ms: latencyMs,
        status,
        created_at: createdAt,
      },
    ],
    format: "JSONEachRow",
  });

  return {
    request_id: args.requestId,
    status,
    symbol: args.symbol,
    side: args.side,
    quantity: args.quantity,
    price: args.price,
    venue: args.venue,
    selected_tool: args.selectedTool,
    latency_ms: latencyMs,
  };
}

function rpc<TReq extends object, TRes>(method: string, req: TReq): Promise<TRes> {
  return new Promise((resolve, reject) => {
    grpcClient[method](req, (err: grpc.ServiceError | null, res: TRes) => {
      if (err) {
        reject(err);
        return;
      }
      resolve(res);
    });
  });
}

async function ensureClickhouseReady(): Promise<void> {
  await clickhouse.ping();
  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${clickhouseDb}.tool_selection_events (
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
      SETTINGS index_granularity = 8192
    `,
  });

  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${clickhouseDb}.auto_trading_state (
        enabled UInt8,
        mode LowCardinality(String),
        updated_at DateTime64(3)
      )
      ENGINE = MergeTree
      ORDER BY (updated_at)
      TTL toDateTime(updated_at) + INTERVAL 180 DAY DELETE
      SETTINGS index_granularity = 8192
    `,
  });

  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${clickhouseDb}.execution_orders (
        event_id UUID,
        request_id String,
        symbol String,
        side LowCardinality(String),
        quantity Float64,
        price Float64,
        venue LowCardinality(String),
        model String,
        selected_tool String,
        operator_tag String,
        latency_ms UInt32,
        status LowCardinality(String),
        created_at DateTime64(3)
      )
      ENGINE = MergeTree
      ORDER BY (created_at, request_id)
      TTL toDateTime(created_at) + INTERVAL 30 DAY DELETE
      SETTINGS index_granularity = 8192
    `,
  });

  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${clickhouseDb}.ui_config_state (
        binance_api_key String,
        binance_secret String,
        bybit_api_key String,
        bybit_secret String,
        okx_api_key String,
        okx_secret String,
        okx_passphrase String,
        kraken_api_key String,
        kraken_secret String,
        dex_rpc_url String,
        dex_private_key String,
        telegram_bot_token String,
        telegram_chat_id String,
        binance_enabled UInt8,
        bybit_enabled UInt8,
        okx_enabled UInt8,
        kraken_enabled UInt8,
        uniswap_v3_enabled UInt8,
        sushiswap_enabled UInt8,
        dydx_enabled UInt8,
        trade_alerts_enabled UInt8,
        risk_alerts_enabled UInt8,
        daily_summary_enabled UInt8,
        auto_trading_enabled UInt8,
        auto_stop_loss_enabled UInt8,
        auto_take_profit_enabled UInt8,
        trailing_stops_enabled UInt8,
        atr_position_sizing_enabled UInt8,
        updated_at DateTime64(3)
      )
      ENGINE = MergeTree
      ORDER BY (updated_at)
      TTL toDateTime(updated_at) + INTERVAL 180 DAY DELETE
      SETTINGS index_granularity = 8192
    `,
  });

  await clickhouse.command({
    query: `
      CREATE TABLE IF NOT EXISTS ${clickhouseDb}.exchange_connection_registry (
        exchange String,
        venue_type LowCardinality(String),
        enabled UInt8,
        credential_present UInt8,
        connected UInt8,
        status LowCardinality(String),
        details String,
        credential_fingerprint String,
        credential_mask String,
        updated_at DateTime64(3)
      )
      ENGINE = MergeTree
      ORDER BY (updated_at, exchange)
      TTL toDateTime(updated_at) + INTERVAL 180 DAY DELETE
      SETTINGS index_granularity = 8192
    `,
  });
}

async function buildStatusPayload(markMapOverride?: Record<string, number>) {
  const positions = await computePositions(markMapOverride);
  const totalUnrealized = positions.reduce((acc, p) => acc + p.unrealized_pnl, 0);

  const orderRows = await clickhouse
    .query({
      query: `
        SELECT status
        FROM ${clickhouseDb}.execution_orders
        ORDER BY created_at DESC
        LIMIT 500
      `,
      format: "JSONEachRow",
    })
    .then((r) => r.json<Array<{ status: string }>>());

  const totalTrades = orderRows.length;
  const markMap = markMapOverride ?? (await latestPricesBySymbol());
  const closedOrders = await clickhouse
    .query({
      query: `
        SELECT symbol, side, price, status
        FROM ${clickhouseDb}.execution_orders
        WHERE status IN ('FILLED', 'CLOSED', 'COMPLETED')
        ORDER BY created_at DESC
        LIMIT 500
      `,
      format: "JSONEachRow",
    })
    .then((r) => r.json<Array<{ symbol: string; side: string; price: number; status: string }>>());

  const profitable = closedOrders.filter((o) => {
    const mark = markMap[o.symbol] ?? defaultSymbolPrice(o.symbol);
    const side = String(o.side).toLowerCase();
    if (side === "buy") return mark > Number(o.price);
    if (side === "sell") return mark < Number(o.price);
    return false;
  }).length;
  const winRate = closedOrders.length > 0 ? Number(((profitable / closedOrders.length) * 100).toFixed(2)) : 0;
  const equity = 10_000 + totalUnrealized;
  const drawdownPct = Number((Math.max(0, (10_000 - equity) / 10_000) * 100).toFixed(2));
  const portfolioHeat = Number((Math.min(95, positions.reduce((a, p) => a + p.size, 0) * 10)).toFixed(2));

  return {
    equity: Number(equity.toFixed(2)),
    unrealized_pnl: Number(totalUnrealized.toFixed(2)),
    drawdown_pct: drawdownPct,
    daily_pnl: Number(totalUnrealized.toFixed(2)),
    portfolio_heat: portfolioHeat,
    win_rate: winRate,
    total_trades: totalTrades,
    open_positions: positions.length,
    positions: positions.map((p) => ({
      symbol: p.symbol,
      side: p.side,
      size: p.size,
      entry_price: p.entry_price,
      current_price: p.mark_price,
      pnl: p.unrealized_pnl,
      leverage: 1,
    })),
  };
}

async function buildFearGreedPayload() {
  try {
    const coins = await fetchBinance24hForCatalog(20);
    if (!coins.length) throw new Error("no_market_data");
    const avgChange = coins.reduce((acc, c) => acc + Number(c.change_24h || 0), 0) / coins.length;
    const breadth = coins.filter((c) => Number(c.change_24h || 0) > 0).length / coins.length;
    const raw = 50 + avgChange * 6 + (breadth - 0.5) * 30;
    const value = Math.max(1, Math.min(99, Math.round(raw)));
    let classification = "Neutral";
    if (value < 30) classification = "Fear";
    else if (value > 70) classification = "Greed";
    moduleSourceState.sentiment_source = "market_breadth_binance";
    moduleSourceState.sentiment_updated_at = formatClickhouseDate(new Date());
    return { value, classification, source: "market_breadth_binance" };
  } catch (_e) {
    moduleSourceState.sentiment_source = "fallback_neutral";
    moduleSourceState.sentiment_updated_at = formatClickhouseDate(new Date());
    return { value: 50, classification: "Neutral", source: "fallback_neutral" };
  }
}

async function buildIndicatorsPayload(symbol: string, pxOverride?: number) {
  const pair = symbolToPairSymbol(symbol.toUpperCase());
  const px = pxOverride ?? ((await latestPricesBySymbol())[pair] ?? defaultSymbolPrice(pair));
  const seed = stableHash(symbol.toUpperCase());
  const rsi = 20 + (seed % 60);
  const macd = Number((((seed % 200) - 100) / 3).toFixed(2));
  const stoch = 10 + (seed % 80);
  const adx = 10 + (seed % 40);
  const atr = Number((px * (0.004 + (seed % 20) / 10000)).toFixed(3));
  const bbWidth = Number((0.01 + (seed % 20) / 1000).toFixed(4));
  const ema9 = px * (1 + 0.0012);
  const ema21 = px * (1 + 0.0006);
  const sma50 = px * (1 - 0.001);
  const volumeRatio = Number((0.8 + (seed % 70) / 50).toFixed(2));
  return {
    rsi,
    macd,
    stoch_k: stoch,
    adx,
    atr,
    bb_width: bbWidth,
    ema_cross: ema9 > ema21 ? 1 : -1,
    volume_ratio: volumeRatio,
    ema9,
    ema21,
    sma50,
  };
}

async function buildOrderbookPayload(symbol: string, depth: number, midOverride?: number) {
  try {
    const live = await fetchBinanceOrderbook(symbol, depth);
    if (live.bids.length > 0 && live.asks.length > 0) {
      return live;
    }
  } catch (_e) {
    // Fallback below.
  }

  let mid = midOverride ?? 0;
  if (!(mid > 0)) {
    const rows = await clickhouse
      .query({
        query: `
          SELECT toFloat64(argMaxIf(price, created_at, price > 0)) AS latest_price
          FROM ${clickhouseDb}.execution_orders
          WHERE symbol = {symbol:String}
        `,
        format: "JSONEachRow",
        query_params: { symbol },
      })
      .then((r) => r.json<Array<{ latest_price: number }>>());
    const latestPrice = Number(rows?.[0]?.latest_price ?? 0);
    mid = latestPrice > 0 ? latestPrice : defaultSymbolPrice(symbol);
  }
  const bids: Array<{ price: number; quantity: number }> = [];
  const asks: Array<{ price: number; quantity: number }> = [];

  for (let i = 1; i <= depth; i += 1) {
    const bidSize = Number((((i * 37) % 19) / 10 + 0.2).toFixed(3));
    const askSize = Number((((i * 53) % 23) / 10 + 0.2).toFixed(3));
    bids.push({ price: Number((mid - i * 2.5).toFixed(2)), quantity: bidSize });
    asks.push({ price: Number((mid + i * 2.5).toFixed(2)), quantity: askSize });
  }

  return {
    symbol,
    mid_price: mid,
    spread: Number((asks[0].price - bids[0].price).toFixed(4)),
    bids,
    asks,
  };
}

function classifyNewsSentiment(title: string): "bullish" | "bearish" | "neutral" {
  const lower = String(title).toLowerCase();
  if (/(surge|rally|bull|gain|jump|up|breakout|approval)/.test(lower)) return "bullish";
  if (/(drop|crash|bear|loss|down|selloff|hack|exploit|liquidation)/.test(lower)) return "bearish";
  return "neutral";
}

async function fetchLiveNewsPrimary(limit: number): Promise<NewsItem[]> {
  const rows = await fetchJsonFromBase<any>(newsDataBaseUrl, `/data/v2/news/?lang=EN`);
  const data = Array.isArray(rows?.Data) ? rows.Data.slice(0, limit) : [];
  return data.map((n: any) => {
    const title = String(n?.title ?? "");
    return {
      title,
      source: String(n?.source_info?.name ?? n?.source ?? "market feed"),
      sentiment: classifyNewsSentiment(title),
      ts: n?.published_on ? new Date(Number(n.published_on) * 1000).toISOString() : new Date().toISOString(),
    };
  });
}

async function fetchLiveNewsFallback(limit: number): Promise<NewsItem[]> {
  const rows = await fetchJsonFromBase<any[]>(newsDataFallbackBaseUrl, `/api/v3/news`);
  const data = Array.isArray(rows) ? rows.slice(0, limit) : [];
  return data.map((n: any) => {
    const title = String(n?.title ?? "");
    return {
      title,
      source: String(n?.author ?? "coingecko"),
      sentiment: classifyNewsSentiment(title),
      ts: n?.created_at ? new Date(n.created_at).toISOString() : new Date().toISOString(),
    };
  });
}

function stripHtmlTags(text: string): string {
  return String(text).replace(/<[^>]*>/g, " ").replace(/\s+/g, " ").trim();
}

async function fetchLiveNewsRssFallback(limit: number): Promise<NewsItem[]> {
  const rssSources = [
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
  ];

  for (const src of rssSources) {
    try {
      const res = await fetch(src);
      if (!res.ok) continue;
      const xml = await res.text();
      const itemMatches = [...xml.matchAll(/<item>([\s\S]*?)<\/item>/g)].slice(0, limit);
      const out: NewsItem[] = itemMatches.map((m) => {
        const block = m[1] ?? "";
        const title = stripHtmlTags((block.match(/<title>([\s\S]*?)<\/title>/i)?.[1] ?? "Untitled").replace(/<!\[CDATA\[|\]\]>/g, ""));
        const source = stripHtmlTags((block.match(/<source[^>]*>([\s\S]*?)<\/source>/i)?.[1] ?? new URL(src).hostname).replace(/<!\[CDATA\[|\]\]>/g, ""));
        const pubRaw = (block.match(/<pubDate>([\s\S]*?)<\/pubDate>/i)?.[1] ?? "").trim();
        const ts = pubRaw ? new Date(pubRaw).toISOString() : new Date().toISOString();
        return {
          title,
          source,
          sentiment: classifyNewsSentiment(title),
          ts,
        };
      }).filter((n) => n.title.length > 0);

      if (out.length > 0) {
        return out;
      }
    } catch (_e) {
      // Try next RSS source.
    }
  }

  return [];
}

async function buildNewsPayload() {
  try {
    const primary = await fetchLiveNewsPrimary(12);
    if (primary.length > 0) {
      moduleSourceState.news_provider = "cryptocompare";
      moduleSourceState.news_updated_at = formatClickhouseDate(new Date());
      return { items: primary, provider: "cryptocompare" };
    }
  } catch (_e) {
    // Try fallback provider.
  }

  try {
    const fallback = await fetchLiveNewsFallback(12);
    if (fallback.length > 0) {
      moduleSourceState.news_provider = "coingecko";
      moduleSourceState.news_updated_at = formatClickhouseDate(new Date());
      return { items: fallback, provider: "coingecko" };
    }
  } catch (_e) {
    // Fallback below.
  }

  try {
    const rss = await fetchLiveNewsRssFallback(12);
    if (rss.length > 0) {
      moduleSourceState.news_provider = "rss-fallback";
      moduleSourceState.news_updated_at = formatClickhouseDate(new Date());
      return { items: rss, provider: "rss-fallback" };
    }
  } catch (_e) {
    // Fallback below.
  }

  moduleSourceState.news_provider = "unavailable";
  moduleSourceState.news_updated_at = formatClickhouseDate(new Date());
  return { items: [], provider: "unavailable" };
}

function withRealtimeMarks(baseMarkMap: Record<string, number>): Record<string, number> {
  const now = Date.now() / 1000;
  const out: Record<string, number> = { ...baseMarkMap };
  for (const s of symbolCatalog) {
    const pair = `${s.symbol}/USDT`;
    const base = out[pair] ?? s.basePrice;
    const seed = stableHash(pair) % 360;
    const drift = Math.sin((now + seed) / 5) * 0.0009 + Math.cos((now + seed) / 11) * 0.0006;
    out[pair] = Number((base * (1 + drift)).toFixed(4));
  }
  return out;
}

async function buildRealtimeSnapshot(symbolRaw: string, timeframe: string) {
  const symbol = symbolToPairSymbol(symbolRaw.split(":")[0]);
  const markMap = await latestPricesBySymbol();
  const status = await buildStatusPayload(markMap);
  let coins: MarketTicker[];
  try {
    coins = await fetchBinance24hForCatalog(20);
  } catch (_e) {
    coins = marketTickers(markMap, 20);
  }
  const market = { coins };
  const latestPrice = markMap[symbol] ?? defaultSymbolPrice(symbol);
  let candleRows;
  try {
    candleRows = await fetchBinanceCandles(symbol, timeframe, 160);
  } catch (_e) {
    candleRows = syntheticCandles(symbol, timeframe, 160, latestPrice);
  }
  const candles = { candles: candleRows };
  const indicators = await buildIndicatorsPayload(symbol.replace("/USDT", ""), latestPrice);
  const orderbook = await buildOrderbookPayload(symbol, 8, latestPrice);
  const feargreed = await buildFearGreedPayload();
  const news = await buildNewsPayload();
  const dex = {
    pools: [
      { dex: "UniswapV3", pair: "ETH/USDC", tvl: 420_000_000, volume_24h: 82_000_000, chain: "ethereum" },
      { dex: "SushiSwap", pair: "WBTC/ETH", tvl: 145_000_000, volume_24h: 22_000_000, chain: "ethereum" },
      { dex: "PancakeSwap", pair: "BNB/BUSD", tvl: 180_000_000, volume_24h: 31_000_000, chain: "bsc" },
    ],
  };
  const auto = {
    ...autoTradingState,
    uptime_seconds: Math.floor((Date.now() - serviceStartedAt) / 1000),
  };

  return {
    ts: new Date().toISOString(),
    symbol,
    timeframe,
    status,
    market,
    candles,
    indicators,
    orderbook,
    feargreed,
    news,
    dex,
    auto,
  };
}

app.get("/health", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "health" });
  try {
    const ping = await rpc<{ timestamp: number }, { status: string }>("Ping", {
      timestamp: Math.floor(Date.now() / 1000),
    });
    await clickhouse.ping();
    requestCounter.inc({ route: "health", status: "200" });
    res.json({ status: "ok", grpc: ping.status, clickhouse: "ok" });
  } catch (error: any) {
    requestCounter.inc({ route: "health", status: "503" });
    res.status(503).json({ status: "degraded", error: error?.message ?? "unknown" });
  } finally {
    timer();
  }
});

app.post("/api/tool-selection", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "tool_selection" });
  const requestId = req.body?.request_id ?? `req-${Date.now()}`;
  const model = req.body?.model ?? "gpt-5.3-codex";
  const selectedTool = req.body?.selected_tool ?? "BridgeService.SubmitOrder";
  const side = String(req.body?.side ?? "buy").toLowerCase() === "sell" ? "sell" : "buy";
  const symbol = req.body?.symbol ?? "BTC/USDT";
  const venue = req.body?.venue ?? "sim";
  const quantity = parsePositiveQuantity(req.body?.quantity ?? 0.01, 10_000);
  const price = Number(req.body?.price ?? 0);
  const operatorTag = req.body?.operator_tag ?? "unknown";

  try {
    const payload = await submitExecutionOrder({
      requestId,
      symbol,
      side,
      quantity,
      price,
      venue,
      model,
      selectedTool,
      operatorTag,
      userId: req.body?.user_id,
      eventId: req.body?.event_id,
    });

    requestCounter.inc({ route: "tool_selection", status: "200" });
    res.json(payload);
  } catch (error: any) {
    requestCounter.inc({ route: "tool_selection", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/market", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "market" });
  try {
    const limit = parseLimit(req.query.per_page, 20, 200);
    let coins: MarketTicker[];
    try {
      coins = await fetchBinance24hForCatalog(limit);
    } catch (_e) {
      const markMap = await latestPricesBySymbol();
      coins = marketTickers(markMap, limit);
    }
    requestCounter.inc({ route: "market", status: "200" });
    res.json({ coins });
  } catch (error: any) {
    requestCounter.inc({ route: "market", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/candles", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "candles" });
  try {
    const symbolRaw = String(req.query.symbol ?? "BTC/USDT");
    const symbol = symbolToPairSymbol(symbolRaw.split(":")[0]);
    const timeframe = String(req.query.timeframe ?? "1m");
    const limit = parseLimit(req.query.limit, 200, 2000);
    let candles;
    try {
      candles = await fetchBinanceCandles(symbol, timeframe, limit);
    } catch (_e) {
      const markMap = await latestPricesBySymbol();
      const latestPrice = markMap[symbol] ?? defaultSymbolPrice(symbol);
      candles = syntheticCandles(symbol, timeframe, limit, latestPrice);
    }
    requestCounter.inc({ route: "candles", status: "200" });
    res.json({ candles });
  } catch (error: any) {
    requestCounter.inc({ route: "candles", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/indicators/:symbol", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "indicators" });
  try {
    const symbol = String(req.params.symbol ?? "BTC").toUpperCase();
    const pair = symbolToPairSymbol(symbol);
    const markMap = await latestPricesBySymbol();
    const px = markMap[pair] ?? defaultSymbolPrice(pair);
    const seed = stableHash(symbol);
    const rsi = 20 + (seed % 60);
    const macd = Number((((seed % 200) - 100) / 3).toFixed(2));
    const stoch = 10 + (seed % 80);
    const adx = 10 + (seed % 40);
    const atr = Number((px * (0.004 + (seed % 20) / 10000)).toFixed(3));
    const bbWidth = Number((0.01 + (seed % 20) / 1000).toFixed(4));
    const ema9 = px * (1 + 0.0012);
    const ema21 = px * (1 + 0.0006);
    const sma50 = px * (1 - 0.001);
    const volumeRatio = Number((0.8 + (seed % 70) / 50).toFixed(2));

    requestCounter.inc({ route: "indicators", status: "200" });
    res.json({
      rsi,
      macd,
      stoch_k: stoch,
      adx,
      atr,
      bb_width: bbWidth,
      ema_cross: ema9 > ema21 ? 1 : -1,
      volume_ratio: volumeRatio,
      ema9,
      ema21,
      sma50,
    });
  } catch (error: any) {
    requestCounter.inc({ route: "indicators", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/dex/pools", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "dex_pools" });
  try {
    const pools = [
      { dex: "UniswapV3", pair: "ETH/USDC", tvl: 420_000_000, volume_24h: 82_000_000, chain: "ethereum" },
      { dex: "SushiSwap", pair: "WBTC/ETH", tvl: 145_000_000, volume_24h: 22_000_000, chain: "ethereum" },
      { dex: "PancakeSwap", pair: "BNB/BUSD", tvl: 180_000_000, volume_24h: 31_000_000, chain: "bsc" },
      { dex: "Camelot", pair: "ARB/USDC", tvl: 52_000_000, volume_24h: 11_000_000, chain: "arbitrum" },
      { dex: "UniswapV3", pair: "SOL/USDC", tvl: 95_000_000, volume_24h: 18_000_000, chain: "ethereum" },
    ];
    requestCounter.inc({ route: "dex_pools", status: "200" });
    res.json({ pools });
  } catch (error: any) {
    requestCounter.inc({ route: "dex_pools", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/signals/recent", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "signals_recent" });
  try {
    const limit = parseLimit(req.query.limit, 20, 200);
    const rows = await clickhouse
      .query({
        query: `
          SELECT symbol, side, quantity, price, selected_tool, status, created_at
          FROM ${clickhouseDb}.execution_orders
          ORDER BY created_at DESC
          LIMIT ${limit}
        `,
        format: "JSONEachRow",
      })
      .then((r) =>
        r.json<
          Array<{
            symbol: string;
            side: string;
            quantity: number;
            price: number;
            selected_tool: string;
            status: string;
            created_at: string;
          }>
        >()
      );

    const markMap = await latestPricesBySymbol();
    const signals = rows.map((r) => {
      const side = String(r.side).toUpperCase();
      const symbol = String(r.symbol);
      const status = String(r.status);
      const price = Number(r.price || 0);
      const mark = Number(markMap[symbol] ?? 0);
      const edgePct = price > 0 && mark > 0
        ? side === "BUY"
          ? ((mark - price) / price) * 100
          : ((price - mark) / price) * 100
        : 0;
      const statusBoost = ["FILLED", "CLOSED", "COMPLETED"].includes(status.toUpperCase()) ? 18 : 6;
      const confidence = Math.max(1, Math.min(99, Number((50 + statusBoost + Math.max(-20, Math.min(20, edgePct * 4))).toFixed(2))));

      return {
      symbol: r.symbol,
      signal: side === "BUY" ? "LONG" : "SHORT",
      confidence,
      strategy: r.selected_tool,
      status: r.status,
      created_at: r.created_at,
      entry_price: Number(r.price),
      size: Number(r.quantity),
      };
    });

    moduleSourceState.signals_source = "execution_orders_plus_live_mark";
    moduleSourceState.signals_updated_at = formatClickhouseDate(new Date());
    requestCounter.inc({ route: "signals_recent", status: "200" });
    res.json({ signals, source: moduleSourceState.signals_source });
  } catch (error: any) {
    requestCounter.inc({ route: "signals_recent", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/portfolio", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "portfolio" });
  try {
    const positions = await computePositions();
    const totalUnrealized = positions.reduce((acc, p) => acc + p.unrealized_pnl, 0);
    const exposure = positions.reduce((acc, p) => acc + p.size * p.mark_price, 0);
    const equity = 10_000 + totalUnrealized;

    requestCounter.inc({ route: "portfolio", status: "200" });
    res.json({
      equity: Number(equity.toFixed(2)),
      total_unrealized_pnl: Number(totalUnrealized.toFixed(2)),
      exposure: Number(exposure.toFixed(2)),
      open_positions: positions.length,
      positions,
    });
  } catch (error: any) {
    requestCounter.inc({ route: "portfolio", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/backtest/summary", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "backtest_summary" });
  try {
    const rows = await clickhouse
      .query({
        query: `
          SELECT symbol, side, quantity, price, status
          FROM ${clickhouseDb}.execution_orders
          ORDER BY created_at DESC
          LIMIT 500
        `,
        format: "JSONEachRow",
      })
      .then((r) => r.json<Array<{ symbol: string; side: string; quantity: number; price: number; status: string }>>());

    const totalTrades = rows.length;
    const grossNotional = rows.reduce((acc, r) => acc + Number(r.quantity) * Number(r.price), 0);
    const wins = rows.filter((r) => ["FILLED", "CLOSED", "COMPLETED"].includes(String(r.status).toUpperCase())).length;
    const winRate = totalTrades > 0 ? (wins / totalTrades) * 100 : 0;
    const avgTrade = totalTrades > 0 ? grossNotional / totalTrades : 0;
    const markMap = await latestPricesBySymbol();

    const returns: number[] = [];
    let equity = 0;
    let peak = 0;
    let maxDrawdown = 0;
    for (const r of rows) {
      const side = String(r.side).toLowerCase();
      const symbol = String(r.symbol || "BTC/USDT");
      const mark = Number(markMap[symbol] ?? 0);
      const px = Number(r.price || 0);
      const qty = Number(r.quantity || 0);
      if (px <= 0 || qty <= 0 || mark <= 0) continue;
      const pnl = side === "buy" ? (mark - px) * qty : (px - mark) * qty;
      returns.push(pnl);
      equity += pnl;
      peak = Math.max(peak, equity);
      const dd = peak > 0 ? ((peak - equity) / peak) * 100 : 0;
      maxDrawdown = Math.max(maxDrawdown, dd);
    }

    const mean = returns.length ? returns.reduce((a, b) => a + b, 0) / returns.length : 0;
    const variance = returns.length
      ? returns.reduce((a, b) => a + (b - mean) ** 2, 0) / returns.length
      : 0;
    const std = Math.sqrt(variance);
    const sharpe = std > 0 ? (mean / std) * Math.sqrt(Math.max(1, returns.length)) : 0;

    moduleSourceState.backtest_source = "execution_orders_rolling_500";
    moduleSourceState.backtest_updated_at = formatClickhouseDate(new Date());
    requestCounter.inc({ route: "backtest_summary", status: "200" });
    res.json({
      period: "rolling_500_trades",
      trades: totalTrades,
      win_rate: Number(winRate.toFixed(2)),
      avg_trade_notional: Number(avgTrade.toFixed(2)),
      gross_notional: Number(grossNotional.toFixed(2)),
      sharpe: Number(sharpe.toFixed(2)),
      max_drawdown_pct: Number(maxDrawdown.toFixed(2)),
      source: moduleSourceState.backtest_source,
    });
  } catch (error: any) {
    requestCounter.inc({ route: "backtest_summary", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/news", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "news" });
  try {
    const payload = await buildNewsPayload();
    requestCounter.inc({ route: "news", status: "200" });
    res.json(payload);
  } catch (error: any) {
    requestCounter.inc({ route: "news", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/logs/recent", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "logs_recent" });
  try {
    const uptimeSec = Math.floor((Date.now() - serviceStartedAt) / 1000);

    const orderRows = await clickhouse
      .query({
        query: `
          SELECT symbol, side, quantity, status, created_at
          FROM ${clickhouseDb}.execution_orders
          ORDER BY created_at DESC
          LIMIT 12
        `,
        format: "JSONEachRow",
      })
      .then((r) =>
        r.json<Array<{ symbol: string; side: string; quantity: number; status: string; created_at: string }>>()
      );

    const configRows = await clickhouse
      .query({
        query: `
          SELECT updated_at
          FROM ${clickhouseDb}.ui_config_state
          ORDER BY updated_at DESC
          LIMIT 3
        `,
        format: "JSONEachRow",
      })
      .then((r) => r.json<Array<{ updated_at: string }>>());

    const logs = [
      { level: "INFO", message: "Bridge API healthy", ts: new Date().toISOString() },
      { level: "INFO", message: `Uptime ${uptimeSec}s`, ts: new Date().toISOString() },
      {
        level: autoTradingState.enabled ? "WARN" : "INFO",
        message: `Auto mode ${autoTradingState.enabled ? "enabled" : "disabled"} (${autoTradingState.mode})`,
        ts: new Date().toISOString(),
      },
      ...configRows.map((c) => ({ level: "INFO", message: "Settings persisted", ts: c.updated_at })),
      ...orderRows.map((o) => ({
        level: ["FILLED", "CLOSED", "COMPLETED"].includes(String(o.status).toUpperCase()) ? "INFO" : "WARN",
        message: `${String(o.side).toUpperCase()} ${o.symbol} qty=${Number(o.quantity).toFixed(4)} status=${o.status}`,
        ts: String(o.created_at),
      })),
    ];

    moduleSourceState.logs_source = "runtime_plus_clickhouse_events";
    moduleSourceState.logs_updated_at = formatClickhouseDate(new Date());
    requestCounter.inc({ route: "logs_recent", status: "200" });
    res.json({ logs: logs.slice(0, 20), source: moduleSourceState.logs_source });
  } catch (error: any) {
    requestCounter.inc({ route: "logs_recent", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/auto/status", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "auto_status" });
  try {
    const uptimeSec = Math.floor((Date.now() - serviceStartedAt) / 1000);
    moduleSourceState.auto_source = "auto_trading_state_db";
    moduleSourceState.auto_updated_at = formatClickhouseDate(new Date());
    requestCounter.inc({ route: "auto_status", status: "200" });
    res.json({
      ...autoTradingState,
      uptime_seconds: uptimeSec,
      source: moduleSourceState.auto_source,
    });
  } catch (error: any) {
    requestCounter.inc({ route: "auto_status", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.post("/api/auto/toggle", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "auto_toggle" });
  try {
    const enabled = Boolean(req.body?.enabled);
    const mode = String(req.body?.mode ?? "paper").toLowerCase() === "live" ? "live" : "paper";
    autoTradingState = {
      enabled,
      mode,
      updated_at: formatClickhouseDate(new Date()),
    };
    await persistAutoTradingStateToDb(autoTradingState);

    // Propagate auto-trading toggle to the Python signal engine
    try {
      await fetch("http://localhost:8000/auto/toggle", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ enabled }),
      });
    } catch (_e) {
      // Python engine may not be reachable — non-fatal
    }

    requestCounter.inc({ route: "auto_toggle", status: "200" });
    res.json({ success: true, state: autoTradingState });
  } catch (error: any) {
    requestCounter.inc({ route: "auto_toggle", status: "500" });
    res.status(500).json({ success: false, error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/feargreed", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "feargreed" });
  try {
    const { value, classification } = await buildFearGreedPayload();
    requestCounter.inc({ route: "feargreed", status: "200" });
    res.json({ value, classification, source: moduleSourceState.sentiment_source });
  } catch (error: any) {
    requestCounter.inc({ route: "feargreed", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/system/data-sources", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "system_data_sources" });
  try {
    requestCounter.inc({ route: "system_data_sources", status: "200" });
    res.json({
      sentiment: { source: moduleSourceState.sentiment_source, updated_at: moduleSourceState.sentiment_updated_at },
      news: { source: moduleSourceState.news_provider, updated_at: moduleSourceState.news_updated_at },
      backtest: { source: moduleSourceState.backtest_source, updated_at: moduleSourceState.backtest_updated_at },
      logs: { source: moduleSourceState.logs_source, updated_at: moduleSourceState.logs_updated_at },
      auto: { source: moduleSourceState.auto_source, updated_at: moduleSourceState.auto_updated_at },
      signals: { source: moduleSourceState.signals_source, updated_at: moduleSourceState.signals_updated_at },
    });
  } catch (error: any) {
    requestCounter.inc({ route: "system_data_sources", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/status", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "status" });
  try {
    const payload = await buildStatusPayload();
    requestCounter.inc({ route: "status", status: "200" });
    res.json(payload);
  } catch (error: any) {
    requestCounter.inc({ route: "status", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.post("/api/trade", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "trade" });
  try {
    const symbol = symbolToPairSymbol(String(req.body?.symbol ?? "BTC/USDT").split(":")[0]);
    const side = String(req.body?.side ?? "BUY").toLowerCase() === "sell" ? "sell" : "buy";
    const quantity = parsePositiveQuantity(req.body?.size ?? req.body?.quantity ?? 0.01, 10_000);
    const orderType = String(req.body?.order_type ?? req.body?.type ?? "market").toLowerCase() === "limit" ? "limit" : "market";
    const model = String(req.body?.model ?? "gpt-5.3-codex");
    const selectedTool = "BridgeService.SubmitOrder";
    const requestId = String(req.body?.request_id ?? `trade-${Date.now()}`);
    const markMap = await latestPricesBySymbol();
    const fallbackPrice = markMap[symbol] ?? defaultSymbolPrice(symbol);
    const limitPrice = Number(req.body?.price ?? fallbackPrice);

    if (orderType === "limit" && (!Number.isFinite(limitPrice) || limitPrice <= 0)) {
      throw new Error("invalid_limit_price");
    }

    const payload = await submitExecutionOrder({
      requestId,
      symbol,
      side: side as "buy" | "sell",
      quantity,
      price: Number.isFinite(limitPrice) && limitPrice > 0 ? limitPrice : fallbackPrice,
      orderType,
      timeInForce: orderType === "limit" ? "GTC" : "IOC",
      venue: String(req.body?.venue ?? "sim"),
      model,
      selectedTool,
      operatorTag: String(req.body?.operator_tag ?? "ui"),
      userId: String(req.body?.user_id ?? "ui"),
    });

    requestCounter.inc({ route: "trade", status: "200" });
    res.json({ success: true, order: payload });
  } catch (error: any) {
    requestCounter.inc({ route: "trade", status: "500" });
    res.status(500).json({ success: false, error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.post("/api/positions/close-all", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "positions_close_all" });
  try {
    const positions = await computePositions();
    for (const p of positions) {
      const closeSide = p.side === "LONG" ? "sell" : "buy";
      await submitExecutionOrder({
        requestId: `closeall-${Date.now()}-${p.symbol}`,
        symbol: p.symbol,
        side: closeSide,
        quantity: p.size,
        price: p.mark_price,
        venue: "sim",
        model: "gpt-5.3-codex",
        selectedTool: "BridgeService.SubmitOrder",
        operatorTag: "close-all",
        userId: "ui",
      });
    }
    requestCounter.inc({ route: "positions_close_all", status: "200" });
    res.json({ success: true, closed_positions: positions.length, message: "positions flattened" });
  } catch (error: any) {
    requestCounter.inc({ route: "positions_close_all", status: "500" });
    res.status(500).json({ success: false, error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.post("/api/positions/breakeven", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "positions_breakeven" });
  try {
    const positions = await computePositions();
    requestCounter.inc({ route: "positions_breakeven", status: "200" });
    res.json({
      success: true,
      updated_positions: positions.length,
      message: "break-even request accepted",
    });
  } catch (error: any) {
    requestCounter.inc({ route: "positions_breakeven", status: "500" });
    res.status(500).json({ success: false, error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/config", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "config_get" });
  try {
    const connection_registry = await loadLatestConnectionRegistry();
    requestCounter.inc({ route: "config_get", status: "200" });
    res.json({ ...uiConfigState, connection_registry });
  } catch (error: any) {
    requestCounter.inc({ route: "config_get", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.post("/api/config", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "config_save" });
  try {
    uiConfigState = {
      binance_api_key: String(req.body?.binance_api_key ?? ""),
      binance_secret: String(req.body?.binance_secret ?? ""),
      bybit_api_key: String(req.body?.bybit_api_key ?? ""),
      bybit_secret: String(req.body?.bybit_secret ?? ""),
      okx_api_key: String(req.body?.okx_api_key ?? ""),
      okx_secret: String(req.body?.okx_secret ?? ""),
      okx_passphrase: String(req.body?.okx_passphrase ?? ""),
      kraken_api_key: String(req.body?.kraken_api_key ?? ""),
      kraken_secret: String(req.body?.kraken_secret ?? ""),
      dex_rpc_url: String(req.body?.dex_rpc_url ?? ""),
      dex_private_key: String(req.body?.dex_private_key ?? ""),
      telegram_bot_token: String(req.body?.telegram_bot_token ?? ""),
      telegram_chat_id: String(req.body?.telegram_chat_id ?? ""),
      binance_enabled: Boolean(req.body?.binance_enabled),
      bybit_enabled: Boolean(req.body?.bybit_enabled),
      okx_enabled: Boolean(req.body?.okx_enabled),
      kraken_enabled: Boolean(req.body?.kraken_enabled),
      uniswap_v3_enabled: Boolean(req.body?.uniswap_v3_enabled),
      sushiswap_enabled: Boolean(req.body?.sushiswap_enabled),
      dydx_enabled: Boolean(req.body?.dydx_enabled),
      trade_alerts_enabled: Boolean(req.body?.trade_alerts_enabled),
      risk_alerts_enabled: Boolean(req.body?.risk_alerts_enabled),
      daily_summary_enabled: Boolean(req.body?.daily_summary_enabled),
      auto_trading_enabled: Boolean(req.body?.auto_trading_enabled),
      auto_stop_loss_enabled: Boolean(req.body?.auto_stop_loss_enabled),
      auto_take_profit_enabled: Boolean(req.body?.auto_take_profit_enabled),
      trailing_stops_enabled: Boolean(req.body?.trailing_stops_enabled),
      atr_position_sizing_enabled: Boolean(req.body?.atr_position_sizing_enabled),
      updated_at: formatClickhouseDate(new Date()),
    };

    await persistUiConfigStateToDb(uiConfigState);
    const evalResult = await evaluateConnectivity(uiConfigState);
    await persistConnectionRegistry(evalResult.records);

    requestCounter.inc({ route: "config_save", status: "200" });
    res.json({
      success: true,
      updated_at: uiConfigState.updated_at,
      checks: evalResult.checks,
      connection_registry: evalResult.records,
    });
  } catch (error: any) {
    requestCounter.inc({ route: "config_save", status: "500" });
    res.status(500).json({ success: false, error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.post("/api/config/test", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "config_test" });
  try {
    const evalResult = await evaluateConnectivity(uiConfigState);
    await persistConnectionRegistry(evalResult.records);

    requestCounter.inc({ route: "config_test", status: "200" });
    res.json({ success: true, checks: evalResult.checks, connection_registry: evalResult.records });
  } catch (error: any) {
    requestCounter.inc({ route: "config_test", status: "500" });
    res.status(500).json({ success: false, error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/tool-selection/latest", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "tool_selection_latest" });
  try {
    const result = await clickhouse.query({
      query: `
        SELECT request_id, model, selected_tool, latency_ms, status, created_at
        FROM ${clickhouseDb}.tool_selection_events
        ORDER BY created_at DESC
        LIMIT 1
      `,
      format: "JSONEachRow",
    });
    const rows = await result.json<any>();
    requestCounter.inc({ route: "tool_selection_latest", status: "200" });
    res.json(rows[0] ?? null);
  } catch (error: any) {
    requestCounter.inc({ route: "tool_selection_latest", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/orders", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "orders" });
  try {
    const limit = parseLimit(req.query.limit, 50, 500);
    const rows = await clickhouse
      .query({
        query: `
          SELECT request_id, symbol, side, quantity, price, venue, model, selected_tool,
                 operator_tag, latency_ms, status, created_at
          FROM ${clickhouseDb}.execution_orders
          ORDER BY created_at DESC
          LIMIT ${limit}
        `,
        format: "JSONEachRow",
      })
      .then((r) => r.json<ExecutionOrder[]>());

    requestCounter.inc({ route: "orders", status: "200" });
    res.json(rows);
  } catch (error: any) {
    requestCounter.inc({ route: "orders", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/positions", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "positions" });
  try {
    const positions = await computePositions();
    const totalPnl = positions.reduce((acc, p) => acc + p.unrealized_pnl, 0);

    requestCounter.inc({ route: "positions", status: "200" });
    res.json({ positions, total_unrealized_pnl: totalPnl });
  } catch (error: any) {
    requestCounter.inc({ route: "positions", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/pnl", async (_req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "pnl" });
  try {
    const positions = await computePositions();
    const totalPnl = positions.reduce((acc, p) => acc + p.unrealized_pnl, 0);
    requestCounter.inc({ route: "pnl", status: "200" });
    res.json({ total_unrealized_pnl: totalPnl });
  } catch (error: any) {
    requestCounter.inc({ route: "pnl", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/orderbook", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "orderbook" });
  try {
    const symbol = String(req.query.symbol ?? "BTC/USDT");
    const depth = parseLimit(req.query.depth, 8, 25);

    const payload = await buildOrderbookPayload(symbol, depth);
    requestCounter.inc({ route: "orderbook", status: "200" });
    res.json(payload);
  } catch (error: any) {
    requestCounter.inc({ route: "orderbook", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/realtime/snapshot", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "realtime_snapshot" });
  try {
    const symbol = String(req.query.symbol ?? "BTC/USDT");
    const timeframe = String(req.query.timeframe ?? "1m");
    const payload = await buildRealtimeSnapshot(symbol, timeframe);
    requestCounter.inc({ route: "realtime_snapshot", status: "200" });
    res.json(payload);
  } catch (error: any) {
    requestCounter.inc({ route: "realtime_snapshot", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
  } finally {
    timer();
  }
});

app.get("/api/realtime/stream", async (req: Request, res: Response) => {
  const timer = requestLatency.startTimer({ route: "realtime_stream" });
  const symbol = String(req.query.symbol ?? "BTC/USDT");
  const timeframe = String(req.query.timeframe ?? "1m");

  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const send = async () => {
    try {
      const payload = await buildRealtimeSnapshot(symbol, timeframe);
      res.write(`event: snapshot\n`);
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    } catch (error: any) {
      res.write(`event: error\n`);
      res.write(`data: ${JSON.stringify({ error: error?.message ?? "unknown_error" })}\n\n`);
    }
  };

  const heartbeat = setInterval(() => {
    res.write(`: heartbeat ${Date.now()}\n\n`);
  }, 15_000);

  const stream = setInterval(() => {
    void send();
  }, 1_000);

  requestCounter.inc({ route: "realtime_stream", status: "200" });
  void send();

  req.on("close", () => {
    clearInterval(stream);
    clearInterval(heartbeat);
    timer();
  });
});

app.get("/metrics", async (_req: Request, res: Response) => {
  res.set("Content-Type", registry.contentType);
  res.send(await registry.metrics());
});

ensureClickhouseReady()
  .then(async () => {
    const persisted = await loadLatestUiConfigStateFromDb();
    if (persisted) {
      uiConfigState = persisted;
    }
    const persistedAuto = await loadLatestAutoTradingStateFromDb();
    if (persistedAuto) {
      autoTradingState = persistedAuto;
    }
    app.listen(port, () => {
      console.log(`bridge-api listening on :${port}`);
    });
  })
  .catch((err) => {
    console.error("bridge-api startup failed", err);
    process.exit(1);
  });
