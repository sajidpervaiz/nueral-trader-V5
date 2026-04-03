import express, { Request, Response } from "express";
import cors from "cors";
import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { createClient } from "@clickhouse/client";
import { randomUUID } from "crypto";
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

function formatClickhouseDate(date: Date): string {
  return date.toISOString().replace("T", " ").replace("Z", "");
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
  const started = Date.now();

  const requestId = req.body?.request_id ?? `req-${Date.now()}`;
  const model = req.body?.model ?? "gpt-5.3-codex";
  const selectedTool = req.body?.selected_tool ?? "BridgeService.SubmitOrder";

  try {
    const orderRes = await rpc<any, any>("SubmitOrder", {
      order_id: "",
      client_order_id: requestId,
      user_id: req.body?.user_id ?? "system",
      symbol: req.body?.symbol ?? "BTC/USDT",
      side: req.body?.side ?? "buy",
      order_type: "market",
      quantity: Number(req.body?.quantity ?? 0.01),
      price: Number(req.body?.price ?? 0),
      time_in_force: "IOC",
      reduce_only: false,
      venue: req.body?.venue ?? "sim",
      idempotency_key: requestId,
      metadata: {
        model,
        selected_tool: selectedTool,
      },
    });

    const latencyMs = Date.now() - started;

    await clickhouse.insert({
      table: `${clickhouseDb}.tool_selection_events`,
      values: [
        {
          event_id: req.body?.event_id ?? randomUUID(),
          request_id: requestId,
          model,
          selected_tool: selectedTool,
          latency_ms: latencyMs,
          status: String(orderRes?.status ?? "SUBMITTED"),
          created_at: formatClickhouseDate(new Date()),
        },
      ],
      format: "JSONEachRow",
    });

    requestCounter.inc({ route: "tool_selection", status: "200" });
    res.json({
      request_id: requestId,
      status: String(orderRes?.status ?? "SUBMITTED"),
      selected_tool: selectedTool,
      latency_ms: latencyMs,
    });
  } catch (error: any) {
    requestCounter.inc({ route: "tool_selection", status: "500" });
    res.status(500).json({ error: error?.message ?? "unknown_error" });
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

app.get("/metrics", async (_req: Request, res: Response) => {
  res.set("Content-Type", registry.contentType);
  res.send(await registry.metrics());
});

ensureClickhouseReady()
  .then(() => {
    app.listen(port, () => {
      console.log(`bridge-api listening on :${port}`);
    });
  })
  .catch((err) => {
    console.error("bridge-api startup failed", err);
    process.exit(1);
  });
