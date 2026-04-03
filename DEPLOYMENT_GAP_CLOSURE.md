# Production Gap Closure Runbook (Docker-Only)

## Goal
Close remaining operational gaps with zero assumptions about local Rust/Node availability.

## 1) Build and launch the stack
```bash
cp .env.production.example .env.production

docker compose -f docker-compose.prod-ready.yml --env-file .env.production build

docker compose -f docker-compose.prod-ready.yml --env-file .env.production up -d clickhouse clickhouse-migrator gateway bridge-api ui-static

docker compose -f docker-compose.prod-ready.yml --env-file .env.production ps
```

## 2) Verify service health
```bash
curl -fsS http://localhost:8080/health
curl -fsS http://localhost:8088/
docker compose -f docker-compose.prod-ready.yml --env-file .env.production logs --tail=100 bridge-api gateway clickhouse
```

## 3) Run end-to-end test
```bash
docker compose -f docker-compose.prod-ready.yml --env-file .env.production --profile e2e up --build --abort-on-container-exit e2e-tests
```

The E2E test validates:
- request submitted through Bridge API (which calls gRPC SubmitOrder)
- event persisted in ClickHouse
- UI static app is reachable and renders

## 4) UI build/output details
UI app location: `ui/app`

NPM scripts:
- `npm run dev`
- `npm run build`
- `npm run preview`

Build output:
- `ui/app/dist`

Containerized build command (no local node assumptions):
```bash
docker compose -f docker-compose.prod-ready.yml --env-file .env.production build ui-static
```

## 5) Rust build verification (containerized)
Rust gateway Dockerfile: `rust/gateway/Dockerfile`

Containerized build command:
```bash
docker compose -f docker-compose.prod-ready.yml --env-file .env.production build gateway
```

This Dockerfile installs Rust toolchain in build stage and emits linux/x86_64 runtime image.

## 6) ClickHouse migrations and permissions
Migration files:
- `db/clickhouse/migrations/001_tool_selection_events.sql`
- `db/clickhouse/migrations/002_tool_selection_latest_view.sql`

Bootstrap script:
- `scripts/clickhouse-migrate.sh`

App user:
- `${CLICKHOUSE_APP_USER}` with SELECT/INSERT/CREATE/ALTER on `${CLICKHOUSE_DB}`

## 7) Required environment variables and secrets
Required:
- `CLICKHOUSE_PASSWORD` (admin password used by migrator; default in sample: `clickhouse_admin`)
- `CLICKHOUSE_APP_PASSWORD`
- `CORS_ALLOW_ORIGINS`

Keep secrets out of git:
- use `.env.production` in deployment target
- use Docker secrets or host secret manager for long-term ops

## 8) Monitoring and health checks
- Bridge API exposes Prometheus metrics at `GET /metrics`
- Health checks are configured in compose for `clickhouse`, `bridge-api`, and `ui-static`
- Gateway exposes gRPC on `:50051`

## 9) Verifiable production-ready milestone
Milestone is reached when all are true:
- `docker compose ... up -d` succeeds
- `clickhouse-migrator` exits successfully
- `curl http://localhost:8080/health` returns status ok
- `docker compose ... --profile e2e up ... e2e-tests` passes
