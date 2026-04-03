#!/usr/bin/env bash
set -euo pipefail

CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-clickhouse}"
CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-9000}"
CLICKHOUSE_USER="${CLICKHOUSE_USER:-default}"
CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD:-}"
CLICKHOUSE_DB="${CLICKHOUSE_DB:-neural_trader}"
APP_USER="${CLICKHOUSE_APP_USER:-nt_app}"
APP_PASSWORD="${CLICKHOUSE_APP_PASSWORD:-nt_app_password}"
READONLY_USER="${CLICKHOUSE_READONLY_USER:-nt_readonly}"
READONLY_PASSWORD="${CLICKHOUSE_READONLY_PASSWORD:-nt_readonly_password}"

base=(clickhouse-client --host "$CLICKHOUSE_HOST" --port "$CLICKHOUSE_PORT" --user "$CLICKHOUSE_USER")
if [[ -n "$CLICKHOUSE_PASSWORD" ]]; then
  base+=(--password "$CLICKHOUSE_PASSWORD")
fi

until "${base[@]}" --query "SELECT 1" >/dev/null 2>&1; do
  echo "waiting for clickhouse..."
  sleep 2
done

echo "creating database and users..."
"${base[@]}" --query "CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};"
"${base[@]}" --query "CREATE USER IF NOT EXISTS ${APP_USER} IDENTIFIED WITH plaintext_password BY '${APP_PASSWORD}';"
"${base[@]}" --query "GRANT SELECT, INSERT, CREATE, ALTER ON ${CLICKHOUSE_DB}.* TO ${APP_USER};"
"${base[@]}" --query "CREATE USER IF NOT EXISTS ${READONLY_USER} IDENTIFIED WITH plaintext_password BY '${READONLY_PASSWORD}';"
"${base[@]}" --query "GRANT SELECT ON ${CLICKHOUSE_DB}.* TO ${READONLY_USER};"

for migration in /migrations/*.sql; do
  echo "applying $migration"
  "${base[@]}" --multiquery < "$migration"
done

echo "clickhouse migrations complete"
