#!/bin/bash
# One-command local dev startup.
# Starts the full stack, waits for health, seeds the DB, and tails logs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "${SCRIPT_DIR}")"

echo "==> Starting infrastructure..."
cd "${PROJECT_ROOT}/infra"
docker compose up -d zookeeper kafka postgres redis

echo "==> Waiting for Kafka and Postgres to be ready..."
until docker compose exec -T kafka kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 &>/dev/null; do
    echo "Kafka not ready, waiting 5s..."
    sleep 5
done
echo "[OK] Kafka ready."

until docker compose exec -T postgres pg_isready -U trading -d trading &>/dev/null; do
    echo "Postgres not ready, waiting 3s..."
    sleep 3
done
echo "[OK] Postgres ready."

echo "==> Seeding test tenants..."
docker compose exec -T postgres psql -U trading -d trading \
    -f /dev/stdin < "${SCRIPT_DIR}/seed-tenants.sql"
echo "[OK] Tenants seeded."

echo "==> Starting pipeline services..."
docker compose up -d exchange-connector candle-aggregator signal-router

echo "==> Starting engine services..."
docker compose up -d signal-generator oms ems

echo "==> Starting observability..."
docker compose up -d prometheus grafana jaeger

echo ""
echo "==> Stack is running:"
echo "  OMS API:     http://localhost:5001/health"
echo "  EMS API:     http://localhost:5002/health"
echo "  Grafana:     http://localhost:3000  (admin/admin)"
echo "  Jaeger:      http://localhost:16686"
echo "  Prometheus:  http://localhost:9090"
echo ""
echo "Tailing logs (Ctrl+C to stop)..."
docker compose logs -f oms ems exchange-connector candle-aggregator signal-generator signal-router
