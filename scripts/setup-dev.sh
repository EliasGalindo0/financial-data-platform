#!/usr/bin/env bash
# Local development setup script
set -euo pipefail

echo "==> Starting infrastructure..."
cd docker && docker compose up -d postgres redis kafka

echo "==> Waiting for PostgreSQL..."
until docker compose exec postgres pg_isready -U finplatform; do sleep 1; done

echo "==> Running migrations..."
cd ..
DATABASE_URL="postgresql://finplatform:dev_password_change_in_prod@localhost:5432/finplatform" \
    sqlx migrate run --source migrations/

echo "==> Creating Kafka topics..."
docker compose -f docker/docker-compose.yml exec kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic financial.transactions.v1 \
    --partitions 12 \
    --replication-factor 1

docker compose -f docker/docker-compose.yml exec kafka \
    kafka-topics --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic financial.transactions.dlq \
    --partitions 1 \
    --replication-factor 1

echo "==> Done! Services available:"
echo "  Ingestion API: http://localhost:8080"
echo "  Reporting API: http://localhost:8082"
echo "  Grafana:       http://localhost:3000 (admin/admin)"
echo "  Jaeger:        http://localhost:16686"
echo "  Prometheus:    http://localhost:9090"
