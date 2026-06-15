#!/bin/bash
# Create all Kafka topics required by the trading pipeline.
# Run inside the kafka container: docker exec infra_kafka_1 bash /create-topics.sh

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKERS:-kafka:9092}"

# Format: "topic-name:partitions:replication-factor"
#
# Central topics (shared across all regions):
#   market-data-raw, market-data-candles, trade-signals
#
# Regional topics (one set per exchange region):
#   {region}.order-requests   — signal router → OMS-{region}
#   {region}.validated-orders — OMS-{region}  → EMS-{region}
#   {region}.order-fills      — EMS-{region}  → OMS-{region}
#
# The signal router is the last central component. Everything after it
# stays within the region, so there are no cross-region Kafka reads.
TOPICS=(
    # Central pipeline topics
    "market-data-raw:3:1"
    "market-data-candles:3:1"
    "trade-signals:3:1"

    # Default (single-region) topics — backwards compatible
    "order-requests:3:1"
    "validated-orders:3:1"
    "order-fills:3:1"

    # Tokyo region — Binance (ap-northeast-1)
    "tokyo.order-requests:3:1"
    "tokyo.validated-orders:3:1"
    "tokyo.order-fills:3:1"

    # Singapore region — Crypto.com (ap-southeast-1)
    "sgp.order-requests:3:1"
    "sgp.validated-orders:3:1"
    "sgp.order-fills:3:1"

    # EU region — Deribit (eu-west-1)
    "eu.order-requests:3:1"
    "eu.validated-orders:3:1"
    "eu.order-fills:3:1"
)

echo "Waiting for Kafka broker at ${KAFKA_BROKER}..."
until kafka-broker-api-versions.sh --bootstrap-server "${KAFKA_BROKER}" &>/dev/null; do
    sleep 2
done
echo "Kafka is ready."

for TOPIC_CONFIG in "${TOPICS[@]}"; do
    TOPIC=$(echo "${TOPIC_CONFIG}" | cut -d: -f1)
    PARTITIONS=$(echo "${TOPIC_CONFIG}" | cut -d: -f2)
    REPLICATION=$(echo "${TOPIC_CONFIG}" | cut -d: -f3)

    kafka-topics.sh \
        --create \
        --if-not-exists \
        --bootstrap-server "${KAFKA_BROKER}" \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}"

    echo "[OK] Created topic: ${TOPIC} (partitions=${PARTITIONS}, replication=${REPLICATION})"
done

echo "All topics created successfully."
