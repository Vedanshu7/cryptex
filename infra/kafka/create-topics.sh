#!/bin/bash
# Create all Kafka topics required by the trading pipeline.
# Run inside the kafka container: docker exec infra_kafka_1 bash /create-topics.sh

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKERS:-kafka:9092}"

# Format: "topic-name:partitions:replication-factor"
TOPICS=(
    "market-data-raw:3:1"
    "market-data-candles:3:1"
    "trade-signals:3:1"
    "order-requests:3:1"
    "validated-orders:3:1"
    "order-fills:3:1"
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
