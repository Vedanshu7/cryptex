#!/bin/bash
# Create all Kafka topics required by the trading pipeline.
# Run inside the kafka container: docker exec infra_kafka_1 bash /create-topics.sh

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKERS:-kafka:9092}"
UNIVERSE_FILE="${UNIVERSE_FILE:-/universe.json}"

# Format: "topic-name:partitions:replication-factor"
#
# Central topics (shared across all regions):
#   market-data-raw, market-data-candles, trade-signals
#
# Regional topics (one set per region in universe.json, e.g. tokyo/sgp/eu):
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
)

echo "Reading regions from ${UNIVERSE_FILE}..."
REGIONS=$(python3 -c "
import json
with open('${UNIVERSE_FILE}') as f:
    universe = json.load(f)
print(' '.join(universe['regions'].keys()))
")
echo "Regions: ${REGIONS}"

for REGION in ${REGIONS}; do
    TOPICS+=(
        "${REGION}.order-requests:3:1"
        "${REGION}.validated-orders:3:1"
        "${REGION}.order-fills:3:1"
    )
done

echo "Waiting for Kafka broker at ${KAFKA_BROKER}..."
until kafka-broker-api-versions --bootstrap-server "${KAFKA_BROKER}" &>/dev/null; do
    sleep 2
done
echo "Kafka is ready."

for TOPIC_CONFIG in "${TOPICS[@]}"; do
    TOPIC=$(echo "${TOPIC_CONFIG}" | cut -d: -f1)
    PARTITIONS=$(echo "${TOPIC_CONFIG}" | cut -d: -f2)
    REPLICATION=$(echo "${TOPIC_CONFIG}" | cut -d: -f3)

    kafka-topics \
        --create \
        --if-not-exists \
        --bootstrap-server "${KAFKA_BROKER}" \
        --topic "${TOPIC}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}"

    echo "[OK] Created topic: ${TOPIC} (partitions=${PARTITIONS}, replication=${REPLICATION})"
done

echo "All topics created successfully."
