#!/bin/bash
# Create all Kafka topics on the CENTRAL broker required by the trading pipeline.
# Run inside the central kafka container: docker exec infra_kafka_1 bash /create-topics.sh
#
# Since the multi-region simulation (kafka-tokyo/kafka-sgp/kafka-eu + MM2), the
# central broker only holds the pipeline's central topics plus the *source* side
# of each region's order-requests topic — MM2 (see mm2.properties) mirrors that
# one topic per region onto the matching regional broker with its name preserved
# (IdentityReplicationPolicy). validated-orders and order-fills are fully local
# to each region's own broker and never touch the central broker at all — see
# create-regional-topics.sh, run against each regional broker instead.

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKERS:-kafka:9092}"
UNIVERSE_FILE="${UNIVERSE_FILE:-/universe.json}"

# Format: "topic-name:partitions:replication-factor"
TOPICS=(
    # Central pipeline topics
    "market-data-raw:3:1"
    "market-data-candles:3:1"
    "trade-signals:3:1"

    # Dead-letter topics. Quix Streams routes a quarantined message back to
    # "{row.topic}.dlq" — row.topic is always the *input* topic a service
    # consumed from (Quix Streams carries the original row context through
    # .apply()/.to_topic(), so this is the same whether the failure happened
    # while processing or while producing downstream). candle_aggregator and
    # exchange_connector both quarantine onto market-data-raw.dlq (the former
    # via Quix Streams' on_processing_error, the latter via a direct
    # DlqPublisher call after its own publish retries fail); signal_router
    # quarantines onto trade-signals.dlq. Nothing consumes these
    # automatically — they're for manual inspection/replay.
    "market-data-raw.dlq:3:1"
    "trade-signals.dlq:3:1"

    # Default (single-region) topics — backwards compatible, used only by the
    # "legacy" compose profile's oms/ems services (no regional broker involved).
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
        # MM2 source side — signal-router publishes here; MM2 mirrors it onto
        # kafka-{region} under the same name (IdentityReplicationPolicy), where
        # oms-{region} actually consumes it. validated-orders/order-fills (and
        # every topic's .dlq companion) live only on the regional broker — see
        # create-regional-topics.sh.
        "${REGION}.order-requests:3:1"
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
