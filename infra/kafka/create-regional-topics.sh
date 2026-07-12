#!/bin/bash
# Create all topics local to ONE region's own Kafka broker (kafka-tokyo, kafka-sgp,
# or kafka-eu). Run once per region, e.g.:
#   docker exec infra_kafka-tokyo-init_1 bash /create-regional-topics.sh
#
# Parameterized by KAFKA_BROKER (the regional broker's own advertised address)
# and REGION (its name in universe.json, e.g. "tokyo"). Each regional broker
# gets its own copy of:
#   {region}.order-requests       — mirrored in by MM2 from the central broker
#                                    (see ../kafka/mm2.properties); also created
#                                    here so oms-{region} has somewhere to
#                                    consume from even before the first mirror
#                                    completes.
#   {region}.validated-orders     — OMS-{region} -> EMS-{region}, fully local
#   {region}.order-fills          — EMS-{region} -> OMS-{region}, fully local
#   {region}.*.dlq                — RetryingDlqDispatcher quarantines onto
#                                    "{consumed-topic}.dlq" on whichever broker
#                                    that consumer is configured against — for
#                                    every regional consumer that's this
#                                    region's own broker, never the central one.
#
# Also auto-provisioned at OMS/EMS startup by KafkaTopicProvisioningService —
# listed here too so a fresh regional broker started via this script alone
# already has them before either service comes up.

set -euo pipefail

KAFKA_BROKER="${KAFKA_BROKER:?KAFKA_BROKER must be set, e.g. kafka-tokyo:9092}"
REGION="${REGION:?REGION must be set, e.g. tokyo}"

TOPICS=(
    "${REGION}.order-requests:3:1"
    "${REGION}.order-requests.dlq:3:1"
    "${REGION}.validated-orders:3:1"
    "${REGION}.validated-orders.dlq:3:1"
    "${REGION}.order-fills:3:1"
    "${REGION}.order-fills.dlq:3:1"
)

echo "Waiting for regional Kafka broker at ${KAFKA_BROKER} (region=${REGION})..."
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

echo "All regional topics created successfully for region=${REGION}."
