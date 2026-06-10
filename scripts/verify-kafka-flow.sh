#!/bin/bash
# Verify messages are flowing through all Kafka topics.
# Run after the full stack is started.

set -euo pipefail

KAFKA_CONTAINER="${1:-infra-kafka-1}"
TIMEOUT=30  # seconds to wait for at least one message

check_topic() {
    local TOPIC="$1"
    echo -n "Checking ${TOPIC}... "

    if timeout "${TIMEOUT}" docker exec "${KAFKA_CONTAINER}" \
        kafka-console-consumer.sh \
        --bootstrap-server localhost:9092 \
        --topic "${TOPIC}" \
        --max-messages 1 \
        --timeout-ms "$((TIMEOUT * 1000))" \
        2>/dev/null | grep -q .; then
        echo "[OK]"
        return 0
    else
        echo "[TIMEOUT - no messages in ${TIMEOUT}s]"
        return 1
    fi
}

FAILED=0

check_topic "market-data-raw"   || FAILED=1
check_topic "market-data-candles" || FAILED=1
check_topic "trade-signals"     || FAILED=1
check_topic "order-requests"    || FAILED=1
check_topic "validated-orders"  || FAILED=1
check_topic "order-fills"       || FAILED=1

if [ "${FAILED}" -eq 0 ]; then
    echo ""
    echo "All Kafka topics are receiving messages."
else
    echo ""
    echo "Some topics did not receive messages. Check service logs."
    exit 1
fi
