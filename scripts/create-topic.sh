#!/bin/bash
set -euo pipefail

topic="$1"
partitions="$2"

docker compose exec kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
