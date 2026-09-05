#!/bin/bash
set -euo pipefail

topic="$1"

docker compose exec -T kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --delete \
    --topic "$topic"

