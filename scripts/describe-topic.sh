#!/bin/bash
set -euo pipefail

topic="$1"

docker compose exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic "$topic"
