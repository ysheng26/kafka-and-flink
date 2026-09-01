#!/usr/bin/env bash

set -euo pipefail

topic="cigarette-prices"

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$topic" \
  --partitions 3 \
  --replication-factor 1

printf '%s\n' \
  '{"brand":"Marlboro","price":13.0,"country":"America"}' \
  '{"brand":"Chunghua","price":14.0,"country":"China"}' \
  '{"brand":"Camel","price":12.0,"country":"America"}' \
  '{"brand":"MildSeven","price":15.0,"country":"Japan"}' \
  | docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic "$topic"

echo "Messages published to $topic"


