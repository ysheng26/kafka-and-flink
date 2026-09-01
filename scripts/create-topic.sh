#!/bin/bash
docker compose exec kafka \
    /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic cigarette-prices \
    --partitions 3 \
    --replication-factor 1
