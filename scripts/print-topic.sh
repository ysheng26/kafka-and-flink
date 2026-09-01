#!/bin/bash
docker compose exec kafka \
    /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic cigarette-prices \
    --from-beginning
