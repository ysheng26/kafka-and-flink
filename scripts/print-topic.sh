#!/bin/bash
# docker compose exec kafka \
#     /opt/kafka/bin/kafka-console-consumer.sh \
#     --bootstrap-server localhost:9092 \
#     --topic cigarette-prices \
#     --from-beginning

docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic cigarette-prices \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true \
  --property key.separator=' | '

