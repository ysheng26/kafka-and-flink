#!/bin/bash

printf '%s\n' \
  'America:{"brand":"Marlboro","price":13.0,"country":"America"}' \
  'China:{"brand":"Chunghua","price":14.0,"country":"China"}' \
  'America:{"brand":"Camel","price":12.0,"country":"America"}' \
  'Japan:{"brand":"MildSeven","price":15.0,"country":"Japan"}' \
  | docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic cigarette-prices \
      --property parse.key=true \
      --property key.separator=:


printf '%s\n' \
  'America:{"brand": "Jack Daniels", "price": 28.0, "country": "America"}' \
  'China:{"brand": "Maotai", "price": 686.0, "country": "China"}' \
  'Scotland:{"brand": "Johnnie Walker", "price": 120.0, "country": "Scotland"}' \
  'America:{"brand": "Titos", "price": 15.0, "country": "America"}' \
  'Japan:{"brand": "Gekkeikan Sake", "price": 15.0, "country": "Japan"}' \
  | docker compose exec -T kafka \
      /opt/kafka/bin/kafka-console-producer.sh \
      --bootstrap-server localhost:9092 \
      --topic alcohol-prices \
      --property parse.key=true \
      --property key.separator=:
