#!/bin/sh
set -eu

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic streamforge.events \
  --partitions 3 \
  --replication-factor 1

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --if-not-exists \
  --topic streamforge.events.dlq \
  --partitions 3 \
  --replication-factor 1
