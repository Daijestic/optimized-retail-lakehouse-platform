#!/usr/bin/env bash

set -Eeuo pipefail

# Ngăn Git Bash trên Windows chuyển /opt/kafka thành đường dẫn Windows.
export MSYS_NO_PATHCONV=1

TOPIC="${KAFKA_TOPIC:-retail-payment-events}"
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
REPLICATION_FACTOR="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_TOPIC_RETENTION_MS:-604800000}"
BOOTSTRAP_SERVER="${KAFKA_INTERNAL_BOOTSTRAP_SERVER:-localhost:19092}"

KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"
KAFKA_CONFIGS="/opt/kafka/bin/kafka-configs.sh"

echo "Checking Kafka broker..."

docker compose exec -T kafka \
  /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  >/dev/null

echo "Creating topic if it does not exist..."
echo "Topic: ${TOPIC}"
echo "Partitions: ${PARTITIONS}"
echo "Replication factor: ${REPLICATION_FACTOR}"
echo "Retention: ${RETENTION_MS} ms"

docker compose exec -T kafka \
  "${KAFKA_TOPICS}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --create \
  --if-not-exists \
  --topic "${TOPIC}" \
  --partitions "${PARTITIONS}" \
  --replication-factor "${REPLICATION_FACTOR}" \
  --config "retention.ms=${RETENTION_MS}" \
  --config "cleanup.policy=delete"

# Đồng bộ lại config nếu topic đã tồn tại từ lần chạy trước.
docker compose exec -T kafka \
  "${KAFKA_CONFIGS}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --alter \
  --entity-type topics \
  --entity-name "${TOPIC}" \
  --add-config "retention.ms=${RETENTION_MS},cleanup.policy=delete"

echo
echo "Topic description:"

docker compose exec -T kafka \
  "${KAFKA_TOPICS}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --topic "${TOPIC}"

echo
echo "Topic configuration:"

docker compose exec -T kafka \
  "${KAFKA_CONFIGS}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --describe \
  --entity-type topics \
  --entity-name "${TOPIC}"

echo
echo "Kafka topic initialization completed."