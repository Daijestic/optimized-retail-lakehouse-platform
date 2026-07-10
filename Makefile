SHELL := bash

.PHONY: check-env pull-core-images show-images

check-env:
	bash scripts/check_environment.sh

pull-core-images:
	docker pull python:3.11.15-slim-bookworm
	docker pull apache/kafka:4.3.1

show-images:
	docker image ls python
	docker image ls apache/kafka

reset-local-data:
	@echo "WARNING: this deletes Kafka, PostgreSQL, and MinIO local data."
	docker compose down -v

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

reset:
	docker compose down -v

smoke-test:
	python scripts/create_buckets.py

restart:
	docker compose down
	docker compose up -d

build:
	docker compose build


.PHONY: create-topics describe-topic kafka-produce-sample kafka-consume-sample kafka-smoke

create-topics:
	bash scripts/create_kafka_topics.sh

describe-topic:
	MSYS_NO_PATHCONV=1 docker compose exec -T kafka \
		/opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:19092 \
		--describe \
		--topic retail-payment-events

kafka-produce-sample:
	printf '%s\n' \
	'order-1001|{"event_id":"evt-day04-001","event_type":"payment_authorized","order_id":"order-1001","payment_id":"payment-1001","amount":150000,"currency":"VND","schema_version":"1.0"}' \
	| MSYS_NO_PATHCONV=1 docker compose exec -T kafka \
		/opt/kafka/bin/kafka-console-producer.sh \
		--bootstrap-server localhost:19092 \
		--topic retail-payment-events \
		--reader-property "parse.key=true" \
		--reader-property "key.separator=|"

kafka-consume-sample:
	MSYS_NO_PATHCONV=1 docker compose exec -T kafka \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 \
		--topic retail-payment-events \
		--from-beginning \
		--max-messages 1 \
		--formatter-property "print.key=true" \
		--formatter-property "print.partition=true" \
		--formatter-property "print.offset=true"

kafka-smoke: create-topics kafka-produce-sample kafka-consume-sample

.PHONY: install-dev test-event-schema export-event-schema

install-dev:
	python -m pip install -r requirements-dev.txt

test-event-schema:
	python -m pytest -q tests/test_event_schema.py

export-event-schema:
	python -m scripts/export_event_schema.py