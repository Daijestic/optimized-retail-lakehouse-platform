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
	python -m scripts.export_event_schema

.PHONY: \
	test \
	test-bad-events \
	producer-dry-run \
	producer-fixed-seed \
	producer-seed-difference \
	producer-run \
	producer-consume

test:
	python -m pytest -q

test-bad-events:
	python -m pytest -q tests/test_bad_events.py

producer-dry-run:
	python -c "from pathlib import Path; Path('artifacts/day06').mkdir(parents=True, exist_ok=True)"
	python -m producer.event_producer --dry-run --seed 42 --data-volume 100 --output artifacts/day06/events-seed-42.jsonl

producer-fixed-seed:
	python -c "from pathlib import Path; Path('artifacts/day06').mkdir(parents=True, exist_ok=True)"
	python -m producer.event_producer --dry-run --seed 42 --data-volume 100 --output artifacts/day06/events-a.jsonl
	python -m producer.event_producer --dry-run --seed 42 --data-volume 100 --output artifacts/day06/events-b.jsonl
	python -c "from pathlib import Path; import hashlib; paths=['artifacts/day06/events-a.jsonl','artifacts/day06/events-b.jsonl']; [print(hashlib.sha256(Path(path).read_bytes()).hexdigest() + '  ' + path) for path in paths]"
	python -c "from pathlib import Path; import sys; same=Path('artifacts/day06/events-a.jsonl').read_bytes() == Path('artifacts/day06/events-b.jsonl').read_bytes(); print('PASS: same seed generated identical files' if same else 'FAIL: same seed generated different files'); sys.exit(0 if same else 1)"

producer-seed-difference:
	python -c "from pathlib import Path; Path('artifacts/day06').mkdir(parents=True, exist_ok=True)"
	python -m producer.event_producer --dry-run --seed 42 --data-volume 100 --output artifacts/day06/events-seed-42.jsonl
	python -m producer.event_producer --dry-run --seed 43 --data-volume 100 --output artifacts/day06/events-seed-43.jsonl
	python -c "from pathlib import Path; import hashlib; paths=['artifacts/day06/events-seed-42.jsonl','artifacts/day06/events-seed-43.jsonl']; [print(hashlib.sha256(Path(path).read_bytes()).hexdigest() + '  ' + path) for path in paths]"
	python -c "from pathlib import Path; import sys; different=Path('artifacts/day06/events-seed-42.jsonl').read_bytes() != Path('artifacts/day06/events-seed-43.jsonl').read_bytes(); print('PASS: different seeds generated different files' if different else 'FAIL: different seeds generated identical files'); sys.exit(0 if different else 1)"

producer-run: create-topics
	python -m producer.event_producer --seed 42 --data-volume 100 --bootstrap-servers localhost:9092 --topic retail-payment-events

producer-consume:
	MSYS_NO_PATHCONV=1 docker compose exec -T kafka \
		/opt/kafka/bin/kafka-console-consumer.sh \
		--bootstrap-server localhost:19092 \
		--topic retail-payment-events \
		--from-beginning \
		--max-messages 20 \
		--timeout-ms 10000 \
		--formatter-property "print.key=true" \
		--formatter-property "print.timestamp=true" \
		--formatter-property "print.partition=true" \
		--formatter-property "print.offset=true"

.PHONY: \
	consumer-run \
	consumer-run-continuous \
	consumer-group-describe \
	test-kafka-consumer

consumer-run:
	python -m ingestion.kafka_consumer \
		--bootstrap-servers localhost:9092 \
		--topic retail-payment-events \
		--group-id bronze-ingestion-v1 \
		--client-id bronze-consumer-local \
		--max-messages 20 \
		--idle-timeout-seconds 10

consumer-run-continuous:
	python -m ingestion.kafka_consumer \
		--bootstrap-servers localhost:9092 \
		--topic retail-payment-events \
		--group-id bronze-ingestion-v1 \
		--client-id bronze-consumer-local \
		--max-messages 0 \
		--idle-timeout-seconds 0

consumer-group-describe:
	MSYS_NO_PATHCONV=1 docker compose exec -T kafka \
		/opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:19092 \
		--describe \
		--group bronze-ingestion-v1

test-kafka-consumer:
	python -m pytest -q tests/test_kafka_consumer.py

.PHONY: \
	bronze-run \
	bronze-run-continuous \
	test-bronze-writer \
	consumer-group-bronze

bronze-run:
	python -m ingestion.bronze_writer \
		--bootstrap-servers localhost:9092 \
		--topic retail-payment-events \
		--group-id bronze-ingestion-v1 \
		--client-id bronze-writer-local \
		--batch-size 100 \
		--batch-wait-seconds 5 \
		--max-messages 100 \
		--idle-timeout-seconds 15

bronze-run-continuous:
	python -m ingestion.bronze_writer \
		--bootstrap-servers localhost:9092 \
		--topic retail-payment-events \
		--group-id bronze-ingestion-v1 \
		--client-id bronze-writer-local \
		--batch-size 100 \
		--batch-wait-seconds 5 \
		--max-messages 0 \
		--idle-timeout-seconds 0

test-bronze-writer:
	python -m pytest -q tests/test_bronze_writer.py

consumer-group-bronze:
	MSYS_NO_PATHCONV=1 docker compose exec -T kafka \
		/opt/kafka/bin/kafka-consumer-groups.sh \
		--bootstrap-server localhost:19092 \
		--describe \
		--group bronze-ingestion-v1

.PHONY: \
	replay-bronze-dry-run \
	replay-bronze \
	test-replay-bronze

replay-bronze-dry-run:
	python -m scripts.replay_bronze \
		--processing-date "$(PROCESSING_DATE)" \
		--dry-run

replay-bronze:
	python -m scripts.replay_bronze \
		--processing-date "$(PROCESSING_DATE)"

test-replay-bronze:
	python -m pytest -q \
		tests/test_replay_bronze.py