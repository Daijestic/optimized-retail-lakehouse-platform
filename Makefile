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