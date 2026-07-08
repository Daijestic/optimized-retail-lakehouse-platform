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