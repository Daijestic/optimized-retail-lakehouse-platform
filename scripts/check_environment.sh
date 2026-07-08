#!/usr/bin/env bash

set -u

has_error=0

print_section() {
  printf "\n========== %s ==========\n" "$1"
}

run_check() {
  local name="$1"
  shift

  printf "\n[%s]\n" "$name"

  if "$@"; then
    printf "Status: OK\n"
  else
    printf "Status: FAILED\n"
    has_error=1
  fi
}

print_section "Operating environment"

run_check "Git" git --version
run_check "Docker client and server" docker version
run_check "Docker Compose" docker compose version
run_check "Docker daemon information" docker info

print_section "Host runtimes"

if command -v python >/dev/null 2>&1; then
  run_check "Host Python" python --version
else
  echo "Host Python: not installed or not on PATH"
  echo "This is acceptable when Python is executed through Docker."
fi

if command -v java >/dev/null 2>&1; then
  run_check "Host Java" java -version
else
  echo "Host Java: not installed or not on PATH"
  echo "This is acceptable when Kafka and Spark run in containers."
fi

print_section "Pinned Docker images"

run_check \
  "Python container" \
  docker run --rm python:3.11.15-slim-bookworm python --version

run_check \
  "Inspect Kafka image" \
  docker image inspect apache/kafka:4.3.1

print_section "Result"

if [ "$has_error" -eq 0 ]; then
  echo "Environment checks passed."
  exit 0
else
  echo "One or more mandatory checks failed."
  exit 1
fi