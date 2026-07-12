# Week 1 Closure Notes

## 1. Phạm vi Tuần 1

Tuần 1 đóng ở luồng tối thiểu:

```text
Synthetic retail/payment producer
→ Apache Kafka KRaft
→ retail-payment-events
→ Kafka CLI consumer
```

Bronze raw immutable, Spark Structured Streaming, Silver, Gold, DLQ, Airflow DAG nghiệp vụ, benchmark implementation và dashboard hoàn chỉnh chưa nằm trong Definition of Done của Tuần 1.

## 2. Trạng thái theo từng ngày

| Ngày | Nội dung | Trạng thái | Bằng chứng trong repo |
|---|---|---|---|
| Ngày 1 | Version locking | Hoàn thành | `docs/version_matrix.md`, `.env.example` |
| Ngày 2 | Kafka/PostgreSQL/MinIO | Hoàn thành | `docker-compose.yml`, `scripts/init_postgres.sql`, `scripts/create_buckets.py` |
| Ngày 3 | Spark/Airflow/Streamlit skeleton | Hoàn thành ở mức skeleton | `docker-compose.yml`, `monitoring/dashboard.py`, `orchestration/` |
| Ngày 4 | Kafka topic và smoke test | Hoàn thành | `scripts/create_kafka_topics.sh`, `Makefile` |
| Ngày 5 | Event schema và data contract | Hoàn thành | `producer/schemas.py`, `docs/data_contract.md`, `docs/generated/retail_payment_event_v1.schema.json` |
| Ngày 6 | Deterministic producer và controlled bad events | Hoàn thành | `producer/event_producer.py`, `producer/bad_event_generator.py`, `tests/test_bad_events.py` |
| Ngày 7 | Verification, docs và cleanup | Hoàn thành trong working tree hiện tại | `docs/week01_notes.md`, consistency checks |

## 3. Stack và version đã khóa

| Công nghệ | Version | Ghi chú |
|---|---|---|
| Python | 3.11.15 | Runtime image `python:3.11.15-slim-bookworm` |
| Java | 17 | JVM baseline cho Kafka/Spark |
| Kafka | 4.3.1 | `apache/kafka:4.3.1` |
| Spark | 3.5.8 | Skeleton local |
| Delta Lake | 3.3.1 | Pinned for later processing stages |
| PostgreSQL | 17.10 | Metadata database local |
| Airflow | 3.2.2 | Standalone skeleton |
| Pydantic | 2.13.4 | Event contract validation |
| confluent-kafka | 2.15.0 | Python Kafka producer client |
| pytest | 9.1.1 | Unit tests |
| boto3 | 1.43.46 | MinIO/S3 smoke test |

## 4. Local infrastructure đã triển khai

Docker Compose định nghĩa các service local: Kafka, PostgreSQL, MinIO, Spark, Airflow và Streamlit.

Các service này phục vụ môi trường học tập/phát triển local, không phải cấu hình production. `docker compose ps` trong lần kiểm tra này cho thấy Kafka, MinIO và PostgreSQL đang healthy; Spark, Airflow và Streamlit đang up ở mức skeleton.

## 5. Thiết kế Kafka KRaft

Project đã chọn Apache Kafka KRaft. Local Kafka chạy một node combined broker/controller:

```text
process.roles=broker,controller
node.id=1
controller listener: CONTROLLER://:29093
host listener: localhost:9092
Docker-network listener: kafka:19092
Kafka CLI trong container: localhost:19092
```

Project sử dụng KRaft và không triển khai ZooKeeper.

## 6. Kafka topic design

Topic chính:

```text
retail-payment-events
```

Thiết kế đã kiểm tra bằng `make describe-topic`:

```text
3 partitions
replication factor 1
retention 7 ngày
cleanup policy delete
record key order_id
```

`order_id` là Kafka record key để giữ locality/order trong cùng partition. Kafka chỉ đảm bảo ordering trong cùng một partition, không đảm bảo ordering trên toàn topic.

`event_id` là khóa deduplication nghiệp vụ ở Silver trong tương lai. `offset` là vị trí record trong Kafka partition và sẽ được lưu ở Bronze để audit/replay.

## 7. PostgreSQL metadata bootstrap

`scripts/init_postgres.sql` tạo ba bảng metadata Tuần 1:

```text
pipeline_runs
data_quality_results
benchmark_runs
```

Script này được mount vào `/docker-entrypoint-initdb.d/001-init-postgres.sql`, nên PostgreSQL chỉ tự chạy khi data directory còn trống.

## 8. MinIO Community bootstrap

Object storage local dùng MinIO Community source build:

```text
MINIO_RELEASE=RELEASE.2025-10-15T17-29-55Z
MINIO_IMAGE=lakehouse-minio:RELEASE.2025-10-15T17-29-55Z
bucket=lakehouse
```

`scripts/create_buckets.py` tạo bucket nếu chưa có và chạy put/get smoke test với object `_healthcheck/day02.txt`. Đây là S3-compatible object storage cho local educational/development environment, không phải production deployment.

Một image yêu cầu license đã được loại bỏ và thay bằng MinIO Community source build.

## 9. Event schema và data contract

Schema version hiện tại là `1.0`. Các field chính:

```text
event_id
event_type
order_id
payment_id
customer_id
store_id
amount
currency
event_time
producer_time
schema_version
idempotency_key
source
```

Validation quan trọng:

```text
UUID
timezone-aware timestamps
producer_time >= event_time
amount không âm
payment events cần payment_id/amount/currency
schema version 1.0
unknown fields rejected
```

JSON Schema generated nằm ở `docs/generated/retail_payment_event_v1.schema.json`.

## 10. Deterministic controlled-event producer

Producer Ngày 6 hỗ trợ các scenario:

```text
valid
duplicate
late
malformed
negative amount
unsupported schema version
```

`data_volume` là tổng số record cuối cùng. Các rate loại trừ nhau. Với config mặc định, 100 records gồm:

```text
valid                        65
duplicate                    10
late                         10
malformed                     5
negative_amount               5
unsupported_schema_version    5
```

Semantics:

```text
duplicate giữ nguyên event_id/key/payload
late event vẫn pass schema
malformed không parse được JSON
negative amount parse JSON nhưng fail schema
unsupported version parse JSON nhưng fail schema
same seed + same config → same generated dataset
```

## 11. Logging và delivery tracking

Producer ghi structured JSON logs qua `logging_config.py`.

Delivery semantics:

```text
produce() chỉ enqueue vào local queue
poll()/flush() phục vụ callback
delivery success được đếm trong callback
flush result phải được kiểm tra
```

Producer idempotence (`enable.idempotence=true`, `acks=all`) chỉ hạn chế duplicate do transport retry. Nó không xóa controlled duplicate do application chủ động gửi để kiểm thử downstream deduplication.

## 12. Tests và verification evidence

| Check | Command | Result | Evidence/Notes |
|---|---|---|---|
| Dev dependencies | `python -m pip install -r requirements-dev.txt` | PASS | Installed pinned `pydantic`, `pytest`, `confluent-kafka`, `boto3` |
| Runtime-name cleanup | `git grep` for the removed event-streaming runtime name | PASS | No matches |
| Object-storage cleanup | `git grep` for the removed object-storage product name | PASS | No matches |
| Credential cleanup | `git grep` for the removed credential-like value | PASS | No matches |
| Makefile module command | `git grep` for the legacy script-path invocation token | PASS | No matches |
| Old topic token | `git grep` for the old underscore topic token | PASS | No matches |
| Metadata-service wording | `git grep` for the external metadata service name | PASS | Only allowed "not used" wording remains |
| Whitespace | `git diff --check` | PASS | Exit 0; only CRLF conversion warnings |
| Python syntax | `python -m py_compile` for producer, logging, schema exporter and bucket scripts | PASS | Exit 0 |
| Unit tests | `python -m pytest -q` | PASS | 25 passed |
| Bad-event tests | `make test-bad-events` | PASS | 13 passed |
| Schema export | `python -m scripts.export_event_schema` | PASS | Schema exported; generated schema diff empty |
| Compose render | `docker compose config` | PASS | Exit 0; Docker config access warning only |
| Service status | `docker compose ps` | PASS | Services up; Kafka/MinIO/PostgreSQL healthy |
| Topic describe | `make describe-topic` | PASS | Topic has 3 partitions, RF 1, `retention.ms=604800000`, `cleanup.policy=delete` |
| MinIO bucket smoke test | `python scripts/create_buckets.py` with `MINIO_*` loaded from `.env` into process env | PASS | Bucket `lakehouse` exists; put/get smoke test passed |
| Dry-run dataset | `make producer-dry-run` | PASS | 100 records generated under ignored `artifacts/day06/` |
| Fixed seed | `make producer-fixed-seed` | PASS | Same seed generated identical files |
| Different seed | `make producer-seed-difference` | PASS | Different seeds generated different files |
| Kafka producer delivery | `make producer-run` | PASS | 100 queued, 100 delivered, 0 failed, 0 undelivered |
| Kafka offset increase | `kafka-get-offsets.sh` before/after | PASS | End offsets increased by 100 |
| Kafka CLI consumer | `make producer-consume` | PASS | Consumer displayed key, partition, offset and value |

## 13. Các lỗi đã gặp và cách xử lý

| Vấn đề | Cách xử lý |
|---|---|
| Git Bash có thể chuyển `/opt/kafka` thành Windows path | Dùng `MSYS_NO_PATHCONV=1` trong Kafka CLI commands |
| MinIO volume permission có thể không tương thích sau khi đổi image | Recreate riêng MinIO volume nếu gặp permission issue |
| Airflow UI cần publish đúng port | Map host `8088` tới container `8080` |
| Airflow 3 dùng Simple Auth Manager | Compose tạo password JSON runtime trong `orchestration/config/`, file này bị ignore |
| Chạy schema exporter bằng đường dẫn file không import được package root ổn định | Dùng `python -m scripts.export_event_schema` |
| Kafka CLI deprecated `--property` | Dùng `--reader-property` và `--formatter-property` |
| Windows/PowerShell thiếu `sha256sum`, `cmp`, `mkdir -p` | Makefile dùng Python chuẩn cho fixed-seed/different-seed targets |

## 14. Security và repository hygiene

Security state hiện tại:

```text
.env không được track
Airflow password JSON không được track
.env.example chỉ dùng placeholder
generated JSONL bị ignore
không commit credential trong working tree hiện tại
```

Một giá trị giống credential từng xuất hiện trong Git history. Working tree hiện tại đã được làm sạch. Nếu giá trị đó từng được dùng thật, cần rotate credential. History không được rewrite trong task này.

## 15. Trade-offs và local limitations

Các limitation cần nhớ:

```text
Một Kafka broker
Replication factor 1
Combined broker/controller
Không high availability
Airflow standalone
Spark mới là skeleton
Streamlit mới là skeleton
MinIO chỉ dùng local
Synthetic data không đại diện hoàn toàn production
Chưa có Kafka consumer group nghiệp vụ
Chưa có Bronze raw immutable
Chưa có checkpoint/replay
Chưa có Silver/Gold/Delta processing
```

## 16. Definition of Done Tuần 1

Tuần 1 được xem là xong khi:

```text
Local stack render được bằng Docker Compose
Kafka KRaft service chạy và topic đúng config
PostgreSQL metadata schema có bootstrap script
MinIO Community bucket smoke test pass
Event schema version 1.0 có tests và generated JSON Schema
Producer deterministic sinh valid/duplicate/late/malformed/invalid-schema scenarios
Dry-run fixed seed tái lập được
Producer có Kafka publish path và delivery callback tracking
README/docs không còn mô tả runtime event-streaming thay thế
Credential-like value không còn trong working tree
```

## 17. Những phần chưa làm

Các phần chưa triển khai trong Tuần 1:

```text
Kafka consumer
Bronze writer
Spark Structured Streaming job
DLQ writer
Silver transformation
Delta Lake tables
Gold metrics
Airflow DAG nghiệp vụ
Streamlit dashboard hoàn chỉnh
CI workflow mới
benchmark implementation
Schema Registry
Kubernetes
cloud deployment
```

## 18. Handoff sang Tuần 2

Luồng cần xây tiếp:

```text
Producer
→ Kafka
→ Bronze raw immutable
```

Mục tiêu Tuần 2:

```text
consumer group
committed offset
source topic/partition/offset
raw payload
ingestion_time
processing_date
ingestion_run_id
MinIO object path
checkpoint
restart behavior
replay
```

Không triển khai các phần này trong task đóng Tuần 1.
