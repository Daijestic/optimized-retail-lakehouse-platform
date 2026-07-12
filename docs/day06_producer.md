# Day 6 — Deterministic Kafka Event Producer

## Mục tiêu

Producer sinh controlled retail/payment events và gửi vào:

```text
retail-payment-events
```

Các scenario được hỗ trợ:

- valid;
- duplicate;
- late;
- malformed JSON;
- negative amount;
- unsupported schema version.

## Data-volume semantics

`data_volume` là tổng số record cuối cùng được sinh ra.

Các scenario rates loại trừ nhau:

- `duplicate_rate`
- `late_event_rate`
- `malformed_rate`
- `negative_amount_rate`
- `unsupported_schema_version_rate`

Tổng các rate phải nhỏ hơn hoặc bằng `1.0`.

Số lượng từng scenario được tính bằng:

```python
int(data_volume * scenario_rate)
```

Phần còn lại là valid records.

Ví dụ với `data_volume=100`:

```text
valid                        65
duplicate                    10
late                         10
malformed                     5
negative_amount               5
unsupported_schema_version    5
```

## Supported scenarios

### 3.1. Valid

- parse được JSON;
- pass `RetailPaymentEvent`;
- dùng `order_id` làm Kafka key.

### 3.2. Duplicate

Duplicate giữ nguyên:

- Kafka key;
- Kafka value;
- `event_id`;
- `idempotency_key`;
- `order_id`.

Duplicate được đặt sau original trong generated sequence.

### 3.3. Late

Late event vẫn pass schema.

Policy:

```text
producer_time - event_time > 30 phút
```

Late không đồng nghĩa malformed hoặc invalid schema.

### 3.4. Malformed

Malformed payload không parse được JSON.

Ví dụ:

```json
{"event_id":"..."
```

### 3.5. Negative amount

Payload parse được JSON nhưng:

```text
amount = "-1000.00"
```

nên fail business schema.

### 3.6. Unsupported schema version

Payload parse được JSON nhưng:

```text
schema_version = "99.0"
```

nên fail schema version `1.0`.

## Fixed-seed reproducibility

Generator dùng:

```python
random.Random(random_seed)
```

Không dùng trong generated dataset:

- `uuid.uuid4()`
- `datetime.now()`
- global random state

Event IDs được tạo bằng seeded random bits.

Timestamp được tạo từ `base_event_time` cố định.

Yêu cầu:

```text
cùng config + cùng seed
→ cùng sequence
→ cùng Kafka key
→ cùng Kafka value
```

`run_id` không thuộc dataset và được tạo mới ở mỗi execution.

## Dry-run

```bash
python -m producer.event_producer \
  --dry-run \
  --seed 42 \
  --data-volume 100 \
  --output artifacts/day06/events-a.jsonl
```

Dry-run không kết nối Kafka.

Mỗi JSONL row chứa:

- `sequence`
- `scenario`
- `key`
- `value`
- `event_id`
- `source_event_id`

## Fixed-seed verification

```bash
make producer-fixed-seed
```

Hai SHA-256 phải giống nhau.

Kiểm tra seed khác:

```bash
make producer-seed-difference
```

Hai file phải khác nhau.

## Unit tests

```bash
python -m pytest \
  -q \
  tests/test_event_schema.py \
  tests/test_bad_events.py
```

Tests kiểm tra:

- same seed;
- different seed;
- exact scenario counts;
- valid schema;
- duplicate identity;
- late threshold;
- malformed JSON;
- negative amount;
- unsupported schema version;
- invalid rate configuration.

## Kafka publishing

Khởi động Kafka:

```bash
docker compose up -d
```

Tạo topic:

```bash
make create-topics
```

Gửi 100 records:

```bash
make producer-run
```

Đọc thử:

```bash
make producer-consume
```

Producer chạy trên Windows host nên bootstrap server là:

```text
localhost:9092
```

Kafka CLI chạy trong Kafka container nên bootstrap server là:

```text
localhost:19092
```

## Delivery callbacks, poll và flush

`produce()` chỉ enqueue message vào local producer queue.

Delivery thành công chỉ được ghi nhận qua delivery callback.

Producer gọi:

```python
poll(0)
```

trong vòng lặp và:

```python
flush(timeout)
```

trước khi kết thúc.

Run chỉ được coi là thành công khi:

```text
delivery_failure_count == 0
undelivered_after_flush == 0
delivery_success_count == queued_count
```

## Producer idempotence

Producer bật:

```properties
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
retries=10
```

Producer idempotence tránh duplicate do transport retry.

Nó không loại bỏ controlled duplicate do application chủ động gửi.

Application-level deduplication theo `event_id` sẽ được thực hiện tại Silver layer.

## Logging

Run summary gồm:

- `run_id`
- `random_seed`
- `event_count`
- `valid`
- `duplicate`
- `late`
- `malformed`
- `negative_amount`
- `unsupported_schema_version`
- `queued_count`
- `delivery_success_count`
- `delivery_failure_count`
- `undelivered_after_flush`
- `duration_seconds`
- `status`

Không log password, credential hoặc toàn bộ environment.

## Commands

```bash
make test
make test-bad-events
make producer-dry-run
make producer-fixed-seed
make producer-seed-difference
make producer-run
make producer-consume
make export-event-schema
```

## Scope limitations

Ngày 6 chỉ hoàn thiện:

```text
Producer → Kafka
```

Chưa thực hiện:

- Kafka consumer group;
- Bronze writer;
- MinIO event storage;
- DLQ;
- Silver validation;
- Spark watermark;
- deduplication.

Các phần này thuộc các tuần tiếp theo.
