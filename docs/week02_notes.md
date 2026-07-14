# Ghi chú Tuần 2 — Kafka đến Bronze bất biến

## 1. Mục tiêu của tuần

Xây dựng luồng ingestion:

```text
Synthetic producer
→ Apache Kafka
→ Python consumer group
→ immutable Bronze JSONL objects trong MinIO
```

Bronze giữ raw Kafka records và technical source metadata để phục vụ audit, reconciliation, replay, debugging và xử lý Silver sau này.

Bronze không:

- validate business rule;
- loại duplicate;
- loại malformed record;
- sửa raw payload.

---

## 2. Ngày 1 — Kafka consumer group và offset

### 2.1. Cấu hình topic

| Cấu hình | Giá trị |
|---|---|
| Topic | `retail-payment-events` |
| Số partition | `3` |
| Replication factor | `1` |
| Broker local | Kafka KRaft một broker |

### 2.2. Các khái niệm đã kiểm chứng

- Offset chỉ có ý nghĩa trong phạm vi một Kafka partition.
- Các consumer cùng group chia nhau partition.
- Các group khác nhau có committed offsets độc lập.
- Committed offset là vị trí của record tiếp theo cần đọc.
- Consumer lag được tính từ log-end offset và committed offset.
- `auto.offset.reset=earliest` chỉ có tác dụng khi không tồn tại committed offset hợp lệ.

### 2.3. Thí nghiệm consumer group

- Group học tập: `bronze-learning-v1`.
- Dừng và khởi động lại cùng group để kiểm tra resume behavior.
- Dùng group thứ hai để đọc Kafka history độc lập.
- Chạy nhiều consumer trong cùng group để quan sát partition assignment và rebalance.

### 2.4. Trạng thái đã quan sát

| Partition | Committed offset | Log-end offset | Lag |
|---:|---:|---:|---:|
| 0 | 194 | 194 | 0 |
| 1 | 150 | 150 | 0 |
| 2 | 160 | 160 | 0 |

### 2.5. Quyết định cho Bronze

| Quyết định | Giá trị |
|---|---|
| Consumer group | Group ID ổn định |
| `enable.auto.commit` | `false` |
| `enable.auto.offset.store` | `false` |
| `auto.offset.reset` | `earliest` |
| Delivery semantics | At-least-once |
| Điểm commit | Chỉ sau khi Bronze write bền vững thành công |

---

## 3. Ngày 2 — Python raw Kafka consumer

### 3.1. Cấu hình

| Cấu hình | Giá trị |
|---|---|
| Client | `confluent-kafka==2.15.0` |
| Topic | `retail-payment-events` |
| Bootstrap server từ Windows host | `localhost:9092` |
| Main Bronze group | `bronze-ingestion-day03-v1` |
| `auto.offset.reset` | `earliest` |
| `enable.auto.commit` | `false` |
| `enable.auto.offset.store` | `false` |

### 3.2. Các raw field được lấy từ Kafka

- `key`;
- `value`;
- `topic`;
- `partition`;
- `offset`;
- Kafka timestamp type và timestamp;
- headers.

### 3.3. Hành vi consumer

- Kafka key và value được giữ ở kiểu `bytes | None`.
- Raw consumer không gọi `json.loads()` trên message value.
- Malformed JSON không làm consumer crash và không bị mất khỏi ingestion.
- Callback assign, revoke và lost được log.
- `consumer.close()` được gọi khi shutdown.
- Không commit offset trước khi Bronze writer xác nhận object đã được ghi bền vững.

### 3.4. Kết quả kiểm tra

- Consumer kết nối thành công tới `localhost:9092`.
- Consumer subscribe đủ ba partition.
- Consumer đọc được valid, duplicate, late, malformed, negative amount và unsupported schema version.
- No-commit test chứng minh record chưa commit được đọc lại sau restart.

---

## 4. Ngày 3 — Kafka đến MinIO Bronze raw

### 4.1. Cấu hình lưu trữ

| Cấu hình | Giá trị |
|---|---|
| Endpoint | `http://localhost:9000` |
| Bucket | `lakehouse` |
| Prefix development | `bronze/events/_unpartitioned` |
| Object format | JSON Lines |
| Content type | `application/x-ndjson` |
| Mã hóa raw bytes | Base64 |

### 4.2. Object key development

```text
bronze/events/_unpartitioned/
topic=<topic>/
partition=<partition>/
offsets=<start-offset>-<end-offset>.jsonl
```

Một object chỉ chứa record của một Kafka partition.

Object key có tính deterministic theo:

```text
topic
+ partition
+ start_offset
+ end_offset
```

### 4.3. Trình tự ghi và commit

1. Poll Kafka records.
2. Group batch theo Kafka partition.
3. Mã hóa raw key/value thành Base64 trong JSONL envelope.
4. Upload một object cho mỗi partition xuất hiện trong batch.
5. Kiểm tra `ContentLength` và checksum metadata.
6. Commit `max_source_offset + 1` cho từng partition đã ghi thành công.

Custom SHA-256 metadata cho biết checksum mong đợi đã được lưu. Muốn kiểm tra độc lập checksum của body đang lưu, cần tải body hoặc dùng checksum response được object store hỗ trợ.

### 4.4. Bằng chứng reconciliation

| Partition | Bronze offset coverage | Records | Objects | Kafka current offset | Kafka log-end offset | Lag |
|---:|---|---:|---:|---:|---:|---:|
| 0 | `0–383` | 384 | 5 | 384 | 384 | 0 |
| 1 | `0–299` | 300 | 4 | 300 | 300 | 0 |
| 2 | `0–319` | 320 | 4 | 320 | 320 | 0 |
| **Tổng** | — | **1004** | **13** | — | — | **0** |

### 4.5. Assertions

- Kafka source records: `1004`.
- Bronze JSONL rows: `1004`.
- Missing source offsets: `0`.
- Overlapping source ranges: `0`.
- Consumer lag sau ingestion: `0`.
- Kết quả: **PASS**.

### 4.6. Failure invariant

```text
Bronze write hoặc verification thất bại
→ không commit Kafka offset
→ source records vẫn được đọc lại sau restart
```

Dừng MinIO trước khi writer khởi động chỉ kiểm tra startup failure. Muốn kiểm chứng đầy đủ, cần inject failure sau khi đã poll record nhưng trước khi object được persist.

### 4.7. Giới hạn cuối Ngày 3

- Historical Day 3 rows chưa có ingestion metadata mới.
- Chưa partition theo `processing_date`.
- Chưa có replay command.
- Chưa có Silver validation hoặc deduplication.
- Rebalance-aware batch flushing được hoãn đến hardening.

---

## 5. Ngày 4 — Bronze metadata và audit context

### 5.1. Source metadata

- `source_record_id`;
- `source_topic`;
- `source_partition`;
- `source_offset`;
- `kafka_timestamp_type`;
- `kafka_timestamp_ms`;
- headers.

### 5.2. Ingestion metadata

- `ingestion_run_id`;
- `ingestion_batch_number`;
- `ingestion_batch_id`;
- `ingestion_time`.

### 5.3. Best-effort event metadata

- `payload_parse_status`;
- `event_id`;
- `event_type`;
- `event_time`;
- `producer_time`;
- `schema_version`.

### 5.4. Chính sách thời gian

- Ingestion timestamp là timezone-aware UTC.
- Timestamp được serialize theo ISO 8601 và kết thúc bằng `Z`.
- `ingestion_time` do Bronze pipeline tạo.
- `event_time`, `producer_time` và Kafka timestamp là các khái niệm độc lập.

### 5.5. Chính sách parse

Bronze parse theo best-effort:

- giữ malformed JSON;
- giữ invalid UTF-8;
- không reject negative amount;
- không reject unsupported schema version;
- raw Kafka key/value là dữ liệu có tính thẩm quyền;
- business validation được để sang Silver.

### 5.6. Chính sách định danh

```text
source_record_id = <topic>:<partition>:<offset>
```

`source_record_id` định danh Kafka record. Nó khác với `event_id` vì controlled duplicate có thể cùng `event_id` nhưng nằm ở các Kafka offset khác nhau.

### 5.7. Bằng chứng đã xác minh

Bằng chứng được lấy từ lần chạy nghiệm thu cuối ngày `2026-07-14`.

| Metric | Kết quả |
|---|---|
| Ingestion run ID | `bronze-week02-final-2026-07-14` |
| Ingestion batch ID | `bronze-week02-final-2026-07-14-batch-000001` |
| Ingestion time | `2026-07-14T07:58:59.458284Z` |
| Record count | `100` |
| Object count | `3` |
| `parsed_object` count | `95` |
| `invalid_json` count | `5` |
| Duplicate event IDs | `10` |
| Duplicate record excess | `10` |
| Kafka lag sau run | `0` |
| Kết quả | **PASS** |

Các field metadata được kiểm tra trong từng row gồm `ingestion_run_id`, `processing_date`, `source_partition`, `source_offset` và `payload_parse_status`.

### 5.8. Ghi chú tương thích

Object được tạo trước Ngày 4 dùng Bronze envelope đời trước và chưa có đủ ingestion metadata. Cuối Tuần 2 cần một clean end-to-end run để tạo evidence cuối cùng.

---

## 6. Ngày 5 — Partition theo `processing_date`

### 6.1. Chính sách ngày xử lý

| Quy tắc | Giá trị |
|---|---|
| Timezone | UTC |
| Field nguồn | `ingestion_time` |
| Định dạng | `YYYY-MM-DD` |
| Có dùng business event time không? | Không |

`processing_date` thể hiện ngày Bronze pipeline ingest dữ liệu.

Không suy ra từ:

- `event_time`;
- `producer_time`;
- ngày local của Windows;
- Kafka record timestamp.

### 6.2. Object layout

```text
bronze/events/
processing_date=<YYYY-MM-DD>/
topic=<kafka-topic>/
partition=<kafka-partition>/
offsets=<start-offset>-<end-offset>.jsonl
```

Các segment `key=value` là prefix phục vụ partition discovery. Chúng không phải thư mục vật lý theo nghĩa filesystem truyền thống.

### 6.3. Bất biến nhất quán

Với mỗi partitioned object:

```text
processing_date trong object key
=
processing-date trong object metadata
=
processing_date trong mọi JSONL row
```

### 6.4. Quyết định thiết kế

`ingestion_run_id` được giữ trong:

- row-level metadata;
- object-level metadata;
- structured logs.

Không dùng `ingestion_run_id` làm physical storage partition vì cardinality cao và dễ tạo nhiều prefix nhỏ.

### 6.5. Historical objects

Các object trước Ngày 5 vẫn nằm tại:

```text
bronze/events/_unpartitioned/
```

Đây là development artifacts, bị loại khỏi replay theo `processing_date`.

Chỉ xóa khi:

1. partitioned path đã được kiểm tra;
2. có clean final Week 2 run;
3. evidence cần thiết đã được giữ lại.

### 6.6. Giới hạn idempotency

Object key deterministic theo:

```text
processing_date
+ topic
+ partition
+ offset range
```

Nếu cùng uncommitted Kafka range bị retry vào ngày UTC khác, at-least-once có thể tạo object thứ hai dưới `processing_date` khác.

Downstream phải dùng source coordinates để reconciliation và deduplication.

### 6.7. Bằng chứng đã xác minh

| Metric | Kết quả |
|---|---|
| Processing date | `2026-07-14` |
| Ingestion run ID | `bronze-week02-final-2026-07-14` |
| Object count | `3` |
| Record count | `100` |
| Path/metadata/row consistency | **PASS** |
| Kafka committed-offset delta | `100` |
| Kafka lag sau ingestion | `0` |

Phân bố source range:

| Partition | Offset range | Records | Objects |
|---:|---|---:|---:|
| 0 | `2550–2587` | 38 | 1 |
| 1 | `2010–2039` | 30 | 1 |
| 2 | `2144–2175` | 32 | 1 |
| **Tổng** | — | **100** | **3** |

Các object key, object metadata và row metadata đều sử dụng cùng `processing_date=2026-07-14`.

---

## 7. Ngày 6 — Replay theo `processing_date`

### 7.1. Phạm vi

| Nội dung | Chính sách |
|---|---|
| Replay selector | Một `processing_date` UTC |
| Nguồn | Partitioned MinIO Bronze objects |
| Output | Local deterministic JSONL |
| Reset Kafka offset | Không |
| Publish lại Kafka | Không |
| Thay đổi Bronze | Không |

### 7.2. Các kiểm tra

- object-key layout;
- object metadata;
- offset overlap;
- offset gap;
- row source coordinates;
- Base64 fields;
- output SHA-256;
- tính deterministic của output.

### 7.3. Bằng chứng đã xác minh

Bằng chứng replay dưới đây được chụp **trước** failure-injection test, tại thời điểm partition ngày `2026-07-14` có đúng ba object của final ingestion run.

| Metric | Kết quả |
|---|---|
| Processing date | `2026-07-14` |
| Replay run ID A | `replay-week02-final-2026-07-14-a` |
| Replay run ID B | `replay-week02-final-2026-07-14-b` |
| Source object count | `3` |
| Replayed record count | `100` |
| Output size | `131396` bytes |
| Output path A | `artifacts/replay/week02-final-a.jsonl` |
| Output path B | `artifacts/replay/week02-final-b.jsonl` |
| Output SHA-256 | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |
| Kafka offsets trước replay | P0=`2588`, P1=`2040`, P2=`2176` |
| Kafka offsets sau replay | Không đổi |
| Bronze object count trước replay | `3` |
| Bronze object count sau replay | `3` |
| Kết quả | **PASS** |

### 7.4. Determinism test

| Kiểm tra | Kết quả |
|---|---|
| SHA-256 output lần 1 | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |
| SHA-256 output lần 2 | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |
| Hai hash giống nhau | **PASS** |

### 7.5. Negative tests đã xác minh

- Replay ngày `2000-01-01` thất bại bằng `FileNotFoundError`.
- Output đã tồn tại bị từ chối bằng `FileExistsError` khi không có `--overwrite`.
- `--overwrite` ghi lại thành công và vẫn tạo cùng checksum.
- Filter `--partition 2` tạo `1` object, `32` records và checksum `00ab8cb3e174e530ed6513073943239f868f55ba23d39d953edbc28bf5e78c27`.

### 7.5. Giới hạn hiện tại

- Một `processing_date` cho mỗi command.
- Chưa replay theo `ingestion_run_id`.
- Chưa replay theo arbitrary Kafka offset range.
- Chưa date-range backfill.
- Replay output là local JSONL, chưa phải Spark/Delta target.
- Historical `_unpartitioned` objects bị loại khỏi replay.

---

## 8. Ngày 7 — Nghiệm thu và đóng Tuần 2

### 8.1. Trạng thái code và test

| Kiểm tra | Kết quả |
|---|---|
| Python compile | **PASS** |
| Test riêng phần Kafka/Bronze/replay | `38 passed in 1.02s` |
| Toàn bộ test suite | `63 passed in 0.65s` |
| Kiểm tra cuối sau cập nhật | `63 passed in 0.60s` |
| `docker compose config` | **PASS** |
| Kafka | Running, healthy |
| MinIO | Running, healthy |
| PostgreSQL | Running, healthy |
| Spark | Running |
| Airflow | Running |
| Streamlit | Running |

Commit nền trước khi cập nhật tài liệu:

```text
d8de484 feat(bronze): replay raw events by processing date
```

### 8.2. Kafka topic và baseline

| Thuộc tính | Kết quả |
|---|---|
| Topic | `retail-payment-events` |
| Partitions | `3` |
| Replication factor | `1` |
| Retention | `604800000 ms` |
| Cleanup policy | `delete` |
| Baseline committed offsets | P0=`2550`, P1=`2010`, P2=`2144` |
| Baseline total committed | `6704` |
| Baseline lag | `0` |

### 8.3. Final producer run

| Metric | Kết quả |
|---|---|
| Producer run ID | `producer-590820bd-5ae9-4914-9871-cf89ce50a38f` |
| Random seed | `42` |
| Event count | `100` |
| Valid | `65` |
| Duplicate | `10` |
| Late | `10` |
| Malformed | `5` |
| Negative amount | `5` |
| Unsupported schema version | `5` |
| Delivery success | `100` |
| Delivery failure | `0` |
| Undelivered after flush | `0` |
| Status | **success** |

Sau producer, lag được phân bố:

```text
P0 = 38
P1 = 30
P2 = 32
Tổng = 100
```

### 8.4. Final ingestion run

| Metric | Kết quả |
|---|---|
| Processing date | `2026-07-14` |
| Ingestion run ID | `bronze-week02-final-2026-07-14` |
| Batch count | `1` |
| Bronze record count | `100` |
| Bronze object count | `3` |
| `parsed_object` | `95` |
| `invalid_json` | `5` |
| Duplicate event IDs | `10` |
| Duplicate record excess | `10` |
| Committed-offset delta | `100` |
| Total committed trước | `6704` |
| Total committed sau | `6804` |
| Final consumer lag | `0` |
| Status | **PASS** |

Committed offsets sau final ingestion:

```text
P0 = 2588
P1 = 2040
P2 = 2176
```

### 8.5. Replay verification

| Metric | Kết quả |
|---|---|
| Replay processing date | `2026-07-14` |
| Source object count | `3` |
| Replayed record count | `100` |
| Output size | `131396` bytes |
| Output SHA-256 A | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |
| Output SHA-256 B | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |
| Hashes match | **PASS** |
| Kafka offsets unchanged | **PASS** |
| Bronze object count unchanged | **PASS** |
| Missing-date negative test | **PASS** |
| Existing-output protection | **PASS** |
| Partition filter | **PASS** |

> Snapshot replay ở trên được tạo trước failure-injection test. Sau failure test và recovery, cùng `processing_date=2026-07-14` có thêm object và record. Vì vậy replay lại toàn bộ ngày ở trạng thái hiện tại sẽ tạo count/checksum khác với snapshot `3 objects / 100 records` nêu trên.

### 8.6. Failure injection và recovery

Failure test thực tế diễn ra theo hai giai đoạn:

1. Batch đầu tiên gồm `100` records được ghi và commit thành công.
2. MinIO bị dừng trong lần flush tiếp theo; writer báo `EndpointConnectionError` và không commit phần dữ liệu đang thất bại.
3. Recovery chạy lại cùng consumer group, đọc lại `200` records chưa commit, ghi thành `2` batch và `4` objects.
4. Consumer group kết thúc ở trạng thái lag bằng `0`.

Committed offsets sau batch đầu của failure scenario:

```text
P0 = 2626
P1 = 2070
P2 = 2208
```

Recovery bắt đầu từ đúng các vị trí trên và kết thúc tại:

```text
P0 = 2702
P1 = 2130
P2 = 2272
LAG = 0
```

Kết quả chứng minh:

```text
output write thất bại
→ phần dữ liệu chưa được commit
→ restart cùng group đọc lại dữ liệu đó
→ ghi thành công
→ commit
```

Lưu ý: failure được inject sau khi một batch trước đó đã commit thành công; vì vậy đây không phải tình huống “toàn bộ failure run chưa commit”.

### 8.7. Kiểm tra bảo mật và repository

| Kiểm tra | Kết quả |
|---|---|
| `.env` được Git track | Không |
| `simple_auth_manager_passwords.json` được Git track | Không |
| `artifacts/replay/` bị ignore | Có |
| `artifacts/week02-final/` bị ignore | Có |
| Secret trong staged diff | Không phát hiện |
| `BRONZE_PREFIX` | `bronze/events` |
| `TODO/FIXME/NOT VERIFIED` | Không phát hiện |

### 8.8. Cảnh báo bảo mật

Lệnh `docker compose config` render cấu hình sau khi resolve biến môi trường, nên output đầy đủ có thể chứa password và credential. Trong lần nghiệm thu, output này đã hiển thị các giá trị nhạy cảm.

Cần thực hiện:

1. đổi toàn bộ password local đã bị lộ;
2. không lưu hoặc chia sẻ output đầy đủ của `docker compose config`;
3. khi chỉ cần validate cấu hình, dùng:

```bash
docker compose config --quiet
```

4. cân nhắc chuyển password sang Docker Compose secrets ở giai đoạn hardening.

### 8.9. Kết luận Tuần 2

Tuần 2 đã hoàn thành luồng:

```text
Producer
→ Kafka
→ raw consumer
→ immutable Bronze JSONL
→ ingestion metadata
→ processing-date partition
→ commit offset sau durable write
→ read-only deterministic replay
```

Các bảo đảm đã có bằng chứng:

- raw Kafka bytes được giữ bằng Base64;
- source coordinates được giữ lại;
- malformed và duplicate records không bị mất;
- commit offset sử dụng `last_processed_offset + 1`;
- lag cuối cùng bằng `0`;
- replay không thay đổi Kafka offsets hoặc Bronze object count;
- replay deterministic khi source snapshot không đổi;
- failure trước commit cho phép dữ liệu được đọc lại.

### 8.10. Giới hạn chuyển sang Tuần 3

- At-least-once, không claim exactly-once.
- Retry khác ngày có thể tạo cùng source range dưới processing date khác.
- Replay chỉ hỗ trợ một processing date cho mỗi command.
- Snapshot checksum phụ thuộc trạng thái object của ngày tại thời điểm replay.
- Chưa có Silver validation, deduplication, late-event classification hoặc DLQ.
- Rebalance-aware flushing và failure matrix rộng hơn được hoãn sang hardening.
