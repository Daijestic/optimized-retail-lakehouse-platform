# Tầng Bronze

## 1. Mục đích

Tầng Bronze lưu giữ các Kafka record ở dạng thô, có thể kiểm tra và phát lại, nhằm phục vụ:

- kiểm toán dữ liệu;
- đối soát từ nguồn Kafka đến object storage;
- điều tra lỗi từ producer hoặc hệ thống upstream;
- replay và xử lý lại ở downstream;
- chuẩn bị đầu vào cho Silver validation, deduplication, xử lý late event và phân luồng DLQ.

Bronze là tầng ingestion kỹ thuật. Đây **không phải** tầng dữ liệu nghiệp vụ đã được làm sạch.

---

## 2. Luồng dữ liệu

```text
Synthetic producer
→ Kafka topic: retail-payment-events
→ Python raw consumer
→ Bronze JSONL objects trong MinIO
→ commit Kafka offset đồng bộ
```

Consumer sử dụng ngữ nghĩa xử lý **at-least-once**:

```text
ghi và kiểm tra Bronze objects thành công
→ commit các Kafka offset tiếp theo
```

Nếu ghi object hoặc bước kiểm tra thất bại trước khi commit, các record vẫn có thể được đọc lại sau khi consumer khởi động lại.

---

## 3. Các bảo đảm cốt lõi

- Kafka key và value được giữ nguyên dưới dạng bytes, sau đó mã hóa Base64 để lưu trong JSONL.
- Mỗi record giữ lại `source_topic`, `source_partition` và `source_offset`.
- Các record malformed, duplicate, late, negative amount và unsupported schema version không bị loại bỏ.
- Bronze không yêu cầu payload phải vượt qua business validation.
- Một JSONL object chỉ chứa record của một Kafka partition.
- Offset chỉ được commit sau khi tất cả object của batch đã được ghi và kiểm tra thành công.
- Mỗi lần chạy ingestion có một `ingestion_run_id`.
- Mỗi batch được flush có một `ingestion_batch_id` duy nhất trong phạm vi run.
- `ingestion_time` là timestamp UTC có timezone.
- `processing_date` được suy ra từ ngày UTC của `ingestion_time`.

---

## 4. Ngữ nghĩa xử lý và commit offset

Project hiện cung cấp ingestion theo ngữ nghĩa **at-least-once**, không tuyên bố end-to-end exactly-once.

### 4.1. Luồng bình thường

```text
poll Kafka
→ tạo Bronze envelope
→ ghi object vào MinIO
→ kiểm tra kích thước và checksum metadata
→ commit max_offset + 1 cho từng partition
```

### 4.2. Lỗi trước khi commit

```text
ghi object hoặc kiểm tra object thất bại
→ không commit Kafka offset
→ record được đọc lại sau khi consumer khởi động lại
```

Cùng một source range chỉ tạo cùng object key khi các giá trị sau không đổi:

```text
processing_date
+ topic
+ partition
+ start_offset
+ end_offset
```

Nếu retry diễn ra ở một ngày UTC khác, cùng source range có thể nằm dưới một `processing_date` khác. Đây là giới hạn hiện tại của thiết kế at-least-once.

---

## 5. Schema của Bronze record

| Field | Kiểu | Bắt buộc | Ý nghĩa |
|---|---|---:|---|
| `record_version` | string | Có | Phiên bản Bronze envelope |
| `ingestion_run_id` | string | Có | Định danh một lần chạy ingestion |
| `ingestion_batch_number` | integer | Có | Số thứ tự batch, bắt đầu từ 1 trong run |
| `ingestion_batch_id` | string | Có | Định danh batch trong phạm vi run |
| `ingestion_time` | UTC timestamp | Có | Thời điểm context của batch được tạo |
| `processing_date` | date string | Có | Ngày UTC suy ra từ `ingestion_time` |
| `source_record_id` | string | Có | Định danh kỹ thuật: `topic:partition:offset` |
| `source_topic` | string | Có | Kafka topic nguồn |
| `source_partition` | integer | Có | Kafka partition nguồn |
| `source_offset` | integer | Có | Kafka offset nguồn |
| `kafka_timestamp_type` | integer | Có | Loại Kafka timestamp do client trả về |
| `kafka_timestamp_ms` | integer/null | Không | Kafka timestamp theo milliseconds |
| `key_base64` | string/null | Không | Kafka key gốc được mã hóa Base64 |
| `value_base64` | string/null | Không | Kafka value gốc được mã hóa Base64 |
| `headers` | array | Có | Kafka headers; giá trị header được mã hóa Base64 |
| `payload_parse_status` | string | Có | Kết quả parse JSON theo kiểu best-effort |
| `event_id` | string/null | Không | `event_id` trích xuất best-effort |
| `event_type` | string/null | Không | `event_type` trích xuất best-effort |
| `event_time` | string/null | Không | `event_time` trích xuất best-effort |
| `producer_time` | string/null | Không | `producer_time` trích xuất best-effort |
| `schema_version` | string/null | Không | `schema_version` trích xuất best-effort |

`value_base64` là payload có tính thẩm quyền. Các field được trích xuất chỉ là metadata thuận tiện cho audit và indexing; chúng chưa phải dữ liệu nghiệp vụ đã được validate.

---

## 6. Trạng thái parse payload

| Trạng thái | Ý nghĩa |
|---|---|
| `parsed_object` | Payload là UTF-8 JSON hợp lệ và giá trị cấp cao nhất là object |
| `invalid_json` | Payload decode được UTF-8 nhưng không phải JSON hợp lệ |
| `invalid_utf8` | Payload không decode được UTF-8 |
| `json_not_object` | Payload là JSON hợp lệ nhưng giá trị cấp cao nhất không phải object |
| `null_payload` | Kafka message có value bằng null |

Negative amount hoặc unsupported schema version vẫn có thể mang trạng thái:

```text
payload_parse_status=parsed_object
```

vì đó là vi phạm business rule hoặc data contract, không phải lỗi cú pháp JSON.

---

## 7. Định danh nguồn

Định danh kỹ thuật của một Kafka record là:

```text
source_record_id = <topic>:<partition>:<offset>
```

Ví dụ:

```text
retail-payment-events:2:320
```

Định danh này khác với `event_id`.

```text
cùng event_id
+ khác Kafka offset
= các Kafka record khác nhau
```

Bronze giữ lại tất cả record. Silver mới thực hiện deduplication theo quy tắc nghiệp vụ.

---

## 8. Partition theo `processing_date`

Bronze object được tổ chức theo ngày ingestion UTC:

```text
bronze/events/
processing_date=<YYYY-MM-DD>/
topic=<kafka-topic>/
partition=<kafka-partition>/
offsets=<start-offset>-<end-offset>.jsonl
```

`processing_date` được suy ra từ `ingestion_time` UTC có timezone.

Không suy ra `processing_date` từ:

- `event_time`;
- `producer_time`;
- ngày local của Windows;
- Kafka record timestamp.

Thiết kế này giúp lọc và replay dữ liệu theo ngày mà Bronze pipeline đã ingest record.

Các đoạn `key=value` là prefix trong object key. Object storage không hoạt động như filesystem truyền thống; giao diện MinIO chỉ hiển thị các prefix phân tách bởi `/` giống thư mục để dễ điều hướng.

---

## 9. Chính sách object key

Object key có tính deterministic khi các giá trị sau giống nhau:

```text
processing_date
+ topic
+ partition
+ start_offset
+ end_offset
```

Ví dụ:

```text
bronze/events/
processing_date=2026-07-13/
topic=retail-payment-events/
partition=00002/
offsets=00000000000000000320-00000000000000000351.jsonl
```

`ingestion_run_id` không được dùng làm physical partition trong object path. Nó vẫn tồn tại ở:

- từng JSONL row;
- object metadata;
- structured log.

Cách này tránh tạo quá nhiều prefix có cardinality cao theo từng run.

### 9.1. Giới hạn retry khác ngày

Nếu cùng một Kafka source range chưa được commit và bị retry vào một ngày UTC khác, các offset đó có thể xuất hiện dưới một `processing_date` mới.

Downstream reconciliation và deduplication phải dựa trên:

```text
source_topic
+ source_partition
+ source_offset
```

không chỉ dựa trên object path.

---

## 10. Object metadata

Mỗi object lưu metadata kỹ thuật để kiểm tra nhanh mà không cần parse toàn bộ JSONL.

| Metadata key | Ý nghĩa |
|---|---|
| `sha256` | SHA-256 tính từ serialized body trước khi upload |
| `record-count` | Số JSONL row |
| `record-version` | Phiên bản Bronze envelope |
| `source-topic` | Kafka topic nguồn |
| `source-partition` | Kafka partition nguồn |
| `start-offset` | Source offset đầu tiên |
| `end-offset` | Source offset cuối cùng |
| `processing-date` | Ngày xử lý UTC |
| `ingestion-run-id` | Định danh ingestion run |
| `ingestion-batch-id` | Định danh batch trong run |
| `ingestion-time` | Timestamp ingestion UTC |

`HeadObject` cho phép đọc kích thước và metadata của object mà không tải body.

Việc so sánh custom metadata `sha256` chỉ chứng minh metadata checksum mong đợi đã được lưu. Muốn kiểm tra độc lập checksum của body đang lưu, cần tải body về hoặc sử dụng checksum response được object store hỗ trợ.

---

## 11. Các bất biến lưu trữ

Với mỗi Bronze object:

1. Mọi row có cùng `source_topic`.
2. Mọi row có cùng `source_partition`.
3. `source_offset` tăng dần trong object.
4. Offset của row đầu và row cuối khớp với range trong object key.
5. `record-count` khớp với số dòng JSONL.
6. `processing_date` trong object key, `processing-date` trong object metadata và `processing_date` trong mọi row phải giống nhau.
7. Kafka key/value gốc có thể được phục hồi chính xác từ Base64.
8. Offset được commit bằng `end_offset + 1`.
9. Duplicate và malformed record vẫn tồn tại trong Bronze.
10. Một object không trộn record từ nhiều Kafka partition.

---

## 12. Những việc Bronze không thực hiện

Bronze không:

- validate business rule;
- loại negative amount;
- loại unsupported schema version;
- deduplicate `event_id`;
- phân loại DLQ;
- sửa timestamp;
- tính business metric;
- sửa Kafka key hoặc value gốc;
- cung cấp end-to-end exactly-once.

Các trách nhiệm này thuộc Silver, Gold hoặc giai đoạn hardening.

---

## 13. Các object development lịch sử

Các object được tạo trước khi có `processing_date` vẫn nằm dưới:

```text
bronze/events/_unpartitioned/
```

Một số object cũ cũng sử dụng Bronze envelope đời trước và chưa có đầy đủ ingestion metadata.

Đây là development evidence. Chúng bị loại khỏi replay theo `processing_date` và chỉ nên xóa sau khi:

1. partitioned layout đã được kiểm tra;
2. đã chạy một lần ingestion sạch để làm evidence cuối Tuần 2;
3. các log, count và bằng chứng cần thiết đã được lưu.

---

## 14. Kết quả đối soát đã xác minh ở Ngày 3

| Partition | Phạm vi offset | Records | Objects | Kafka lag |
|---:|---|---:|---:|---:|
| 0 | `0–383` | 384 | 5 | 0 |
| 1 | `0–299` | 300 | 4 | 0 |
| 2 | `0–319` | 320 | 4 | 0 |
| **Tổng** | — | **1004** | **13** | **0** |

Tại thời điểm kiểm tra:

```text
Kafka records = 1004
Bronze JSONL rows = 1004
missing source offsets = 0
overlapping source ranges = 0
```

Kết quả: **PASS**.

---

## 15. Replay Bronze

Replay Bronze là thao tác chỉ đọc.

Selector của MVP:

```text
processing_date=YYYY-MM-DD
```

Replay utility:

1. liệt kê các JSONL object của ngày được chọn;
2. kiểm tra object key và object metadata;
3. kiểm tra source coordinates;
4. giữ nguyên từng Bronze row;
5. tạo local JSONL artifact có thứ tự deterministic;
6. tạo manifest riêng cho replay run.

Replay không thay đổi:

- Kafka consumer-group offsets;
- Kafka topic;
- Bronze object;
- nội dung Bronze record.

Bằng chứng replay ngày `2026-07-14`:

| Metric | Kết quả |
|---|---|
| Source objects | `3` |
| Replayed records | `100` |
| Output size | `131396` bytes |
| SHA-256 lần 1 | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |
| SHA-256 lần 2 | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |
| Kafka offsets thay đổi | Không |
| Bronze object count thay đổi | Không |
| Kết quả | **PASS** |

> Bằng chứng này là snapshot trước failure-injection test. Các object được ghi thêm sau đó làm thay đổi replay result của toàn bộ `processing_date=2026-07-14`.

Xem thêm: `docs/replay_strategy.md`.

---

## 16. Bằng chứng nghiệm thu cuối Tuần 2

### 16.1. Final ingestion run

| Metric | Kết quả |
|---|---|
| Processing date | `2026-07-14` |
| Run ID | `bronze-week02-final-2026-07-14` |
| Records | `100` |
| Objects | `3` |
| `parsed_object` | `95` |
| `invalid_json` | `5` |
| Duplicate event IDs | `10` |
| Committed-offset delta | `100` |
| Final lag | `0` |
| Status | **PASS** |

Source ranges:

```text
partition 0: 2550–2587, 38 records
partition 1: 2010–2039, 30 records
partition 2: 2144–2175, 32 records
```

### 16.2. Failure injection

Failure scenario cho thấy:

```text
một batch trước đó ghi và commit thành công
→ MinIO mất kết nối trong lần flush kế tiếp
→ writer thất bại bằng EndpointConnectionError
→ phần dữ liệu chưa commit được đọc lại khi recovery
→ recovery ghi 200 records trong 2 batches
→ final lag = 0
```

Offsets sau batch cuối đã commit trước lỗi:

```text
P0 = 2626
P1 = 2070
P2 = 2208
```

Offsets sau recovery:

```text
P0 = 2702
P1 = 2130
P2 = 2272
```

Điều này phù hợp với ngữ nghĩa at-least-once: dữ liệu chưa được commit phải có khả năng được đọc lại.

### 16.3. Test suite

```text
38 targeted Kafka/Bronze/replay tests passed
63 full repository tests passed
```

---

## 17. Giới hạn hiện tại

- Các object dưới `_unpartitioned` dùng layout development cũ.
- Production path chưa tải lại body của mọi object để kiểm tra checksum độc lập.
- Replay MVP mới hỗ trợ một `processing_date` cho mỗi command.
- Replay checksum là checksum của snapshot object tại thời điểm chạy, không phải checksum cố định vĩnh viễn cho một ngày có thể tiếp tục nhận object.
- Rebalance-aware flushing và failure-injection matrix rộng hơn được để sang hardening.
- Retry khác ngày có thể tạo cùng source range ở nhiều processing-date prefix.
- PostgreSQL ingestion-run metadata được hoãn đến giai đoạn metadata hardening.
