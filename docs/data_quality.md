# Silver Data Quality

## 1. Mục tiêu

Silver Data Quality là lớp kiểm soát dữ liệu nằm giữa Bronze raw immutable và các bước xử lý Silver tiếp theo.

Mục tiêu của lớp này:

- ngăn record không hợp lệ đi vào `silver_clean_events`;
- giữ nguyên bằng chứng raw để audit, debug và replay;
- phân loại lỗi bằng `reason_code` ổn định, có thể tổng hợp bằng máy;
- tạo đầu vào đáng tin cậy cho deduplication, late-event handling và Gold metrics;
- bảo đảm cùng một raw input luôn tạo cùng một kết quả validation;
- không làm mất hoặc sửa dữ liệu nguồn trong Bronze.

Luồng tổng quát:

```text
Kafka
→ Bronze raw immutable
→ decode raw payload
→ parse JSON
→ validate contract và business rules
→ valid candidate hoặc DLQ candidate
→ deduplication
→ late-event handling
→ Silver clean
```

Phạm vi Tuần 3 Ngày 1 chỉ gồm:

- định nghĩa validation rules;
- định nghĩa `reason_code`;
- định nghĩa thứ tự ưu tiên lỗi;
- viết unit tests cho validation;
- ghi lại quyết định thiết kế trong tài liệu này.

Chưa thuộc phạm vi Ngày 1:

- Spark Silver transformation;
- ghi dữ liệu vào DLQ;
- deduplication;
- watermark;
- late-event routing;
- quality summary;
- Delta Lake output.

---

## 2. Vị trí trong kiến trúc

```text
Producer
→ Kafka
→ Bronze raw immutable
→ Silver validation
   ├── invalid → silver_dlq_bad_events candidate
   └── valid   → deduplication candidate
                    ├── duplicate → silver_duplicate_events
                    └── unique    → late-event handling
                                       ├── late  → silver_late_events
                                       └── clean → silver_clean_events
→ Gold metrics
```

Validation chỉ quyết định record có tuân thủ data contract và business rules hay không.

Validation không quyết định:

- record có phải duplicate không;
- record có đến muộn không;
- record có được tính vào Gold hay không.

Duplicate và late event có thể vẫn là record hợp lệ về schema và business rule.

---

## 3. Input từ Bronze

Bronze lưu Kafka record dưới dạng metadata envelope. Các field kỹ thuật quan trọng gồm:

```text
record_version
ingestion_run_id
ingestion_batch_number
ingestion_batch_id
ingestion_time
processing_date
source_record_id
source_topic
source_partition
source_offset
kafka_timestamp_type
kafka_timestamp_ms
key_base64
value_base64
headers
payload_parse_status
event_id
event_type
event_time
producer_time
schema_version
```

### 3.1. Raw source of truth

`value_base64` là raw payload có tính thẩm quyền.

Các field như:

```text
event_id
event_type
event_time
producer_time
schema_version
```

đã được Bronze trích xuất theo best-effort để hỗ trợ audit. Chúng không thay thế việc Silver parse và validate lại `value_base64`.

Lý do:

- extraction tại Bronze có thể thất bại;
- malformed JSON có thể khiến các field trích xuất thành `null`;
- extraction logic có thể có bug;
- logic extraction có thể thay đổi giữa các phiên bản;
- replay phải có khả năng tái tạo kết quả từ raw payload;
- Silver phải kiểm tra đúng payload mà producer đã gửi.

### 3.2. Không mutate Bronze

Validator không được:

- thay đổi `value_base64`;
- ghi đè field trong Bronze envelope;
- sửa amount, currency, timestamp hoặc schema version;
- xóa duplicate;
- loại bỏ record mà không trả kết quả validation.

---

## 4. Nguyên tắc validation

### 4.1. Stateless

Validation không giữ state giữa các record.

Nó không cần biết:

- `event_id` đã xuất hiện trước đó hay chưa;
- consumer offset hiện tại;
- watermark hiện tại;
- database đang có dữ liệu gì.

Deduplication mới là bước cần state.

### 4.2. Deterministic

Quy tắc bắt buộc:

```text
same raw input
→ same ValidationResult
```

Validator không dùng:

- random;
- current time;
- database state;
- Kafka consumer state;
- network call;
- môi trường bên ngoài để quyết định record valid hay invalid.

### 4.3. Một source record, một primary reason

Policy MVP:

```text
một source record
→ một validation result
→ tối đa một primary reason_code
```

Nếu record sai nhiều rule, validator trả lỗi có priority cao nhất.

Ví dụ:

```json
{
  "event_id": "not-a-uuid",
  "schema_version": "99.0",
  "amount": "-100",
  "currency": "ABC"
}
```

Primary reason:

```text
UNSUPPORTED_SCHEMA_VERSION
```

Các lỗi còn lại không tạo thêm DLQ record trong MVP.

Lợi ích:

- không phóng đại `dlq_count`;
- dễ kiểm tra idempotency khi rerun;
- dễ group lỗi theo `reason_code`;
- giữ quan hệ một-một với source topic/partition/offset;
- dễ giải thích trong monitoring và phỏng vấn.

Limitation:

- primary reason không thể hiện toàn bộ lỗi có trong record;
- giai đoạn hardening có thể bổ sung `all_reason_codes` nếu cần.

---

## 5. Validation order

Validation phải chạy theo thứ tự cố định:

```text
1. Raw payload tồn tại
2. Kiểu dữ liệu của value_base64
3. Base64 decoding
4. UTF-8 decoding
5. JSON parsing
6. JSON top-level object
7. Schema version
8. Event identity và event type
9. Event time và producer time
10. Common required fields
11. Amount và currency
12. Payment-specific fields
```

Không được kiểm tra business fields trước khi xác nhận raw payload parse được.

Ví dụ payload:

```text
{"event_id":
```

phải được phân loại là:

```text
MALFORMED_JSON
```

không phải `MISSING_EVENT_ID`.

### 5.1. Priority ranges

| Priority range | Nhóm kiểm tra |
|---:|---|
| 100–140 | Raw payload, Base64, UTF-8 và JSON |
| 200–210 | Schema version |
| 300–330 | Event identity và event type |
| 340–380 | Timestamp |
| 400–410 | Common required fields |
| 500–550 | Amount, currency và payment rules |

Priority cách nhau để có thể chèn rule mới mà không cần đánh số lại toàn bộ catalogue.

---

## 6. Missing, null và invalid

Quy ước MVP:

```text
field không tồn tại → missing
field = null        → missing
field = ""          → missing nếu field yêu cầu non-empty string
field có sai format → invalid
```

Ví dụ thiếu field:

```json
{}
```

Kết quả:

```text
MISSING_EVENT_ID
```

Ví dụ field tồn tại nhưng sai format:

```json
{
  "event_id": "not-a-uuid"
}
```

Kết quả:

```text
INVALID_EVENT_ID
```

Missing và invalid phải được tách riêng để monitoring phân biệt:

- upstream quên gửi field;
- upstream có gửi nhưng giá trị sai.

---

## 7. Reason-code catalogue

| Priority | `reason_code` | Ý nghĩa | Ví dụ |
|---:|---|---|---|
| 100 | `MISSING_RAW_PAYLOAD` | Không có raw payload | `value_base64=null` |
| 105 | `INVALID_RAW_PAYLOAD_TYPE` | Raw payload không phải string | `value_base64=123` |
| 110 | `RAW_PAYLOAD_DECODE_ERROR` | Base64 không hợp lệ | `value_base64="%%%"` |
| 120 | `INVALID_UTF8` | Bytes không decode được UTF-8 | Raw bytes không hợp lệ |
| 130 | `MALFORMED_JSON` | JSON syntax không hợp lệ | `{"event_id":` |
| 140 | `JSON_NOT_OBJECT` | Top-level JSON không phải object | `[]`, `"hello"`, `123`, `null` |
| 200 | `MISSING_SCHEMA_VERSION` | Thiếu schema version | Không có key hoặc `null` |
| 210 | `UNSUPPORTED_SCHEMA_VERSION` | Schema version không được hỗ trợ | `"99.0"` |
| 300 | `MISSING_EVENT_ID` | Thiếu event ID | Không có key, `null` hoặc rỗng |
| 310 | `INVALID_EVENT_ID` | Event ID không phải UUID | `"not-a-uuid"` |
| 320 | `MISSING_EVENT_TYPE` | Thiếu event type | Không có key, `null` hoặc rỗng |
| 330 | `INVALID_EVENT_TYPE` | Event type không được hỗ trợ | `"payment_done"` |
| 340 | `MISSING_EVENT_TIME` | Thiếu event time | Không có key hoặc `null` |
| 350 | `INVALID_EVENT_TIME` | Event time sai hoặc thiếu timezone | `"2026-07-20T08:00:00"` |
| 360 | `MISSING_PRODUCER_TIME` | Thiếu producer time | Không có key hoặc `null` |
| 370 | `INVALID_PRODUCER_TIME` | Producer time sai hoặc thiếu timezone | Giá trị không phải ISO timestamp |
| 380 | `PRODUCER_TIME_BEFORE_EVENT_TIME` | Producer time sớm hơn event time | Producer time nhỏ hơn event time |
| 400 | `MISSING_REQUIRED_FIELD` | Thiếu common required field | Thiếu `order_id` |
| 410 | `INVALID_REQUIRED_FIELD` | Common field sai type hoặc rỗng | `order_id=123` |
| 500 | `INVALID_AMOUNT` | Amount không phải finite decimal string | `"abc"`, `100`, `"NaN"` |
| 510 | `NEGATIVE_AMOUNT` | Amount nhỏ hơn 0 | `"-100.00"` |
| 520 | `INVALID_CURRENCY` | Currency ngoài danh sách hỗ trợ | `"EUR"` |
| 530 | `AMOUNT_CURRENCY_MISMATCH` | Amount và currency không cùng tồn tại | Có amount nhưng currency null |
| 540 | `MISSING_PAYMENT_ID` | Payment event thiếu payment ID | `payment_id=null` |
| 545 | `INVALID_PAYMENT_ID` | Payment ID sai type hoặc rỗng | `payment_id=123` |
| 550 | `MISSING_PAYMENT_DETAILS` | Payment event thiếu amount/currency | Cả amount và currency null |

### 7.1. `reason_code`

`reason_code` phải:

- ổn định;
- machine-readable;
- viết bằng uppercase snake case;
- không chứa dữ liệu biến đổi của record;
- dùng được để group và tính metric.

Đúng:

```text
NEGATIVE_AMOUNT
```

Sai:

```text
AMOUNT_MINUS_100_INVALID
```

### 7.2. `reason_detail`

`reason_detail` dùng để debug một record cụ thể.

Ví dụ:

```text
reason_code   = NEGATIVE_AMOUNT
reason_detail = amount='-100.00' must be greater than or equal to zero
```

`reason_detail` không được chứa:

- password;
- access key;
- secret;
- toàn bộ environment;
- dữ liệu nhạy cảm không cần thiết.

---

## 8. Validation rules

### 8.1. Raw payload

`value_base64` phải:

- tồn tại;
- không null/rỗng;
- là string;
- là Base64 hợp lệ;
- decode được thành UTF-8.

Base64 phải được decode theo strict validation. Ký tự ngoài Base64 alphabet không được âm thầm bỏ qua.

### 8.2. JSON

Payload sau UTF-8 decoding phải:

- parse được bằng JSON parser;
- có top-level là object.

Các giá trị sau là JSON hợp lệ về cú pháp nhưng không phải retail/payment event object:

```json
[]
```

```json
"hello"
```

```json
123
```

```json
null
```

Tất cả phải nhận:

```text
JSON_NOT_OBJECT
```

### 8.3. Schema version

MVP chỉ hỗ trợ:

```text
1.0
```

Silver phải kiểm tra `schema_version` trực tiếp trong raw dictionary trước khi gọi bất kỳ model nào có default value.

Lý do: default của model có thể che mất bằng chứng rằng upstream không gửi field bắt buộc.

### 8.4. Event ID

`event_id` phải:

- tồn tại;
- không null/rỗng;
- là string;
- là UUID hợp lệ.

`event_id` là deduplication key ở bước sau, nhưng validator chưa kiểm tra uniqueness.

### 8.5. Event type

`event_type` phải thuộc danh sách:

```text
order_created
order_confirmed
payment_authorized
payment_failed
order_shipped
order_delivered
refund_requested
```

### 8.6. Timestamp

`event_time` và `producer_time` phải:

- tồn tại;
- là string timestamp;
- parse được theo ISO 8601/RFC 3339;
- có timezone.

Ngoài ra:

```text
producer_time >= event_time
```

Late-event classification chưa được thực hiện trong validator này.

Một event có thể cũ hơn ingestion time nhưng vẫn hợp lệ về contract.

### 8.7. Common required fields

Các field sau phải là non-empty string:

```text
order_id
customer_id
store_id
idempotency_key
source
```

Thiếu field nhận:

```text
MISSING_REQUIRED_FIELD
```

Có field nhưng sai type hoặc rỗng nhận:

```text
INVALID_REQUIRED_FIELD
```

### 8.8. Amount

`amount`:

- phải là decimal string hoặc null;
- không nhận JSON float/int trong contract MVP;
- phải là finite decimal;
- phải lớn hơn hoặc bằng 0.

Ví dụ hợp lệ:

```json
{
  "amount": "150000.00"
}
```

Ví dụ không hợp lệ:

```json
{
  "amount": -100
}
```

```json
{
  "amount": "NaN"
}
```

```json
{
  "amount": "-100.00"
}
```

### 8.9. Currency

Currency được hỗ trợ:

```text
VND
USD
```

Giá trị khác nhận:

```text
INVALID_CURRENCY
```

### 8.10. Amount/currency relationship

`amount` và `currency` phải:

```text
cùng tồn tại
hoặc cùng null
```

Có một field nhưng thiếu field còn lại nhận:

```text
AMOUNT_CURRENCY_MISMATCH
```

### 8.11. Payment events

Các event sau được coi là payment events:

```text
payment_authorized
payment_failed
refund_requested
```

Chúng phải có:

```text
payment_id
amount
currency
```

Thiếu `payment_id` nhận:

```text
MISSING_PAYMENT_ID
```

`payment_id` sai type nhận:

```text
INVALID_PAYMENT_ID
```

Thiếu amount/currency nhận:

```text
MISSING_PAYMENT_DETAILS
```

---

## 9. ValidationResult contract

Validator trả một kết quả logic tương đương:

```text
is_valid
reason_code
reason_detail
parsed_event
```

### 9.1. Valid result

```text
is_valid     = true
reason_code  = null
reason_detail = null
parsed_event = parsed JSON object
```

### 9.2. Invalid trước khi JSON parse

```text
is_valid     = false
reason_code  = một transport/parsing reason
reason_detail = chi tiết lỗi
parsed_event = null
```

### 9.3. Invalid sau khi JSON parse

```text
is_valid     = false
reason_code  = một contract/business reason
reason_detail = chi tiết lỗi
parsed_event = parsed JSON object
```

Giữ `parsed_event` ở các lỗi sau parse giúp debug nhưng không thay thế `original_payload` trong DLQ.

---

## 10. Routing policy

### 10.1. Validation failure

```text
validation fail
→ silver_dlq_bad_events candidate
```

Ngày 1 chưa ghi DLQ. Validator chỉ trả `ValidationResult`.

### 10.2. Validation success

```text
validation pass
→ valid candidate
→ deduplication
→ late-event handling
```

### 10.3. Duplicate

Duplicate không phải validation failure.

```text
duplicate
→ silver_duplicate_events
```

Duplicate record vẫn phải được giữ để audit và không được tính hai lần ở Gold.

### 10.4. Late event

Late event hợp lệ cũng không mặc định là validation failure.

```text
late
→ silver_late_events
```

Policy MVP:

```text
allowed_lateness_minutes = 30
```

Watermark và late-event routing thuộc các ngày sau của Tuần 3.

### 10.5. Clean event

Record chỉ vào:

```text
silver_clean_events
```

sau khi:

- validation pass;
- không phải duplicate cần loại khỏi clean;
- được xử lý theo late-event policy.

---

## 11. DLQ contract dự kiến

Ngày 3, mỗi DLQ record cần có:

```text
original_payload
reason_code
reason_detail
source_topic
source_partition
source_offset
detected_at
processing_run_id
```

Yêu cầu:

- giữ raw payload gốc;
- giữ đầy đủ Kafka source metadata;
- không sửa Bronze;
- có thể truy từ DLQ record về source record;
- rerun cùng input không làm sai số lượng logical DLQ records.

---

## 12. Test strategy

Unit tests của validation phải độc lập với:

- Kafka;
- MinIO;
- Spark;
- PostgreSQL;
- Docker Compose.

### 12.1. Happy path

Ít nhất kiểm tra:

- valid `payment_authorized`;
- valid `payment_failed`;
- valid `order_created` với amount/currency null.

### 12.2. Raw payload và parsing

Kiểm tra:

- missing `value_base64`;
- null `value_base64`;
- sai type;
- invalid Base64;
- invalid UTF-8;
- malformed JSON;
- JSON array/string/number/null.

### 12.3. Contract fields

Kiểm tra:

- missing/unsupported schema version;
- missing/invalid event ID;
- missing/invalid event type;
- missing/invalid timestamps;
- producer time trước event time;
- missing/invalid common required fields.

### 12.4. Business rules

Kiểm tra:

- invalid amount;
- negative amount;
- invalid currency;
- amount/currency mismatch;
- payment event thiếu payment ID;
- payment event thiếu payment details.

### 12.5. Correctness properties

Bắt buộc có test chứng minh:

- nhiều lỗi cùng lúc trả primary reason đúng priority;
- cùng input trả cùng `ValidationResult`;
- validator không mutate input;
- valid result không có reason;
- invalid result luôn có `reason_code` và `reason_detail`.

---

## 13. Commands xác minh

Chạy targeted tests:

```bash
python -m pytest -vv tests/test_quality_rules.py
```

Chạy full regression:

```bash
python -m pytest -q
```

Kiểm tra syntax/import:

```bash
python -m py_compile \
  quality/validation_rules.py \
  tests/test_quality_rules.py
```

Review diff:

```bash
git diff --check
git diff --stat
git diff
git status
```

---

## 14. Definition of Done — Tuần 3 Ngày 1

Ngày 1 chỉ được đóng khi:

- [ ] `quality/validation_rules.py` không còn rỗng;
- [ ] có `ReasonCode` ổn định;
- [ ] có `ValidationResult`;
- [ ] validation theo priority cố định;
- [ ] mỗi invalid record có đúng một primary reason;
- [ ] decode Base64 nghiêm ngặt;
- [ ] phân biệt invalid UTF-8 và malformed JSON;
- [ ] phân biệt JSON object với array/scalar/null;
- [ ] phân biệt missing và invalid;
- [ ] kiểm tra schema version trước model default;
- [ ] rules bám đúng data contract;
- [ ] duplicate và late không bị xem là validation failure;
- [ ] targeted tests pass;
- [ ] full regression tests pass;
- [ ] `git diff --check` pass;
- [ ] tài liệu khớp với code;
- [ ] không sửa file ngoài scope mà chưa review;
- [ ] commit không chứa secret.

---

## 15. Evidence

Trạng thái tại thời điểm tạo tài liệu:

```text
Thiết kế validation: đã xác định
Tài liệu data quality: đã soạn
Targeted tests: chưa xác minh trong môi trường local
Full regression: chưa xác minh trong môi trường local
Commit: chưa thực hiện
```

Chỉ thay trạng thái thành `PASS` sau khi command thực sự chạy thành công và output đã được kiểm tra.

---

## 16. Limitations

Phiên bản Ngày 1 có các giới hạn:

- chỉ hỗ trợ schema version `1.0`;
- mỗi record chỉ lưu một primary reason;
- chưa lưu danh sách tất cả lỗi;
- chưa dùng Spark DataFrame;
- chưa ghi DLQ;
- chưa xử lý duplicate;
- chưa xử lý late event và watermark;
- chưa ghi Silver/Delta output;
- chưa tạo quality summary;
- chưa có integration test Bronze → Silver.

Các phần này thuộc các ngày tiếp theo của Tuần 3 hoặc giai đoạn hardening.
