# Data Contract

## 1. Mục đích

File này định nghĩa **hợp đồng dữ liệu** giữa bên sinh dữ liệu sự kiện bán lẻ/thanh toán và các tầng xử lý downstream trong project Lakehouse.

Trong project này, data contract được dùng để trả lời các câu hỏi:

- Producer phải gửi event theo cấu trúc nào?
- Trường nào bắt buộc, trường nào optional?
- Kiểu dữ liệu của từng trường là gì?
- Giá trị nào được xem là hợp lệ?
- Khi schema thay đổi thì xử lý như thế nào?
- Khi event vi phạm contract thì route về đâu?
- Silver layer cần kiểm tra những rule nào trước khi đưa dữ liệu vào Gold?

Data contract không chỉ là tài liệu mô tả. Nó phải được dùng làm cơ sở để viết:

- `producer/schemas.py`
- `quality/validation_rules.py`
- `tests/test_event_schema.py`
- `tests/test_bad_events.py`
- `tests/test_quality_rules.py`
- `processing/silver_transform.py`
- `processing/dlq_writer.py`

---

## 2. Phạm vi áp dụng

Data contract này áp dụng cho topic/event stream chính của MVP:

```text
retail_events
```

Topic này chứa các event mô phỏng hệ thống bán lẻ/thanh toán, bao gồm:

- `order_created`
- `order_cancelled`
- `payment_authorized`
- `payment_failed`
- `refund_requested`
- `refund_completed`

Trong MVP, toàn bộ event đi vào một topic `retail_events` để giảm độ phức tạp. Việc tách riêng `orders`, `payments`, `refunds` có thể đưa vào future work.

---

## 3. Producer và consumer

### 3.1. Data producer

Producer chính:

```text
producer/event_producer.py
```

Nhiệm vụ của producer:

- sinh event hợp lệ;
- sinh duplicate events có chủ đích;
- sinh late events có chủ đích;
- sinh malformed events có chủ đích;
- gửi event vào Kafka/Redpanda topic `retail_events`;
- đảm bảo có fixed random seed để benchmark có thể tái lập.

---

### 3.2. Data consumers

Các consumer/downstream chính:

```text
ingestion/kafka_consumer.py
processing/silver_transform.py
processing/gold_aggregations.py
monitoring/dashboard.py
benchmark/*.py
```

Vai trò:

- Bronze consumer đọc event từ Kafka/Redpanda và ghi raw event vào Bronze.
- Silver transform parse, validate, dedup, xử lý late event và route bad records vào DLQ.
- Gold aggregation chỉ đọc dữ liệu sạch từ Silver để tính metric.
- Benchmark và monitoring dùng dữ liệu từ Silver/Gold/metadata tables.

---

## 4. Nguyên tắc thiết kế contract

Data contract của project tuân theo các nguyên tắc sau:

1. **Rõ schema**: mỗi field có tên, kiểu dữ liệu, required/optional và ý nghĩa rõ ràng.
2. **Rõ semantics**: field không chỉ đúng kiểu dữ liệu mà còn phải đúng ý nghĩa nghiệp vụ.
3. **Rõ quality rules**: contract phải có rule validate được bằng code.
4. **Rõ xử lý vi phạm**: event lỗi không được âm thầm drop; phải đưa vào DLQ hoặc bảng lỗi tương ứng.
5. **Rõ versioning**: contract có `schema_version` để kiểm soát thay đổi schema.
6. **Rõ idempotency**: mỗi event phải có `event_id` để dedup và tránh double count.
7. **Rõ event-time**: project phân biệt `event_time`, `producer_time` và `ingestion_time`.

---

## 5. Event schema tổng quát

Mỗi event hợp lệ nên có cấu trúc tổng quát như sau:

```json
{
  "event_id": "evt_000001",
  "event_type": "payment_authorized",
  "schema_version": "v1",
  "event_time": "2026-07-01T10:00:00Z",
  "producer_time": "2026-07-01T10:00:02Z",
  "order_id": "ord_000001",
  "customer_id": "cus_000001",
  "payment_id": "pay_000001",
  "amount": 350000,
  "currency": "VND",
  "payment_method": "card",
  "status": "authorized",
  "failure_reason": null,
  "metadata": {
    "producer": "synthetic_event_producer",
    "source_system": "retail_payment_simulator"
  }
}
```

---

## 6. Field-level contract

| Field | Kiểu dữ liệu | Required | Mô tả | Rule chính |
|---|---|---:|---|---|
| `event_id` | string | Có | Định danh duy nhất của event | Không null, không rỗng, unique trong Silver clean |
| `event_type` | string | Có | Loại event nghiệp vụ | Thuộc danh sách event type hợp lệ |
| `schema_version` | string | Có | Version của schema | MVP hỗ trợ `v1` |
| `event_time` | timestamp/string ISO-8601 | Có | Thời điểm nghiệp vụ xảy ra | Không null, parse được timestamp |
| `producer_time` | timestamp/string ISO-8601 | Có | Thời điểm producer gửi event | Không null, parse được timestamp |
| `order_id` | string | Có với order/payment/refund events | Định danh đơn hàng | Không rỗng |
| `customer_id` | string | Có | Định danh khách hàng | Không rỗng |
| `payment_id` | string | Có với payment/refund events | Định danh giao dịch thanh toán | Không rỗng với payment/refund |
| `amount` | decimal/number | Có với payment/refund events | Số tiền giao dịch | `amount >= 0` |
| `currency` | string | Có với payment/refund events | Đơn vị tiền tệ | Thuộc `VND`, `USD` |
| `payment_method` | string | Optional | Phương thức thanh toán | Nếu có thì thuộc danh sách hợp lệ |
| `status` | string | Optional | Trạng thái nghiệp vụ | Phụ thuộc `event_type` |
| `failure_reason` | string/null | Optional | Lý do thất bại | Chỉ có ý nghĩa với `payment_failed` |
| `metadata` | object | Optional | Metadata từ producer | Nếu có thì phải parse được JSON object |

---

## 7. Danh sách event type hợp lệ

```text
order_created
order_cancelled
payment_authorized
payment_failed
refund_requested
refund_completed
```

### 7.1. `order_created`

Event khi đơn hàng được tạo.

Required fields:

```text
event_id
event_type
schema_version
event_time
producer_time
order_id
customer_id
amount
currency
```

Business rule:

- `amount >= 0`
- `currency in ['VND', 'USD']`
- `status` nếu có nên là `created`

---

### 7.2. `order_cancelled`

Event khi đơn hàng bị hủy.

Required fields:

```text
event_id
event_type
schema_version
event_time
producer_time
order_id
customer_id
```

Business rule:

- `status` nếu có nên là `cancelled`
- `amount` có thể null hoặc bằng 0 tùy generator config

---

### 7.3. `payment_authorized`

Event khi thanh toán thành công/được xác nhận.

Required fields:

```text
event_id
event_type
schema_version
event_time
producer_time
order_id
customer_id
payment_id
amount
currency
payment_method
```

Business rule:

- `amount > 0`
- `currency in ['VND', 'USD']`
- `payment_method in ['card', 'bank_transfer', 'e_wallet', 'cod']`
- event này được dùng để tính `revenue`

---

### 7.4. `payment_failed`

Event khi thanh toán thất bại.

Required fields:

```text
event_id
event_type
schema_version
event_time
producer_time
order_id
customer_id
payment_id
currency
payment_method
failure_reason
```

Business rule:

- `failure_reason` không được null
- `amount` có thể bằng 0 hoặc bằng số tiền attempt tùy thiết kế producer
- event này được dùng để tính `payment_failure_rate`

---

### 7.5. `refund_requested`

Event khi yêu cầu hoàn tiền được tạo.

Required fields:

```text
event_id
event_type
schema_version
event_time
producer_time
order_id
customer_id
payment_id
amount
currency
```

Business rule:

- `amount > 0`
- refund amount không nên lớn hơn payment amount trong dữ liệu thực tế
- trong MVP, rule này có thể chưa validate cross-event nếu scope còn nhỏ

---

### 7.6. `refund_completed`

Event khi hoàn tiền hoàn tất.

Required fields:

```text
event_id
event_type
schema_version
event_time
producer_time
order_id
customer_id
payment_id
amount
currency
```

Business rule:

- `amount > 0`
- event này có thể được dùng để tính net revenue trong future work

---

## 8. Event-time contract

Project phân biệt ba loại thời gian:

| Field | Ý nghĩa | Dùng để làm gì |
|---|---|---|
| `event_time` | Thời điểm nghiệp vụ thật sự xảy ra | Tính Gold metrics theo giờ/ngày |
| `producer_time` | Thời điểm producer gửi event | Debug delay từ producer |
| `ingestion_time` | Thời điểm data platform nhận event | Đo freshness và latency |

`ingestion_time` không bắt buộc producer gửi. Field này được thêm bởi Bronze ingestion job.

Rule bắt buộc:

```text
event_time not null
producer_time not null
producer_time parse được timestamp
event_time parse được timestamp
```

Rule cảnh báo:

```text
producer_time < event_time
```

Nếu `producer_time < event_time`, event không nhất thiết bị loại ngay, nhưng cần được flag là bất thường vì producer time thường không nên trước event time trong dữ liệu mô phỏng.

---

## 9. Late-event contract

Project sử dụng cấu hình:

```text
allowed_lateness_minutes = 30
```

Cách phân loại:

```text
late_by_minutes = ingestion_time - event_time
```

| Điều kiện | Hành động |
|---|---|
| `late_by_minutes <= 30` | Event vẫn có thể vào Silver clean nếu các rule khác hợp lệ |
| `late_by_minutes > 30` | Event được route vào `silver_late_events` hoặc cần xử lý bằng backfill |

Lưu ý:

- Late event không đồng nghĩa với malformed event.
- Late event có thể đúng schema nhưng đến muộn.
- Quyết định đưa late event vào Gold hay không phụ thuộc policy của project.

Trong MVP, đề xuất:

```text
Late trong ngưỡng 30 phút: đưa vào clean và cập nhật Gold.
Late quá ngưỡng 30 phút: đưa vào silver_late_events, không tự động cộng vào Gold realtime.
```

---

## 10. Deduplication contract

### 10.1. Idempotency key

Key chính để dedup:

```text
event_id
```

Rule:

```text
Mỗi event_id chỉ được xuất hiện một lần trong silver_clean_events.
```

Nếu cùng `event_id` xuất hiện nhiều lần:

- bản đầu tiên hợp lệ được giữ trong `silver_clean_events`;
- các bản còn lại được đưa vào `silver_duplicate_events`;
- duplicate events không được dùng để tính Gold metrics.

---

### 10.2. Vì sao không dedup theo `order_id`?

Không nên dedup theo `order_id` vì một đơn hàng có thể có nhiều event hợp lệ:

```text
order_created
payment_authorized
refund_requested
```

Nếu dedup theo `order_id`, pipeline có thể xóa nhầm các event hợp lệ.

`event_id` là key đúng hơn cho event-level deduplication.

---

## 11. Data quality rules bắt buộc ở Silver

Silver layer phải validate các rule sau:

```text
event_id not null
event_id không rỗng
event_type thuộc danh sách hợp lệ
schema_version thuộc danh sách hỗ trợ
event_time not null và parse được
producer_time not null và parse được
order_id not null với order/payment/refund events
customer_id not null
payment_id not null với payment/refund events
amount >= 0 với các event có amount
currency in ['VND', 'USD'] với các event có currency
payment_method hợp lệ nếu xuất hiện
raw_payload parse được
```

Các rule này được triển khai trong:

```text
quality/validation_rules.py
quality/run_quality_checks.py
processing/silver_transform.py
```

---

## 12. Hành động khi contract bị vi phạm

| Loại vi phạm | Ví dụ | Hành động |
|---|---|---|
| Missing required field | thiếu `event_id` | Route vào `silver_dlq_bad_events` |
| Invalid type | `amount = "ba trăm nghìn"` | Route vào `silver_dlq_bad_events` |
| Invalid enum | `currency = "ABC"` | Route vào `silver_dlq_bad_events` |
| Unsupported schema | `schema_version = "v99"` | Route vào `silver_dlq_bad_events` |
| Duplicate event | trùng `event_id` | Route vào `silver_duplicate_events` |
| Late beyond threshold | late hơn 30 phút | Route vào `silver_late_events` |
| Suspicious timestamp | `producer_time < event_time` | Flag warning hoặc route tùy severity |

Nguyên tắc:

```text
Không drop dữ liệu lỗi một cách âm thầm.
Mọi bản ghi lỗi phải có reason_code để debug/audit.
```

---

## 13. Reason codes

Các `reason_code` nên dùng trong Silver/DLQ:

```text
missing_event_id
missing_event_type
unsupported_event_type
missing_schema_version
unsupported_schema_version
invalid_event_time
invalid_producer_time
missing_order_id
missing_customer_id
missing_payment_id
invalid_amount
negative_amount
unsupported_currency
unsupported_payment_method
invalid_json_payload
duplicate_event_id
late_beyond_allowed_lateness
producer_time_before_event_time
```

---

## 14. Bronze contract

Bronze không sửa dữ liệu. Bronze phải lưu raw event và metadata ingestion.

Bronze schema đề xuất:

| Field | Mô tả |
|---|---|
| `raw_payload` | JSON gốc từ Kafka/Redpanda |
| `source_topic` | Kafka topic |
| `source_partition` | Kafka partition |
| `source_offset` | Kafka offset |
| `ingestion_time` | Thời điểm ghi vào Bronze |
| `processing_date` | Ngày xử lý, dùng để partition Bronze |
| `ingestion_run_id` | ID của lần ingest |
| `event_id` | Có thể parse nếu parse được |
| `event_type` | Có thể parse nếu parse được |
| `schema_version` | Có thể parse nếu parse được |

Rule:

```text
Bronze không drop malformed records.
Bronze không dedup.
Bronze không sửa amount âm.
Bronze không sửa schema lỗi.
```

---

## 15. Silver contract

Silver chia dữ liệu thành các bảng:

```text
silver_clean_events
silver_duplicate_events
silver_late_events
silver_dlq_bad_events
silver_quality_summary
```

### 15.1. `silver_clean_events`

Chỉ chứa event hợp lệ, không duplicate, không vi phạm rule bắt buộc.

### 15.2. `silver_duplicate_events`

Chứa event trùng `event_id`.

### 15.3. `silver_late_events`

Chứa event đến muộn hơn `allowed_lateness_minutes`.

### 15.4. `silver_dlq_bad_events`

Chứa event malformed/invalid.

### 15.5. `silver_quality_summary`

Tổng hợp số lượng:

```text
total_events
clean_events
duplicate_events
late_events
dlq_events
quality_pass_rate
```

---

## 16. Gold contract

Gold chỉ đọc từ `silver_clean_events` và các bảng summary đã được validate.

Gold tables trong MVP:

```text
gold_order_metrics_hourly
gold_order_metrics_daily
gold_payment_metrics_hourly
gold_pipeline_health
gold_data_quality_summary
```

Gold không được đọc trực tiếp từ Bronze để tính business metrics.

---

## 17. Metric contract

### 17.1. Revenue

Metric:

```text
revenue
```

Definition:

```text
Tổng amount của các event payment_authorized hợp lệ, sau khi dedup theo event_id.
```

Source:

```text
silver_clean_events
```

Filter:

```text
event_type = 'payment_authorized'
```

Exclusions:

```text
duplicate events
DLQ events
unsupported schema_version
negative amount events
late events beyond allowed lateness nếu chưa backfill
```

---

### 17.2. Payment failure rate

Metric:

```text
payment_failure_rate
```

Definition:

```text
failed_payments / total_payment_attempts
```

Trong đó:

```text
failed_payments = count(payment_failed)
total_payment_attempts = count(payment_authorized) + count(payment_failed)
```

Source:

```text
silver_clean_events
```

---

### 17.3. Freshness seconds

Metric:

```text
freshness_seconds
```

Definition:

```text
current_time - max(ingestion_time hoặc event_time đã xử lý thành công)
```

Mục đích:

```text
Đo dashboard/pipeline có bị stale không.
```

---

## 18. Schema versioning

MVP hỗ trợ:

```text
schema_version = 'v1'
```

Các version chưa hỗ trợ:

```text
v2
v3
v99
```

Rule:

```text
Nếu schema_version không thuộc danh sách supported_versions thì route vào DLQ.
```

Danh sách supported version trong MVP:

```python
SUPPORTED_SCHEMA_VERSIONS = ["v1"]
```

---

## 19. Schema evolution policy

Trong MVP:

- chỉ hỗ trợ `v1`;
- schema evolution được ghi trong tài liệu, chưa nhất thiết phải implement đầy đủ;
- nếu thêm field optional thì không phá vỡ contract;
- nếu xóa/đổi tên field required thì là breaking change.

Quy tắc:

| Thay đổi | Loại thay đổi | Cách xử lý |
|---|---|---|
| Thêm optional field | Non-breaking | Cho phép |
| Thêm required field | Breaking | Tạo schema version mới |
| Đổi tên field | Breaking | Tạo schema version mới |
| Đổi kiểu dữ liệu field | Breaking | Tạo schema version mới |
| Thêm event_type mới | Có thể breaking | Cập nhật validation rules và tests |

---

## 20. Ví dụ event hợp lệ

```json
{
  "event_id": "evt_000001",
  "event_type": "payment_authorized",
  "schema_version": "v1",
  "event_time": "2026-07-01T10:00:00Z",
  "producer_time": "2026-07-01T10:00:02Z",
  "order_id": "ord_000001",
  "customer_id": "cus_000001",
  "payment_id": "pay_000001",
  "amount": 350000,
  "currency": "VND",
  "payment_method": "card",
  "status": "authorized",
  "failure_reason": null,
  "metadata": {
    "producer": "synthetic_event_producer",
    "source_system": "retail_payment_simulator"
  }
}
```

Expected result:

```text
Bronze: lưu raw event
Silver: vào silver_clean_events
Gold: được dùng để tính revenue
```

---

## 21. Ví dụ duplicate event

```json
{
  "event_id": "evt_000002",
  "event_type": "payment_authorized",
  "schema_version": "v1",
  "event_time": "2026-07-01T10:05:00Z",
  "producer_time": "2026-07-01T10:05:02Z",
  "order_id": "ord_000002",
  "customer_id": "cus_000002",
  "payment_id": "pay_000002",
  "amount": 200000,
  "currency": "VND",
  "payment_method": "e_wallet"
}
```

Nếu event này xuất hiện hai lần với cùng `event_id`, expected result:

```text
Bản đầu: silver_clean_events
Bản sau: silver_duplicate_events
Gold: chỉ cộng revenue một lần
```

---

## 22. Ví dụ malformed event

```json
{
  "event_id": null,
  "event_type": "payment_authorized",
  "schema_version": "v1",
  "event_time": "2026-07-01T10:00:00Z",
  "producer_time": "2026-07-01T10:00:02Z",
  "order_id": "ord_000003",
  "customer_id": "cus_000003",
  "payment_id": "pay_000003",
  "amount": -50000,
  "currency": "ABC",
  "payment_method": "card"
}
```

Expected result:

```text
Bronze: vẫn lưu raw event
Silver: route vào silver_dlq_bad_events
Reason codes: missing_event_id, negative_amount, unsupported_currency
Gold: không được dùng để tính revenue
```

---

## 23. Ví dụ late event

```json
{
  "event_id": "evt_late_001",
  "event_type": "payment_authorized",
  "schema_version": "v1",
  "event_time": "2026-07-01T10:00:00Z",
  "producer_time": "2026-07-01T11:00:00Z",
  "order_id": "ord_late_001",
  "customer_id": "cus_late_001",
  "payment_id": "pay_late_001",
  "amount": 500000,
  "currency": "VND",
  "payment_method": "bank_transfer"
}
```

Nếu `ingestion_time - event_time > 30 phút`, expected result:

```text
Bronze: lưu raw event
Silver: route vào silver_late_events
Gold realtime: không tự động cộng nếu policy không cho phép
Backfill: có thể xử lý sau
```

---

## 24. Test cases bắt buộc

### 24.1. Event schema tests

File:

```text
tests/test_event_schema.py
```

Test cases:

```text
valid payment_authorized event pass
missing event_id fail
unsupported event_type fail
unsupported schema_version fail
invalid timestamp fail
negative amount fail
unsupported currency fail
missing payment_id for payment event fail
```

---

### 24.2. Bad event tests

File:

```text
tests/test_bad_events.py
```

Test cases:

```text
malformed JSON route to DLQ
invalid currency route to DLQ
negative amount route to DLQ
unsupported schema_version route to DLQ
```

---

### 24.3. Dedup tests

File:

```text
tests/test_dedup.py
```

Test cases:

```text
duplicate event_id only one record in clean output
duplicate payment_authorized does not increase revenue
duplicate event is stored in silver_duplicate_events
```

---

### 24.4. Late event tests

File:

```text
tests/test_late_events.py
```

Test cases:

```text
event within allowed lateness is accepted
event beyond allowed lateness is routed to late_events
late event does not silently disappear
```

---

## 25. Definition of Done

Data contract được xem là hoàn thành khi:

```text
[ ] Tài liệu định nghĩa rõ schema event v1
[ ] Có danh sách event_type hợp lệ
[ ] Có field-level contract
[ ] Có rule cho duplicate events
[ ] Có rule cho late events
[ ] Có rule cho malformed events
[ ] Có rule cho DLQ
[ ] Có reason_code cho bad records
[ ] Có schema versioning policy
[ ] Có ví dụ valid/duplicate/malformed/late events
[ ] Có test cases tương ứng
[ ] Silver và Gold design bám theo contract này
```

---

## 26. Tóm tắt ngắn gọn

Data contract trong project này định nghĩa ranh giới giữa producer và data platform.

Producer có thể sinh dữ liệu lỗi có chủ đích để kiểm thử, nhưng downstream phải xử lý có kiểm soát:

```text
Valid event       → Silver clean → Gold metrics
Duplicate event   → Silver duplicate → Không double count
Late event        → Silver late hoặc backfill policy
Malformed event   → DLQ → Không vào Gold
```

Mục tiêu cuối cùng là đảm bảo Gold metrics không chỉ có dữ liệu, mà dữ liệu đó phải đúng, trace được và có thể kiểm chứng bằng tests.

---

## 27. Nguồn tham khảo

- Open Data Contract Standard: https://bitol-io.github.io/open-data-contract-standard/v3.1.0/
- Data Contract Specification: https://github.com/datacontract/datacontract-specification
- IBM Data Quality Dimensions: https://www.ibm.com/think/topics/data-quality-dimensions
- Great Expectations Data Docs: https://docs.greatexpectations.io/docs/0.18/reference/learn/terms/data_docs/
- Apache Spark Structured Streaming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
