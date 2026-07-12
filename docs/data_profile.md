# Data Profile

## 1. Mục đích

File này mô tả **hồ sơ dữ liệu** của bộ synthetic retail/payment events dùng trong project Lakehouse.

Data profile giúp trả lời các câu hỏi:

- Dataset mô phỏng loại dữ liệu nào?
- Có bao nhiêu event?
- Tỷ lệ duplicate, late, malformed là bao nhiêu?
- Các event type được phân phối như thế nào?
- Dữ liệu có skew không?
- Benchmark có dùng fixed random seed không?
- Các đặc điểm nào được kiểm soát để benchmark có thể tái lập?
- Synthetic data có giới hạn gì khi so với production?

Data profile là tài liệu nối giữa:

- `producer/config.py`
- `benchmark/benchmark_config.yaml`
- `docs/data_contract.md`
- `docs/dataset_limitations.md`
- `docs/benchmark_methodology.md`
- `tests/test_event_schema.py`
- `tests/test_bad_events.py`

---

## 2. Vai trò của data profile trong project

Trong project này, dữ liệu là synthetic data, tức dữ liệu được sinh nhân tạo để mô phỏng các tình huống thường gặp trong hệ thống bán lẻ/thanh toán.

Synthetic data không được dùng để claim rằng kết quả benchmark đại diện tuyệt đối cho production. Nó được dùng để tạo môi trường kiểm thử có kiểm soát, trong đó ta có thể chủ động tạo:

- valid events;
- duplicate events;
- late events;
- malformed events;
- negative amount;
- unsupported schema version;
- skew traffic;
- small files.

Mục tiêu của data profile là ghi rõ dữ liệu được sinh như thế nào để người khác có thể hiểu, kiểm tra và tái lập benchmark.

---

## 3. Dataset overview

Tên dataset:

```text
synthetic-retail-payment-dataset
```

Mục tiêu mô phỏng:

```text
Retail/payment event stream cho Lakehouse near real-time analytics.
```

Nguồn dữ liệu:

```text
producer/event_producer.py
```

Đích đến ban đầu:

```text
Kafka topic: retail-payment-events
```

Các tầng sử dụng dataset:

```text
Bronze: raw immutable events
Silver: validated/deduplicated/late/DLQ events
Gold: analytics metrics and pipeline health metrics
Benchmark: performance experiments
Monitoring: freshness, quality, file layout, query time
```

---

## 4. Reproducibility

Để benchmark tái lập, dataset phải được sinh bằng fixed random seed.

Cấu hình mặc định:

```yaml
random_seed: 42
data_volume: 100000
```

Nguyên tắc:

```text
Cùng random_seed + cùng data_volume + cùng config lỗi
→ producer phải sinh dataset tương đương nhau.
```

Các experiment không được so sánh nếu input data khác nhau mà không ghi rõ.

---

## 5. Cấu hình sinh dữ liệu mặc định

Cấu hình đề xuất cho MVP:

```yaml
dataset:
  name: synthetic-retail-payment-dataset
  random_seed: 42
  data_volume: 100000
  start_time: "2026-07-01T00:00:00Z"
  duration_hours: 24
  currency_distribution:
    VND: 0.95
    USD: 0.05
  duplicate_rate: 0.05
  late_event_rate: 0.03
  malformed_rate: 0.01
  negative_amount_rate: 0.005
  unsupported_schema_version_rate: 0.005
  allowed_lateness_minutes: 30
  skew_mode: none
```

Ý nghĩa:

| Config | Ý nghĩa |
|---|---|
| `random_seed` | Seed cố định để tái lập dữ liệu |
| `data_volume` | Số lượng event gốc cần sinh |
| `start_time` | Thời điểm bắt đầu của event stream |
| `duration_hours` | Khoảng thời gian nghiệp vụ được mô phỏng |
| `duplicate_rate` | Tỷ lệ event bị lặp |
| `late_event_rate` | Tỷ lệ event đến muộn |
| `malformed_rate` | Tỷ lệ event sai cấu trúc hoặc thiếu field |
| `negative_amount_rate` | Tỷ lệ event có amount âm |
| `unsupported_schema_version_rate` | Tỷ lệ event có schema version không hỗ trợ |
| `allowed_lateness_minutes` | Ngưỡng late event trong policy |
| `skew_mode` | Cấu hình mô phỏng skew traffic |

---

## 6. Event types

Dataset MVP có các event type sau:

```text
order_created
order_cancelled
payment_authorized
payment_failed
refund_requested
refund_completed
```

Phân phối mặc định:

| Event type | Tỷ lệ đề xuất | Mục đích |
|---|---:|---|
| `order_created` | 35% | Mô phỏng đơn hàng mới |
| `payment_authorized` | 35% | Tính revenue |
| `payment_failed` | 15% | Tính payment failure rate |
| `order_cancelled` | 5% | Mô phỏng hủy đơn |
| `refund_requested` | 5% | Mô phỏng yêu cầu hoàn tiền |
| `refund_completed` | 5% | Mô phỏng hoàn tiền hoàn tất |

Lưu ý:

Các tỷ lệ này là giả định để kiểm thử pipeline. Không được claim rằng chúng phản ánh đúng tỷ lệ trong production.

---

## 7. Time profile

Dataset cần mô phỏng event-time rõ ràng để phục vụ window aggregation.

Các field thời gian:

| Field | Ý nghĩa |
|---|---|
| `event_time` | Thời điểm nghiệp vụ xảy ra |
| `producer_time` | Thời điểm producer gửi event |
| `ingestion_time` | Thời điểm pipeline ghi event vào Bronze |

Mặc định:

```text
event_time nằm trong khoảng start_time → start_time + duration_hours
producer_time thường sau event_time 0–5 giây
ingestion_time được thêm khi consumer ghi Bronze
```

Late event được tạo bằng cách làm `producer_time` hoặc `ingestion_time` trễ hơn `event_time`.

---

## 8. Late event profile

Cấu hình mặc định:

```yaml
late_event_rate: 0.03
allowed_lateness_minutes: 30
```

Nghĩa là khoảng 3% event được sinh dưới dạng late event.

Phân loại đề xuất:

| Nhóm late event | Tỷ lệ trong late events | Ý nghĩa |
|---|---:|---|
| Late nhẹ | 60% | Trễ 1–10 phút |
| Late trung bình | 30% | Trễ 10–30 phút |
| Late quá ngưỡng | 10% | Trễ hơn 30 phút |

Expected handling:

```text
Late <= allowed_lateness_minutes:
  có thể vào silver_clean_events nếu hợp lệ

Late > allowed_lateness_minutes:
  route vào silver_late_events hoặc xử lý bằng backfill
```

---

## 9. Duplicate profile

Cấu hình mặc định:

```yaml
duplicate_rate: 0.05
```

Nghĩa là khoảng 5% event được gửi lặp lại.

Cách tạo duplicate:

- giữ nguyên `event_id`;
- giữ nguyên payload hoặc thay đổi nhẹ metadata producer;
- gửi lại event sau một khoảng delay nhỏ.

Expected handling:

```text
Bản đầu tiên hợp lệ → silver_clean_events
Bản lặp lại → silver_duplicate_events
Gold metrics → chỉ tính một lần
```

Mục tiêu benchmark/correctness:

```text
Chứng minh duplicate payment_authorized không làm revenue tăng sai.
```

---

## 10. Malformed event profile

Cấu hình mặc định:

```yaml
malformed_rate: 0.01
negative_amount_rate: 0.005
unsupported_schema_version_rate: 0.005
```

Các lỗi cần mô phỏng:

| Lỗi | Ví dụ | Expected handling |
|---|---|---|
| Missing field | thiếu `event_id` | DLQ |
| Invalid type | `amount = "ba trăm nghìn"` | DLQ |
| Negative amount | `amount = -100000` | DLQ |
| Unsupported currency | `currency = "ABC"` | DLQ |
| Unsupported schema | `schema_version = "v99"` | DLQ |
| Invalid timestamp | `event_time = "yesterday"` | DLQ |
| Invalid JSON | raw payload không parse được | DLQ |

Mục tiêu:

```text
Bad records không được đi vào Gold.
DLQ phải lưu reason_code để debug/audit.
```

---

## 11. Amount profile

### 11.1. Currency

Phân phối mặc định:

| Currency | Tỷ lệ |
|---|---:|
| VND | 95% |
| USD | 5% |

### 11.2. Amount range

Đề xuất cho `VND`:

```text
min_amount = 10000
max_amount = 5000000
```

Đề xuất cho `USD`:

```text
min_amount = 1
max_amount = 200
```

### 11.3. Distribution

MVP có thể dùng phân phối đơn giản:

```text
uniform hoặc log-normal nhẹ
```

Nếu muốn thực tế hơn, có thể dùng:

```text
nhiều đơn nhỏ, ít đơn rất lớn
```

Tức là distribution bị lệch phải nhẹ.

Không nên claim distribution này là production-like tuyệt đối.

---

## 12. Customer/order/payment profile

### 12.1. Customer

Cấu hình đề xuất:

```yaml
num_customers: 10000
```

Pattern:

- nhiều khách hàng có ít event;
- một số khách hàng có nhiều event hơn để mô phỏng skew nhẹ.

### 12.2. Order

Mỗi order có thể có các event:

```text
order_created
payment_authorized hoặc payment_failed
order_cancelled optional
refund_requested optional
refund_completed optional
```

Trong MVP, không bắt buộc phải mô phỏng lifecycle hoàn hảo cho từng order, nhưng cần đủ nhất quán để tính metrics.

### 12.3. Payment

Payment event cần có:

```text
payment_id
payment_method
amount
currency
```

Payment methods đề xuất:

| Method | Tỷ lệ |
|---|---:|
| `card` | 35% |
| `e_wallet` | 35% |
| `bank_transfer` | 20% |
| `cod` | 10% |

---

## 13. Skew profile

`skew_mode` dùng để mô phỏng traffic không đều.

Các mode đề xuất:

```text
none
customer_skew
time_skew
event_type_skew
```

### 13.1. `none`

Dữ liệu phân phối tương đối đều.

Dùng cho MVP baseline.

---

### 13.2. `customer_skew`

Một nhóm nhỏ customer tạo nhiều event hơn.

Ví dụ:

```text
5% customers tạo 50% events
```

Dùng để kiểm tra pipeline khi dữ liệu bị skew theo customer.

---

### 13.3. `time_skew`

Một số khung giờ có traffic cao hơn.

Ví dụ:

```text
10:00–12:00 có traffic gấp 3 lần bình thường
20:00–22:00 có traffic gấp 5 lần bình thường
```

Dùng để kiểm tra micro-batch, latency và file count.

---

### 13.4. `event_type_skew`

Một loại event chiếm tỷ lệ lớn bất thường.

Ví dụ:

```text
payment_failed tăng đột biến từ 15% lên 40%
```

Dùng để kiểm tra monitoring dashboard có phát hiện payment failure spike không.

---

## 14. File layout profile

Dataset được dùng để tạo hai trạng thái file layout chính:

### 14.1. Small files state

Tạo bằng cách:

```text
micro-batch nhỏ
ghi nhiều lần
không compaction
```

Expected profile:

```text
file_count cao
average_file_size_mb thấp
query_time_seconds cao hơn
```

---

### 14.2. Compacted state

Tạo bằng cách:

```text
chạy compaction job
```

Expected profile:

```text
file_count giảm
average_file_size_mb tăng
query_time_seconds giảm nếu workload phù hợp
compaction_runtime_seconds > 0
```

---

## 15. Bronze profile

Bronze lưu raw immutable events.

Partition đề xuất:

```text
processing_date
```

Bronze fields:

```text
raw_payload
source_topic
source_partition
source_offset
ingestion_time
processing_date
ingestion_run_id
event_id nếu parse được
event_type nếu parse được
schema_version nếu parse được
```

Bronze expected behavior:

```text
Không drop malformed event
Không dedup
Không sửa amount âm
Không sửa schema lỗi
```

---

## 16. Silver profile

Silver chia thành:

```text
silver_clean_events
silver_duplicate_events
silver_late_events
silver_dlq_bad_events
silver_quality_summary
```

Expected counts:

Nếu `data_volume = 100000`:

| Nhóm | Tỷ lệ dự kiến | Số lượng xấp xỉ |
|---|---:|---:|
| Clean base events | 90–95% | 90000–95000 |
| Duplicate events | 5% | ~5000 |
| Late events | 3% | ~3000 |
| Malformed events | 1% | ~1000 |
| Negative amount | 0.5% | ~500 |
| Unsupported schema | 0.5% | ~500 |

Lưu ý:

Các nhóm có thể overlap nếu generator cho phép một event vừa late vừa malformed. Trong MVP nên cấu hình rõ có cho overlap hay không.

Đề xuất MVP:

```text
Không overlap các nhóm lỗi chính trong baseline để dễ kiểm chứng.
Sau baseline có thể bật overlap trong stress scenario.
```

---

## 17. Gold profile

Gold metrics cần được tạo từ `silver_clean_events`.

Gold tables:

```text
gold_order_metrics_hourly
gold_order_metrics_daily
gold_payment_metrics_hourly
gold_pipeline_health
gold_data_quality_summary
```

Expected metrics:

```text
revenue_per_hour
orders_per_hour
payment_failure_rate
freshness_seconds
duplicate_count
late_event_count
dlq_count
quality_pass_rate
processing_time_seconds
query_time_seconds
file_count
average_file_size_mb
```

Gold không được đọc trực tiếp từ Bronze để tính business metrics.

---

## 18. Benchmark dataset scenarios

### 18.1. Scenario A — Baseline clean dataset

Mục tiêu:

```text
Kiểm tra pipeline chạy đúng khi hầu hết dữ liệu hợp lệ.
```

Config:

```yaml
duplicate_rate: 0.00
late_event_rate: 0.00
malformed_rate: 0.00
negative_amount_rate: 0.00
unsupported_schema_version_rate: 0.00
skew_mode: none
```

---

### 18.2. Scenario B — Data quality stress

Mục tiêu:

```text
Kiểm tra Silver validation và DLQ.
```

Config:

```yaml
duplicate_rate: 0.05
late_event_rate: 0.03
malformed_rate: 0.02
negative_amount_rate: 0.01
unsupported_schema_version_rate: 0.01
skew_mode: none
```

---

### 18.3. Scenario C — Small files benchmark

Mục tiêu:

```text
Tạo nhiều small files để benchmark compaction.
```

Config:

```yaml
data_volume: 1000000
micro_batch_size: 1000
num_micro_batches: 1000
compaction_enabled: false
```

---

### 18.4. Scenario D — Incremental benchmark

Mục tiêu:

```text
So sánh full refresh vs incremental.
```

Config:

```yaml
initial_data_volume: 1000000
incremental_data_volume: 100000
random_seed: 42
incremental_seed: 43
```

---

### 18.5. Scenario E — Skew stress

Mục tiêu:

```text
Kiểm tra pipeline khi event distribution không đều.
```

Config:

```yaml
skew_mode: time_skew
peak_hours:
  - "10:00-12:00"
  - "20:00-22:00"
peak_multiplier: 5
```

---

## 19. Data profiling metrics cần lưu

Mỗi lần sinh dataset, nên lưu profile summary:

```text
profile_id
random_seed
data_volume
event_type_distribution
duplicate_count
late_event_count
malformed_count
negative_amount_count
unsupported_schema_version_count
min_event_time
max_event_time
min_amount
max_amount
avg_amount
p50_amount
p95_amount
currency_distribution
payment_method_distribution
skew_mode
created_at
```

File output đề xuất:

```text
benchmark/results/data_profile_summary.csv
```

Hoặc:

```text
docs/generated_data_profile.md
```

---

## 20. Expected data profile example

Ví dụ khi chạy:

```yaml
random_seed: 42
data_volume: 100000
duplicate_rate: 0.05
late_event_rate: 0.03
malformed_rate: 0.01
negative_amount_rate: 0.005
unsupported_schema_version_rate: 0.005
```

Expected summary:

| Metric | Expected value |
|---|---:|
| Base events | 100000 |
| Duplicate events | ~5000 |
| Late events | ~3000 |
| Malformed events | ~1000 |
| Negative amount events | ~500 |
| Unsupported schema version events | ~500 |
| Currency VND | ~95% |
| Currency USD | ~5% |

Sai số nhỏ có thể chấp nhận tùy cách generator làm tròn.

---

## 21. Data quality dimensions mapping

Các rule trong data contract/data profile có thể map vào các chiều data quality:

| Dimension | Trong project này |
|---|---|
| Completeness | Required fields không null |
| Validity | Field đúng type, enum hợp lệ, timestamp parse được |
| Uniqueness | `event_id` unique trong Silver clean |
| Timeliness | Late event, freshness_seconds |
| Consistency | Schema version, event_type và status hợp lệ |
| Accuracy | Khó xác nhận tuyệt đối do synthetic data, chỉ kiểm tra rule-based accuracy |

Lưu ý:

Vì dataset là synthetic, accuracy theo nghĩa phản ánh đúng thế giới thật không thể được claim tuyệt đối.

---

## 22. Giới hạn của data profile

Data profile này có các giới hạn:

- không dùng dữ liệu production thật;
- distribution là giả định;
- không phản ánh đầy đủ hành vi người dùng thật;
- không mô phỏng đầy đủ fraud, bot traffic, campaign traffic hoặc outage thực tế;
- không mô phỏng đầy đủ schema drift phức tạp;
- không phản ánh chính xác latency của cloud object storage thật;
- benchmark trên local machine không đại diện tuyệt đối cho production.

Cách viết trong README/report:

```text
Synthetic data được dùng để kiểm soát các kịch bản duplicate, late events,
malformed records và small files. Kết quả benchmark chỉ phản ánh workload
mô phỏng trong môi trường local, không được claim đại diện tuyệt đối cho production.
```

---

## 23. Checklist data profile

Data profile được xem là hoàn thành khi:

```text
[ ] Có random_seed
[ ] Có data_volume
[ ] Có event_type_distribution
[ ] Có duplicate_rate
[ ] Có late_event_rate
[ ] Có malformed_rate
[ ] Có negative_amount_rate
[ ] Có unsupported_schema_version_rate
[ ] Có skew_mode
[ ] Có expected counts
[ ] Có amount/currency/payment_method profile
[ ] Có benchmark scenarios
[ ] Có data profiling metrics cần lưu
[ ] Có limitation rõ ràng
[ ] Có mapping sang data quality dimensions
```

---

## 24. Tóm tắt ngắn gọn

Data profile là tài liệu mô tả dataset synthetic được sinh ra để kiểm thử Lakehouse pipeline.

Nó giúp project không bị mơ hồ ở phần dữ liệu:

```text
Dữ liệu bao nhiêu dòng?
Sinh bằng seed nào?
Duplicate bao nhiêu phần trăm?
Late events bao nhiêu phần trăm?
Malformed records bao nhiêu phần trăm?
Skew mode là gì?
Benchmark có dùng cùng input không?
```

Nếu `data_contract.md` trả lời câu hỏi “dữ liệu hợp lệ phải trông như thế nào”, thì `data_profile.md` trả lời câu hỏi “dataset mô phỏng của project có đặc điểm gì và được sinh ra như thế nào”.

---

## 25. Nguồn tham khảo

- IBM Synthetic Data: https://www.ibm.com/think/topics/synthetic-data
- IBM Data Quality Dimensions: https://www.ibm.com/think/topics/data-quality-dimensions
- Great Expectations Data Docs: https://docs.greatexpectations.io/docs/0.18/reference/learn/terms/data_docs/
- Open Data Contract Standard: https://bitol-io.github.io/open-data-contract-standard/v3.1.0/
- Apache Spark Structured Streaming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
