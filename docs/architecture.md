# Kiến trúc hệ thống

## 1. Mục tiêu của tài liệu

Tài liệu này mô tả kiến trúc tổng thể của project:

**Xây dựng và đánh giá nền tảng Lakehouse tối ưu hiệu năng cho phân tích dữ liệu bán lẻ/thanh toán gần thời gian thực**.

Tài liệu tập trung trả lời các câu hỏi:

1. Hệ thống gồm những thành phần nào?
2. Dữ liệu đi qua pipeline như thế nào?
3. Vì sao chọn kiến trúc Lakehouse thay vì Data Lake, Data Warehouse hoặc PostgreSQL đơn thuần?
4. Vì sao project được định nghĩa là **micro-batch near real-time**, không phải hard real-time?
5. Vai trò của Apache Kafka KRaft, Bronze, Silver, Gold, Delta Lake, Airflow và Monitoring là gì?
6. MVP cần làm đến đâu để có thể bảo vệ được?
7. Những phần nào là stretch goal, chỉ làm sau khi MVP đã ổn?

---

## 2. Tư duy kiến trúc chính

Project này không được thiết kế theo kiểu ghép nhiều công nghệ cho đủ tech stack.

Không phải:

```text
Em dùng Kafka.
Em dùng Spark.
Em dùng Airflow.
Em dùng Delta Lake.
Em dùng Streamlit.
```

Mà phải hiểu theo hướng:

```text
Apache Kafka KRaft mô phỏng event stream và các lỗi thực tế như duplicate, late, malformed events.
Bronze giữ raw immutable data để audit và replay.
Silver xử lý validation, deduplication, late-event flag và DLQ.
Delta Lake bổ sung transaction log, schema enforcement, time travel và compaction.
Gold tạo business metrics, data quality metrics và pipeline health metrics.
Benchmark chứng minh full refresh vs incremental và small files vs compaction khác nhau thế nào.
Airflow kiểm soát workflow, retry, rerun, backfill và idempotency.
Monitoring dashboard giúp phát hiện freshness issue, DLQ spike, duplicate spike, query slowdown và file-count growth.
```

Nói ngắn gọn:

```text
Project không chỉ chứng minh pipeline chạy được.
Project phải chứng minh dữ liệu đúng, metric đúng, pipeline rerun an toàn và benchmark có phương pháp.
```

---

## 3. Kiến trúc tổng thể

```text
Synthetic Retail / Payment Events
        ↓
Producer
valid + duplicate + late + malformed events
        ↓
Apache Kafka KRaft
Event streaming layer
        ↓
Bronze Layer
Raw immutable events + source offset + ingestion metadata + replay
        ↓
Silver Layer
Validation + deduplication + late-event flag + DLQ + quality summary
        ↓
Delta Lake Tables
Transaction log + schema enforcement/evolution + time travel + compaction
        ↓
Gold Layer
Business metrics + data quality metrics + pipeline health metrics
        ↓
Benchmark Layer
Full refresh vs incremental
Small files vs compaction
        ↓
Airflow Orchestration
Retry + backfill + idempotent rerun
        ↓
Monitoring Dashboard
Freshness + DLQ + duplicate + query time + file count
        ↓
README / Report / Demo
Trade-offs + limitations + analytics/AI support
```

---

## 4. Sơ đồ kiến trúc

![Architecture Diagram](assets/architecture_diagram.png)

---

## 5. Chế độ xử lý: micro-batch near real-time

Project này được định nghĩa là:

```text
Micro-batch near real-time analytics
```

Project **không phải**:

```text
Hard real-time system
Millisecond-level streaming
Sub-second fraud detection system
```

Trong project này, dữ liệu được sinh liên tục từ producer và đi qua Apache Kafka KRaft. Tuy nhiên, Spark Structured Streaming sẽ xử lý dữ liệu theo các batch nhỏ.

Latency mục tiêu:

```text
1–5 phút
```

Đây là mức phù hợp cho dashboard retail/payment như:

- doanh thu theo giờ;
- số đơn hàng theo giờ;
- tỷ lệ thanh toán thất bại;
- số lượng duplicate events;
- số lượng DLQ records;
- freshness của pipeline.

### 5.1. Vì sao không gọi là hard real-time?

Hard real-time thường yêu cầu xử lý trong giới hạn thời gian cực kỳ nghiêm ngặt. Ví dụ hệ thống điều khiển thiết bị, giao dịch tài chính yêu cầu phản ứng tức thì, hoặc hệ thống an toàn.

Project này không có yêu cầu đó. Dashboard retail/payment chấp nhận độ trễ vài phút.

Vì vậy, cách diễn đạt đúng là:

```text
Project sử dụng Spark Structured Streaming ở chế độ micro-batch.
Dữ liệu được xử lý gần thời gian thực với latency mục tiêu 1–5 phút.
Project không xử lý từng event ở mức millisecond.
```

### 5.2. Câu trả lời phỏng vấn

> Project của em dùng Spark Structured Streaming theo micro-batch. Dữ liệu được đọc liên tục từ Kafka nhưng được xử lý theo các batch nhỏ. Em gọi đây là near real-time vì latency mục tiêu khoảng 1–5 phút, phù hợp dashboard retail/payment. Em không gọi là hard real-time vì project không xử lý từng event ở mức millisecond và không có ràng buộc thời gian cứng.

---

## 6. Thành phần 1: Producer

### 6.1. Vai trò

Producer có nhiệm vụ sinh dữ liệu sự kiện bán lẻ/thanh toán giả lập.

Producer không chỉ sinh dữ liệu đẹp, mà phải sinh cả lỗi có chủ đích để kiểm thử pipeline.

Các loại event cần sinh:

```text
valid events
duplicate events
late events
malformed events
negative amount events
unsupported schema_version events
```

### 6.2. Ví dụ event hợp lệ

```json
{
  "event_id": "evt_000001",
  "event_type": "payment_authorized",
  "order_id": "ord_000123",
  "payment_id": "pay_000456",
  "customer_id": "cus_000789",
  "amount": 350000,
  "currency": "VND",
  "event_time": "2026-07-01T10:00:00Z",
  "producer_time": "2026-07-01T10:00:02Z",
  "schema_version": "v1"
}
```

### 6.3. Producer config cần có

```yaml
random_seed: 42
data_volume: 100000
duplicate_rate: 0.05
late_event_rate: 0.03
malformed_rate: 0.01
negative_amount_rate: 0.005
unsupported_schema_version_rate: 0.005
skew_mode: none
```

### 6.4. Lý do cần fixed random seed

Fixed random seed giúp benchmark có thể tái lập.

Nếu mỗi lần sinh dữ liệu khác nhau, kết quả benchmark sẽ khó so sánh.

Ví dụ không nên so sánh:

```text
Full refresh chạy trên dataset A.
Incremental chạy trên dataset B khác hoàn toàn.
```

Cách đúng:

```text
Full refresh và incremental phải chạy trên cùng data volume, cùng seed, cùng error rates.
```

---

## 7. Thành phần 2: Apache Kafka KRaft

### 7.1. Vai trò

Apache Kafka KRaft được dùng làm Kafka event-streaming layer.

Kafka giúp project mô phỏng các vấn đề thực tế:

- event được publish liên tục;
- consumer đọc theo offset;
- event có thể bị retry;
- duplicate có thể xuất hiện;
- consumer có thể replay lại từ offset;
- pipeline có thể trace event từ Bronze về source topic/partition/offset.

### 7.2. Topic design

Tuần 1 dùng một topic duy nhất:

```text
retail-payment-events
```

Topic này chứa nhiều loại event:

```text
order_created
order_confirmed
payment_authorized
payment_failed
order_shipped
order_delivered
refund_requested
```

Thiết kế topic local:

```text
partitions: 3
replication factor: 1
retention.ms: 604800000
cleanup.policy: delete
Kafka key: order_id
```

`order_id` là Kafka record key để các event cùng đơn hàng thường đi vào cùng partition. Kafka chỉ đảm bảo ordering trong cùng một partition, không đảm bảo ordering trên toàn topic. `event_id` vẫn là khóa deduplication nghiệp vụ ở Silver trong tương lai.

---

## 8. Thành phần 3: Bronze Layer

### 8.1. Vai trò

Bronze là tầng lưu dữ liệu thô.

Nguyên tắc chính:

```text
Bronze không sửa dữ liệu.
Bronze không drop duplicate.
Bronze không loại malformed records.
Bronze giữ raw payload để audit và replay.
```

Bronze trả lời câu hỏi:

```text
Nguồn đã gửi gì vào hệ thống?
Event này đến từ topic nào, partition nào, offset nào?
Event được ingest lúc nào?
Nếu Silver xử lý sai, có thể replay từ đâu?
```

### 8.2. Bronze schema đề xuất

```text
event_id
event_type
raw_payload
source_topic
source_partition
source_offset
event_time
producer_time
ingestion_time
processing_date
schema_version
ingestion_run_id
```

### 8.3. Partition cho Bronze

Bronze nên partition theo:

```text
processing_date
```

Ví dụ:

```text
s3://lakehouse/bronze/retail-payment-events/processing_date=2026-07-01/part-001.json
s3://lakehouse/bronze/retail-payment-events/processing_date=2026-07-01/part-002.json
```

Lý do:

- Bronze phục vụ audit ingestion;
- late event có thể có `event_time` cũ nhưng `ingestion_time` mới;
- partition theo `processing_date` giúp replay theo lần xử lý dễ hơn.

### 8.4. Câu trả lời phỏng vấn

> Bronze của em giữ raw immutable events để audit và replay. Em không sửa dữ liệu ở Bronze vì nếu logic Silver bị sai, em cần dữ liệu gốc để chạy lại. Bronze cũng lưu source topic, partition và offset để trace event từ Lakehouse ngược về Kafka.

---

## 9. Thành phần 4: Silver Layer

### 9.1. Vai trò

Silver là tầng làm sạch và chuẩn hóa dữ liệu.

Silver xử lý:

```text
schema validation
deduplication
late-event handling
DLQ
quality summary
```

Silver không phải dashboard layer. Silver là tầng dữ liệu sạch ở mức event/entity.

### 9.2. Output của Silver

Silver nên có các bảng/thư mục sau:

```text
silver_clean_events
silver_duplicate_events
silver_late_events
silver_dlq_bad_events
silver_quality_summary
```

### 9.3. Data quality rules bắt buộc

```text
event_id not null
event_id unique trong clean output
event_type thuộc danh sách hợp lệ
event_time not null
amount >= 0
currency in ['VND', 'USD']
payment_id not null với payment events
schema_version supported
raw_payload parse được
producer_time >= event_time hoặc được flag nếu bất thường
```

### 9.4. Deduplication

Dedup key:

```text
event_id
```

Rule:

```text
Mỗi event_id chỉ được xuất hiện một lần trong silver_clean_events.
Các bản ghi trùng event_id được đưa vào silver_duplicate_events.
```

### 9.5. Late-event handling

Project cần phân biệt:

```text
event_time      = thời điểm nghiệp vụ xảy ra
producer_time   = thời điểm producer gửi event
ingestion_time  = thời điểm data platform nhận event
```

Policy đề xuất:

```yaml
allowed_lateness_minutes: 30
```

Rule:

```text
Nếu event đến muộn nhưng nằm trong allowed lateness → vẫn xử lý vào clean events.
Nếu event đến quá muộn → đưa vào silver_late_events hoặc xử lý bằng backfill.
```

### 9.6. DLQ

DLQ là nơi lưu các bad records không đủ điều kiện vào clean output.

Ví dụ:

```json
{
  "event_id": null,
  "event_type": "payment_authorized",
  "amount": -100000,
  "currency": "ABC"
}
```

DLQ giúp:

- ngăn bad records đi vào Gold;
- debug lỗi producer hoặc schema;
- đo `dlq_count` theo thời gian;
- chứng minh data quality gate hoạt động.

---

## 10. Thành phần 5: Delta Lake Tables

### 10.1. Vai trò

Delta Lake được chọn làm table format chính cho MVP.

Delta Lake nằm chủ yếu ở Silver và Gold:

```text
Bronze: raw JSON/Parquet append-only
Silver: Delta tables
Gold: Delta tables
```

### 10.2. Vì sao dùng Delta Lake?

Delta Lake phù hợp MVP vì:

- dễ tích hợp với Spark local;
- có transaction log;
- hỗ trợ ACID transactions;
- hỗ trợ schema enforcement;
- hỗ trợ time travel;
- hỗ trợ merge/update/delete;
- phù hợp benchmark small files vs compaction.

### 10.3. Vì sao chưa dùng Iceberg ở MVP?

Apache Iceberg rất tốt cho Lakehouse hiện đại, đặc biệt ở:

- hidden partitioning;
- partition evolution;
- schema evolution;
- multi-engine support.

Tuy nhiên, Iceberg có thể làm scope phình to vì cần hiểu thêm catalog, table metadata, Trino/Flink/Spark integration.

Vì vậy, quyết định kiến trúc là:

```text
MVP: Delta Lake
Future work: Iceberg + Trino / advanced partition benchmark
```

### 10.4. Câu trả lời phỏng vấn

> Em chọn Delta Lake cho MVP vì dễ triển khai với Spark local, dễ demo transaction log, schema enforcement, time travel và compaction. Iceberg là hướng rất tốt cho Lakehouse hiện đại, nhất là hidden partitioning và partition evolution, nhưng em để ở future work để tránh scope quá rộng.

---

## 11. Thành phần 6: Gold Layer

### 11.1. Vai trò

Gold là tầng phục vụ analytics, dashboard và báo cáo.

Gold không chứa raw events lung tung. Gold chứa metrics đã được định nghĩa rõ ràng.

### 11.2. Gold tables đề xuất

```text
gold_order_metrics_hourly
gold_order_metrics_daily
gold_payment_metrics_hourly
gold_pipeline_health
gold_data_quality_summary
```

### 11.3. Ví dụ metrics

Business metrics:

```text
revenue_per_hour
orders_per_hour
payment_failure_rate
refund_count
```

Operational metrics:

```text
freshness_seconds
pipeline_success_rate
last_successful_run
dlq_count
duplicate_count
late_event_count
quality_pass_rate
processing_time_seconds
query_time_seconds
file_count
average_file_size_mb
compaction_runtime_seconds
```

### 11.4. Metric definition bắt buộc

Mỗi Gold metric phải có định nghĩa.

Ví dụ:

```text
Metric: revenue_per_hour

Definition:
Tổng amount của các event payment_authorized hợp lệ trong một hourly window,
sau khi đã dedup theo event_id và loại bỏ bad events.

Grain:
1 row per hour.

Source:
silver_clean_events.

Exclusions:
duplicate events, DLQ events, unsupported schema_version, negative amount events.
```

### 11.5. Correctness tests bắt buộc

Gold phải có tests chứng minh:

```text
Duplicate payment_authorized không làm revenue tăng.
Bad event không vào Gold.
Payment failure rate tính đúng.
Freshness_seconds không null.
Gold rerun không tạo duplicate.
```

---

## 12. Thành phần 7: Benchmark Layer

### 12.1. Vai trò

Benchmark layer dùng để đánh giá hiệu năng và trade-off.

MVP bắt buộc có 2 benchmark:

```text
Full refresh vs incremental
Small files vs compaction
```

### 12.2. Benchmark 1: Full refresh vs incremental

Mục tiêu:

```text
Đánh giá incremental processing có giảm processing_time_seconds và rows_scanned so với full refresh không.
```

Metrics:

```text
processing_time_seconds
rows_scanned
rows_written
input_rows
output_rows
correctness_status
```

Kết luận cần phân tích:

```text
Incremental có thể nhanh hơn vì đọc ít dữ liệu hơn,
nhưng khó hơn vì cần unique key, idempotency, late-event handling và backfill strategy.
```

### 12.3. Benchmark 2: Small files vs compaction

Mục tiêu:

```text
Đánh giá small files ảnh hưởng query_time_seconds, file_count và average_file_size_mb như thế nào.
Đánh giá compaction cải thiện query performance nhưng tốn thêm compaction_runtime_seconds ra sao.
```

Metrics:

```text
file_count
average_file_size_mb
query_time_seconds
compaction_runtime_seconds
```

Kết luận cần phân tích:

```text
Compaction có thể giảm query time,
nhưng tốn compute/I/O và thời gian maintenance.
```

### 12.4. Nguyên tắc benchmark

```text
Mỗi experiment chạy ít nhất 3 lần.
Có warm-up run nếu cần.
Dùng fixed random seed.
Lưu data_volume.
Lưu Spark config.
Lưu table layout.
Lưu query text hoặc query hash.
Không so sánh hai experiment nếu input data khác nhau.
Không kết luận chỉ dựa trên một lần chạy.
```

---

## 13. Thành phần 8: Airflow Orchestration

### 13.1. Vai trò

Airflow không xử lý dữ liệu lớn trực tiếp. Airflow dùng để orchestration.

Airflow quản lý:

- task dependency;
- retry;
- rerun;
- backfill;
- failure handling;
- lịch chạy pipeline;
- logging trạng thái từng task.

### 13.2. DAG đề xuất

```text
generate_events
        ↓
ingest_to_bronze
        ↓
validate_bronze
        ↓
process_silver_incremental
        ↓
run_quality_checks
        ↓
build_gold_metrics
        ↓
run_incremental_benchmark
        ↓
run_compaction_benchmark
        ↓
update_monitoring_summary
```

### 13.3. Điều quan trọng khi dùng Airflow

Điểm quan trọng không phải là có Airflow UI.

Điểm quan trọng là:

```text
Retry có làm duplicate không?
Rerun cùng processing_date có làm Gold metrics sai không?
Data quality fail thì pipeline có dừng trước Gold không?
Backfill ngày cũ có ảnh hưởng ngày hiện tại không?
```

### 13.4. Câu trả lời phỏng vấn

> Airflow trong project của em dùng để orchestrate pipeline, không dùng để xử lý dữ liệu lớn. Em quan tâm nhất đến retry, rerun, backfill và idempotency. Nếu task retry mà không idempotent, Gold metrics có thể bị double count. Vì vậy project có test để chứng minh rerun cùng processing_date không tạo duplicate.

---

## 14. Thành phần 9: Monitoring Dashboard

### 14.1. Vai trò

Monitoring dashboard dùng để theo dõi sức khỏe của data platform.

Dashboard gồm 2 nhóm:

```text
Business dashboard
Operational dashboard
```

### 14.2. Business dashboard

Trả lời câu hỏi kinh doanh:

```text
Doanh thu mỗi giờ là bao nhiêu?
Có bao nhiêu đơn hàng mỗi giờ?
Tỷ lệ thanh toán thất bại là bao nhiêu?
Refund có tăng bất thường không?
```

Metrics:

```text
revenue_per_hour
orders_per_hour
payment_failure_rate
refund_count
```

### 14.3. Operational dashboard

Trả lời câu hỏi vận hành:

```text
Pipeline có stale không?
DLQ có tăng bất thường không?
Duplicate có spike không?
Late events có tăng không?
Query có chậm đi không?
File count có tăng quá nhanh không?
Average file size có quá nhỏ không?
Compaction có hiệu quả không?
```

Metrics:

```text
freshness_seconds
pipeline_success_rate
last_successful_run
dlq_count
duplicate_count
late_event_count
quality_pass_rate
processing_time_seconds
query_time_seconds
file_count
average_file_size_mb
compaction_runtime_seconds
```

---

## 15. Data flow chi tiết

### 15.1. Luồng ingest

```text
Producer sinh event
        ↓
Gửi vào Kafka topic retail-payment-events
        ↓
Consumer đọc topic theo offset
        ↓
Ghi raw event vào Bronze
        ↓
Lưu ingestion metadata
```

### 15.2. Luồng xử lý Silver

```text
Đọc Bronze theo processing_date hoặc ingestion_run_id
        ↓
Parse raw_payload
        ↓
Validate schema và business rules
        ↓
Tách clean, duplicate, late, DLQ
        ↓
Ghi Silver Delta tables
        ↓
Ghi quality summary
```

### 15.3. Luồng xử lý Gold

```text
Đọc silver_clean_events
        ↓
Tính business metrics theo grain
        ↓
Tính data quality metrics
        ↓
Tính pipeline health metrics
        ↓
Ghi Gold Delta tables
        ↓
Chạy correctness tests
```

### 15.4. Luồng benchmark

```text
Chuẩn bị benchmark config
        ↓
Chạy full refresh benchmark
        ↓
Chạy incremental benchmark
        ↓
Chạy small files benchmark
        ↓
Chạy compaction
        ↓
Chạy query sau compaction
        ↓
Lưu benchmark_summary.csv
        ↓
Vẽ chart trong dashboard/report
```

---

## 16. Local infrastructure dự kiến

Project local nên dùng Docker Compose.

Các service tối thiểu:

```text
Apache Kafka KRaft
MinIO
PostgreSQL metadata
Spark local
Airflow
Streamlit
```

Vai trò:

| Service | Vai trò |
|---|---|
| Apache Kafka KRaft | Event streaming layer |
| MinIO | Object storage mô phỏng S3 |
| PostgreSQL | Metadata database |
| Spark | Processing engine |
| Delta Lake | Lakehouse table format |
| Airflow | Orchestration |
| Streamlit | Monitoring dashboard |

---

## 17. Metadata cần lưu

### 17.1. Pipeline metadata

```text
pipeline_runs
task_runs
ingestion_runs
data_quality_results
benchmark_runs
file_layout_metrics
```

### 17.2. Vì sao cần metadata?

Metadata giúp trả lời:

```text
Run nào đã chạy?
Task nào fail?
Input bao nhiêu rows?
Output bao nhiêu rows?
DLQ bao nhiêu records?
Duplicate bao nhiêu records?
Benchmark chạy với seed nào?
Spark config là gì?
File count trước/sau compaction là bao nhiêu?
```

Nếu thiếu metadata, project rất khó debug và benchmark sẽ bị cảm tính.

---

## 18. Quyết định kiến trúc quan trọng

### 18.1. Chọn Lakehouse thay vì PostgreSQL-only

Không chọn PostgreSQL-only vì project cần mô phỏng:

- event streaming;
- raw immutable storage;
- replay;
- Bronze/Silver/Gold;
- table format;
- file layout;
- small files;
- compaction;
- benchmark trên data lake/lakehouse.

PostgreSQL rất tốt cho OLTP và metadata nhỏ, nhưng không phải trọng tâm của Lakehouse file-based analytics.

### 18.2. Chọn Bronze/Silver/Gold thay vì một bảng duy nhất

Một bảng duy nhất sẽ khó audit và debug.

Bronze/Silver/Gold giúp chia trách nhiệm:

```text
Bronze = dữ liệu gốc
Silver = dữ liệu sạch
Gold = metrics phục vụ analytics
```

### 18.3. Chọn Delta Lake cho MVP

Delta Lake giúp MVP vừa đủ mạnh, vừa không quá rộng.

Iceberg được ghi vào future work để thể hiện hiểu biết nhưng không làm scope MVP bị vỡ.

### 18.4. Chọn benchmark từ tuần 1

Benchmark methodology phải có từ tuần 1 để tránh tình trạng cuối project mới đo số liệu cảm tính.

Benchmark phải lưu:

```text
run_id
random_seed
data_volume
Spark config
table layout
query hash
run number
runtime metrics
correctness status
```

---

## 19. MVP scope

MVP bắt buộc gồm:

```text
[ ] Producer sinh valid, duplicate, late, malformed events
[ ] Apache Kafka KRaft nhận events
[ ] Bronze raw immutable + source offset + ingestion metadata
[ ] Replay guide
[ ] Silver validation + dedup + late-event flag + DLQ
[ ] Gold metric definitions + correctness tests
[ ] Delta Lake tables cho Silver/Gold
[ ] Benchmark full refresh vs incremental
[ ] Benchmark small files vs compaction
[ ] Airflow DAG end-to-end có retry và rerun không duplicate
[ ] Monitoring dashboard có freshness, DLQ count, duplicate count, query time, file count
[ ] README có trade-off, benchmark methodology, dataset limitation, micro-batch explanation
```

---

## 20. Stretch goals

Chỉ làm sau khi MVP đã chạy ổn:

```text
[ ] Partition benchmark
[ ] Spark AQE benchmark
[ ] CI/CD nâng cao
[ ] Airflow hardening nâng cao
[ ] Monitoring trend nâng cao
[ ] Report đẹp
[ ] Demo video polish
[ ] Interview package
```

Nguyên tắc:

```text
Nếu MVP chưa xong, không làm stretch goal.
Nếu benchmark chính chưa có số liệu, không polish demo.
Nếu correctness tests chưa pass, không tối ưu performance.
```

---

## 21. Các rủi ro kiến trúc

| Rủi ro | Mức độ | Cách kiểm soát |
|---|---|---|
| Scope creep do ôm quá nhiều tool | Cao | Chốt MVP trước, stretch goal làm sau |
| Benchmark cảm tính | Cao | Tạo methodology từ tuần 1, chạy nhiều lần, lưu seed/config/query hash |
| Synthetic data không đại diện production | Trung bình | Ghi limitation rõ, không claim production performance |
| Nhầm near real-time với hard real-time | Cao | Ghi rõ micro-batch, target latency 1–5 phút |
| Retry/rerun làm duplicate Gold | Cao | Thiết kế idempotency và test rerun |
| Small files làm query chậm | Trung bình | Có benchmark small files vs compaction |
| Data quality fail nhưng vẫn publish Gold | Cao | Data quality gate trước Gold |

---

## 22. Cách kể kiến trúc trong phỏng vấn

> Em thiết kế project theo hướng Lakehouse cho bài toán retail/payment near real-time analytics. Dữ liệu được sinh từ producer và gửi vào Apache Kafka KRaft để mô phỏng event stream. Bronze giữ raw immutable events kèm source offset và ingestion metadata để audit và replay. Silver xử lý validation, deduplication theo event_id, late-event flag và DLQ. Gold tạo business metrics như revenue, orders, payment failure rate và pipeline health metrics như freshness, duplicate count, DLQ count.
>
> Em dùng Delta Lake cho Silver và Gold để có transaction log, schema enforcement, time travel và compaction. Sau đó em benchmark full refresh vs incremental và small files vs compaction theo methodology có fixed seed, data volume, Spark config, query hash và nhiều lần chạy. Airflow dùng để orchestrate pipeline, kiểm soát retry/rerun/backfill và đảm bảo idempotency. Dashboard không chỉ hiển thị business metrics mà còn theo dõi operational metrics như freshness, query time và file count.
>
> Project này là micro-batch near real-time với target latency 1–5 phút, không phải hard real-time. Dữ liệu là synthetic nên em ghi rõ limitation và không claim kết quả đại diện tuyệt đối cho production.

---

## 23. Nguồn tham khảo

1. Michael Armbrust, Ali Ghodsi, Reynold Xin, Matei Zaharia. **Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics**. CIDR 2021.
2. Apache Kafka Documentation. **Introduction to Apache Kafka**.
3. Apache Spark Documentation. **Structured Streaming Programming Guide**.
4. Delta Lake Documentation. **Welcome to the Delta Lake documentation**.
5. Databricks Documentation. **What is the medallion lakehouse architecture?**
6. Apache Airflow Documentation. **DAGs and Tasks**.
