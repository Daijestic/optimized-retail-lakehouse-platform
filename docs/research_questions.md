# Research Questions

## 1. Mục đích của tài liệu

Tài liệu này định nghĩa các **câu hỏi nghiên cứu** cho đề tài:

**Xây dựng và đánh giá nền tảng Lakehouse tối ưu hiệu năng cho phân tích dữ liệu bán lẻ/thanh toán gần thời gian thực**.

Tên tiếng Anh:

**Design and Evaluation of a Performance-Optimized Lakehouse Platform for Near Real-time Retail and Payment Analytics**.

Mục đích của file này là giúp project không bị biến thành một project “lắp nhiều công cụ cho đủ tech stack”. Thay vào đó, toàn bộ hệ thống được xây dựng theo logic:

```text
Vấn đề → Câu hỏi nghiên cứu → Thiết kế hệ thống → Thực nghiệm → Kết quả → Trade-off → Kết luận
```

Các câu hỏi nghiên cứu trong tài liệu này sẽ được dùng để:

- định hướng thiết kế kiến trúc;
- xác định phạm vi MVP;
- thiết kế benchmark methodology;
- xác định metrics cần đo;
- viết README/report;
- chuẩn bị câu trả lời phỏng vấn;
- tránh scope creep trong quá trình làm đồ án.

---

## 2. Bối cảnh nghiên cứu

Trong bài toán bán lẻ/thanh toán gần thời gian thực, dữ liệu thường được sinh ra liên tục dưới dạng event stream. Một số event có thể hợp lệ, nhưng cũng có thể xảy ra các trường hợp như:

- event bị trùng do retry;
- event đến muộn so với thời điểm nghiệp vụ;
- event sai schema;
- event thiếu trường bắt buộc;
- event có giá trị không hợp lệ;
- pipeline fail rồi rerun;
- full refresh chậm khi dữ liệu tăng;
- small files làm query chậm;
- dashboard bị stale nếu dữ liệu không được cập nhật kịp.

Vì vậy, project không chỉ cần xây dựng pipeline Kafka/Redpanda → Bronze → Silver → Gold, mà còn phải chứng minh:

- dữ liệu được kiểm soát chất lượng;
- metrics ở Gold layer là đúng;
- pipeline có thể rerun an toàn;
- benchmark có phương pháp;
- các tối ưu như incremental processing và compaction có hiệu quả trong điều kiện đo cụ thể;
- các trade-off được giải thích rõ ràng.

---

## 3. Câu hỏi nghiên cứu tổng quát

Câu hỏi nghiên cứu tổng quát của đề tài là:

> Làm thế nào để xây dựng một nền tảng Lakehouse có khả năng xử lý dữ liệu retail/payment gần thời gian thực, đảm bảo chất lượng dữ liệu và correctness của metrics, đồng thời đánh giá được hiệu quả của các chiến lược tối ưu như incremental processing và compaction bằng benchmark có phương pháp?

Câu hỏi này được chia thành các nhóm nhỏ bên dưới.

---

# Nhóm A — Kiến trúc Lakehouse

## RQ1. Vì sao Lakehouse phù hợp hơn Data Lake hoặc Data Warehouse thuần trong bài toán retail/payment near real-time analytics?

### Mục tiêu

Làm rõ lý do chọn kiến trúc Lakehouse thay vì chỉ dùng:

- PostgreSQL;
- Data Warehouse truyền thống;
- Data Lake dạng file thô;
- dashboard đơn giản trên database.

### Giả thuyết ban đầu

Lakehouse phù hợp vì project cần đồng thời:

- lưu raw events để replay và audit;
- xử lý dữ liệu dạng stream/micro-batch;
- kiểm soát schema và chất lượng dữ liệu;
- hỗ trợ analytics/dashboard;
- quản lý dữ liệu bằng table format;
- tối ưu query performance;
- hỗ trợ benchmark và monitoring.

Data Lake thuần linh hoạt nhưng yếu ở transaction, schema management và query optimization. Data Warehouse mạnh cho BI nhưng không tự nhiên cho raw event stream, replay, file layout optimization và ML/data science workload.

### Bằng chứng cần tạo ra

- `docs/architecture.md`
- `docs/technology_choices.md`
- `docs/lakehouse_design.md`
- sơ đồ kiến trúc tổng thể
- README giải thích vì sao chọn Lakehouse

### Metrics hoặc tiêu chí đánh giá

- Khả năng lưu raw data để replay.
- Khả năng tạo Silver/Gold tables.
- Khả năng hỗ trợ benchmark.
- Khả năng theo dõi freshness, DLQ, duplicate, query time và file count.

---

## RQ2. Bronze/Silver/Gold architecture giúp cải thiện auditability, data quality và analytics-readiness như thế nào?

### Mục tiêu

Giải thích vai trò của từng layer:

```text
Bronze = raw immutable data + replay + audit
Silver = validated + deduplicated + late-event handled + DLQ
Gold = business metrics + data quality metrics + pipeline health metrics
```

### Giả thuyết ban đầu

Chia dữ liệu theo Bronze/Silver/Gold giúp pipeline rõ trách nhiệm hơn:

- Bronze giữ dữ liệu gốc để debug và replay.
- Silver kiểm soát chất lượng dữ liệu trước khi phục vụ analytics.
- Gold chỉ chứa dữ liệu đã sẵn sàng cho dashboard và report.

### Bằng chứng cần tạo ra

- `docs/bronze_layer.md`
- `docs/silver_layer.md`
- `docs/gold_layer.md`
- `docs/replay_strategy.md`
- `docs/metric_definitions.md`
- bảng `silver_clean_events`
- bảng `silver_duplicate_events`
- bảng `silver_late_events`
- bảng `silver_dlq_bad_events`
- bảng `gold_payment_metrics_hourly`
- bảng `gold_pipeline_health`

### Metrics hoặc tiêu chí đánh giá

- `total_events`
- `clean_events`
- `duplicate_count`
- `late_event_count`
- `dlq_count`
- `quality_pass_rate`
- `freshness_seconds`

---

## RQ3. Pipeline này là batch, streaming hay micro-batch, và lựa chọn đó ảnh hưởng đến latency, complexity và correctness như thế nào?

### Mục tiêu

Làm rõ project dùng **micro-batch near real-time**, không phải hard real-time.

### Quyết định thiết kế

Project sử dụng Spark Structured Streaming ở chế độ micro-batch.

Latency mục tiêu:

```text
1–5 phút
```

Đây là near real-time analytics, không phải hard real-time hay millisecond-level streaming.

### Giả thuyết ban đầu

Micro-batch phù hợp với dashboard retail/payment vì:

- đơn giản hơn hard real-time;
- đủ nhanh cho dashboard vận hành;
- phù hợp Spark Structured Streaming;
- dễ benchmark và debug hơn;
- phù hợp môi trường local của đồ án.

### Bằng chứng cần tạo ra

- `docs/architecture.md` ghi rõ processing mode.
- `docs/late_events_and_watermark.md` giải thích event-time và watermark.
- README có section “Micro-batch Near Real-time”.

### Metrics hoặc tiêu chí đánh giá

- `freshness_seconds`
- `processing_time_seconds`
- `last_successful_run`
- tỷ lệ batch chạy thành công

---

# Nhóm B — Data Quality và Correctness

## RQ4. Data quality checks ở Silver layer giúp ngăn malformed/invalid events ảnh hưởng đến Gold metrics như thế nào?

### Mục tiêu

Đảm bảo dữ liệu lỗi không được đưa vào Gold layer.

### Các rule bắt buộc

Silver layer cần kiểm tra:

- `event_id` không null;
- `event_type` thuộc danh sách hợp lệ;
- `event_time` không null;
- `amount >= 0`;
- `currency` thuộc `VND` hoặc `USD`;
- `payment_id` không null với payment events;
- `schema_version` được hỗ trợ;
- `raw_payload` parse được;
- `producer_time >= event_time` hoặc được flag nếu bất thường.

### Giả thuyết ban đầu

Nếu data quality gate hoạt động đúng, malformed/invalid events sẽ vào DLQ thay vì đi vào Gold, từ đó metrics như revenue và payment failure rate không bị sai.

### Bằng chứng cần tạo ra

- `quality/validation_rules.py`
- `quality/run_quality_checks.py`
- `tests/test_quality_rules.py`
- `docs/data_quality.md`
- bảng `silver_dlq_bad_events`
- bảng `silver_quality_summary`

### Metrics hoặc tiêu chí đánh giá

- `dlq_count`
- `quality_pass_rate`
- số lượng bad records theo reason
- test chứng minh bad events không vào Gold

---

## RQ5. Deduplication theo `event_id` ảnh hưởng đến correctness của revenue/payment metrics như thế nào?

### Mục tiêu

Chứng minh duplicate events nếu không xử lý sẽ làm sai metrics, đặc biệt là revenue.

### Ví dụ vấn đề

```text
evt_001 | payment_authorized | 100000
evt_002 | payment_authorized | 200000
evt_002 | payment_authorized | 200000  <-- duplicate
```

Nếu không dedup, revenue sẽ bị cộng thừa 200000.

### Giả thuyết ban đầu

Deduplication theo `event_id` ở Silver layer giúp tránh double-count revenue và làm Gold metrics đúng hơn.

### Bằng chứng cần tạo ra

- `processing/dedup.py`
- `tests/test_dedup.py`
- `tests/test_metrics_correctness.py`
- bảng `silver_duplicate_events`
- test “duplicate payment_authorized không làm revenue tăng”

### Metrics hoặc tiêu chí đánh giá

- `duplicate_count`
- `revenue_without_dedup`
- `revenue_with_dedup`
- `revenue_correctness_diff`

---

## RQ6. Late-event handling với allowed lateness policy ảnh hưởng đến freshness và metric correctness như thế nào?

### Mục tiêu

Đánh giá trade-off giữa dữ liệu mới nhanh và dữ liệu đúng khi có event đến muộn.

### Quyết định thiết kế ban đầu

MVP sử dụng:

```text
allowed_lateness_minutes = 30
```

Event đến muộn trong ngưỡng được xử lý vào Silver clean và cập nhật Gold metrics. Event quá ngưỡng được đưa vào `silver_late_events` hoặc xử lý bằng backfill.

### Giả thuyết ban đầu

Allowed lateness ngắn giúp dashboard fresh hơn nhưng có thể bỏ sót late events hợp lệ. Allowed lateness dài giúp metrics đúng hơn với late events nhưng làm tăng độ phức tạp và có thể ảnh hưởng freshness/resource usage.

### Bằng chứng cần tạo ra

- `processing/late_event_handler.py`
- `tests/test_late_events.py`
- `docs/late_events_and_watermark.md`
- bảng `silver_late_events`
- metric `freshness_seconds`

### Metrics hoặc tiêu chí đánh giá

- `late_event_count`
- `late_events_accepted`
- `late_events_rejected`
- `freshness_seconds`
- `revenue_correctness_diff`

---

## RQ7. DLQ giúp debug và audit bad records như thế nào?

### Mục tiêu

Chứng minh pipeline không âm thầm drop bad records, mà lưu chúng lại để debug, audit và phân tích nguyên nhân lỗi.

### Giả thuyết ban đầu

DLQ giúp:

- phát hiện producer gửi sai schema;
- đo số lượng bad records theo thời gian;
- debug lỗi dữ liệu;
- bảo vệ Gold layer khỏi dữ liệu không hợp lệ;
- tăng khả năng audit của pipeline.

### Bằng chứng cần tạo ra

- `processing/dlq_writer.py`
- bảng `silver_dlq_bad_events`
- dashboard hiển thị `dlq_count`
- `docs/data_quality.md`

### Metrics hoặc tiêu chí đánh giá

- `dlq_count`
- `dlq_count_by_reason`
- `dlq_rate`
- `quality_pass_rate`

---

# Nhóm C — Incremental Processing

## RQ8. Incremental processing cải thiện `processing_time_seconds` và `rows_scanned` so với full refresh như thế nào?

### Mục tiêu

So sánh hiệu năng giữa full refresh và incremental processing.

### Giả thuyết ban đầu

Incremental processing sẽ giảm `rows_scanned` và `processing_time_seconds` khi dữ liệu mới chỉ chiếm một phần nhỏ so với toàn bộ dataset.

### Thiết kế benchmark

So sánh hai strategy:

```text
Strategy A: Full refresh
Strategy B: Incremental processing
```

Giữ nguyên:

- input data;
- random seed;
- data volume;
- Spark config;
- query logic;
- table layout.

### Bằng chứng cần tạo ra

- `processing/full_refresh_job.py`
- `processing/incremental_job.py`
- `benchmark/run_full_refresh_benchmark.py`
- `benchmark/run_incremental_benchmark.py`
- `benchmark/results/incremental_benchmark_results.csv`
- biểu đồ runtime full refresh vs incremental

### Metrics hoặc tiêu chí đánh giá

- `processing_time_seconds`
- `rows_scanned`
- `rows_written`
- `data_volume`
- `run_number`
- `min_runtime`
- `avg_runtime`
- `max_runtime`
- `correctness_status`

---

## RQ9. Incremental processing làm tăng độ phức tạp về unique key, idempotency, backfill và late events như thế nào?

### Mục tiêu

Không chỉ chứng minh incremental nhanh hơn, mà còn phân tích rủi ro correctness của incremental pipeline.

### Giả thuyết ban đầu

Incremental processing có thể nhanh hơn full refresh nhưng khó đúng hơn vì:

- cần unique key;
- cần xử lý duplicate;
- cần idempotency khi rerun;
- cần xử lý late events;
- cần backfill strategy;
- nếu append mù thì Gold metrics có thể bị double count.

### Bằng chứng cần tạo ra

- `tests/test_idempotency.py`
- `docs/incremental_processing.md`
- `docs/retry_and_idempotency.md`
- Airflow rerun test

### Metrics hoặc tiêu chí đánh giá

- Gold output không đổi khi rerun cùng input.
- Duplicate input không làm revenue tăng.
- Late event update đúng affected window.
- Backfill không ảnh hưởng ngày không liên quan.

---

# Nhóm D — Small Files và Compaction

## RQ10. Small files ảnh hưởng đến `query_time_seconds`, `file_count` và `average_file_size_mb` như thế nào?

### Mục tiêu

Chứng minh nhiều small files có thể làm query chậm do tăng overhead list/open/read metadata file.

### Giả thuyết ban đầu

Khi `file_count` tăng và `average_file_size_mb` giảm, `query_time_seconds` có xu hướng tăng đối với cùng một query.

### Thiết kế benchmark

Tạo trạng thái table có nhiều file nhỏ bằng micro-batch writes, sau đó đo query time.

### Bằng chứng cần tạo ra

- `processing/compaction_job.py`
- `benchmark/run_compaction_benchmark.py`
- `docs/small_files_and_compaction.md`
- `benchmark/results/compaction_benchmark_results.csv`

### Metrics hoặc tiêu chí đánh giá

- `file_count`
- `average_file_size_mb`
- `query_time_seconds`
- `data_volume`
- `query_hash`

---

## RQ11. Compaction cải thiện query performance như thế nào và tốn thêm `compaction_runtime_seconds` ra sao?

### Mục tiêu

Đánh giá trade-off của compaction.

### Giả thuyết ban đầu

Compaction giúp giảm `file_count`, tăng `average_file_size_mb` và cải thiện `query_time_seconds`, nhưng tốn thêm `compaction_runtime_seconds` và compute/I/O.

### Thiết kế benchmark

So sánh:

```text
State A: many small files
State B: after compaction
```

Giữ nguyên:

- input data;
- query text;
- Spark config;
- table layout;
- machine/local environment.

### Bằng chứng cần tạo ra

- chart file count before/after;
- chart average file size before/after;
- chart query time before/after;
- chart compaction runtime;
- README trade-off small files vs compaction.

### Metrics hoặc tiêu chí đánh giá

- `file_count_before`
- `file_count_after`
- `average_file_size_before_mb`
- `average_file_size_after_mb`
- `query_time_before_seconds`
- `query_time_after_seconds`
- `compaction_runtime_seconds`

---

# Nhóm E — Orchestration và Reliability

## RQ12. Airflow retry/rerun/backfill có thể gây duplicate hoặc sai Gold metrics không, và idempotency xử lý vấn đề đó như thế nào?

### Mục tiêu

Kiểm tra pipeline có an toàn khi task fail rồi retry/rerun hay không.

### Giả thuyết ban đầu

Nếu task không idempotent, Airflow retry/rerun có thể làm Gold metrics bị duplicate. Nếu pipeline dùng unique key, dedup và merge/upsert đúng cách, rerun cùng `processing_date` không làm thay đổi kết quả cuối cùng.

### Bằng chứng cần tạo ra

- `orchestration/dags/lakehouse_optimization_dag.py`
- `tests/test_airflow_rerun_idempotency.py`
- `docs/orchestration.md`
- `docs/retry_and_idempotency.md`
- screenshot Airflow DAG graph

### Metrics hoặc tiêu chí đánh giá

- rerun cùng `processing_date` không tạo duplicate;
- Gold row count không tăng sai sau rerun;
- revenue không đổi sau rerun cùng input;
- log có `run_id`, `task_name`, `status`.

---

## RQ13. Data quality gate trong DAG ngăn publish Gold metrics sai như thế nào?

### Mục tiêu

Đảm bảo nếu data quality fail thì pipeline dừng trước khi build/publish Gold metrics.

### Giả thuyết ban đầu

Data quality gate giúp ngăn bad data đi vào Gold và dashboard. Nếu Silver validation fail vượt ngưỡng cho phép, DAG phải stop trước task `build_gold_metrics`.

### Bằng chứng cần tạo ra

- Airflow DAG có task `run_quality_checks` trước `build_gold_metrics`.
- Test hoặc demo data quality fail thì Gold không được update.
- Log failed/retry/success rõ ràng.

### Metrics hoặc tiêu chí đánh giá

- `quality_pass_rate`
- `dlq_count`
- `dag_status`
- `gold_publish_status`

---

# Nhóm F — Monitoring và Observability

## RQ14. Monitoring dashboard có phát hiện được freshness issue, DLQ spike, duplicate spike, query slowdown và file-count growth không?

### Mục tiêu

Xây dựng dashboard không chỉ phục vụ business, mà còn phục vụ vận hành data platform.

### Giả thuyết ban đầu

Operational dashboard giúp phát hiện các vấn đề:

- dữ liệu bị stale;
- DLQ tăng bất thường;
- duplicate tăng bất thường;
- query chậm dần;
- file count tăng gây small-files problem;
- compaction cần chạy hoặc đang tốn chi phí.

### Bằng chứng cần tạo ra

- `monitoring/dashboard.py`
- `monitoring/pipeline_health.sql`
- `monitoring/benchmark_charts.py`
- `monitoring/data_quality_charts.py`
- `monitoring/file_layout_charts.py`
- screenshot dashboard

### Metrics hoặc tiêu chí đánh giá

- `freshness_seconds`
- `dlq_count`
- `duplicate_count`
- `late_event_count`
- `query_time_seconds`
- `file_count`
- `average_file_size_mb`
- `compaction_runtime_seconds`
- `pipeline_success_rate`

---

## RQ15. Operational dashboard khác business dashboard ở điểm nào?

### Mục tiêu

Phân biệt dashboard phục vụ kinh doanh và dashboard phục vụ vận hành pipeline.

### Business dashboard

Trả lời câu hỏi:

- doanh thu mỗi giờ là bao nhiêu;
- có bao nhiêu đơn hàng;
- payment failure rate là bao nhiêu;
- xu hướng bán hàng như thế nào.

### Operational dashboard

Trả lời câu hỏi:

- pipeline có chạy thành công không;
- dữ liệu có stale không;
- DLQ có tăng không;
- duplicate có tăng không;
- query có chậm không;
- file layout có xấu đi không.

### Bằng chứng cần tạo ra

Dashboard có tối thiểu các section:

- Business Overview;
- Payment Monitoring;
- Data Quality Monitoring;
- Pipeline Health;
- Benchmark Results;
- File Layout Health.

---

# Nhóm G — Benchmark Validity và Limitation

## RQ16. Benchmark methodology cần được thiết kế từ đầu như thế nào để kết quả full refresh, incremental, compaction và query-time có thể so sánh được?

### Mục tiêu

Đảm bảo benchmark không cảm tính và có thể tái lập.

### Quy tắc benchmark

1. Mỗi experiment chạy ít nhất 3 lần.
2. Có warm-up run nếu cần.
3. Lưu min/avg/max runtime.
4. Lưu `data_volume`.
5. Lưu Spark config.
6. Lưu table layout.
7. Lưu query text hoặc query hash.
8. Lưu `run_id`.
9. Dùng fixed random seed.
10. Không so sánh hai experiment nếu input data khác nhau.
11. Không kết luận chỉ dựa trên một lần chạy.

### Bằng chứng cần tạo ra

- `docs/benchmark_methodology.md`
- `benchmark/benchmark_config.yaml`
- `benchmark/results/benchmark_summary.csv`

### Metrics hoặc tiêu chí đánh giá

- `run_id`
- `experiment_name`
- `strategy_name`
- `run_number`
- `random_seed`
- `data_volume`
- `spark_config`
- `table_layout`
- `query_hash`
- `processing_time_seconds`
- `query_time_seconds`
- `correctness_status`

---

## RQ17. Synthetic data ảnh hưởng đến khả năng khái quát hóa kết quả benchmark như thế nào?

### Mục tiêu

Làm rõ limitation của synthetic data để tránh claim quá mức.

### Giả thuyết ban đầu

Synthetic data giúp kiểm soát duplicate, late events, malformed events và small files. Tuy nhiên, kết quả benchmark từ synthetic data và môi trường local không đại diện tuyệt đối cho production.

### Bằng chứng cần tạo ra

- `docs/dataset_design.md`
- `docs/dataset_limitations.md`
- `docs/data_profile.md`
- producer config có fixed random seed và configurable error rates

### Metrics hoặc tiêu chí đánh giá

- `random_seed`
- `data_volume`
- `duplicate_rate`
- `late_event_rate`
- `malformed_rate`
- `negative_amount_rate`
- `unsupported_schema_version_rate`
- `skew_mode`

### Câu kết luận cần ghi trong README/report

Project sử dụng synthetic data để đánh giá hành vi pipeline trong các tình huống được kiểm soát. Kết quả benchmark không được claim là đại diện tuyệt đối cho production. Khi áp dụng thực tế cần validate lại với dữ liệu thật, distribution thật, traffic burst, schema drift, data skew và môi trường cloud/object storage thật.

---

# Nhóm H — Stretch Goals sau MVP

Các câu hỏi trong nhóm này chỉ thực hiện sau khi MVP đã hoàn thành.

---

## RQ18. Partitioning strategy ảnh hưởng đến query time, file count và small-files risk như thế nào?

### Mục tiêu

Đánh giá partition strategy sau MVP.

### Strategy có thể so sánh

```text
Strategy A: no partition
Strategy B: partition by event_date
Strategy C: partition by event_date + event_type
```

### Giả thuyết ban đầu

Partition theo `event_date` có thể cải thiện query date-range, nhưng partition quá chi tiết có thể làm tăng `file_count` và small-files risk.

### Metrics hoặc tiêu chí đánh giá

- `query_time_seconds`
- `file_count`
- `average_file_size_mb`
- `partition_count`
- `files_scanned`

---

## RQ19. Spark AQE ảnh hưởng đến query runtime, shuffle partitions và physical plan như thế nào?

### Mục tiêu

Đánh giá Spark Adaptive Query Execution sau MVP.

### Strategy có thể so sánh

```text
Strategy A: spark.sql.adaptive.enabled = false
Strategy B: spark.sql.adaptive.enabled = true
```

### Giả thuyết ban đầu

AQE có thể cải thiện runtime bằng cách tối ưu query plan dựa trên runtime statistics, nhưng hiệu quả phụ thuộc query pattern, data volume, data skew và join strategy.

### Metrics hoặc tiêu chí đánh giá

- `job_runtime_seconds`
- `shuffle_read_bytes`
- `shuffle_write_bytes`
- `num_shuffle_partitions`
- physical plan before/after
- `query_time_seconds`

---

## RQ20. CI/CD nâng cao giúp phát hiện regression trong data quality, idempotency và metric correctness như thế nào?

### Mục tiêu

Đánh giá khả năng phát hiện lỗi sớm trước khi merge code.

### Giả thuyết ban đầu

CI/CD giúp phát hiện regression trong:

- schema validation;
- data quality rules;
- deduplication;
- late-event handling;
- idempotency;
- metric correctness.

### Bằng chứng cần tạo ra

- `.github/workflows/ci.yml`
- `docker-compose.ci.yml`
- `tests/integration/test_pipeline_smoke.py`
- `docs/ci_cd.md`

### Metrics hoặc tiêu chí đánh giá

- pytest pass/fail;
- integration smoke test pass/fail;
- import check pass/fail;
- docker compose config pass/fail.

---

# 4. Mapping Research Questions với MVP

| RQ | Nội dung | MVP hay Stretch |
|---|---|---|
| RQ1 | Vì sao chọn Lakehouse | MVP |
| RQ2 | Bronze/Silver/Gold | MVP |
| RQ3 | Micro-batch near real-time | MVP |
| RQ4 | Data quality checks | MVP |
| RQ5 | Deduplication | MVP |
| RQ6 | Late-event handling | MVP |
| RQ7 | DLQ | MVP |
| RQ8 | Full refresh vs incremental | MVP |
| RQ9 | Idempotency/backfill/late event complexity | MVP |
| RQ10 | Small files impact | MVP |
| RQ11 | Compaction trade-off | MVP |
| RQ12 | Airflow retry/rerun/idempotency | MVP |
| RQ13 | Data quality gate trong DAG | MVP |
| RQ14 | Monitoring dashboard | MVP |
| RQ15 | Operational vs business dashboard | MVP |
| RQ16 | Benchmark methodology | MVP |
| RQ17 | Synthetic data limitation | MVP |
| RQ18 | Partition benchmark | Stretch |
| RQ19 | Spark AQE benchmark | Stretch |
| RQ20 | CI/CD nâng cao | Stretch |

---

# 5. Câu hỏi phỏng vấn liên quan

## 5.1. Kiến trúc

1. Vì sao chọn Lakehouse thay vì Data Lake hoặc Data Warehouse thuần?
2. Vì sao không chỉ dùng PostgreSQL?
3. Vì sao cần Bronze/Silver/Gold?
4. Project này là batch, streaming hay micro-batch?
5. Near real-time khác hard real-time như thế nào?

## 5.2. Data quality và correctness

1. Data quality gate là gì?
2. DLQ dùng để làm gì?
3. Dedup theo `event_id` hay `order_id`?
4. Duplicate event làm sai revenue như thế nào?
5. Late event xử lý như thế nào?
6. Watermark dùng để làm gì?

## 5.3. Incremental và benchmark

1. Full refresh khác incremental processing như thế nào?
2. Vì sao full refresh dễ đúng nhưng không scale?
3. Incremental có thể sai ở đâu?
4. Idempotency là gì?
5. Backfill là gì?
6. Benchmark thế nào để không cảm tính?

## 5.4. File layout và compaction

1. Small files problem là gì?
2. Vì sao nhiều small files làm query chậm?
3. Compaction làm gì?
4. Compaction có phải lúc nào cũng tốt không?
5. Đo hiệu quả compaction bằng metrics nào?

## 5.5. Orchestration và monitoring

1. Airflow dùng để làm gì?
2. Airflow có xử lý dữ liệu lớn không?
3. Retry có thể gây lỗi gì?
4. Monitoring dashboard theo dõi gì?
5. Business dashboard khác operational dashboard như thế nào?

---

# 6. Nguồn tham khảo

1. Lakehouse paper, CIDR 2021: https://www.cidrdb.org/cidr2021/papers/cidr2021_paper17.pdf
2. Apache Spark Structured Streaming Programming Guide: https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html
3. PySpark `withWatermark` documentation: https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.withWatermark.html
4. Delta Lake documentation: https://docs.delta.io/
5. Delta Lake concurrency control: https://docs.delta.io/concurrency-control/
6. Apache Airflow DAG documentation: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html
7. AutoComp paper on small files and compaction: https://arxiv.org/abs/2504.04186
8. dbt incremental models: https://docs.getdbt.com/docs/build/incremental-models

---

# 7. Kết luận

Các Research Questions trong tài liệu này là xương sống của toàn bộ project.

Project không được đánh giá theo kiểu “có dùng Kafka, Spark, Airflow, Delta hay không”, mà được đánh giá theo khả năng trả lời các câu hỏi:

- Vì sao chọn kiến trúc này?
- Dữ liệu lỗi được xử lý thế nào?
- Metrics có đúng không?
- Incremental có thực sự hiệu quả không?
- Compaction có trade-off gì?
- Pipeline rerun có an toàn không?
- Benchmark có đáng tin không?
- Synthetic data có giới hạn gì?

Nếu trả lời được các câu hỏi này bằng tài liệu, test, benchmark và dashboard, project sẽ có logic nghiên cứu rõ ràng và thể hiện được tư duy Data Engineering tốt hơn nhiều so với một project chỉ demo công cụ.
