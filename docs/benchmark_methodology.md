# Phương pháp benchmark

## 1. Mục đích của tài liệu

Tài liệu này mô tả phương pháp benchmark cho project:

**Xây dựng và đánh giá nền tảng Lakehouse tối ưu hiệu năng cho phân tích dữ liệu bán lẻ/thanh toán gần thời gian thực.**

Benchmark trong project này không nhằm mục đích chứng minh hệ thống local có hiệu năng tương đương production. Mục tiêu chính là so sánh các chiến lược xử lý dữ liệu trong cùng một điều kiện kiểm soát, từ đó rút ra trade-off về hiệu năng, độ đúng dữ liệu và độ phức tạp vận hành.

Các benchmark bắt buộc trong MVP gồm:

1. Full refresh vs incremental processing.
2. Small files vs compaction.

Các benchmark nâng cao sau MVP gồm:

1. Partition strategy benchmark.
2. Spark AQE off vs on.

---

## 2. Nguyên tắc cốt lõi

Một benchmark chỉ có giá trị khi có thể trả lời rõ các câu hỏi sau:

- Đang đo cái gì?
- Vì sao cần đo?
- Dữ liệu đầu vào là gì?
- Cấu hình chạy là gì?
- Mỗi experiment chạy mấy lần?
- Kết quả có đúng về mặt dữ liệu không?
- Kết quả có thể tái lập không?
- Trade-off của từng chiến lược là gì?

Project này tuân thủ các nguyên tắc sau:

1. Cố định input data khi so sánh hai chiến lược.
2. Dùng fixed random seed để sinh dữ liệu synthetic.
3. Chỉ thay đổi một biến chính trong mỗi experiment.
4. Lưu đầy đủ Spark config, data volume, table layout và query hash.
5. Mỗi experiment chạy ít nhất 3 lần.
6. Có warm-up run nếu cần.
7. Không kết luận chỉ dựa trên một lần chạy.
8. Benchmark result chỉ được chấp nhận nếu correctness tests pass.
9. Không so sánh hai kết quả nếu input data khác nhau.
10. Không claim kết quả local/synthetic đại diện tuyệt đối cho production.

---

## 3. Phân biệt test và benchmark

### 3.1. Test

Test trả lời câu hỏi:

> Kết quả có đúng không?

Ví dụ:

- Duplicate payment event có làm revenue bị cộng đôi không?
- Bad event có bị chặn trước Gold không?
- Payment failure rate có tính đúng không?
- Rerun cùng một input có tạo duplicate output không?

Các test chính:

- `tests/test_event_schema.py`
- `tests/test_bad_events.py`
- `tests/test_dedup.py`
- `tests/test_late_events.py`
- `tests/test_quality_rules.py`
- `tests/test_idempotency.py`
- `tests/test_metrics_correctness.py`
- `tests/test_airflow_rerun_idempotency.py`

### 3.2. Benchmark

Benchmark trả lời câu hỏi:

> Chiến lược nào hiệu quả hơn trong cùng một điều kiện đo?

Ví dụ:

- Full refresh mất bao lâu?
- Incremental processing giảm được bao nhiêu rows scanned?
- Small files làm query chậm hơn bao nhiêu?
- Compaction giảm file count và query time như thế nào?

Một pipeline nhanh nhưng tính sai dữ liệu thì không được xem là tốt. Vì vậy mỗi benchmark phải đi kèm `correctness_status`.

---

## 4. Môi trường benchmark

Benchmark được chạy trong môi trường local bằng Docker Compose.

Các thành phần chính:

- Apache Kafka KRaft: mô phỏng event streaming.
- MinIO: mô phỏng object storage kiểu S3.
- Spark/PySpark: xử lý Bronze, Silver, Gold và benchmark.
- Delta Lake: table format chính cho Silver/Gold.
- PostgreSQL: lưu metadata benchmark và pipeline runs.
- Airflow: orchestration.
- Streamlit: monitoring dashboard.

Cần ghi rõ môi trường trong mỗi lần benchmark:

```yaml
runtime_environment:
  execution_mode: local
  os: windows_or_linux_or_macos
  cpu: "ghi rõ CPU"
  memory_gb: "ghi rõ RAM"
  docker_version: "ghi rõ version"
  spark_version: "ghi rõ version"
  delta_version: "ghi rõ version"
  python_version: "ghi rõ version"
```

Nếu thay đổi máy hoặc thay đổi cấu hình Docker/Spark, không nên so sánh trực tiếp với benchmark cũ nếu không ghi chú rõ.

---

## 5. Dataset benchmark

Project sử dụng synthetic retail/payment events.

### 5.1. Lý do dùng synthetic data

Synthetic data giúp chủ động tạo các tình huống cần kiểm thử:

- valid events;
- duplicate events;
- late events;
- malformed events;
- negative amount;
- unsupported schema version;
- skewed data;
- small files.

Nhờ đó benchmark có thể kiểm soát input và so sánh công bằng giữa các chiến lược.

### 5.2. Cấu hình dataset

Mỗi benchmark phải ghi lại các thông tin sau:

```yaml
dataset:
  random_seed: 42
  data_volume: 1000000
  duplicate_rate: 0.05
  late_event_rate: 0.03
  malformed_rate: 0.01
  negative_amount_rate: 0.005
  unsupported_schema_version_rate: 0.005
  skew_mode: none
```

Ý nghĩa:

- `random_seed`: đảm bảo sinh lại được cùng một dataset.
- `data_volume`: số lượng event sinh ra.
- `duplicate_rate`: tỷ lệ event bị lặp.
- `late_event_rate`: tỷ lệ event đến muộn.
- `malformed_rate`: tỷ lệ event sai schema hoặc thiếu trường.
- `negative_amount_rate`: tỷ lệ event có amount âm.
- `unsupported_schema_version_rate`: tỷ lệ event có schema version không hỗ trợ.
- `skew_mode`: chế độ tạo lệch dữ liệu nếu cần kiểm tra skew.

### 5.3. Giới hạn của synthetic data

Synthetic data không phản ánh hoàn toàn production data.

Các giới hạn chính:

- distribution có thể đơn giản hơn dữ liệu thật;
- traffic burst có thể chưa giống thực tế;
- data skew có thể chưa đủ phức tạp;
- schema drift được mô phỏng có kiểm soát, không tự nhiên như production;
- benchmark chạy local nên không phản ánh đầy đủ cloud object storage latency;
- số lượng concurrent users/query thấp hơn production.

Do đó, kết quả benchmark chỉ dùng để so sánh các strategy trong điều kiện kiểm soát của project. Không claim kết quả đại diện tuyệt đối cho production.

---

## 6. Benchmark 1: Full refresh vs incremental processing

### 6.1. Mục tiêu

Trả lời các câu hỏi:

- Incremental processing có giảm `processing_time_seconds` so với full refresh không?
- Incremental processing có giảm `rows_scanned` không?
- Incremental processing có giữ được correctness không?
- Incremental processing làm tăng độ phức tạp gì về unique key, idempotency, late events và backfill?

### 6.2. Full refresh

Full refresh xử lý lại toàn bộ dữ liệu từ Silver để tạo lại Gold.

Luồng xử lý:

```text
silver_clean_events
    ↓ đọc toàn bộ
aggregate theo hour/day
    ↓ overwrite hoặc rebuild
Gold metrics
```

Ưu điểm:

- dễ hiểu;
- dễ kiểm tra correctness;
- phù hợp làm baseline;
- xử lý backfill đơn giản hơn.

Nhược điểm:

- chậm khi data volume tăng;
- tốn compute vì đọc lại toàn bộ dữ liệu;
- không phù hợp khi cần cập nhật gần thời gian thực với dữ liệu lớn.

### 6.3. Incremental processing

Incremental processing chỉ xử lý dữ liệu mới hoặc các window bị ảnh hưởng.

Luồng xử lý đề xuất:

```text
new_or_changed_silver_events
    ↓ xác định affected windows
recompute metrics cho affected windows
    ↓ MERGE vào Gold
Gold metrics
```

Không append mù vào Gold. Với Gold hourly metrics, nên xác định các `window_start` bị ảnh hưởng, tính lại toàn bộ metric của các window đó từ Silver, rồi MERGE/overwrite theo grain.

Ưu điểm:

- giảm rows scanned;
- giảm processing time khi dữ liệu mới nhỏ so với dữ liệu lịch sử;
- phù hợp hơn với near real-time analytics.

Nhược điểm:

- khó đúng hơn full refresh;
- cần unique key;
- cần idempotency;
- cần late-event handling;
- cần backfill strategy;
- nếu append mù có thể double count revenue.

### 6.4. Biến kiểm soát

Khi so sánh full refresh và incremental, cần giữ nguyên:

- input dataset;
- random seed;
- data volume;
- Spark config;
- table format;
- query logic;
- metric definitions;
- local environment.

Biến chính được thay đổi:

- strategy: `full_refresh` hoặc `incremental`.

### 6.5. Metrics cần đo

- `processing_time_seconds`
- `rows_scanned`
- `rows_written`
- `input_rows`
- `output_rows`
- `affected_windows`
- `correctness_status`

### 6.6. Điều kiện pass

Benchmark này chỉ pass nếu:

- full refresh correctness pass;
- incremental correctness pass;
- rerun incremental cùng input không tạo duplicate;
- duplicate event không làm revenue tăng;
- late event trong allowed lateness được xử lý đúng;
- bad events không đi vào Gold.

### 6.7. Cách kết luận

Kết luận nên viết theo dạng:

> Trong workload mô phỏng của project, incremental processing giảm rows scanned và processing time so với full refresh khi chỉ có một phần nhỏ dữ liệu mới. Tuy nhiên incremental processing phức tạp hơn vì cần unique key, idempotency, late-event handling và backfill strategy.

Không kết luận:

> Incremental luôn tốt hơn full refresh.

---

## 7. Benchmark 2: Small files vs compaction

### 7.1. Mục tiêu

Trả lời các câu hỏi:

- Small files ảnh hưởng `query_time_seconds`, `file_count` và `average_file_size_mb` như thế nào?
- Compaction có cải thiện query performance không?
- Compaction tốn thêm `compaction_runtime_seconds` bao nhiêu?
- Lợi ích query có xứng đáng với chi phí maintenance không?

### 7.2. Small files state

Small files state là trạng thái table có nhiều file nhỏ, thường sinh ra do micro-batch streaming.

Ví dụ:

```text
file_count = 10000
average_file_size_mb = 1.5
```

Vấn đề:

- query engine phải list nhiều file;
- phải open nhiều file;
- phải đọc metadata của nhiều file;
- tạo nhiều task nhỏ;
- tăng overhead so với lượng dữ liệu thực sự cần đọc.

### 7.3. Compacted state

Compacted state là trạng thái sau khi gộp nhiều file nhỏ thành ít file lớn hơn.

Ví dụ:

```text
file_count = 200
average_file_size_mb = 100
```

Compaction có thể cải thiện query time nhưng tốn thêm compute và I/O.

### 7.4. Biến kiểm soát

Khi so sánh small files và compacted files, cần giữ nguyên:

- input data;
- row count;
- schema;
- query text;
- Spark config;
- partition strategy;
- table format;
- local environment.

Biến chính được thay đổi:

- file layout: `small_files` hoặc `compacted`.

### 7.5. Metrics cần đo

- `file_count_before`
- `average_file_size_before_mb`
- `query_time_before_seconds`
- `compaction_runtime_seconds`
- `file_count_after`
- `average_file_size_after_mb`
- `query_time_after_seconds`
- `query_time_improvement_percent`

### 7.6. Điều kiện pass

Benchmark này chỉ pass nếu:

- row count trước và sau compaction không đổi;
- revenue hoặc metric chính không đổi;
- correctness tests pass;
- query text giống nhau trước và sau compaction;
- table layout được ghi lại rõ ràng.

### 7.7. Cách kết luận

Kết luận nên viết theo dạng:

> Compaction làm giảm file_count và tăng average_file_size_mb, từ đó cải thiện query_time_seconds trong workload này. Tuy nhiên compaction tốn thêm compaction_runtime_seconds, nên đây là trade-off giữa read/query performance và maintenance cost.

Không kết luận:

> Compaction lúc nào cũng tốt.

---

## 8. Benchmark 3: Partition strategy benchmark

Benchmark này là stretch goal, chỉ làm sau khi MVP ổn.

### 8.1. Mục tiêu

Trả lời các câu hỏi:

- Partition theo `event_date` có cải thiện query time cho date-range queries không?
- Partition theo `event_date + event_type` có làm tăng small-files risk không?
- Partition strategy nên chọn dựa trên query pattern như thế nào?

### 8.2. Strategies

Các strategy đề xuất:

1. `no_partition`
2. `partition_by_event_date`
3. `partition_by_event_date_and_event_type`

### 8.3. Metrics

- `query_time_seconds`
- `files_scanned`
- `file_count`
- `average_file_size_mb`
- `partition_count`

### 8.4. Cách kết luận

Kết luận nên viết theo dạng:

> Partition by event_date cải thiện query time cho date-range queries trong workload này. Tuy nhiên partition quá chi tiết có thể làm tăng file_count và tạo thêm small files, đặc biệt khi mỗi partition có ít rows.

Không kết luận:

> Partition càng nhiều càng nhanh.

---

## 9. Benchmark 4: Spark AQE off vs on

Benchmark này là stretch goal.

### 9.1. Mục tiêu

Trả lời các câu hỏi:

- Spark AQE có giảm job runtime trong workload của project không?
- AQE ảnh hưởng shuffle partitions, join strategy và physical plan như thế nào?
- AQE có luôn tốt hơn không?

### 9.2. Strategies

```yaml
strategy_a:
  name: aqe_off
  spark.sql.adaptive.enabled: false

strategy_b:
  name: aqe_on
  spark.sql.adaptive.enabled: true
```

### 9.3. Metrics

- `job_runtime_seconds`
- `shuffle_read_bytes`
- `shuffle_write_bytes`
- `num_shuffle_partitions`
- `physical_plan_before`
- `physical_plan_after`

### 9.4. Cách kết luận

Kết luận nên viết theo dạng:

> Trong workload này, AQE on giảm runtime trung bình từ X xuống Y và thay đổi physical plan ở các bước shuffle/join. Tuy nhiên hiệu quả AQE phụ thuộc data volume, query pattern, join strategy và data skew.

Không kết luận:

> AQE luôn làm Spark nhanh hơn.

---

## 10. File cấu hình benchmark

Tạo file:

```text
benchmark/benchmark_config.yaml
```

Nội dung đề xuất:

```yaml
project:
  name: optimized-retail-lakehouse-platform
  environment: local
  processing_mode: micro_batch_near_real_time
  storage: minio
  table_format: delta

runtime_environment:
  os: windows_or_linux_or_macos
  cpu: unknown
  memory_gb: unknown
  docker_version: unknown
  spark_version: unknown
  delta_version: unknown
  python_version: unknown

dataset:
  random_seed: 42
  data_volume: 1000000
  duplicate_rate: 0.05
  late_event_rate: 0.03
  malformed_rate: 0.01
  negative_amount_rate: 0.005
  unsupported_schema_version_rate: 0.005
  skew_mode: none

spark:
  app_name: lakehouse_benchmark
  master: local[*]
  adaptive_enabled: true
  shuffle_partitions: 200
  input_file_max_partition_bytes: 134217728

benchmark:
  repeat_runs: 3
  warmup_runs: 1
  save_query_hash: true
  save_spark_config: true
  save_table_layout: true
  require_correctness_pass: true

experiments:
  - name: full_refresh_vs_incremental
    enabled: true
    strategies:
      - full_refresh
      - incremental

  - name: small_files_vs_compaction
    enabled: true
    strategies:
      - small_files
      - compacted

  - name: partition_strategy
    enabled: false
    strategies:
      - no_partition
      - partition_by_event_date
      - partition_by_event_date_and_event_type

  - name: spark_aqe
    enabled: false
    strategies:
      - aqe_off
      - aqe_on
```

---

## 11. Schema kết quả benchmark

Tất cả benchmark nên được tổng hợp vào:

```text
benchmark/results/benchmark_summary.csv
```

Schema chuẩn:

```text
run_id
experiment_name
strategy_name
data_volume
random_seed
spark_config_hash
table_layout
query_name
query_hash
run_number
processing_time_seconds
query_time_seconds
rows_scanned
rows_written
file_count
average_file_size_mb
compaction_runtime_seconds
correctness_status
created_at
notes
```

Ví dụ:

```csv
run_id,experiment_name,strategy_name,data_volume,random_seed,run_number,processing_time_seconds,query_time_seconds,rows_scanned,rows_written,file_count,average_file_size_mb,correctness_status,created_at
run_001,full_refresh_vs_incremental,full_refresh,1000000,42,1,120.5,,1000000,2400,,,pass,2026-07-01T10:00:00
run_002,full_refresh_vs_incremental,incremental,1000000,42,1,18.7,,100000,240,,,pass,2026-07-01T10:10:00
run_003,small_files_vs_compaction,small_files,1000000,42,1,,15.4,,,10000,1.5,pass,2026-07-01T10:20:00
run_004,small_files_vs_compaction,compacted,1000000,42,1,,4.8,,,200,100,pass,2026-07-01T10:30:00
```

---

## 12. Query hash

Mỗi query benchmark cần lưu `query_hash`.

Mục đích:

- đảm bảo các strategy chạy cùng một query;
- tránh sửa query giữa chừng rồi so sánh sai;
- giúp tái lập benchmark.

Ví dụ query:

```sql
SELECT
  window_start,
  SUM(revenue) AS total_revenue
FROM gold_payment_metrics_hourly
WHERE event_date = '2026-07-01'
GROUP BY window_start;
```

Có thể tính hash bằng Python:

```python
import hashlib

query = """
SELECT
  window_start,
  SUM(revenue) AS total_revenue
FROM gold_payment_metrics_hourly
WHERE event_date = '2026-07-01'
GROUP BY window_start;
"""

query_hash = hashlib.md5(query.encode("utf-8")).hexdigest()
print(query_hash)
```

---

## 13. Spark config hash

Mỗi benchmark cần lưu Spark config hoặc hash của Spark config.

Ví dụ:

```python
import hashlib
import json

spark_config = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.files.maxPartitionBytes": "134217728"
}

config_text = json.dumps(spark_config, sort_keys=True)
spark_config_hash = hashlib.md5(config_text.encode("utf-8")).hexdigest()
print(spark_config_hash)
```

Nếu Spark config thay đổi, kết quả benchmark không nên so sánh trực tiếp nếu không ghi chú.

---

## 14. Table layout metadata

Mỗi benchmark cần lưu table layout.

Ví dụ:

```yaml
table_layout:
  table_name: silver_clean_events
  table_format: delta
  partition_columns:
    - event_date
  file_count: 10000
  average_file_size_mb: 1.5
  total_size_mb: 15000
  compaction_status: not_compacted
```

Với compaction benchmark, cần lưu trước và sau:

```yaml
before_compaction:
  file_count: 10000
  average_file_size_mb: 1.5

after_compaction:
  file_count: 200
  average_file_size_mb: 100
```

---

## 15. Quy trình chạy benchmark

Quy trình chuẩn:

```text
1. Sinh synthetic data bằng fixed random seed.
2. Ghi dữ liệu vào Bronze.
3. Xử lý Silver với validation/dedup/late/DLQ.
4. Chạy correctness tests.
5. Nếu tests pass, chạy benchmark.
6. Chạy warm-up nếu cần.
7. Chạy mỗi strategy ít nhất 3 lần.
8. Lưu kết quả từng run.
9. Tính min/avg/max runtime.
10. Vẽ biểu đồ.
11. Viết kết luận trade-off.
```

---

## 16. Điều kiện chấp nhận kết quả benchmark

Một kết quả benchmark được chấp nhận nếu:

- input data được ghi rõ;
- random seed được ghi rõ;
- data volume được ghi rõ;
- Spark config được ghi rõ;
- query text hoặc query hash được ghi rõ;
- table layout được ghi rõ;
- experiment chạy ít nhất 3 lần;
- correctness tests pass;
- không thay đổi nhiều biến cùng lúc;
- kết luận có nêu limitation.

Một kết quả benchmark không được chấp nhận nếu:

- chỉ chạy một lần;
- không lưu seed;
- không lưu data volume;
- thay đổi cả data volume và strategy cùng lúc;
- query khác nhau giữa hai strategy;
- không kiểm tra correctness;
- kết luận quá rộng so với dữ liệu đo được.

---

## 17. Biểu đồ cần tạo

### 17.1. Full refresh vs incremental

Biểu đồ cần có:

- runtime full refresh vs incremental;
- rows scanned full refresh vs incremental;
- min/avg/max runtime;
- correctness status.

### 17.2. Small files vs compaction

Biểu đồ cần có:

- file count before/after;
- average file size before/after;
- query time before/after;
- compaction runtime;
- query time improvement percentage.

### 17.3. Stretch goals

Nếu làm partition benchmark:

- query time theo partition strategy;
- file count theo partition strategy;
- average file size theo partition strategy.

Nếu làm AQE benchmark:

- runtime AQE off/on;
- shuffle read/write bytes;
- số partition trước/sau;
- explain plan comparison.

---

## 18. Cách viết kết luận benchmark

Kết luận benchmark nên có 4 phần:

1. Kết quả đo được.
2. Giải thích nguyên nhân kỹ thuật.
3. Trade-off.
4. Limitation.

Ví dụ:

> Incremental processing giảm runtime trung bình từ 120 giây xuống 19 giây trong workload 1 triệu events với 100 nghìn events mới. Nguyên nhân là incremental chỉ xử lý affected windows thay vì đọc lại toàn bộ Silver table. Tuy nhiên incremental yêu cầu unique key, idempotency, late-event handling và backfill strategy. Kết quả này chỉ phản ánh synthetic dataset và môi trường local, không đại diện tuyệt đối cho production.

Ví dụ khác:

> Compaction giảm file count từ 10.000 xuống 200 và giảm query time trung bình từ 15,4 giây xuống 4,8 giây. Nguyên nhân là query engine giảm overhead list/open/read metadata của nhiều small files. Tuy nhiên compaction tốn thêm 65 giây maintenance runtime. Vì vậy compaction là trade-off giữa query performance và chi phí vận hành.

---

## 19. Rủi ro benchmark và cách kiểm soát

| Rủi ro | Mức độ | Cách kiểm soát | Bằng chứng |
|---|---:|---|---|
| Kết luận từ một lần chạy | Cao | Chạy ít nhất 3 lần | `run_number`, min/avg/max |
| Input data khác nhau | Cao | Fixed seed, lưu data volume | `random_seed`, `data_volume` |
| Query khác nhau | Cao | Lưu query hash | `query_hash` |
| Spark config khác nhau | Cao | Lưu config/hash | `spark_config_hash` |
| Pipeline nhanh nhưng sai | Cao | Bắt buộc correctness tests | `correctness_status` |
| Synthetic data không đại diện production | Trung bình | Ghi limitation | `dataset_limitations.md` |
| So sánh quá nhiều biến | Cao | Chỉ thay đổi một biến chính | experiment design |
| Demo đẹp nhưng thiếu số liệu | Trung bình | Có CSV + chart | `benchmark_summary.csv` |

---

## 20. Câu trả lời phỏng vấn

### Benchmark của em có đáng tin không?

Benchmark của em không chạy một lần rồi kết luận. Em dùng fixed random seed, cố định data volume, lưu Spark config, table layout, query hash và run_id. Mỗi experiment chạy ít nhất 3 lần, có warm-up nếu cần, và chỉ thay đổi một biến chính trong mỗi experiment. Ngoài runtime, em còn lưu rows scanned, rows written, file count, average file size và correctness status. Vì vậy kết quả có thể kiểm tra lại và không chỉ là số đo cảm tính.

### Vì sao phải có correctness tests trong benchmark?

Vì một pipeline nhanh nhưng tính sai dữ liệu thì không có giá trị. Ví dụ incremental append mù có thể nhanh hơn MERGE nhưng có thể làm revenue bị double count khi retry hoặc duplicate event xuất hiện. Vì vậy benchmark chỉ được chấp nhận nếu correctness tests pass.

### Vì sao không dùng TPC-DS làm benchmark chính?

TPC-DS là benchmark chuẩn cho decision-support workload, nhưng project này tập trung vào Lakehouse pipeline cho retail/payment near real-time với các vấn đề cụ thể như duplicate events, late events, DLQ, full refresh vs incremental và small files vs compaction. Vì vậy project thiết kế benchmark riêng bám theo research questions. Tuy nhiên, project vẫn học theo tư duy benchmark chuẩn: cố định input, chạy nhiều lần, lưu config, lưu query hash và phân tích trade-off.

---

## 21. Tài liệu tham khảo

- Apache Spark SQL Performance Tuning: https://spark.apache.org/docs/latest/sql-performance-tuning.html
- Apache Spark Structured Streaming Programming Guide: https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- Delta Lake Optimizations: https://docs.delta.io/optimizations-oss/
- Delta Lake Update and Merge: https://docs.delta.io/delta-update/
- dbt Incremental Models: https://docs.getdbt.com/docs/build/incremental-models
- TPC-DS Benchmark: https://www.tpc.org/tpcds/
- AutoComp: Automated Data Compaction for Log-Structured Tables in Data Lakes: https://arxiv.org/abs/2504.04186
