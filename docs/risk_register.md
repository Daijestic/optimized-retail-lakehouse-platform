# Risk Register

## 1. Mục đích của tài liệu

Tài liệu này ghi lại các rủi ro chính của project:

**Xây dựng và đánh giá nền tảng Lakehouse tối ưu hiệu năng cho phân tích dữ liệu bán lẻ/thanh toán gần thời gian thực.**

Mục tiêu của `risk_register.md` là giúp project không bị trượt khỏi phạm vi MVP, không benchmark cảm tính, không dùng sai thuật ngữ real-time, không né hạn chế của synthetic data và không biến demo thành một danh sách tool rời rạc.

Tài liệu này cần được tạo ngay từ tuần 1 và cập nhật trong suốt quá trình làm project.

---

## 2. Cách đánh giá rủi ro

Mỗi rủi ro được đánh giá theo 3 tiêu chí:

| Tiêu chí | Ý nghĩa |
|---|---|
| Xác suất | Khả năng rủi ro xảy ra |
| Tác động | Mức độ ảnh hưởng nếu rủi ro xảy ra |
| Mức độ ưu tiên | Mức cần theo dõi và xử lý |

Thang đánh giá:

| Mức | Ý nghĩa |
|---|---|
| Thấp | Có thể xảy ra nhưng ít ảnh hưởng hoặc dễ xử lý |
| Trung bình | Có khả năng xảy ra và ảnh hưởng đáng kể |
| Cao | Rất dễ xảy ra hoặc ảnh hưởng trực tiếp đến khả năng bảo vệ/demo |

---

## 3. Nguyên tắc quản lý rủi ro

Project này áp dụng 5 nguyên tắc:

1. **MVP-first**: hoàn thành Phần A trước, chỉ làm stretch goals sau khi MVP ổn.
2. **Correctness before performance**: dữ liệu và metrics phải đúng trước khi tối ưu hiệu năng.
3. **Benchmark có phương pháp từ tuần 1**: không chạy một lần rồi kết luận.
4. **Ghi rõ limitation**: synthetic data và local environment không đại diện tuyệt đối cho production.
5. **Problem-driven storytelling**: giải thích project theo vấn đề, thiết kế, thực nghiệm, kết quả và trade-off; không kể theo kiểu liệt kê tool.

---

## 4. Bảng rủi ro tổng thể

| ID | Rủi ro | Xác suất | Tác động | Ưu tiên | Cách kiểm soát | Bằng chứng cần có |
|---|---|---|---|---|---|---|
| R01 | Scope creep do ôm quá nhiều công nghệ | Cao | Cao | Cao | Chia rõ MVP và stretch goals | README có mục MVP vs Stretch |
| R02 | Benchmark methodology làm quá muộn | Cao | Cao | Cao | Viết methodology từ tuần 1 | `docs/benchmark_methodology.md` |
| R03 | Benchmark không tái lập được | Cao | Cao | Cao | Lưu seed, data volume, Spark config, query hash, run_id | `benchmark/benchmark_config.yaml`, `benchmark_summary.csv` |
| R04 | Synthetic data bị bắt bẻ là không đại diện production | Trung bình | Cao | Cao | Ghi rõ limitation và data profile | `docs/dataset_limitations.md`, `docs/data_profile.md` |
| R05 | Dùng sai thuật ngữ real-time | Cao | Trung bình | Cao | Ghi rõ micro-batch near real-time, target latency 1–5 phút | `docs/architecture.md` |
| R06 | Pipeline chạy được nhưng Gold metrics sai | Trung bình | Cao | Cao | Correctness tests cho revenue, failure rate, dedup, late events | `tests/test_metrics_correctness.py` |
| R07 | Duplicate events làm double-count revenue | Cao | Cao | Cao | Dedup theo `event_id`, test no double count | `tests/test_dedup.py` |
| R08 | Late events làm thiếu metric cũ | Trung bình | Cao | Cao | Dùng event-time, allowed lateness, affected window recomputation | `docs/late_events_and_watermark.md` |
| R09 | Data quality gate thiếu chặt chẽ | Trung bình | Cao | Cao | Validation rules ở Silver, DLQ cho bad records | `quality/validation_rules.py`, `silver_dlq_bad_events` |
| R10 | Incremental job không idempotent | Cao | Cao | Cao | MERGE/upsert, rerun same input không đổi output | `tests/test_idempotency.py` |
| R11 | Airflow retry/rerun tạo duplicate | Trung bình | Cao | Cao | Task idempotent, kiểm tra rerun cùng `processing_date` | `tests/test_airflow_rerun_idempotency.py` |
| R12 | Small files làm query chậm nhưng không đo được | Trung bình | Trung bình | Trung bình | Lưu file_count, average_file_size, query_time | `benchmark_results_compaction.csv` |
| R13 | Compaction được mô tả một chiều, thiếu trade-off | Trung bình | Trung bình | Trung bình | Đo cả query_time và compaction_runtime | `docs/small_files_and_compaction.md` |
| R14 | Delta Lake setup local bị lỗi | Trung bình | Trung bình | Trung bình | MVP chỉ dùng Delta; Iceberg để future work | `docs/table_format_choice.md` |
| R15 | Monitoring chỉ là dashboard đẹp, không có operational metrics | Trung bình | Trung bình | Trung bình | Dashboard phải có freshness, DLQ, duplicate, query_time, file_count | `monitoring/dashboard.py` |
| R16 | Demo video đẹp nhưng thiếu số liệu benchmark | Trung bình | Cao | Cao | Chỉ polish demo sau khi có benchmark_summary.csv | `benchmark/results/benchmark_summary.csv` |
| R17 | Không phân biệt business dashboard và operational dashboard | Trung bình | Trung bình | Trung bình | Tách rõ 2 nhóm dashboard | `docs/monitoring.md` |
| R18 | Local environment không phản ánh cloud production | Trung bình | Trung bình | Trung bình | Ghi rõ limitation và cloud mapping future work | `docs/future_work.md` |
| R19 | CI/CD quá nặng, khó chạy trên GitHub Actions | Trung bình | Trung bình | Trung bình | CI nhẹ: pytest, import check, docker compose config | `.github/workflows/ci.yml` nếu có |
| R20 | Report kể theo tool thay vì kể theo vấn đề | Cao | Trung bình | Cao | Dùng flow Problem → Design → Experiment → Result → Trade-off | `README.md`, `demo/demo_script.md` |

---

## 5. Chi tiết từng rủi ro

## R01 — Scope creep do ôm quá nhiều công nghệ

### Mô tả

Project có nhiều công nghệ hấp dẫn: Kafka, Spark, Delta Lake, Iceberg, Trino, dbt, Airflow, Streamlit, CI/CD, AQE, partition benchmark, cloud deployment. Nếu làm tất cả ngay từ đầu, project dễ bị vỡ tiến độ.

### Hậu quả

- MVP không hoàn thành.
- Có nhiều tool nhưng không có benchmark rõ.
- Không đủ thời gian viết report, demo và test.
- Dễ bị hỏi sâu vào phần chưa làm chắc.

### Cách kiểm soát

Chia project thành 2 phần:

- **MVP bắt buộc**: Producer, Apache Kafka KRaft, Bronze, Silver, Gold, Delta Lake, benchmark full refresh vs incremental, benchmark small files vs compaction, Airflow, monitoring.
- **Stretch goals**: Iceberg/Trino, partition benchmark nâng cao, Spark AQE benchmark, CI/CD nâng cao, cloud deployment.

### Bằng chứng

- `README.md` có mục MVP và Stretch Goals.
- `docs/future_work.md` ghi rõ phần nào chưa làm.
- Không đưa Iceberg/Trino/dbt vào MVP nếu chưa có thời gian.

---

## R02 — Benchmark methodology làm quá muộn

### Mô tả

Nếu đến cuối project mới nghĩ cách benchmark, kết quả dễ bị cảm tính: input khác nhau, config khác nhau, chạy một lần rồi kết luận.

### Hậu quả

- Benchmark không đáng tin.
- Không giải thích được vì sao strategy A nhanh hơn strategy B.
- Không trả lời được khi bị hỏi về seed, data volume, query hash, Spark config.

### Cách kiểm soát

Tạo ngay từ tuần 1:

- `docs/benchmark_methodology.md`
- `benchmark/benchmark_config.yaml`

Benchmark phải có:

- fixed random seed;
- data volume rõ ràng;
- Spark config;
- table layout;
- query text hoặc query hash;
- run_id;
- ít nhất 3 lần chạy;
- correctness status.

### Bằng chứng

- `docs/benchmark_methodology.md`
- `benchmark/benchmark_config.yaml`
- `benchmark/results/benchmark_summary.csv`

---

## R03 — Benchmark không tái lập được

### Mô tả

Benchmark không tái lập được nếu mỗi lần chạy dùng data khác, random seed khác, query khác hoặc Spark config khác.

### Hậu quả

- Không biết sự khác biệt runtime đến từ tối ưu hay từ input khác nhau.
- Không thể so sánh công bằng full refresh vs incremental.
- Không thể so sánh small files vs compaction.

### Cách kiểm soát

Mỗi benchmark run phải lưu:

```text
run_id
experiment_name
strategy_name
data_volume
random_seed
spark_config
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
created_at
```

### Bằng chứng

- `benchmark/results/benchmark_summary.csv`
- `benchmark/collect_metrics.py`
- `benchmark/benchmark_runner.py`

---

## R04 — Synthetic data bị bắt bẻ là không đại diện production

### Mô tả

Project dùng synthetic data để mô phỏng retail/payment events. Đây là hợp lý cho đồ án, nhưng không được claim kết quả đại diện tuyệt đối cho production.

### Hậu quả

- Bị hỏi: “Dữ liệu giả thì kết quả benchmark có ý nghĩa gì?”
- Bị đánh giá là thiếu thực tế nếu không nói rõ limitation.

### Cách kiểm soát

Ghi rõ trong `docs/dataset_limitations.md`:

- Dữ liệu là synthetic.
- Mục tiêu là kiểm thử pipeline behavior trong điều kiện kiểm soát.
- Có fixed seed và configurable error rates.
- Không claim production performance.
- Với production cần validate lại distribution, skew, burst traffic, schema drift, cloud object storage latency.

### Bằng chứng

- `docs/dataset_limitations.md`
- `docs/data_profile.md`
- Producer config có `random_seed`, `duplicate_rate`, `late_event_rate`, `malformed_rate`, `skew_mode`.

---

## R05 — Dùng sai thuật ngữ real-time

### Mô tả

Project có thể bị gọi sai là real-time hoặc hard real-time. Thực tế project dùng Spark Structured Streaming theo micro-batch, phù hợp near real-time analytics.

### Hậu quả

- Bị hỏi vì sao latency không phải millisecond.
- Bị bắt bẻ về hard real-time.
- Mục tiêu kỹ thuật bị hiểu sai.

### Cách kiểm soát

Ghi rõ trong `docs/architecture.md`:

```text
Project này là micro-batch near real-time analytics.
Target latency: 1–5 phút.
Project không phải hard real-time hoặc millisecond-level streaming.
```

### Bằng chứng

- `docs/architecture.md`
- `README.md` có mục Processing Mode.
- Demo không claim sub-second latency.

---

## R06 — Pipeline chạy được nhưng Gold metrics sai

### Mô tả

Pipeline có thể chạy end-to-end nhưng vẫn tính sai revenue, payment failure rate hoặc freshness.

### Hậu quả

- Dashboard đẹp nhưng số liệu sai.
- Benchmark nhanh nhưng không có giá trị vì output sai.
- Project mất trọng tâm Data Engineering correctness.

### Cách kiểm soát

Gold metrics phải có:

- metric definitions;
- grain rõ ràng;
- source rõ ràng;
- exclusions rõ ràng;
- correctness tests.

### Bằng chứng

- `docs/metric_definitions.md`
- `tests/test_metrics_correctness.py`
- `gold_payment_metrics_hourly`
- `gold_data_quality_summary`

---

## R07 — Duplicate events làm double-count revenue

### Mô tả

Duplicate event thường xuất hiện do retry, producer gửi lại, consumer đọc lại hoặc task rerun.

### Hậu quả

- Revenue bị cộng đôi.
- Payment count sai.
- Business dashboard không đáng tin.

### Cách kiểm soát

- Dùng `event_id` làm idempotency key.
- Silver dedup theo `event_id`.
- Duplicate records đưa vào `silver_duplicate_events`.
- Gold chỉ đọc từ `silver_clean_events`.

### Bằng chứng

- `processing/dedup.py`
- `tests/test_dedup.py`
- Test duplicate payment_authorized không làm revenue tăng.

---

## R08 — Late events làm thiếu metric cũ

### Mô tả

Late event có `event_time` cũ nhưng `ingestion_time` mới. Nếu incremental chỉ lọc theo `event_time > last_processed_time`, event cũ đến muộn có thể bị bỏ qua.

### Hậu quả

- Revenue theo giờ bị thiếu.
- Payment failure rate theo window cũ bị sai.
- Dashboard có vẻ fresh nhưng không correct.

### Cách kiểm soát

- Phân biệt `event_time`, `producer_time`, `ingestion_time`.
- Dùng allowed lateness policy.
- Dùng affected window recomputation.
- Late quá ngưỡng đưa vào `silver_late_events` hoặc xử lý bằng backfill.

### Bằng chứng

- `docs/late_events_and_watermark.md`
- `processing/late_event_handler.py`
- `tests/test_late_events.py`

---

## R09 — Data quality gate thiếu chặt chẽ

### Mô tả

Nếu validation ở Silver quá lỏng, malformed records có thể đi vào Gold.

### Hậu quả

- Dashboard sai.
- Metric không đáng tin.
- Không chứng minh được data quality.

### Cách kiểm soát

Các rule bắt buộc:

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

### Bằng chứng

- `quality/validation_rules.py`
- `quality/run_quality_checks.py`
- `tests/test_quality_rules.py`
- `silver_dlq_bad_events`

---

## R10 — Incremental job không idempotent

### Mô tả

Incremental pipeline dễ sai nếu append mù. Khi rerun cùng input, output có thể bị ghi thêm lần nữa.

### Hậu quả

- Gold metrics bị double count.
- Airflow retry gây sai dữ liệu.
- Backfill không an toàn.

### Cách kiểm soát

- Không append mù vào Gold.
- Xác định affected windows.
- Tính lại metric cho affected windows từ Silver.
- MERGE vào Gold theo grain.

### Bằng chứng

- `processing/incremental_job.py`
- `tests/test_idempotency.py`
- Rerun same input, Gold output không đổi.

---

## R11 — Airflow retry/rerun tạo duplicate

### Mô tả

Airflow retry task khi fail. Nếu task ghi dữ liệu không idempotent, retry có thể tạo duplicate.

### Hậu quả

- Gold metrics sai.
- Pipeline không an toàn khi vận hành.
- Backfill ngày cũ làm sai dữ liệu hiện tại.

### Cách kiểm soát

- Mỗi task có `run_id`, `processing_date`.
- Output có thể overwrite/merge an toàn theo key.
- Data quality fail thì dừng trước Gold.
- Test rerun cùng `processing_date`.

### Bằng chứng

- `orchestration/dags/lakehouse_optimization_dag.py`
- `tests/test_airflow_rerun_idempotency.py`
- Screenshot DAG graph.

---

## R12 — Small files làm query chậm nhưng không đo được

### Mô tả

Micro-batch dễ sinh nhiều small files. Nếu không đo file_count và average_file_size, project không chứng minh được vấn đề file layout.

### Hậu quả

- Không có bằng chứng cho benchmark compaction.
- Không giải thích được vì sao query chậm.

### Cách kiểm soát

Lưu các metrics:

- file_count;
- average_file_size_mb;
- query_time_seconds;
- compaction_runtime_seconds.

### Bằng chứng

- `benchmark/run_compaction_benchmark.py`
- `benchmark/results/compaction_benchmark_results.csv`
- `docs/small_files_and_compaction.md`

---

## R13 — Compaction được mô tả một chiều, thiếu trade-off

### Mô tả

Nếu chỉ nói compaction làm query nhanh hơn, phần phân tích chưa đủ. Compaction cũng tốn compute, I/O và thời gian maintenance.

### Hậu quả

- Bị hỏi: “Compaction có phải lúc nào cũng tốt không?”
- Thiếu tư duy trade-off.

### Cách kiểm soát

Benchmark phải đo cả:

- query_time_before;
- query_time_after;
- compaction_runtime_seconds;
- file_count before/after;
- average_file_size before/after.

### Bằng chứng

- `benchmark/results/compaction_benchmark_results.csv`
- README trade-off: small files vs compaction.

---

## R14 — Delta Lake setup local bị lỗi

### Mô tả

Lakehouse table format có thể gặp lỗi dependency, Spark package version hoặc local setup.

### Hậu quả

- Tuần Delta bị kéo dài.
- Không demo được transaction log/compaction.
- Scope bị chậm.

### Cách kiểm soát

- MVP chọn Delta Lake vì dễ hơn Iceberg trong local Spark setup.
- Chỉ dùng Iceberg ở phần lý thuyết/future work.
- Tạo script nhỏ test Delta table trước khi tích hợp toàn pipeline.

### Bằng chứng

- `lakehouse/create_delta_tables.py`
- `docs/table_format_choice.md`
- `docs/delta_vs_iceberg.md`

---

## R15 — Monitoring chỉ là dashboard đẹp, không có operational metrics

### Mô tả

Dashboard chỉ hiển thị revenue/order là chưa đủ. Project cần operational monitoring để chứng minh pipeline health.

### Hậu quả

- Dashboard giống bài BI bình thường.
- Không chứng minh được data observability.

### Cách kiểm soát

Dashboard phải có:

- freshness_seconds;
- pipeline_success_rate;
- dlq_count;
- duplicate_count;
- late_event_count;
- quality_pass_rate;
- processing_time_seconds;
- query_time_seconds;
- file_count;
- average_file_size_mb;
- compaction_runtime_seconds.

### Bằng chứng

- `monitoring/dashboard.py`
- `docs/monitoring.md`
- Screenshot dashboard.

---

## R16 — Demo video đẹp nhưng thiếu số liệu benchmark

### Mô tả

Nếu tập trung polish demo quá sớm, project có thể đẹp nhưng thiếu bằng chứng kỹ thuật.

### Hậu quả

- Demo không chứng minh được tối ưu.
- Không có bảng benchmark để đưa vào README/report.

### Cách kiểm soát

Chỉ polish demo sau khi có:

- benchmark full refresh vs incremental;
- benchmark small files vs compaction;
- correctness tests pass;
- dashboard có operational metrics.

### Bằng chứng

- `benchmark/results/benchmark_summary.csv`
- `README.md` có Benchmark Results.
- `demo/demo_script.md` dùng số liệu thật.

---

## R17 — Không phân biệt business dashboard và operational dashboard

### Mô tả

Business dashboard trả lời câu hỏi kinh doanh. Operational dashboard trả lời câu hỏi pipeline có khỏe không. Nếu trộn lẫn, người xem khó hiểu mục tiêu monitoring.

### Hậu quả

- Không giải thích rõ monitoring.
- Dashboard thiếu trọng tâm.

### Cách kiểm soát

Tách dashboard thành:

- Business Overview;
- Payment Monitoring;
- Data Quality Monitoring;
- Pipeline Health;
- Benchmark Results;
- File Layout Health.

### Bằng chứng

- `monitoring/dashboard.py`
- `docs/monitoring.md`

---

## R18 — Local environment không phản ánh cloud production

### Mô tả

Project chạy local bằng Docker Compose, MinIO, Spark local. Đây là hợp lý cho đồ án, nhưng không thể claim giống production cloud.

### Hậu quả

- Bị hỏi về cloud scalability, IAM, cost, object storage latency.
- Benchmark bị hiểu sai là production-level.

### Cách kiểm soát

Ghi rõ limitation:

- local environment;
- MinIO thay vì S3/GCS/ADLS;
- synthetic data;
- không benchmark cost cloud;
- cloud deployment là future work.

### Bằng chứng

- `docs/dataset_limitations.md`
- `docs/future_work.md`
- README có mục Limitations.

---

## R19 — CI/CD quá nặng, khó chạy trên GitHub Actions

### Mô tả

Nếu CI cố chạy toàn bộ Kafka/Spark/Airflow/benchmark đầy đủ, pipeline CI có thể quá nặng và dễ fail.

### Hậu quả

- CI không ổn định.
- Tốn thời gian debug infrastructure thay vì project chính.

### Cách kiểm soát

MVP CI chỉ cần:

- pytest unit tests;
- import check;
- docker compose config;
- small benchmark smoke test nếu đủ nhẹ.

### Bằng chứng

- `.github/workflows/ci.yml` nếu triển khai.
- `tests/integration/test_pipeline_smoke.py` nếu làm stretch goal.

---

## R20 — Report kể theo tool thay vì kể theo vấn đề

### Mô tả

Nhiều project Data Engineering bị kể theo kiểu: “em dùng Kafka, Spark, Airflow, Delta”. Cách kể này yếu vì không chứng minh được vì sao cần tool.

### Hậu quả

- Người nghe thấy project giống lắp tool.
- Không thấy research questions và benchmark.
- Khó nổi bật khi phỏng vấn.

### Cách kiểm soát

Kể theo flow:

```text
Problem
→ Architecture
→ Failure cases
→ Experiments
→ Results
→ Trade-offs
→ Limitations
```

### Bằng chứng

- `README.md`
- `report/report.md`
- `demo/demo_script.md`
- `docs/interview_questions.md`

---

## 6. Rủi ro cần theo dõi theo từng giai đoạn

## Tuần 1

Rủi ro chính:

- scope creep;
- benchmark methodology muộn;
- synthetic data limitation chưa rõ;
- micro-batch vs real-time chưa rõ.

Bằng chứng cần có:

- `docs/problem_statement.md`
- `docs/architecture.md`
- `docs/research_questions.md`
- `docs/benchmark_methodology.md`
- `docs/risk_register.md`
- `docs/dataset_limitations.md`
- `benchmark/benchmark_config.yaml`

---

## Tuần 2–4

Rủi ro chính:

- local infrastructure khó chạy;
- logging thiếu metadata;
- Bronze không replay được;
- Kafka offset không được lưu.

Bằng chứng cần có:

- Docker Compose chạy được;
- MinIO có bucket;
- Apache Kafka KRaft nhận message test;
- PostgreSQL có metadata tables;
- Bronze có source_topic/source_partition/source_offset.

---

## Tuần 5–7

Rủi ro chính:

- Silver validation thiếu rule;
- dedup sai key;
- late event làm sai Gold;
- Gold metric không có definition.

Bằng chứng cần có:

- `tests/test_dedup.py`
- `tests/test_late_events.py`
- `tests/test_quality_rules.py`
- `tests/test_metrics_correctness.py`
- `docs/metric_definitions.md`

---

## Tuần 8–12

Rủi ro chính:

- Delta setup lỗi;
- benchmark không tái lập;
- incremental không idempotent;
- compaction thiếu trade-off.

Bằng chứng cần có:

- Silver/Gold Delta tables;
- benchmark CSV;
- charts before/after;
- correctness_status pass;
- test idempotency pass.

---

## Tuần 13–16

Rủi ro chính:

- Airflow retry tạo duplicate;
- monitoring thiếu operational metrics;
- README thiếu limitation;
- demo đẹp nhưng không có số liệu.

Bằng chứng cần có:

- Airflow DAG screenshot;
- rerun test pass;
- monitoring dashboard screenshot;
- README có benchmark results, trade-offs, limitations;
- demo script theo problem-driven storytelling.

---

## 7. Definition of Done cho file này

File `docs/risk_register.md` được xem là hoàn thành khi có đủ:

- [ ] Danh sách rủi ro chính của MVP.
- [ ] Mức xác suất, tác động và ưu tiên.
- [ ] Cách kiểm soát từng rủi ro.
- [ ] Bằng chứng cần tạo ra cho từng rủi ro.
- [ ] Rủi ro về scope creep.
- [ ] Rủi ro về benchmark methodology.
- [ ] Rủi ro về synthetic data.
- [ ] Rủi ro về micro-batch/near real-time.
- [ ] Rủi ro về correctness.
- [ ] Rủi ro về idempotency.
- [ ] Rủi ro về small files/compaction.
- [ ] Rủi ro về monitoring/demo/report.

---

## 8. Cách trả lời phỏng vấn ngắn gọn

Nếu được hỏi:

> Em kiểm soát rủi ro project như thế nào?

Có thể trả lời:

> Em chia project thành MVP và stretch goals để tránh scope creep. MVP tập trung vào pipeline Kafka → Bronze → Silver → Gold → Delta Lake, benchmark full refresh vs incremental, benchmark small files vs compaction, Airflow retry/idempotency và monitoring. Em tạo risk register từ tuần 1 để theo dõi các rủi ro chính như benchmark không tái lập, synthetic data không đại diện production, nhầm lẫn micro-batch với hard real-time, duplicate events làm sai revenue và Airflow rerun tạo duplicate. Với mỗi rủi ro, em định nghĩa cách kiểm soát và bằng chứng cần tạo ra, ví dụ fixed seed, benchmark config, correctness tests, idempotency tests, dataset limitation và dashboard operational metrics.

---

## 9. Trạng thái cập nhật

| Ngày | Người cập nhật | Nội dung thay đổi |
|---|---|---|
| YYYY-MM-DD | Người thực hiện project | Tạo bản đầu tiên của risk register |
