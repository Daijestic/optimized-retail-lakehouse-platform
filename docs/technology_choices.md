# Lựa chọn công nghệ

## 1. Mục đích của tài liệu

Tài liệu này giải thích các lựa chọn công nghệ chính trong project:

**Xây dựng và đánh giá nền tảng Lakehouse tối ưu hiệu năng cho phân tích dữ liệu bán lẻ/thanh toán gần thời gian thực**.

Mục tiêu của tài liệu không phải là liệt kê nhiều tool cho “đủ stack”, mà là giải thích:

- mỗi công nghệ giải quyết vấn đề gì;
- vì sao chọn công nghệ đó trong phạm vi MVP;
- công nghệ đó nằm ở đâu trong kiến trúc tổng thể;
- có lựa chọn thay thế nào;
- trade-off của từng lựa chọn là gì;
- phần nào được đưa vào MVP, phần nào để future work.

Tư duy chính của project:

```text
Không chọn tool vì tool phổ biến.
Chọn tool vì nó giúp giải quyết một vấn đề cụ thể của data platform.
```

---

## 2. Nguyên tắc lựa chọn công nghệ

Các công nghệ trong project được chọn theo 7 nguyên tắc sau.

### 2.1. MVP-first

Project ưu tiên hoàn thành MVP có thể bảo vệ được trước, sau đó mới mở rộng sang các phần nâng cao.

MVP bắt buộc gồm:

- sinh synthetic retail/payment events;
- ingest event stream vào Bronze;
- xử lý Silver gồm validation, deduplication, late-event flag và DLQ;
- tạo Gold metrics có correctness tests;
- dùng Delta Lake làm table format chính;
- benchmark full refresh vs incremental;
- benchmark small files vs compaction;
- orchestration bằng Airflow;
- monitoring dashboard;
- README/report có benchmark, trade-off và limitation.

Các phần như Iceberg, Trino, Spark AQE benchmark nâng cao, partition benchmark nâng cao, CI/CD nâng cao được để ở future work hoặc stretch goals.

---

### 2.2. Local-first

Project chạy trước trên máy cá nhân bằng Docker Compose, MinIO, Redpanda/Kafka, Spark local, PostgreSQL, Airflow và Streamlit.

Lý do:

- dễ phát triển và debug;
- không phát sinh chi phí cloud;
- dễ demo khi bảo vệ;
- phù hợp với scope đồ án/fresher portfolio;
- vẫn mô phỏng được kiến trúc production ở mức concept.

Trade-off:

- kết quả benchmark local không đại diện tuyệt đối cho production;
- latency, I/O và object storage behavior khác cloud thật;
- không đánh giá được đầy đủ IAM, autoscaling, cost optimization và multi-user concurrency.

---

### 2.3. Problem-driven

Mỗi công nghệ phải gắn với một vấn đề cụ thể:

| Vấn đề | Công nghệ được chọn |
|---|---|
| Mô phỏng event stream, duplicate, retry | Kafka/Redpanda |
| Lưu raw data dạng object storage | MinIO |
| Xử lý batch/micro-batch, dedup, aggregation | Spark/PySpark |
| Quản lý table, schema, transaction, compaction | Delta Lake |
| Lưu metadata pipeline và benchmark | PostgreSQL |
| Orchestration, retry, rerun, backfill | Airflow |
| Dashboard monitoring | Streamlit |
| Kiểm tra correctness | pytest |
| Tạo dữ liệu giả có kiểm soát | Faker/Pydantic/custom producer |
| Local reproducible environment | Docker Compose |

---

### 2.4. Dễ giải thích khi phỏng vấn

Project hướng tới Fresher/Junior Data Engineer, vì vậy công nghệ cần đủ thực tế nhưng không quá rộng.

Ưu tiên các công nghệ dễ giải thích theo câu chuyện:

```text
Kafka/Redpanda → Bronze → Silver → Gold → Delta Lake → Benchmark → Airflow → Monitoring
```

Không nên thêm quá nhiều công nghệ nếu không có research question hoặc benchmark đi kèm.

---

### 2.5. Có bằng chứng, không chỉ có tool

Mỗi công nghệ phải tạo ra bằng chứng trong repo.

Ví dụ:

| Công nghệ | Bằng chứng cần có |
|---|---|
| Kafka/Redpanda | event được publish/consume, có topic, offset metadata |
| Bronze | raw immutable files, source_offset, replay guide |
| Silver | clean events, duplicate events, late events, DLQ |
| Delta Lake | Delta tables, transaction log, compaction demo |
| Spark | jobs xử lý Silver/Gold và benchmark |
| Airflow | DAG chạy end-to-end, retry/rerun không duplicate |
| Streamlit | dashboard có freshness, DLQ, duplicate, query time, file count |
| pytest | tests pass cho dedup, data quality, metric correctness, idempotency |

---

### 2.6. Benchmark-aware

Vì project có mục tiêu “đánh giá và tối ưu hiệu năng”, lựa chọn công nghệ phải hỗ trợ benchmark.

MVP cần benchmark:

1. Full refresh vs incremental.
2. Small files vs compaction.

Vì vậy cần:

- Spark để chạy transform và đo runtime;
- Delta Lake để hỗ trợ table format và compaction;
- PostgreSQL hoặc CSV để lưu benchmark results;
- fixed random seed để tái lập input data;
- benchmark_config.yaml để lưu cấu hình;
- pytest để đảm bảo strategy nhanh hơn nhưng không sai correctness.

---

### 2.7. Có thể mapping lên cloud sau này

Mặc dù project chạy local, kiến trúc phải có thể mapping sang cloud:

| Local | AWS tương ứng | GCP tương ứng |
|---|---|---|
| MinIO | Amazon S3 | Google Cloud Storage |
| Redpanda/Kafka | Amazon MSK | Pub/Sub hoặc Managed Kafka |
| Spark local | AWS Glue/EMR | Dataproc |
| PostgreSQL metadata | RDS PostgreSQL | Cloud SQL PostgreSQL |
| Airflow local | MWAA | Cloud Composer |
| Streamlit local | ECS/EC2/App Runner | Cloud Run/GCE |
| Delta Lake | Delta on S3 | Delta on GCS |

Phần cloud deployment không nằm trong MVP, nhưng README nên giải thích mapping này ở mức future work.

---

## 3. Tổng quan stack được chọn

Kiến trúc công nghệ tổng thể:

```text
Synthetic Retail / Payment Events
        ↓
Python Producer
        ↓
Kafka / Redpanda
        ↓
Bronze Layer on MinIO
        ↓
Spark / PySpark
        ↓
Silver Delta Tables
        ↓
Gold Delta Tables
        ↓
Benchmark Layer
        ↓
Airflow DAG
        ↓
PostgreSQL Metadata
        ↓
Streamlit Monitoring Dashboard
```

---

## 4. Docker Compose

### 4.1. Vai trò

Docker Compose được dùng để chạy toàn bộ local data platform bằng một file cấu hình.

Các service dự kiến:

- Redpanda hoặc Kafka;
- MinIO;
- PostgreSQL;
- Spark;
- Airflow;
- Streamlit.

Docker Compose phù hợp vì project có nhiều service cần chạy cùng nhau, có network chung, volume riêng và biến môi trường riêng.

### 4.2. Vì sao chọn Docker Compose?

Lý do:

- dễ setup trên máy cá nhân;
- tái lập môi trường tốt hơn chạy thủ công từng service;
- chỉ cần một lệnh `docker compose up` để khởi động platform;
- phù hợp với demo và onboarding;
- dễ định nghĩa network, volume, port mapping.

### 4.3. Vì sao chưa dùng Kubernetes?

Không chọn Kubernetes cho MVP vì:

- tăng độ phức tạp hạ tầng;
- không cần thiết cho đồ án local;
- dễ làm lệch trọng tâm từ Data Engineering sang Platform Engineering;
- tốn thời gian debug cluster thay vì tập trung vào pipeline, quality và benchmark.

### 4.4. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Dễ chạy local | Không phản ánh đầy đủ production orchestration |
| Dễ demo | Không có autoscaling thật |
| Dễ quản lý service | Networking/volume local khác cloud |
| Phù hợp MVP | Không thay thế Kubernetes production |

---

## 5. Kafka hoặc Redpanda

### 5.1. Vai trò

Kafka/Redpanda là event streaming layer.

Trong project, nó dùng để mô phỏng luồng sự kiện bán lẻ/thanh toán:

- `order_created`;
- `payment_authorized`;
- `payment_failed`;
- `refund_requested`;
- `order_cancelled`.

Producer gửi events vào topic. Consumer đọc events từ topic và ghi vào Bronze.

### 5.2. Vì sao cần event streaming layer?

Vì project không chỉ xử lý file CSV có sẵn. Project cần mô phỏng các vấn đề thực tế:

- event đến liên tục;
- duplicate do retry;
- late events;
- malformed events;
- replay theo offset;
- ingestion metadata;
- near real-time analytics.

Nếu chỉ đọc CSV batch, project sẽ thiếu các vấn đề quan trọng của Data Engineering pipeline.

### 5.3. Kafka hay Redpanda?

MVP có thể dùng Redpanda nếu muốn nhẹ hơn khi chạy local, nhưng vẫn giữ Kafka-compatible API.

Cách ghi trong README:

```text
Project sử dụng Redpanda trong môi trường local để mô phỏng Kafka-compatible event streaming. Về mặt kiến trúc, thành phần này đại diện cho Kafka/Event Streaming Layer.
```

### 5.4. Vì sao không chỉ dùng file input?

Không chỉ dùng file input vì:

- khó mô phỏng offset;
- khó mô phỏng retry/duplicate theo event stream;
- khó giải thích near real-time ingestion;
- ít giống bài toán production;
- không thể hiện rõ event-driven architecture.

### 5.5. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Mô phỏng event stream thực tế | Tăng độ phức tạp local setup |
| Có topic/offset/replay | Cần hiểu producer/consumer |
| Tạo được duplicate/late events | Có thể overkill nếu chỉ làm dashboard |
| Phù hợp near real-time | Cần monitoring thêm |

---

## 6. Python Producer

### 6.1. Vai trò

Python Producer sinh synthetic retail/payment events và gửi vào Kafka/Redpanda.

Producer cần sinh được:

- valid events;
- duplicate events;
- late events;
- malformed events;
- negative amount events;
- unsupported schema version events;
- skewed events nếu bật `skew_mode`.

### 6.2. Vì sao tự viết producer?

Tự viết producer giúp kiểm soát input cho benchmark.

Các tham số cần có:

```yaml
random_seed: 42
data_volume: 1000000
duplicate_rate: 0.05
late_event_rate: 0.03
malformed_rate: 0.01
negative_amount_rate: 0.005
unsupported_schema_version_rate: 0.005
skew_mode: none
```

Nếu dùng dataset có sẵn, bạn khó kiểm soát duplicate rate, late event rate và malformed rate.

### 6.3. Công nghệ hỗ trợ

- Python: viết producer nhanh, dễ test;
- Pydantic: định nghĩa schema và validate event;
- Faker: sinh dữ liệu giả như customer, location, product;
- pytest: test schema và bad event generator.

### 6.4. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Kiểm soát được dữ liệu đầu vào | Synthetic data không đại diện tuyệt đối production |
| Có fixed seed để benchmark | Cần viết data profile rõ |
| Sinh được lỗi có chủ đích | Có nguy cơ data quá đơn giản nếu thiết kế kém |
| Phù hợp research questions | Cần ghi limitation trong docs |

---

## 7. MinIO

### 7.1. Vai trò

MinIO được dùng làm object storage local, mô phỏng S3-compatible storage.

Trong project, MinIO lưu:

- Bronze raw events;
- Silver Delta tables;
- Gold Delta tables;
- benchmark outputs;
- file layout để đo small files/compaction.

### 7.2. Vì sao chọn MinIO?

Lý do:

- chạy local được bằng Docker;
- tương thích S3 API ở mức phù hợp cho học tập;
- mô phỏng object storage trong kiến trúc Lakehouse;
- không tốn chi phí cloud;
- phù hợp để hiểu bucket, prefix, object, file layout.

### 7.3. Vì sao không dùng local filesystem đơn giản?

Không chỉ dùng local filesystem vì:

- Lakehouse hiện đại thường đặt dữ liệu trên object storage;
- MinIO giúp tư duy gần hơn với S3/GCS/ADLS;
- dễ mô phỏng bucket layout Bronze/Silver/Gold;
- phù hợp khi sau này mapping lên cloud.

### 7.4. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Chạy local, không tốn cloud cost | Không giống hoàn toàn S3 production |
| S3-compatible | Benchmark local không đại diện cloud latency |
| Phù hợp Lakehouse | Cần setup credentials/bucket |
| Dễ demo file layout | Không có IAM/cost model thật |

---

## 8. Apache Spark / PySpark

### 8.1. Vai trò

Spark/PySpark là processing engine chính.

Trong project, Spark xử lý:

- đọc Bronze;
- parse raw payload;
- validate schema;
- dedup events;
- xử lý late events;
- ghi Silver;
- tính Gold metrics;
- chạy full refresh job;
- chạy incremental job;
- chạy compaction benchmark;
- đo processing time và query time.

### 8.2. Vì sao chọn Spark?

Lý do:

- phổ biến trong Data Engineering;
- hỗ trợ batch và Structured Streaming;
- xử lý được DataFrame API;
- tích hợp tốt với Delta Lake;
- phù hợp benchmark full refresh vs incremental;
- phù hợp xử lý dữ liệu lớn hơn pandas.

### 8.3. Spark Structured Streaming trong project

Project dùng Spark Structured Streaming theo hướng micro-batch near real-time.

Ghi rõ:

```text
Project này không phải hard real-time.
Project dùng micro-batch near real-time với target latency 1–5 phút.
```

### 8.4. Vì sao không dùng Flink cho MVP?

Apache Flink rất mạnh cho true streaming/low-latency stateful processing, nhưng không chọn cho MVP vì:

- scope sẽ rộng hơn;
- setup local phức tạp hơn;
- project tập trung Lakehouse + benchmark + correctness;
- Spark + Delta phù hợp hơn để hoàn thành MVP;
- mục tiêu là near real-time analytics 1–5 phút, không phải millisecond-level streaming.

### 8.5. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Phổ biến trong DE | Tốn tài nguyên hơn pandas |
| Hỗ trợ batch + streaming | Cần hiểu Spark config |
| Tích hợp Delta tốt | Debug local đôi khi phức tạp |
| Phù hợp benchmark | Có overhead với data quá nhỏ |

---

## 9. Delta Lake

### 9.1. Vai trò

Delta Lake là table format chính trong MVP.

Delta được dùng cho:

- Silver clean events;
- Silver duplicate/late/DLQ nếu cần;
- Gold metrics;
- transaction log;
- schema enforcement/evolution;
- merge/upsert;
- time travel;
- compaction;
- benchmark small files vs compaction.

### 9.2. Vì sao chọn Delta Lake cho MVP?

Lý do:

- dễ tích hợp với Spark local;
- dễ demo transaction log;
- dễ giải thích ACID trên data lake;
- hỗ trợ schema enforcement;
- hỗ trợ time travel;
- hỗ trợ merge/update/delete;
- có cơ chế tối ưu liên quan đến small files/compaction;
- phù hợp với benchmark bắt buộc của project.

### 9.3. Vì sao chưa chọn Iceberg làm table format chính?

Apache Iceberg rất tốt cho Lakehouse hiện đại, đặc biệt ở:

- hidden partitioning;
- partition evolution;
- schema evolution;
- time travel;
- multi-engine support.

Tuy nhiên, Iceberg được để ở future work vì:

- setup catalog có thể làm scope rộng hơn;
- nếu thêm Trino/Iceberg ngay từ đầu, dễ bị trễ MVP;
- project cần hoàn thành correctness + benchmark trước;
- Delta đủ tốt để chứng minh Lakehouse concept trong MVP.

### 9.4. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Dễ chạy với Spark | Ít nhấn mạnh hidden partitioning như Iceberg |
| Có transaction log dễ demo | Multi-engine story cần giải thích thêm |
| Hỗ trợ merge/compaction | Một số tính năng nâng cao tùy môi trường |
| Phù hợp MVP | Iceberg vẫn nên nhắc trong future work |

---

## 10. Apache Parquet

### 10.1. Vai trò

Parquet là file format dạng cột dùng trong Lakehouse.

Trong project:

- Bronze có thể lưu JSONL hoặc Parquet raw;
- Silver/Gold Delta tables lưu dữ liệu dưới dạng Parquet files kèm Delta transaction log.

### 10.2. Vì sao chọn Parquet?

Lý do:

- định dạng mở;
- dạng cột, phù hợp analytics;
- hỗ trợ compression;
- được Spark/Delta/Iceberg/Trino/DuckDB hỗ trợ rộng rãi;
- phù hợp query chỉ đọc một số cột.

### 10.3. Vì sao không dùng CSV cho Silver/Gold?

CSV không phù hợp cho Silver/Gold vì:

- không lưu schema tốt;
- query chậm hơn format cột;
- nén kém hơn;
- dễ lỗi delimiter/encoding;
- không phù hợp benchmark Lakehouse nghiêm túc.

CSV có thể dùng cho sample output nhỏ, nhưng không nên dùng làm storage chính cho Silver/Gold.

---

## 11. PostgreSQL metadata database

### 11.1. Vai trò

PostgreSQL được dùng làm metadata database cho pipeline và benchmark.

Các bảng metadata dự kiến:

- `pipeline_runs`;
- `task_runs`;
- `ingestion_runs`;
- `data_quality_results`;
- `benchmark_runs`;
- `file_layout_metrics`.

### 11.2. Vì sao cần metadata database?

Nếu chỉ ghi log ra console, rất khó theo dõi pipeline.

Metadata database giúp lưu:

- run_id;
- task_name;
- status;
- input_rows;
- output_rows;
- started_at;
- ended_at;
- error_message;
- benchmark results;
- file_count;
- average_file_size;
- freshness_seconds.

### 11.3. Vì sao chọn PostgreSQL?

Lý do:

- phổ biến;
- open-source;
- dễ chạy bằng Docker;
- phù hợp lưu metadata dạng relational;
- dễ query từ Streamlit dashboard;
- dễ mở rộng khi cần.

### 11.4. Vì sao không lưu metadata bằng CSV?

Không chỉ dùng CSV vì:

- khó query dashboard;
- khó join nhiều bảng metadata;
- khó quản lý concurrent write;
- không giống production metadata tracking;
- không phù hợp khi Airflow và monitoring cùng đọc/ghi.

### 11.5. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Dễ dùng, phổ biến | Thêm một service cần vận hành |
| Query metadata tốt | Không phải nơi lưu big data chính |
| Hợp dashboard | Cần thiết kế schema metadata |
| Hỗ trợ audit pipeline | Cần backup nếu production |

---

## 12. Apache Airflow

### 12.1. Vai trò

Airflow được dùng để orchestration pipeline.

Airflow không xử lý dữ liệu lớn trực tiếp. Airflow điều phối task:

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

### 12.2. Vì sao chọn Airflow?

Lý do:

- phổ biến trong Data Engineering;
- mô tả workflow bằng DAG;
- có retry policy;
- hỗ trợ rerun/backfill;
- có UI để demo pipeline;
- giúp kiểm tra idempotency khi task chạy lại.

### 12.3. Điểm cần chứng minh

Không chỉ cần Airflow UI chạy được. Project cần chứng minh:

- DAG chạy end-to-end;
- task có retry policy;
- rerun cùng `processing_date` không tạo duplicate;
- data quality fail thì dừng trước Gold;
- có log failed/retry/success;
- có screenshot DAG graph.

### 12.4. Vì sao không dùng cron?

Cron chỉ lập lịch đơn giản, không đủ cho project này vì:

- khó mô tả dependency phức tạp;
- khó retry theo task;
- khó backfill/rerun có kiểm soát;
- không có UI pipeline;
- không thể hiện rõ orchestration trong Data Engineering.

### 12.5. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| DAG rõ ràng | Setup hơi nặng |
| Retry/rerun/backfill | Không xử lý big data trực tiếp |
| Có UI demo | Cần thiết kế idempotent task |
| Phổ biến trong DE | Có thể overkill nếu pipeline quá nhỏ |

---

## 13. Streamlit

### 13.1. Vai trò

Streamlit được dùng để xây monitoring dashboard.

Dashboard gồm 2 nhóm:

1. Business dashboard.
2. Operational dashboard.

Business metrics:

- revenue per hour;
- orders per hour;
- payment failure rate.

Operational metrics:

- freshness seconds;
- pipeline success rate;
- DLQ count;
- duplicate count;
- late event count;
- processing time;
- query time;
- file count;
- average file size;
- compaction runtime.

### 13.2. Vì sao chọn Streamlit?

Lý do:

- dễ viết bằng Python;
- phù hợp dashboard nội bộ/demo;
- nhanh hơn so với xây frontend riêng;
- dễ đọc dữ liệu từ PostgreSQL/CSV;
- phù hợp portfolio.

### 13.3. Vì sao không dùng Power BI/Tableau?

Power BI/Tableau rất tốt cho BI dashboard, nhưng project này cần operational dashboard gắn với pipeline metadata và benchmark results.

Streamlit phù hợp hơn vì:

- dễ tích hợp Python;
- dễ hiển thị benchmark charts;
- dễ đọc metadata từ PostgreSQL;
- dễ version control trong GitHub;
- không cần tool thương mại.

### 13.4. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Nhanh, dễ demo | Không mạnh bằng BI enterprise |
| Python-native | UI không chuyên nghiệp bằng frontend riêng |
| Hợp dashboard kỹ thuật | Cần tự thiết kế layout |
| Dễ đưa vào repo | Không phải monitoring system production như Grafana |

---

## 14. pytest

### 14.1. Vai trò

pytest dùng để kiểm tra correctness.

Các test bắt buộc:

- `test_event_schema.py`;
- `test_bad_events.py`;
- `test_dedup.py`;
- `test_late_events.py`;
- `test_quality_rules.py`;
- `test_idempotency.py`;
- `test_metrics_correctness.py`;
- `test_airflow_rerun_idempotency.py`.

### 14.2. Vì sao cần test?

Benchmark nhanh nhưng sai thì không có giá trị.

Project cần test để chứng minh:

- bad event không vào Gold;
- duplicate không làm tăng revenue;
- late event được xử lý đúng policy;
- payment failure rate tính đúng;
- rerun không tạo duplicate;
- incremental job idempotent.

### 14.3. Vì sao chọn pytest?

Lý do:

- phổ biến trong Python;
- dễ viết unit test và data test nhỏ;
- dễ chạy trong CI;
- phù hợp kiểm tra logic producer, validation, dedup và metric correctness.

---

## 15. GitHub Actions

### 15.1. Vai trò trong MVP

GitHub Actions chỉ cần CI nhẹ trong MVP.

CI nên chạy:

- pytest;
- Python import check;
- docker compose config check;
- small benchmark smoke test nếu kịp.

### 15.2. Vì sao chưa CI/CD nâng cao?

CI/CD nâng cao được để stretch goal vì:

- MVP cần ưu tiên pipeline, correctness và benchmark;
- full benchmark không nên chạy trong CI vì tốn thời gian;
- CI local smoke test là đủ cho tuần 16.

### 15.3. Trade-off

| Ưu điểm | Nhược điểm |
|---|---|
| Tự động bắt lỗi cơ bản | Không chạy full benchmark |
| Hợp portfolio GitHub | Cần setup workflow |
| Tăng độ tin cậy repo | CI local khác môi trường production |

---

## 16. Công nghệ không đưa vào MVP

### 16.1. Apache Iceberg

Iceberg để future work.

Lý do:

- rất tốt cho hidden partitioning và partition evolution;
- phù hợp multi-engine Lakehouse;
- nhưng setup catalog và query engine có thể làm scope rộng.

Cách viết trong README:

```text
MVP sử dụng Delta Lake để tập trung vào transaction log, schema enforcement và compaction benchmark. Apache Iceberg được xem là future work để nghiên cứu hidden partitioning, partition evolution và multi-engine interoperability.
```

---

### 16.2. Trino

Trino để future work.

Lý do:

- rất tốt cho SQL query engine trên Lakehouse;
- phù hợp nếu dùng Iceberg/Delta multi-engine;
- nhưng MVP có thể dùng Spark SQL và Streamlit trước.

---

### 16.3. dbt

dbt chưa bắt buộc trong MVP.

Lý do:

- dbt rất tốt cho SQL transformation, data modeling và incremental models;
- nhưng project MVP đã có Spark jobs, Delta tables và benchmark;
- thêm dbt quá sớm có thể làm scope rộng.

Có thể đưa dbt vào future work:

```text
Future work: dùng dbt cho Gold layer modeling, metric definitions và incremental SQL models.
```

---

### 16.4. Kubernetes

Kubernetes không nằm trong MVP.

Lý do:

- project chưa cần production orchestration;
- Docker Compose đủ cho local data platform;
- thêm Kubernetes sẽ làm scope lệch sang infrastructure.

---

### 16.5. Flink

Flink không nằm trong MVP.

Lý do:

- project là micro-batch near real-time, không phải hard real-time;
- Spark Structured Streaming đủ cho target latency 1–5 phút;
- Flink có thể là future work nếu muốn so sánh streaming engine.

---

## 17. Bảng tổng hợp lựa chọn công nghệ

| Layer | Công nghệ chọn | Vai trò | MVP hay Future Work |
|---|---|---|---|
| Local environment | Docker Compose | Chạy multi-service local | MVP |
| Event streaming | Redpanda/Kafka | Mô phỏng event stream | MVP |
| Data generation | Python + Faker/Pydantic | Sinh synthetic events | MVP |
| Object storage | MinIO | Lưu Bronze/Silver/Gold | MVP |
| Processing | Spark/PySpark | Transform, benchmark | MVP |
| Streaming mode | Spark Structured Streaming micro-batch | Near real-time 1–5 phút | MVP |
| Table format | Delta Lake | ACID, schema, compaction | MVP |
| File format | Parquet | Columnar analytics format | MVP |
| Metadata DB | PostgreSQL | Lưu pipeline/benchmark metadata | MVP |
| Orchestration | Airflow | DAG, retry, backfill | MVP |
| Dashboard | Streamlit | Monitoring dashboard | MVP |
| Testing | pytest | Correctness tests | MVP |
| CI | GitHub Actions | CI nhẹ | MVP cuối |
| Table format nâng cao | Iceberg | Hidden partitioning, partition evolution | Future work |
| SQL query engine | Trino | Multi-engine SQL query | Future work |
| Transform framework | dbt | Gold modeling/incremental SQL | Future work |
| Container orchestration | Kubernetes | Production deployment | Future work |
| Streaming engine | Flink | Low-latency true streaming | Future work |

---

## 18. Câu trả lời phỏng vấn mẫu

### 18.1. Vì sao không chỉ dùng PostgreSQL?

Không chỉ dùng PostgreSQL vì project không chỉ cần lưu dữ liệu và làm dashboard. Bài toán cần mô phỏng event stream, duplicate events, late events, malformed records, replay từ Bronze, xử lý Silver, tạo Gold metrics, benchmark full refresh vs incremental và small files vs compaction. PostgreSQL phù hợp để lưu metadata pipeline và benchmark, nhưng không phải storage chính cho Lakehouse data ở quy mô file/object storage.

---

### 18.2. Vì sao dùng Kafka/Redpanda?

Kafka/Redpanda được dùng để mô phỏng event streaming layer. Nó giúp project tạo được dữ liệu đến liên tục, duplicate do retry, late events và replay theo offset. Nếu chỉ đọc file batch, project sẽ thiếu các vấn đề thực tế của pipeline near real-time.

---

### 18.3. Vì sao dùng MinIO?

MinIO được dùng để mô phỏng S3-compatible object storage trên local. Vì Lakehouse thường lưu dữ liệu trên object storage, MinIO giúp project thực hành bucket, prefix, file layout, Bronze/Silver/Gold và small files/compaction mà không cần dùng cloud thật.

---

### 18.4. Vì sao dùng Spark?

Spark được dùng làm processing engine để đọc Bronze, xử lý Silver, tạo Gold metrics và chạy benchmark. Spark phù hợp vì hỗ trợ batch, Structured Streaming, DataFrame API và tích hợp tốt với Delta Lake.

---

### 18.5. Vì sao chọn Delta Lake thay vì Iceberg?

MVP chọn Delta Lake vì dễ tích hợp với Spark local, dễ demo transaction log, schema enforcement, time travel, merge và compaction. Iceberg rất tốt ở hidden partitioning và partition evolution, nhưng được để future work để tránh scope quá rộng.

---

### 18.6. Vì sao dùng Airflow?

Airflow được dùng để orchestration pipeline, không phải để xử lý dữ liệu lớn. Airflow giúp mô tả DAG, task dependency, retry, rerun và backfill. Điểm quan trọng trong project là chứng minh task rerun không tạo duplicate và data quality fail thì không publish Gold metrics sai.

---

### 18.7. Vì sao dùng Streamlit?

Streamlit được dùng để xây monitoring dashboard nhanh bằng Python. Dashboard không chỉ hiển thị business metrics như revenue và payment failure rate, mà còn hiển thị operational metrics như freshness, DLQ count, duplicate count, query time, file count và compaction runtime.

---

## 19. Kết luận

Stack công nghệ của project được chọn theo hướng MVP-first, local-first và problem-driven.

Công nghệ cốt lõi của MVP:

```text
Docker Compose
Kafka/Redpanda
Python Producer
MinIO
Spark/PySpark
Delta Lake
PostgreSQL
Airflow
Streamlit
pytest
```

Các công nghệ này không được dùng để “làm đẹp tech stack”, mà để giải quyết các vấn đề cụ thể:

- event streaming;
- raw immutable Bronze;
- data quality ở Silver;
- correctness ở Gold;
- transaction và table management bằng Delta;
- benchmark hiệu năng;
- retry/rerun an toàn bằng Airflow;
- monitoring pipeline health.

Các công nghệ nâng cao như Iceberg, Trino, dbt, Kubernetes và Flink được đưa vào future work để project có hướng mở rộng nhưng không làm vỡ scope MVP.

---

## 20. Tài liệu tham khảo

1. Apache Kafka Documentation — https://kafka.apache.org/documentation/
2. Apache Spark Structured Streaming Programming Guide — https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html
3. Apache Spark SQL Performance Tuning — https://spark.apache.org/docs/latest/sql-performance-tuning.html
4. Delta Lake Documentation — https://docs.delta.io/
5. Delta Lake on Databricks — https://docs.databricks.com/aws/en/delta/
6. Apache Airflow Documentation — https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html
7. Docker Compose Documentation — https://docs.docker.com/compose/
8. MinIO — https://www.min.io/
9. PostgreSQL — https://www.postgresql.org/
10. Streamlit Documentation — https://docs.streamlit.io/
