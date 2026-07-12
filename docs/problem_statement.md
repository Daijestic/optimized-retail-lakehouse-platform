# Phát biểu bài toán

## 1. Tên đề tài

| Ngôn ngữ | Tên đề tài |
|---|---|
| **Tiếng Việt** | Xây dựng và đánh giá nền tảng Lakehouse tối ưu hiệu năng cho phân tích dữ liệu bán lẻ/thanh toán gần thời gian thực. |
| **Tiếng Anh** | Design and Evaluation of a Performance-Optimized Lakehouse Platform for Near Real-time Retail and Payment Analytics. |

---

## 2. Bối cảnh bài toán

Trong các hệ thống bán lẻ và thanh toán hiện đại, dữ liệu không chỉ được sinh ra theo dạng bảng truyền thống, mà còn xuất hiện liên tục dưới dạng sự kiện.

Ví dụ:

- Khách hàng tạo đơn hàng.
- Thanh toán thành công.
- Thanh toán thất bại.
- Hoàn tiền.
- Đơn hàng bị hủy.
- Sự kiện đến muộn do lỗi mạng hoặc retry.
- Sự kiện bị gửi trùng do producer hoặc pipeline chạy lại.
- Bản ghi lỗi do sai schema hoặc thiếu trường bắt buộc.

Các sự kiện này thường cần được xử lý gần thời gian thực để phục vụ các nhu cầu như:

- Theo dõi doanh thu theo giờ.
- Theo dõi tỷ lệ thanh toán thất bại.
- Phát hiện bất thường trong số lượng đơn hàng.
- Kiểm tra dữ liệu có bị trễ hay không.
- Kiểm soát chất lượng dữ liệu trước khi đưa vào dashboard.
- Đánh giá hiệu năng của pipeline khi dữ liệu tăng lên.

Nếu chỉ lưu dữ liệu vào một cơ sở dữ liệu quan hệ như PostgreSQL rồi làm dashboard, hệ thống sẽ khó thể hiện được các vấn đề thực tế của Data Engineering như dữ liệu đến muộn, duplicate events, malformed records, replay, backfill, incremental processing, small files, compaction và data freshness.

Nếu chỉ dùng Data Lake dạng file thô, hệ thống có thể lưu dữ liệu linh hoạt nhưng dễ gặp vấn đề về quản lý schema, chất lượng dữ liệu, transaction, versioning và hiệu năng truy vấn. Paper Lakehouse chỉ ra rằng kiến trúc Lakehouse ra đời để kết hợp ưu điểm của Data Lake và Data Warehouse, dựa trên định dạng dữ liệu mở như Parquet/ORC, đồng thời bổ sung các năng lực quản lý và tối ưu cho analytics.

Vì vậy, đề tài này lựa chọn xây dựng một nền tảng Lakehouse ở quy mô mô phỏng, tập trung vào khả năng xử lý dữ liệu bán lẻ/thanh toán gần thời gian thực, kiểm soát chất lượng dữ liệu, đảm bảo correctness của metrics và đánh giá hiệu năng thông qua benchmark.

---

## 3. Vấn đề cần giải quyết

Đề tài tập trung giải quyết các vấn đề chính sau.

### 3.1. Dữ liệu sự kiện có thể bị trùng

Trong hệ thống streaming, cùng một event có thể xuất hiện nhiều lần do retry, lỗi mạng, producer gửi lại, consumer đọc lại hoặc task pipeline chạy lại.

Ví dụ:

```text
evt_001 | payment_authorized | 100000
evt_002 | payment_authorized | 200000
evt_002 | payment_authorized | 200000  <-- duplicate
```

Nếu không xử lý duplicate, doanh thu có thể bị cộng sai.

Vì vậy, pipeline cần có cơ chế deduplication theo `event_id` ở Silver layer và cần có test chứng minh duplicate payment events không làm tăng sai revenue.

### 3.2. Dữ liệu có thể đến muộn

Một event có thể xảy ra ở thời điểm nghiệp vụ trước đó nhưng được gửi vào hệ thống muộn hơn.

Ví dụ:

```text
event_time      = 10:00
ingestion_time  = 10:25
```

Nếu pipeline đã tính xong metric cho khung giờ `10:00–11:00` trước khi event này đến, Gold metrics có thể bị thiếu dữ liệu.

Vì vậy, project cần phân biệt rõ:

| Trường thời gian | Ý nghĩa |
|---|---|
| `event_time` | Thời điểm nghiệp vụ xảy ra. |
| `producer_time` | Thời điểm producer gửi event. |
| `ingestion_time` | Thời điểm data platform nhận event. |

Project sử dụng khái niệm allowed lateness và watermark để kiểm soát late events. Spark Structured Streaming hỗ trợ xử lý streaming theo event-time và watermark để xử lý dữ liệu đến muộn.

### 3.3. Dữ liệu có thể sai schema hoặc không hợp lệ

Một số event có thể bị lỗi:

```json
{
  "event_id": null,
  "event_type": "payment_authorized",
  "amount": -100000,
  "currency": "ABC"
}
```

Các lỗi có thể gồm:

- Thiếu `event_id`.
- `amount` âm.
- `currency` không được hỗ trợ.
- Thiếu `payment_id` với payment event.
- `schema_version` không được hỗ trợ.
- Raw payload không parse được.
- `event_time` bị null.

Nếu các bản ghi này đi vào Gold layer, dashboard và metrics có thể sai.

Vì vậy, project cần có data quality gate ở Silver layer, đồng thời đưa bad records vào DLQ để debug và audit.

### 3.4. Full refresh dễ đúng nhưng khó mở rộng

Full refresh nghĩa là mỗi lần chạy pipeline sẽ xử lý lại toàn bộ dữ liệu.

Cách này dễ hiểu và dễ kiểm tra correctness, nhưng khi dữ liệu tăng lên, chi phí xử lý và thời gian chạy sẽ tăng theo.

Ví dụ:

```text
Ngày 1   : 1 triệu events
Ngày 2   : 2 triệu events
Ngày 30  : 30 triệu events
```

Nếu mỗi lần đều đọc lại toàn bộ 30 triệu events, pipeline sẽ chậm dần.

Vì vậy, project cần benchmark giữa:

- Full refresh.
- Incremental processing.

Mục tiêu không chỉ là chứng minh incremental nhanh hơn, mà còn phân tích trade-off: incremental giảm rows scanned và processing time nhưng làm tăng độ phức tạp về unique key, idempotency, late events và backfill.

### 3.5. Small files làm query chậm

Trong pipeline micro-batch, mỗi batch nhỏ có thể ghi ra nhiều file nhỏ. Sau nhiều lần chạy, Lakehouse table có thể có rất nhiều small files.

Ví dụ:

```text
10.000 files, mỗi file 1–2 MB
```

Khi query, engine phải list, mở, đọc metadata và quản lý rất nhiều file. Điều này làm tăng overhead và khiến query chậm.

Vì vậy, project cần benchmark:

- Trạng thái nhiều small files.
- Trạng thái sau compaction.

Delta Lake có các tính năng như auto compaction, data skipping và các cơ chế tối ưu giúp cải thiện hiệu năng trên dữ liệu Lakehouse.

### 3.6. Pipeline retry hoặc rerun có thể làm sai dữ liệu

Trong thực tế, task pipeline có thể fail rồi chạy lại. Nếu task không idempotent, việc retry hoặc rerun có thể tạo duplicate output.

Ví dụ:

```text
Lần chạy 1  : cộng revenue của evt_001
Lần retry   : lại cộng revenue của evt_001
```

Kết quả là Gold metrics bị double count.

Vì vậy, project cần đưa pipeline vào Airflow và kiểm tra rằng retry/rerun với cùng `processing_date` không tạo duplicate. Airflow sử dụng DAG để mô tả workflow gồm các task và dependency giữa chúng.

---

## 4. Mục tiêu của đề tài

Đề tài có các mục tiêu chính sau.

### 4.1. Xây dựng pipeline Lakehouse end-to-end

Pipeline cần mô phỏng được luồng dữ liệu:

```text
Synthetic Retail/Payment Events
        ↓
Producer
        ↓
Apache Kafka KRaft
        ↓
Bronze Layer
        ↓
Silver Layer
        ↓
Delta Lake Tables
        ↓
Gold Layer
        ↓
Benchmark
        ↓
Airflow
        ↓
Monitoring Dashboard
```

Apache Kafka KRaft được dùng để mô phỏng event streaming. Apache Kafka hỗ trợ publish/subscribe event streams, lưu trữ streams bền vững và xử lý streams khi chúng xảy ra hoặc xử lý lại về sau.

### 4.2. Đảm bảo chất lượng dữ liệu và correctness của metrics

Project cần chứng minh:

- Bad records không đi vào Gold.
- Duplicate events không làm doanh thu bị cộng đôi.
- Late events được xử lý theo allowed lateness policy.
- Gold metrics có định nghĩa rõ ràng.
- Correctness tests chạy được bằng `pytest`.

### 4.3. Đánh giá hiệu năng bằng benchmark có phương pháp

Các benchmark bắt buộc trong MVP:

1. Full refresh vs incremental processing.
2. Small files vs compaction.

Mỗi benchmark cần lưu:

| Nhóm thông tin | Trường cần lưu |
|---|---|
| Định danh experiment | `run_id`, `experiment_name`, `strategy_name`, `run_number` |
| Dữ liệu đầu vào | `data_volume`, `random_seed` |
| Cấu hình chạy | `spark_config`, `table_layout` |
| Query | `query_name`, `query_hash` |
| Runtime | `processing_time_seconds`, `query_time_seconds`, `compaction_runtime_seconds` |
| Dữ liệu xử lý | `rows_scanned`, `rows_written` |
| File layout | `file_count`, `average_file_size_mb` |
| Thời gian ghi nhận | `created_at` |

Mỗi experiment cần chạy ít nhất 3 lần, có fixed random seed, có data volume rõ ràng và không so sánh hai kết quả nếu input data khác nhau.

### 4.4. Theo dõi pipeline bằng monitoring dashboard

Dashboard không chỉ hiển thị business metrics, mà còn phải hiển thị operational metrics.

| Nhóm metrics | Metrics cần theo dõi |
|---|---|
| **Business metrics** | `revenue_per_hour`, `orders_per_hour`, `payment_failure_rate` |
| **Operational metrics** | `freshness_seconds`, `pipeline_success_rate`, `DLQ count`, `duplicate_count`, `late_event_count`, `processing_time`, `query_time`, `file_count`, `average_file_size`, `compaction_runtime` |

---

## 5. Phạm vi đề tài

### 5.1. Phạm vi MVP

MVP bao gồm:

- Producer sinh valid, duplicate, late và malformed events.
- Apache Kafka KRaft nhận event stream.
- Bronze layer lưu raw immutable events.
- Bronze lưu source offset và ingestion metadata.
- Silver layer xử lý validation, deduplication, late-event flag và DLQ.
- Gold layer tạo business metrics và pipeline health metrics.
- Delta Lake table format cho Silver/Gold.
- Benchmark full refresh vs incremental.
- Benchmark small files vs compaction.
- Airflow DAG end-to-end.
- Monitoring dashboard.
- README/report có trade-off và limitation.
- Pytest cho data quality, dedup, idempotency và metric correctness.

### 5.2. Ngoài phạm vi MVP

Các phần sau được xem là stretch goals, chỉ làm sau khi MVP đã ổn:

- Partition benchmark nâng cao.
- Spark AQE benchmark.
- CI/CD nâng cao.
- Airflow hardening nâng cao.
- Monitoring trend nâng cao.
- Demo video polish.
- Interview package.
- Release package hoàn chỉnh.

---

## 6. Chế độ xử lý dữ liệu

Project này là:

> **Micro-batch near real-time analytics**

Project không phải hard real-time.

Dữ liệu được đọc liên tục từ Kafka nhưng được xử lý theo các batch nhỏ. Latency mục tiêu là khoảng 1–5 phút, phù hợp với dashboard retail/payment.

Spark Structured Streaming mặc định xử lý stream bằng micro-batch engine, tức stream được xử lý như chuỗi các batch nhỏ.

Vì vậy, project không claim xử lý từng event ở mức millisecond.

---

## 7. Vì sao chọn Lakehouse?

Lakehouse phù hợp với bài toán này vì cần kết hợp nhiều yêu cầu:

- Lưu dữ liệu raw để replay và audit.
- Xử lý dữ liệu streaming/micro-batch.
- Quản lý schema và table.
- Hỗ trợ transaction, versioning và time travel.
- Phục vụ analytics/dashboard.
- Tối ưu query performance.
- Hỗ trợ benchmark và monitoring.

Data Lake thuần linh hoạt nhưng yếu ở transaction, schema management, data quality và query optimization. Data Warehouse mạnh cho BI nhưng không tự nhiên cho raw event stream, replay, ML/data science và file layout optimization.

Lakehouse kết hợp ưu điểm của cả hai hướng: lưu trữ mở và linh hoạt như Data Lake, đồng thời bổ sung các đặc tính quản lý và tối ưu giống Data Warehouse. Nghiên cứu Lakehouse CIDR 2021 nêu Lakehouse giúp xử lý các vấn đề như data staleness, reliability, total cost of ownership, data lock-in và limited use-case support.

---

## 8. Vì sao chọn Delta Lake cho MVP?

MVP chọn Delta Lake làm table format chính vì:

- Dễ tích hợp với Spark local.
- Dễ demo transaction log.
- Hỗ trợ ACID transactions.
- Hỗ trợ schema enforcement.
- Hỗ trợ time travel.
- Hỗ trợ merge/update/delete.
- Phù hợp benchmark small files vs compaction.

Delta Lake mở rộng Parquet bằng transaction log để hỗ trợ ACID transactions và scalable metadata handling.

Apache Iceberg là hướng rất tốt cho Lakehouse hiện đại, đặc biệt ở hidden partitioning và partition evolution, nhưng trong project này nên để ở phần future work để tránh scope quá rộng.

---

## 9. Giới hạn của đề tài

Project sử dụng synthetic data, không dùng dữ liệu production thật.

Synthetic data giúp kiểm soát các tình huống cần kiểm thử:

- Duplicate events.
- Late events.
- Malformed events.
- Negative amount.
- Unsupported schema version.
- Skew mode.
- Small files.

Tuy nhiên, kết quả benchmark không được claim là đại diện tuyệt đối cho production.

Các giới hạn chính:

- Dữ liệu là synthetic.
- Môi trường chạy là local.
- Storage dùng MinIO thay vì cloud object storage thật.
- Workload được thiết kế cho mục tiêu học tập và đánh giá kiến trúc.
- Latency mục tiêu là near real-time 1–5 phút, không phải hard real-time.
- Kết quả benchmark phụ thuộc cấu hình máy, Spark config, data volume và query pattern.

Khi triển khai production thật, cần kiểm tra lại:

- Distribution của dữ liệu thật.
- Traffic burst.
- Data skew.
- Schema drift.
- Cloud object storage latency.
- Concurrent users.
- Security/IAM.
- Cost trên cloud.
- SLA/SLO thực tế.

---

## 10. Kết quả mong muốn

Sau khi hoàn thành MVP, project cần chứng minh được:

- Pipeline ingest được event stream từ Kafka vào Bronze.
- Bronze giữ raw immutable data và có thể replay.
- Silver xử lý được validation, deduplication, late events và DLQ.
- Gold tạo được metrics đúng và có correctness tests.
- Delta Lake table format được dùng để quản lý Silver/Gold.
- Full refresh vs incremental benchmark có số liệu rõ ràng.
- Small files vs compaction benchmark có số liệu rõ ràng.
- Airflow DAG chạy end-to-end và rerun không tạo duplicate.
- Monitoring dashboard hiển thị được freshness, DLQ, duplicate, query time và file count.
- README/report trình bày được problem, architecture, benchmark, result, trade-off và limitation.

---

## 11. Kể ngắn gọn về bài toán

Project này không chỉ là một pipeline `Kafka → Spark → Dashboard`.

Project bắt đầu từ các vấn đề thực tế của data platform:

- Dữ liệu có thể đến muộn.
- Dữ liệu có thể bị duplicate.
- Dữ liệu có thể sai schema.
- Full refresh chậm khi dữ liệu tăng.
- Small files làm query chậm.
- Pipeline retry/rerun có thể làm Gold metrics sai.
- Dashboard có thể stale nếu không theo dõi freshness.

Để giải quyết, project thiết kế một nền tảng Lakehouse gồm Apache Kafka KRaft, Bronze, Silver, Gold, Delta Lake, benchmark, Airflow và monitoring.

Mục tiêu cuối cùng là chứng minh không chỉ pipeline chạy được, mà còn:

- Dữ liệu đúng.
- Metric đúng.
- Benchmark có phương pháp.
- Pipeline có thể rerun an toàn.
- Hệ thống có monitoring.
- Trade-off được giải thích rõ ràng.
