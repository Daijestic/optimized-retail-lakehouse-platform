# Giới hạn của bộ dữ liệu mô phỏng

## 1. Mục đích của file

File này ghi rõ cách project sử dụng **synthetic data** và các giới hạn đi kèm.

Project này không dùng dữ liệu production thật. Thay vào đó, project tự sinh dữ liệu bán lẻ/thanh toán mô phỏng để kiểm thử pipeline Lakehouse trong các tình huống có kiểm soát.

Mục tiêu của synthetic data trong project không phải là mô phỏng hoàn hảo toàn bộ thị trường bán lẻ thực tế, mà là tạo ra các kịch bản đủ rõ để đánh giá hành vi của pipeline trước các vấn đề Data Engineering thường gặp:

- duplicate events;
- late events;
- malformed events;
- negative amount;
- unsupported schema version;
- data skew;
- burst traffic;
- small files;
- full refresh chậm;
- incremental processing;
- compaction.

File này cần được đọc cùng với:

- `docs/problem_statement.md`;
- `docs/architecture.md`;
- `docs/research_questions.md`;
- `docs/benchmark_methodology.md`;
- `docs/data_profile.md`;
- `benchmark/benchmark_config.yaml`.

---

## 2. Synthetic data là gì trong project này?

Trong project này, synthetic data là dữ liệu được sinh nhân tạo bằng code, thay vì lấy trực tiếp từ hệ thống bán lẻ hoặc thanh toán thật.

Dữ liệu được sinh ra bởi producer với các tham số có thể cấu hình, ví dụ:

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

Mỗi event mô phỏng một sự kiện nghiệp vụ, ví dụ:

```json
{
  "event_id": "evt_000001",
  "event_type": "payment_authorized",
  "order_id": "ord_000123",
  "payment_id": "pay_000456",
  "customer_id": "cus_000789",
  "amount": 350000,
  "currency": "VND",
  "event_time": "2026-07-02T10:00:00Z",
  "producer_time": "2026-07-02T10:00:02Z",
  "schema_version": "v1"
}
```

Các event này được gửi vào Kafka/Redpanda, ghi xuống Bronze, xử lý ở Silver, tổng hợp ở Gold và dùng cho benchmark.

---

## 3. Vì sao project dùng synthetic data?

Project dùng synthetic data vì các lý do sau.

### 3.1. Không có quyền truy cập dữ liệu production thật

Dữ liệu bán lẻ/thanh toán thật thường chứa thông tin nhạy cảm như:

- thông tin khách hàng;
- lịch sử giao dịch;
- thông tin thanh toán;
- hành vi mua hàng;
- địa chỉ giao hàng;
- thông tin thiết bị hoặc phiên đăng nhập.

Vì vậy, dùng dữ liệu thật trong project cá nhân hoặc đồ án có thể gây rủi ro về bảo mật, quyền riêng tư và pháp lý.

Synthetic data giúp tránh việc lưu trữ hoặc công khai dữ liệu thật.

---

### 3.2. Có thể kiểm soát lỗi dữ liệu

Nếu dùng một file CSV sạch có sẵn, project sẽ khó chứng minh các vấn đề thực tế của Data Engineering.

Synthetic data cho phép chủ động tạo lỗi có kiểm soát:

```text
duplicate_rate = 0.05
late_event_rate = 0.03
malformed_rate = 0.01
```

Nhờ đó project có thể kiểm thử:

- Silver có phát hiện duplicate không;
- Silver có đưa bad records vào DLQ không;
- Gold có tránh double-count revenue không;
- late events có được xử lý theo allowed lateness policy không;
- monitoring có phát hiện DLQ spike hoặc duplicate spike không.

---

### 3.3. Có thể tái lập benchmark

Benchmark cần chạy lại nhiều lần với cùng một input.

Nếu dữ liệu thay đổi ngẫu nhiên mỗi lần chạy, kết quả benchmark sẽ khó so sánh.

Do đó, project dùng:

```yaml
random_seed: 42
```

Điều này giúp producer sinh lại cùng một tập dữ liệu với cùng cấu hình, từ đó benchmark có thể tái lập.

Ví dụ:

```text
Run 1: data_volume = 1,000,000, seed = 42
Run 2: data_volume = 1,000,000, seed = 42
Run 3: data_volume = 1,000,000, seed = 42
```

Khi đó, nếu benchmark full refresh và incremental khác nhau, khác biệt chủ yếu đến từ strategy xử lý, không phải do input thay đổi ngẫu nhiên.

---

### 3.4. Có thể tạo các kịch bản benchmark rõ ràng

Project cần benchmark ít nhất hai nhóm:

1. Full refresh vs incremental processing.
2. Small files vs compaction.

Synthetic data giúp tạo dữ liệu với nhiều quy mô khác nhau:

```text
100,000 events
1,000,000 events
5,000,000 events
```

Hoặc tạo nhiều micro-batch nhỏ để sinh small files:

```text
micro_batch_interval = 1 minute
small_file_mode = true
```

Nhờ vậy project có thể đo:

- `processing_time_seconds`;
- `query_time_seconds`;
- `rows_scanned`;
- `rows_written`;
- `file_count`;
- `average_file_size_mb`;
- `compaction_runtime_seconds`.

---

## 4. Synthetic data không chứng minh điều gì?

Synthetic data hữu ích cho kiểm thử có kiểm soát, nhưng không được hiểu sai.

Project này **không claim** các điều sau:

```text
Benchmark đại diện tuyệt đối cho production.
Kết quả runtime trên máy local sẽ giống cloud.
Distribution của synthetic data giống hoàn toàn dữ liệu thật.
Tỷ lệ duplicate/late/malformed trong project giống doanh nghiệp thật.
Query pattern trong project giống toàn bộ workload thực tế.
Compaction strategy trong project là tối ưu cho mọi hệ thống.
```

Nói cách khác, synthetic data giúp đánh giá tương đối giữa các strategy trong cùng điều kiện kiểm soát, không chứng minh hiệu năng tuyệt đối trong production.

---

## 5. Các giới hạn chính của synthetic data

### 5.1. Giới hạn về phân phối dữ liệu

Dữ liệu thật thường có phân phối phức tạp hơn dữ liệu mô phỏng.

Ví dụ trong thực tế:

- doanh số tăng mạnh vào giờ cao điểm;
- cuối tuần khác ngày thường;
- ngày lễ có burst traffic;
- một số sản phẩm hoặc cửa hàng chiếm phần lớn giao dịch;
- một số phương thức thanh toán có failure rate cao hơn;
- một số khách hàng hoặc khu vực tạo ra data skew.

Nếu synthetic data chỉ phân phối đều, benchmark có thể quá đơn giản.

Vì vậy, producer cần hỗ trợ `skew_mode`.

Ví dụ:

```yaml
skew_mode: none
```

```yaml
skew_mode: hot_payment_method
```

```yaml
skew_mode: hot_customer
```

```yaml
skew_mode: burst_traffic
```

---

### 5.2. Giới hạn về data skew

Data skew xảy ra khi dữ liệu tập trung quá nhiều vào một vài key.

Ví dụ:

```text
80% events thuộc về 5% customer_id
70% payment_failed thuộc về một payment_method
90% traffic rơi vào 2 giờ cao điểm
```

Data skew có thể làm Spark job chậm do một vài partition xử lý quá nhiều dữ liệu.

Nếu synthetic data không mô phỏng skew, project có thể bỏ sót bottleneck quan trọng.

Trong MVP, project chỉ cần ghi rõ giới hạn này. Nếu còn thời gian, có thể thêm `skew_mode` để tạo dữ liệu lệch và đưa Spark AQE benchmark vào stretch goal.

---

### 5.3. Giới hạn về schema drift

Dữ liệu thật có thể thay đổi schema theo thời gian.

Ví dụ ban đầu event có schema:

```json
{
  "event_id": "evt_001",
  "amount": 350000,
  "currency": "VND"
}
```

Sau đó producer thêm trường mới:

```json
{
  "event_id": "evt_001",
  "amount": 350000,
  "currency": "VND",
  "payment_method": "bank_transfer",
  "bank_code": "VCB"
}
```

Hoặc thay đổi schema sai cách:

```json
{
  "event_id": "evt_001",
  "amount": "350000",
  "currency": "VND"
}
```

Trong MVP, project chỉ mô phỏng schema drift ở mức đơn giản bằng `schema_version` và `unsupported_schema_version_rate`.

Project không claim đã bao phủ toàn bộ các tình huống schema evolution phức tạp như production.

---

### 5.4. Giới hạn về late events

Late events trong project được sinh theo một phân phối mô phỏng.

Ví dụ:

```yaml
late_event_rate: 0.03
max_late_minutes: 120
allowed_lateness_minutes: 30
```

Trong thực tế, late events có thể phụ thuộc vào:

- lỗi mạng;
- mobile app offline;
- retry policy của producer;
- downtime của Kafka/consumer;
- batch import từ hệ thống cũ;
- timezone;
- clock drift giữa các service.

Do đó, late-event benchmark trong project chỉ đánh giá pipeline theo policy mô phỏng, không đại diện tuyệt đối cho mọi tình huống production.

---

### 5.5. Giới hạn về duplicate events

Duplicate events trong project được sinh theo `duplicate_rate`.

Ví dụ:

```yaml
duplicate_rate: 0.05
```

Trong thực tế, duplicate có thể xuất hiện theo cụm, không phân phối đều.

Ví dụ:

```text
Một service bị lỗi và retry toàn bộ batch 10,000 events.
Một Airflow task rerun cùng processing_date.
Consumer ghi thành công nhưng commit offset thất bại.
```

MVP chỉ cần mô phỏng duplicate ở mức event-level. Các failure scenario phức tạp có thể đưa vào future work hoặc Airflow hardening phase.

---

### 5.6. Giới hạn về malformed records

Malformed records trong project được sinh bằng một số rule cố định:

- thiếu `event_id`;
- `amount < 0`;
- `currency` không thuộc danh sách hỗ trợ;
- `payment_id` null với payment event;
- `schema_version` không hỗ trợ;
- raw payload không parse được.

Trong thực tế, malformed data có thể phức tạp hơn:

- encoding lỗi;
- nested JSON thiếu field;
- field đổi kiểu dữ liệu;
- timestamp sai timezone;
- duplicate key trong JSON;
- giá trị hợp lệ về schema nhưng sai về nghiệp vụ.

Do đó, DLQ trong MVP chứng minh được tư duy data quality, nhưng không bao phủ toàn bộ lỗi dữ liệu thực tế.

---

### 5.7. Giới hạn về môi trường local

Project chạy trong môi trường local bằng Docker Compose.

Môi trường local khác production ở nhiều điểm:

- CPU/RAM hạn chế;
- không có autoscaling;
- network đơn giản hơn;
- MinIO không phản ánh hoàn toàn latency của cloud object storage;
- ít concurrent users;
- không có IAM/security production-grade;
- không có chi phí cloud thật;
- không có multi-cluster workload.

Do đó, benchmark local chỉ dùng để so sánh strategy trong cùng môi trường, không dùng để dự đoán chính xác chi phí hoặc latency trên cloud.

---

### 5.8. Giới hạn về storage

Project dùng MinIO để mô phỏng object storage kiểu S3.

MinIO hữu ích cho local development vì cung cấp object storage tương thích S3, nhưng vẫn khác cloud object storage thật ở:

- latency;
- throughput;
- consistency behavior;
- IAM/security integration;
- network distance;
- storage class;
- cost model;
- request cost.

Vì vậy, kết quả file layout benchmark trên MinIO chỉ nên được xem là kết quả trong môi trường mô phỏng local.

---

### 5.9. Giới hạn về processing mode

Project dùng micro-batch near real-time, không phải hard real-time.

Mục tiêu latency:

```text
1–5 phút
```

Điều này phù hợp với dashboard retail/payment, nhưng không phù hợp với các use case yêu cầu phản ứng ở mức millisecond như:

- fraud blocking ngay tại thời điểm thanh toán;
- high-frequency trading;
- điều khiển thiết bị thời gian thực;
- safety-critical systems.

Project không claim xử lý sub-second hoặc millisecond-level latency.

---

## 6. Cách kiểm soát giới hạn của synthetic data

Để synthetic data vẫn có giá trị trong project, cần áp dụng các biện pháp kiểm soát sau.

### 6.1. Dùng fixed random seed

Mỗi lần sinh dữ liệu phải lưu:

```text
random_seed
```

Ví dụ:

```yaml
random_seed: 42
```

Điều này giúp benchmark có thể tái lập.

---

### 6.2. Lưu data volume

Mỗi benchmark phải ghi rõ quy mô dữ liệu:

```text
data_volume = 100000
data_volume = 1000000
data_volume = 5000000
```

Không được so sánh hai strategy nếu data volume khác nhau.

---

### 6.3. Lưu error rates

Mỗi lần sinh dữ liệu phải lưu các tỷ lệ lỗi:

```yaml
duplicate_rate: 0.05
late_event_rate: 0.03
malformed_rate: 0.01
negative_amount_rate: 0.005
unsupported_schema_version_rate: 0.005
```

Nếu thay đổi error rate, đó là một experiment khác.

---

### 6.4. Lưu data profile

Sau khi sinh dữ liệu, cần tạo file:

```text
docs/data_profile.md
```

File này nên ghi:

- tổng số events;
- số valid events;
- số duplicate events;
- số late events;
- số malformed events;
- distribution theo `event_type`;
- distribution theo `currency`;
- min/max/avg amount;
- min/max event_time;
- min/max ingestion_time;
- skew mode;
- random seed.

---

### 6.5. Lưu benchmark metadata

Mỗi benchmark phải lưu:

- `run_id`;
- `experiment_name`;
- `strategy_name`;
- `random_seed`;
- `data_volume`;
- `spark_config`;
- `table_layout`;
- `query_hash`;
- `run_number`;
- `processing_time_seconds`;
- `query_time_seconds`;
- `rows_scanned`;
- `rows_written`;
- `file_count`;
- `average_file_size_mb`;
- `compaction_runtime_seconds`;
- `correctness_status`;
- `created_at`.

---

## 7. Cách viết limitation trong README/report

Trong README hoặc report, không nên viết:

```text
Benchmark chứng minh incremental luôn nhanh hơn full refresh.
Benchmark chứng minh compaction luôn tốt hơn.
Synthetic data đại diện cho production.
```

Nên viết:

```text
Benchmark trong project được thực hiện trên synthetic retail/payment events với fixed random seed, data volume cố định và configurable error rates. Kết quả dùng để so sánh tương đối giữa các strategy trong cùng môi trường local. Project không claim kết quả đại diện tuyệt đối cho production. Khi triển khai thật, cần validate lại với dữ liệu thật, traffic pattern thật, data skew, schema drift, cloud object storage và workload thực tế.
```

---

## 8. Cách trả lời khi phỏng vấn

### Câu hỏi: Vì sao dùng synthetic data?

Trả lời:

> Em dùng synthetic data vì không có quyền truy cập dữ liệu production thật và cũng không nên dùng dữ liệu thanh toán thật trong project cá nhân. Synthetic data giúp em kiểm soát các tình huống cần kiểm thử như duplicate events, late events, malformed records, negative amount và small files. Nhờ có fixed random seed, em có thể tái lập benchmark nhiều lần với cùng input.

---

### Câu hỏi: Synthetic data có làm project kém thực tế không?

Trả lời:

> Có giới hạn, nên em ghi rõ trong dataset limitations. Synthetic data không phản ánh hoàn hảo production distribution, traffic burst, data skew hoặc schema drift. Tuy nhiên, mục tiêu của project không phải dự đoán chính xác production performance, mà là đánh giá hành vi pipeline dưới các kịch bản có kiểm soát. Với dữ liệu thật, các benchmark cần được validate lại.

---

### Câu hỏi: Benchmark của em có đáng tin không nếu dùng synthetic data?

Trả lời:

> Benchmark đáng tin trong phạm vi so sánh tương đối vì em kiểm soát input bằng fixed seed, data volume, Spark config, query hash và table layout. Mỗi experiment chạy ít nhất 3 lần và có correctness status. Tuy nhiên, em không claim kết quả đại diện tuyệt đối cho production. Em chỉ kết luận trong phạm vi workload mô phỏng của project.

---

### Câu hỏi: Nếu có dữ liệu thật thì em sẽ cải thiện gì?

Trả lời:

> Nếu có dữ liệu thật, em sẽ profile distribution thật, kiểm tra skew, traffic burst, schema drift, tỷ lệ duplicate/late/malformed thật, sau đó điều chỉnh producer để synthetic data gần với dữ liệu thật hơn. Em cũng sẽ chạy lại benchmark trên cloud object storage thật và so sánh với kết quả local.

---

## 9. Definition of Done cho file này

File này được xem là hoàn thành khi:

- [ ] Giải thích rõ synthetic data là gì trong project.
- [ ] Nêu rõ vì sao dùng synthetic data.
- [ ] Ghi rõ synthetic data không đại diện tuyệt đối cho production.
- [ ] Liệt kê các giới hạn về distribution, skew, schema drift, late events, duplicate events và malformed records.
- [ ] Ghi rõ project chạy local bằng MinIO/Docker Compose nên benchmark không phải production benchmark.
- [ ] Ghi rõ project là micro-batch near real-time, không phải hard real-time.
- [ ] Nêu cách kiểm soát limitation bằng fixed seed, data profile, benchmark metadata và correctness tests.
- [ ] Có câu trả lời phỏng vấn về synthetic data limitation.

---

## 10. Tóm tắt ngắn

Project dùng synthetic data để kiểm soát lỗi và tái lập benchmark.

Synthetic data giúp kiểm thử:

- duplicate events;
- late events;
- malformed records;
- small files;
- full refresh vs incremental;
- compaction.

Nhưng synthetic data có giới hạn:

- không phản ánh hoàn hảo dữ liệu thật;
- không đại diện tuyệt đối cho production;
- không thay thế được benchmark trên cloud và workload thật.

Vì vậy, mọi kết luận trong project phải được viết theo hướng:

```text
Trong workload mô phỏng và môi trường local của project, strategy A cho kết quả tốt hơn strategy B theo các metrics đã đo.
```

Không viết theo hướng:

```text
Strategy A luôn tốt hơn strategy B trong mọi hệ thống production.
```
