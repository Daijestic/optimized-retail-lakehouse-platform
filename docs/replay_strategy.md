# Chiến lược Replay Bronze

## 1. Mục đích

Replay đọc lại các record Bronze bất biến để chạy lại downstream processing sau khi:

- sửa code;
- thay đổi data-quality rule;
- xử lý lỗi pipeline;
- cần tái tạo Silver hoặc Gold;
- cần điều tra một nhóm source record cụ thể.

Replay không yêu cầu producer gửi lại event và không phụ thuộc Kafka còn giữ dữ liệu cũ trong thời gian retention hay không.

---

## 2. Phạm vi MVP

MVP của Tuần 2 hỗ trợ replay theo **một `processing_date` UTC** cho mỗi command.

Nguồn replay:

```text
bronze/events/
processing_date=<YYYY-MM-DD>/
```

Replay command thực hiện:

1. liệt kê toàn bộ JSONL object dưới prefix của ngày được chọn;
2. parse topic, partition và offset range từ object key;
3. từ chối các offset range bị overlap;
4. từ chối gap theo mặc định;
5. kiểm tra object metadata;
6. kiểm tra technical Bronze envelope;
7. giữ nguyên từng Bronze row;
8. ghi local JSONL artifact theo thứ tự deterministic;
9. tạo replay manifest riêng.

---

## 3. Chính sách chỉ đọc

Replay không:

- reset Kafka consumer-group offsets;
- commit Kafka offsets;
- publish record trở lại Kafka;
- ghi, copy hoặc xóa Bronze object;
- thay đổi raw payload bytes;
- áp dụng Silver business validation;
- deduplicate `event_id`;
- sửa source coordinates.

Replay chỉ sử dụng các thao tác đọc như:

```text
ListObjectsV2
HeadObject
GetObject
```

---

## 4. Phân biệt retry, replay và backfill

### Retry

Chạy lại một thao tác vừa thất bại, thường trong cùng ingestion run.

```text
ghi MinIO thất bại
→ retry cùng batch
```

### Replay

Đọc lại dữ liệu đã persist trong Bronze để chạy lại downstream.

```text
Bronze của ngày 2026-07-13
→ chạy lại Silver
```

### Backfill

Xử lý dữ liệu lịch sử cho một khoảng ngày hoặc một khoảng source range.

```text
processing_date từ 2026-07-01 đến 2026-07-10
→ xây lại Silver/Gold
```

MVP Ngày 6 chỉ thực hiện replay theo một ngày.

---

## 5. Thứ tự output

Replay sắp xếp object và row theo:

```text
source_topic
→ source_partition
→ source_offset
```

Kafka chỉ bảo đảm thứ tự trong từng topic-partition, không có global order giữa nhiều partition.

Vì vậy replay artifact không được hiểu là một event stream có thứ tự toàn cục.

---

## 6. Định danh record

Định danh nguồn vẫn là:

```text
source_topic
+ source_partition
+ source_offset
```

Không dùng `event_id` làm replay identity vì controlled duplicate có thể:

```text
cùng event_id
+ khác Kafka offset
```

Replay phải giữ lại cả hai record.

---

## 7. Chính sách `processing_date`

`processing_date` là ngày UTC được suy ra từ Bronze `ingestion_time`.

Không suy ra từ:

- `event_time`;
- `producer_time`;
- Kafka timestamp;
- ngày local của Windows.

Các object dưới:

```text
bronze/events/_unpartitioned/
```

không được đưa vào replay theo `processing_date`.

---

## 8. Kiểm tra tính toàn vẹn

Replay thực hiện technical validation, không thực hiện business validation.

### 8.1. Object key

Object key phải tuân theo:

```text
bronze/events/
processing_date=<YYYY-MM-DD>/
topic=<topic>/
partition=<partition>/
offsets=<start-offset>-<end-offset>.jsonl
```

### 8.2. Object metadata

Các giá trị sau phải khớp với object key:

- `processing-date`;
- `source-topic`;
- `source-partition`;
- `start-offset`;
- `end-offset`;
- `record-version`.

### 8.3. Row metadata

Mỗi row phải có:

- `record_version=bronze-raw-v1`;
- `processing_date` đúng;
- `source_topic` đúng;
- `source_partition` đúng;
- `source_offset` đúng;
- `source_record_id` đúng;
- `key_base64` và `value_base64` hợp lệ nếu không null.

### 8.4. Offset range

Trong cùng một topic-partition:

```text
object A: offsets 0–79
object B: offsets 80–179
```

là liên tục và hợp lệ.

Overlap luôn bị từ chối:

```text
object A: offsets 0–99
object B: offsets 80–179
```

Gap bị từ chối theo mặc định:

```text
object A: offsets 0–79
object B: offsets 90–179
```

Có thể dùng `--allow-gaps` trong tình huống điều tra đặc biệt, nhưng overlap vẫn luôn làm replay thất bại.

---

## 9. Xử lý malformed payload

Malformed raw payload không làm replay thất bại nếu outer Bronze envelope vẫn hợp lệ.

Ví dụ outer row:

```json
{
  "payload_parse_status": "invalid_json",
  "value_base64": "eyJldmVudF9pZCI6ImJyb2tlbiI="
}
```

`value_base64` có thể decode thành raw bytes không phải JSON hợp lệ. Đây vẫn là dữ liệu Bronze hợp lệ và phải được giữ nguyên.

Replay chỉ yêu cầu:

- Bronze envelope parse được;
- Base64 hợp lệ;
- source metadata khớp;
- offset range khớp.

---

## 10. Tính deterministic

Khi Bronze source không đổi:

```text
cùng processing_date
+ cùng object filters
→ cùng thứ tự object
→ cùng JSONL rows
→ cùng output bytes
→ cùng SHA-256
```

Replay manifest có thể khác vì mỗi lần chạy có `replay_run_id` mới.

Replay JSONL phải có checksum giống nhau khi input và filter giống nhau.

---

## 11. Hành vi khi lỗi

Replay thất bại khi:

- không có object cho ngày được yêu cầu;
- object key sai layout;
- object metadata không khớp với path;
- source ranges overlap;
- source ranges có gap mà không dùng `--allow-gaps`;
- outer Bronze JSONL envelope không hợp lệ;
- row source coordinates không khớp object range;
- trường Base64 bị hỏng;
- output path đã tồn tại nhưng không dùng `--overwrite`.

Replay thất bại không thay đổi Kafka hoặc Bronze.

Để tránh tạo output dở dang, local artifact được ghi theo quy trình:

```text
ghi temporary file
→ hoàn thành toàn bộ validation và write
→ os.replace() sang final output
```

---

## 12. Output

Output mặc định:

```text
artifacts/replay/
processing_date=<date>/
bronze-replay.jsonl
```

Manifest:

```text
bronze-replay.jsonl.manifest.json
```

Manifest nên chứa tối thiểu:

- `replay_run_id`;
- `processing_date`;
- bucket và source prefix;
- số object;
- số record;
- danh sách topic-partition và offset range;
- output path;
- output size;
- output SHA-256;
- trạng thái replay.

Local replay artifact là runtime output và không commit vào Git.

---

## 13. Bằng chứng cần thu thập

Với mỗi live replay, ghi lại:

- `processing_date`;
- `replay_run_id`;
- source object count;
- replayed record count;
- output path;
- output SHA-256;
- Kafka offsets trước và sau replay;
- Bronze object count trước và sau replay;
- checksum của hai lần replay cùng input.

Kỳ vọng:

```text
Kafka committed offsets trước = sau
Bronze object count trước = sau
SHA-256 run A = SHA-256 run B
```

---

## 14. Giới hạn MVP

- Chỉ replay một `processing_date` cho mỗi command.
- Chưa replay theo `ingestion_run_id`.
- Chưa replay theo arbitrary topic-partition-offset range.
- Chưa hỗ trợ date-range backfill.
- Output là local JSONL, chưa phải Spark/Delta target.
- Các object `_unpartitioned` bị loại khỏi replay.
- Chưa ghi replay-run metadata vào PostgreSQL.
- Chưa hỗ trợ object version ID.

---

## 15. Hướng hardening sau này

Các phiên bản sau có thể bổ sung:

- replay theo `ingestion_run_id`;
- replay theo topic, partition và offset range;
- replay nhiều ngày;
- backfill theo date range;
- đưa replay output trực tiếp vào Spark Silver job;
- PostgreSQL replay-run metadata;
- replay quality gate;
- replay từ object version ID;
- manifest có source object checksum đầy đủ.


---

## 16. Cách chạy

Dry run:

```bash
python -m scripts.replay_bronze \
  --processing-date YYYY-MM-DD \
  --dry-run
```

Replay toàn bộ ngày:

```bash
python -m scripts.replay_bronze \
  --processing-date YYYY-MM-DD \
  --run-id replay-example
```

Replay một Kafka partition:

```bash
python -m scripts.replay_bronze \
  --processing-date YYYY-MM-DD \
  --partition 2 \
  --output artifacts/replay/partition-2.jsonl
```

Ghi đè output local đã tồn tại:

```bash
python -m scripts.replay_bronze \
  --processing-date YYYY-MM-DD \
  --output artifacts/replay/replay.jsonl \
  --overwrite
```

---

## 17. Kết quả xác minh ngày 2026-07-14

### 17.1. Dry run

| Metric | Kết quả |
|---|---|
| Processing date | `2026-07-14` |
| Object count | `3` |
| Partition 0 | offsets `2550–2587`, 38 records |
| Partition 1 | offsets `2010–2039`, 30 records |
| Partition 2 | offsets `2144–2175`, 32 records |
| Status | `dry_run` |

### 17.2. Replay deterministic

| Metric | Run A | Run B |
|---|---|---|
| Replay run ID | `replay-week02-final-2026-07-14-a` | `replay-week02-final-2026-07-14-b` |
| Object count | `3` | `3` |
| Record count | `100` | `100` |
| Output size | `131396` bytes | `131396` bytes |
| SHA-256 | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` | `3c19549d2d93d14e7bb7ffd74253c33b8d7b7f2e3bad37c338ddaaf3989c16c4` |

Kết quả: **PASS**.

### 17.3. Tính chỉ đọc

Kafka consumer-group state trước và sau replay giống nhau:

```text
P0 = 2588
P1 = 2040
P2 = 2176
LAG = 0
```

Bronze object count trước và sau replay:

```text
3 → 3
```

Kết quả:

```text
Kafka offsets unchanged = PASS
Bronze object count unchanged = PASS
```

### 17.4. Negative tests

- Ngày `2000-01-01` không có dữ liệu: `FileNotFoundError`.
- Output đã tồn tại nhưng không có `--overwrite`: `FileExistsError`.
- Có `--overwrite`: replay thành công và checksum không đổi.
- Filter `--partition 2`: `1` object, `32` records, SHA-256 `00ab8cb3e174e530ed6513073943239f868f55ba23d39d953edbc28bf5e78c27`.

---

## 18. Phạm vi của bằng chứng snapshot

Kết quả `3 objects / 100 records` được chụp trước failure-injection test.

Sau đó, failure scenario và recovery ghi thêm object vào cùng:

```text
processing_date=2026-07-14
```

Vì vậy, chạy lại replay toàn bộ ngày sau thời điểm đó sẽ thấy nhiều object/record hơn và tạo checksum khác. Điều này không phá vỡ tính deterministic.

Quy tắc đúng là:

```text
cùng source object snapshot
+ cùng filter
→ cùng output checksum
```

Không nên hiểu là:

```text
cùng processing_date ở mọi thời điểm
→ checksum luôn cố định
```

Nếu cần một replay snapshot bất biến trong tương lai, có thể bổ sung selector theo `ingestion_run_id`, source object manifest hoặc object version ID.
