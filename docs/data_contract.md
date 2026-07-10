# Hợp đồng dữ liệu sự kiện bán lẻ và thanh toán

## 1. Thông tin hợp đồng

| Thuộc tính | Giá trị |
|---|---|
| Tên hợp đồng | Sự kiện bán lẻ và thanh toán |
| Phiên bản lược đồ | `1.0` |
| Trạng thái | Đang hoạt động |
| Chủ sở hữu | Dự án Lakehouse |
| Trình tạo sự kiện | `synthetic-retail-producer` |
| Chủ đề Kafka | `retail-payment-events` |
| Khóa bản ghi | `order_id` |
| Định dạng tuần tự hóa | JSON mã hóa UTF-8 |

## 2. Mục đích

Hợp đồng này định nghĩa cấu trúc, ý nghĩa và các quy tắc kiểm tra tính hợp lệ cho các sự kiện bán lẻ/thanh toán được trình tạo sự kiện gửi vào Kafka.

## 3. Các loại sự kiện được hỗ trợ

- `order_created`
- `order_confirmed`
- `payment_authorized`
- `payment_failed`
- `order_shipped`
- `order_delivered`
- `refund_requested`

## 4. Định nghĩa các trường

| Trường | Kiểu dữ liệu | Bắt buộc | Mô tả |
|---|---|---:|---|
| `event_id` | UUID | Có | Mã định danh sự kiện duy nhất |
| `event_type` | Kiểu liệt kê | Có | Loại sự kiện nghiệp vụ |
| `order_id` | Chuỗi | Có | Đơn hàng liên quan |
| `payment_id` | Chuỗi/null | Theo điều kiện | Bắt buộc đối với các sự kiện thanh toán/hoàn tiền |
| `customer_id` | Chuỗi | Có | Mã định danh khách hàng |
| `store_id` | Chuỗi | Có | Mã định danh cửa hàng |
| `amount` | Chuỗi số thập phân/null | Theo điều kiện | Giá trị giao dịch |
| `currency` | Kiểu liệt kê/null | Theo điều kiện | `VND` hoặc `USD` |
| `event_time` | Thời gian có thông tin múi giờ | Có | Thời điểm xảy ra sự kiện nghiệp vụ |
| `producer_time` | Thời gian có thông tin múi giờ | Có | Thời điểm trình tạo sự kiện tạo dữ liệu |
| `schema_version` | Chuỗi | Có | Phiên bản hợp đồng |
| `idempotency_key` | Chuỗi | Có | Khóa thử lại/đảm bảo tính lũy đẳng |
| `source` | Chuỗi | Có | Trình tạo sự kiện |

## 5. Chính sách dấu thời gian

Tất cả dấu thời gian phải:

- chứa thông tin múi giờ;
- sử dụng UTC khi có thể;
- được tuần tự hóa theo định dạng RFC 3339/ISO 8601.

Ví dụ:

```text
2026-07-10T03:00:00Z
2026-07-10T10:00:00+07:00
```

Trong bộ dữ liệu tổng hợp, `producer_time` không được sớm hơn `event_time`.

## 6. Chính sách tiền tệ

`amount` được biểu diễn dưới dạng chuỗi số thập phân.

Ví dụ:

```json
"amount": "150000.00"
```

Không được sử dụng số dấu phẩy động nhị phân cho các phép tính tài chính.

`amount` và `currency` phải cùng tồn tại hoặc cùng có giá trị `null`.

## 7. Yêu cầu đối với sự kiện thanh toán

Các loại sự kiện sau yêu cầu phải có:

- `payment_id`;
- `amount`;
- `currency`.

Các loại sự kiện áp dụng:

- `payment_authorized`;
- `payment_failed`;
- `refund_requested`.

## 8. Chính sách lũy đẳng

`event_id` xác định một sự kiện.

`idempotency_key` xác định các lần thử lặp lại của cùng một thao tác logic.

Đối với quá trình sinh dữ liệu MVP, `idempotency_key` có thể bằng `event_id`.

Các sự kiện kiểm thử trùng lặp sử dụng lại cả hai giá trị này.

## 9. Chính sách phân vùng Kafka

Khóa bản ghi Kafka là `order_id`.

Các sự kiện thuộc cùng một đơn hàng thông thường nên được ghi vào cùng một phân vùng Kafka.

Thứ tự chỉ được bảo đảm trong phạm vi một phân vùng, không được bảo đảm trên toàn bộ chủ đề.

## 10. Chính sách tương thích

Các phiên bản lược đồ được hỗ trợ:

- `1.0`.

Các thay đổi tương thích ngược có thể bao gồm:

- thêm một trường tùy chọn mới;
- bổ sung tài liệu;
- nới lỏng một ràng buộc không quan trọng.

Các thay đổi phá vỡ tính tương thích bao gồm:

- xóa một trường bắt buộc;
- thay đổi kiểu dữ liệu của một trường;
- thay đổi ý nghĩa của một trường;
- đổi tên một trường hiện có;
- chuyển một trường tùy chọn thành trường bắt buộc;
- thay đổi ngữ nghĩa của `amount` hoặc dấu thời gian.

Các thay đổi phá vỡ tính tương thích yêu cầu một phiên bản lược đồ mới.

## 11. Ví dụ hợp lệ

```json
{
  "event_id": "5e84235a-82a4-4f5a-862a-10c903c2173f",
  "event_type": "payment_authorized",
  "order_id": "order-1001",
  "payment_id": "payment-1001",
  "customer_id": "customer-1001",
  "store_id": "store-001",
  "amount": "150000.00",
  "currency": "VND",
  "event_time": "2026-07-10T03:00:00Z",
  "producer_time": "2026-07-10T03:00:01Z",
  "schema_version": "1.0",
  "idempotency_key": "5e84235a-82a4-4f5a-862a-10c903c2173f",
  "source": "synthetic-retail-producer"
}
```

## 12. Các ví dụ không hợp lệ

### Giá trị `amount` âm

```json
{
  "amount": "-1000.00"
}
```

### Đơn vị tiền tệ không được hỗ trợ

```json
{
  "currency": "ABC"
}
```

### Thiếu mã thanh toán

```json
{
  "event_type": "payment_authorized",
  "payment_id": null
}
```

### Phiên bản lược đồ không được hỗ trợ

```json
{
  "schema_version": "99.0"
}
```

## 13. Xử lý lỗi

Các sự kiện hợp lệ từ trình tạo sự kiện phải vượt qua bước kiểm tra tính hợp lệ bằng Pydantic trước khi được xuất bản.

Các kịch bản kiểm thử sự kiện lỗi được chủ ý cho đi vòng qua mô hình sự kiện hợp lệ.

Tầng Bronze phải bảo toàn tải trọng gốc, bao gồm cả các bản ghi sai định dạng và không hợp lệ. Tầng Silver chịu trách nhiệm phân loại, đưa dữ liệu vào DLQ và áp dụng các quy tắc chất lượng.

## 14. Hạn chế

- Chưa sử dụng Schema Registry trong MVP.
- Tải trọng JSON có kích thước lớn hơn các định dạng nhị phân.
- Quá trình kiểm tra tính hợp lệ hiện được thực hiện trong mã nguồn ứng dụng Python.
- Chỉ hỗ trợ `VND` và `USD`.
- Chỉ hỗ trợ phiên bản lược đồ `1.0`.

---

## 17. Bước 7 — Chạy kiểm thử

Từ thư mục gốc của kho mã nguồn:

```bash
python -m pytest -q tests/test_event_schema.py
```

Kết quả mong muốn:

```text
12 passed
```

Chạy ở chế độ chi tiết nếu có lỗi:

```bash
python -m pytest -vv tests/test_event_schema.py
```

Chạy riêng một kiểm thử:

```bash
python -m pytest -vv \
  tests/test_event_schema.py::test_negative_amount_fails
```

Pytest tự động tìm các tệp và hàm có tên theo quy ước `test_*`.
