# Quy ước lưu trữ dữ liệu

Bucket: `s3://<S3_BUCKET>` (vùng ap-southeast-1). Mọi thời gian theo UTC.

| Tầng   | Đường dẫn                                                                      | Định dạng          |
|--------|--------------------------------------------------------------------------------|--------------------|
| Bronze | bronze/gdelt/{events,gkg}/ingest_date=YYYY-MM-DD/<YYYYMMDDHHMMSS>.<loại>.gz  | File gốc nén gzip  |
| Silver | silver/gdelt/{events,gkg}/dt=YYYY-MM-DD/                                     | Parquet            |
| Gold   | gold/<ten_bang>/                                                               | Parquet            |

Nguyên tắc:
- Bronze bất biến: chỉ thêm file mới, không sửa, không xoá.
- Mốc 15 phút trong tên file GDELT là khoá chống trùng: file đã có ở bronze thì không tải lại.
- Phân vùng kiểu Hive (`ten_cot=gia_tri`) để Spark tự nhận ra cột phân vùng.
- Tên thư mục, bảng, cột viết snake_case.