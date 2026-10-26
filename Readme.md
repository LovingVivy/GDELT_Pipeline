# Phát hiện sự kiện và xu hướng tin tức toàn cầu thời gian thực từ GDELT

**Bài toán:** mỗi 15 phút, đọc tin tức của cả thế giới và tự động trả lời: chỗ nào đang có chuyện bất thường, và thế giới đang nói về những chủ đề gì.

```
GDELT ─► Crawler ─► Kafka ─► Spark Structured Streaming ─► S3 (bronze / silver / gold) ─► ClickHouse ─► FastAPI ─► Dashboard
                └──────────────── GKG ─► S3 bronze ─► Spark batch (TF-IDF + LDA) ──┘
                              Airflow điều phối crawler và job GKG
```

## Đầu vào

GDELT là một dự án cho máy đọc tin tức từ hàng nghìn trang báo trên khắp thế giới, rồi chuyển nội dung thành dữ liệu có cấu trúc. Nó phát hành hai loại:

- **Events** — “ai làm gì với ai, ở đâu”. Một dòng không phải một bài báo, mà là một hành động máy trích ra từ bài báo. Ví dụ dòng ISTANBUL → SAUDI ARABIA, mã 04 (Tham vấn), tại Riyadh mà bạn đã thấy trong inspect_silver.py. Mỗi hành động được phân loại theo bộ mã CAMEO (20 nhóm, từ “tuyên bố”, “hợp tác” đến “biểu tình”, “tấn công”) và chấm điểm tone (giọng điệu tích cực hay tiêu cực).
- **GKG** — “bài báo nói về chủ đề gì”. Mỗi dòng là một bài báo, kèm tiêu đề, nhân vật, tổ chức, chủ đề.

## Đầu ra

**1. “Có chuyện gì bất thường đang xảy ra không?” — phát hiện sự kiện (bước 6)**

Đây là phần cốt lõi. Hệ thống không chờ người đọc báo; nó tự thấy khi một nơi bỗng được nhắc tới nhiều bất thường.
Ví dụ thật từ bản dự án trước: bình thường Philippines có khoảng 2–3 bài mỗi 15 phút. Lúc 16:30 bỗng có 36 bài từ nhiều trang khác nhau. Hệ thống tự ghi vào bảng bursts: “Philippines, đột biến, z = 13,8”. Người dùng bấm vào thì thấy danh sách bài báo đứng sau cái đỉnh đó, và đọc là biết chuyện gì xảy ra.
Nó phát hiện ở ba mức:

- Theo quốc gia: “Sri Lanka đang được nhắc tới gấp 12 lần bình thường”.
- Theo loại sự kiện: “Số sự kiện biểu tình trên toàn cầu tăng vọt”.
- Theo quốc gia × loại: “Úc · Kêu gọi” — cụ thể hơn, gần với một sự kiện thật.

**2. Tình hình đang diễn biến ra sao — xu hướng thời gian thực (bước 5)**

Bảng agg_events_15m trả lời các câu hỏi như:

- Quốc gia nào được nhắc nhiều nhất trong 24 giờ qua? (bản trước: Mỹ khoảng 30%, rồi Ấn Độ, Anh…)
- Tỉ lệ sự kiện xung đột so với hợp tác đang tăng hay giảm?
- Tone của tin tức về một nước đang xấu đi hay tốt lên?

**3. Tuần này thế giới đang nói về chủ đề gì? — xu hướng theo tuần (bước 7)**

LDA đọc khoảng 160 nghìn tiêu đề trong tuần và tự gom thành khoảng 12 chủ đề, mà không ai phải dạy nó trước. Ở bản trước, nó tự tìm ra những nhóm như court · supreme · case · appeal (tư pháp) hay election · minister · party (bầu cử). TF-IDF thì cho biết mỗi ngày nổi lên nhân vật, tổ chức nào — bản trước có những cái tên như Marco Rubio, Jeffrey Archer.

## Đối tượng sử dụng

Nhà báo, phóng viên, tòa soạn, nhà phân tích rủi ro doanh nghiệp đa quốc gia, nhà nghiên cứu,...

---

# Cách chạy dự án

## 1. Chuẩn bị

| Cần có | Ghi chú |
|---|---|
| Docker Desktop (Windows/macOS) hoặc Docker Engine + Compose v2 (Linux) | Cấp cho Docker **khoảng 10–12 GB RAM**, 6 CPU. Ổ chứa dữ liệu Docker nên trống ≥ 15 GB |
| Python 3.12 | Chỉ để chạy script kiểm tra và test trên máy; mọi thành phần chính đều chạy trong container |
| Tài khoản AWS | Một bucket S3 và một IAM user riêng cho dự án |
| Node.js 20 (tuỳ chọn) | Chỉ cần khi muốn sửa giao diện dashboard |

Các cổng mà dự án dùng trên máy: `3000, 8000, 8082, 8089, 8090, 8091, 8123, 4041, 4042, 29092`. Nếu máy đang chạy dịch vụ khác trên các cổng này, đổi vế trái của `ports:` trong `docker-compose.yml`.

## 2. Tạo bucket S3 và IAM user

1. Vào S3, tạo bucket (ví dụ `gdelt-lake-<tên-bạn>`) ở vùng **ap-southeast-1**, giữ nguyên *Block all public access*.
2. Vào IAM, tạo user (ví dụ `gdelt-pipeline`), **không** cấp quyền console. Gắn inline policy chỉ cho phép dùng đúng bucket này (thay `TEN_BUCKET`):

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       { "Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": "arn:aws:s3:::TEN_BUCKET" },
       { "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"], "Resource": "arn:aws:s3:::TEN_BUCKET/*" }
     ]
   }
   ```

3. Tạo **Access key** cho user đó (loại *Application running outside AWS*).

> Mỗi người trong nhóm nên có IAM user và access key riêng. Không gửi file `.env` cho nhau, không commit `.env` lên git (đã có trong `.gitignore`).

## 3. Cấu hình `.env`

```bash
cp .env.example .env
```

Mở `.env` và điền:

| Biến | Ý nghĩa |
|---|---|
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Access key của IAM user ở bước 2 |
| `AWS_DEFAULT_REGION` | `ap-southeast-1` (vùng của bucket) |
| `S3_BUCKET` | Tên bucket |
| `KAFKA_BOOTSTRAP_SERVERS` | Giữ `localhost:29092` (dùng khi chạy script trên máy; container tự dùng `kafka:9092`) |
| `CLICKHOUSE_USER`, `CLICKHOUSE_PASSWORD` | Tài khoản ClickHouse sẽ được tạo ở lần chạy đầu; đặt mật khẩu tuỳ ý |
| `AIRFLOW_ADMIN_PASSWORD` (tuỳ chọn) | Mật khẩu tài khoản `admin` của Airflow, mặc định `admin` |

## 4. Kiểm tra kết nối S3 (trên máy)

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate      macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
python scripts/check_s3.py
```

Script ghi, liệt kê, đọc lại rồi xoá một object nhỏ. Thấy báo thành công là khoá và quyền IAM đã đúng.

## 5. Build và khởi động

Chạy lần lượt (lần đầu build image mất khoảng 5–10 phút):

```bash
# 1. Build image Spark (dùng chung cho master, worker và các job) và image crawler (Airflow dùng để chạy tác vụ)
docker compose build spark-master ingest

# 2. Bật Kafka trước, rồi tạo topic gdelt.events (Kafka đã tắt tự tạo topic)
docker compose up -d kafka
docker compose run --rm ingest init

# 3. Bật toàn bộ hệ thống
docker compose up -d --build
```

Ở lần chạy đầu, ClickHouse tự tạo các bảng từ `serving/clickhouse/init/*.sql`, Airflow tự tạo cơ sở dữ liệu metadata và tài khoản `admin`.

## 6. Nạp dữ liệu ban đầu

Bộ phát hiện đột biến cần ít nhất 6 giờ dữ liệu nền (tốt nhất là 24 giờ), nên nạp bù khoảng 30 giờ gần nhất:

```bash
docker compose run --rm ingest events --hours 30
```

Lệnh này idempotent: chạy lại thì các file đã có ở bronze sẽ được bỏ qua. Vài file mới nhất thường báo `missing` (GDELT công bố trễ khoảng 45–60 phút), lần chạy sau sẽ tự lấy.

Sau đó Spark tự xử lý: job silver đọc Kafka mỗi phút, job gold tổng hợp và phát hiện đột biến rồi đẩy sang ClickHouse. Chờ vài phút rồi mở dashboard.

Phân tích chủ đề GKG (TF-IDF + LDA) chạy lần đầu bằng tay: mở Airflow → DAG **`gdelt_gkg_manual`** → nút ▶ **Trigger DAG w/ config**:

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `days` | 7 | Số ngày GKG cần phân tích. Để `1` khi chạy thử (khoảng 10 phút) |
| `step_minutes` | 60 | 60 = mỗi giờ lấy một file, 15 = lấy mọi file (nặng gấp 4) |
| `topics` | 12 | Số chủ đề LDA |

## 7. Kiểm tra hệ thống

```bash
python scripts/check_pipeline.py
```

Script kiểm tra lần lượt S3, Kafka, Spark, ClickHouse (độ mới của dữ liệu), Airflow, API và dashboard. Kết quả mong đợi là tất cả `PASS`; Ở mục ClickHouse, `WARN` nghĩa là dữ liệu mới nhất đã cũ 2–6 giờ (ví dụ vừa bật lại máy), `FAIL` là cũ hơn 6 giờ: chạy lệnh nạp bù ở bước 6.

Chạy test đơn vị của crawler:

```bash
cd ingestion
python -m pytest tests
```

## 8. Các giao diện

| Giao diện | Địa chỉ | Ghi chú |
|---|---|---|
| Dashboard | http://localhost:3000 | Đột biến, diễn biến, bản đồ, chủ đề tuần |
| API (Swagger) | http://localhost:8000/api/docs | 11 endpoint đọc tầng gold |
| Airflow | http://localhost:8089 | `admin` / `admin` (hoặc `AIRFLOW_ADMIN_PASSWORD`) |
| Spark master | http://localhost:8090 | 1 worker; job silver (2 core) và gold (1 core) chạy liên tục |
| Spark job silver / gold | http://localhost:4041 · http://localhost:4042 | Tab *Structured Streaming* để xem từng micro-batch |
| Kafka UI | http://localhost:8082 | Topic `gdelt.events` |
| ClickHouse | http://localhost:8123/play | Đăng nhập bằng `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD`; bảng `fct_events`, `agg_events_15m`, `bursts`, `gkg_*` |

## 9. Vận hành hằng ngày

| Việc | Lệnh / thao tác |
|---|---|
| Xem trạng thái các container | `docker compose ps` |
| Xem log một dịch vụ | `docker compose logs -f spark-gold` |
| Tắt hệ thống (giữ dữ liệu) | `docker compose down` |
| Bật lại | `docker compose up -d` — Airflow tự nạp lại 3 giờ gần nhất; nếu tắt lâu hơn thì chạy thêm lệnh nạp bù ở bước 6 |
| Nạp bù dữ liệu Events | `docker compose run --rm ingest events --hours 12` |
| Tải lại GKG bằng tay | `docker compose run --rm ingest gkg --days 1 --step-minutes 60` |
| Bật lịch GKG hằng tuần | Airflow → bật (unpause) DAG `gdelt_gkg_weekly`, chạy 02:00 UTC thứ Hai |
| Truy vấn ClickHouse từ dòng lệnh | `docker compose exec clickhouse clickhouse-client --user <user> --password <mật khẩu>` |

**Các DAG Airflow:**

| DAG | Lịch | Tác vụ |
|---|---|---|
| `gdelt_events_ingest` | phút 3, 18, 33, 48 mỗi giờ | Crawler Events, nhìn lại 3 giờ |
| `gdelt_gkg_weekly` | 02:00 UTC thứ Hai (mặc định tạm dừng) | Tải GKG 7 ngày → TF-IDF + LDA |
| `gdelt_gkg_manual` | chạy tay | Như trên, chọn được số ngày và số chủ đề |

Mỗi tác vụ chạy trong một container riêng (DockerOperator) và tự thử lại 2 lần, cách nhau 2 phút.

**Khi sửa mã:**

- Sửa crawler (`ingestion/`): `docker compose build ingest`. Lần chạy Airflow tiếp theo sẽ dùng bản mới.
- Sửa job Spark (`processing/jobs/`): mã được mount vào container nên chỉ cần khởi động lại job: `docker compose restart spark-streaming spark-gold`. Nếu sửa `processing/Dockerfile` thì chạy `docker compose build spark-master` rồi `docker compose up -d`.
- Sửa API: `docker compose up -d --build api`.
- Sửa giao diện: trong `serving/dashboard` chạy `npm install` rồi `npm run dev` (http://localhost:5173, tự chuyển `/api` sang cổng 8000). Xong thì `docker compose up -d --build dashboard`.

**Làm lại từ đầu:** `docker compose down -v` xoá toàn bộ volume (Kafka, ClickHouse, checkpoint Spark, Airflow), còn dữ liệu trên S3 thì vẫn giữ nguyên. Checkpoint Spark (trong volume) và thư mục `_spark_metadata` của tầng silver (trên S3) phải đi cùng nhau: xoá một bên mà giữ bên kia thì job silver sẽ bỏ qua dữ liệu mới mà không báo lỗi. Vì vậy sau `down -v` phải **xoá luôn prefix `silver/` và `gold/` trên S3** (giữ `bronze/`), rồi làm lại từ bước 5 và nạp lại bằng `docker compose run --rm ingest events --hours 30 --force` (`--force` gửi lại cả những file đã có ở bronze).

## 10. Lỗi hay gặp

| Hiện tượng | Nguyên nhân và cách xử lý |
|---|---|
| `spark-streaming` khởi động lại liên tục, log báo không thấy topic | Chưa tạo topic: `docker compose run --rm ingest init` |
| Airflow báo `ImageNotFound: gdelt-ingest` | Chưa build image crawler: `docker compose build ingest` |
| `port is already allocated` | Cổng đang bị chương trình khác dùng; tắt chương trình đó hoặc đổi cổng trong `docker-compose.yml` |
| Dashboard báo lỗi 502 sau khi build lại API | `docker compose restart dashboard` |
| Mở được container nhưng không vào được `localhost:<cổng>` sau khi Docker khởi động lại | `docker compose up -d --force-recreate` |
| Crawler báo `missing` cho vài file mới nhất | Bình thường: GDELT công bố trễ; lần chạy sau tự lấy |
| Đèn trạng thái trên dashboard màu vàng/đỏ | Dữ liệu cũ hơn 2 giờ: kiểm tra DAG `gdelt_events_ingest` trên Airflow, hoặc chạy lệnh nạp bù |
| `AccessDenied` khi ghi S3 | Sai tên bucket hoặc policy IAM; chạy lại `python scripts/check_s3.py` |
| Gõ SQL ClickHouse trong PowerShell bị lỗi cú pháp | PowerShell 5.1 làm mất dấu nháy; truyền câu SQL qua stdin: `"SELECT 1" \| docker compose exec -T clickhouse sh -c 'clickhouse-client --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD'` |
| (Windows) Docker Desktop crash ngay khi bật, log có `remove ...\AppData\Local\Docker\run\dockerInference: The file cannot be accessed by the system` | Socket cũ bị hỏng sau một lần Docker tắt đột ngột (ví dụ ổ C đầy). Thoát Docker, đổi tên thư mục `%LOCALAPPDATA%\Docker\run` (và `%LOCALAPPDATA%\docker-secrets-engine` nếu lỗi nhắc tới nó) rồi bật lại. Luôn tắt Docker bằng *Quit Docker Desktop* |

## Cấu trúc thư mục

```
ingestion/            crawler GDELT (Python): tải file, ghi S3 bronze, gửi Kafka
  gdelt_ingest/         __main__.py (CLI init/events/gkg), config.py, source.py, sinks.py
  tests/                test pytest cho phần sinh mốc thời gian và đường dẫn bronze
processing/           image Spark và các job
  jobs/                 events_silver.py, gold_events.py, bursts.py, gkg_weekly.py, ...
  lookups/              bảng mã CAMEO và quốc gia FIPS
serving/
  clickhouse/init/      DDL các bảng ClickHouse
  api/                  FastAPI (11 endpoint)
  dashboard/            React + Vite + Recharts + Leaflet, chạy sau nginx
orchestration/dags/   các DAG Airflow
scripts/              check_s3.py, check_pipeline.py, kafka_smoke_test.py, fetch_lookups.py
```

**Quy ước lưu trữ trên S3:**

| Tầng | Đường dẫn | Định dạng |
|---|---|---|
| Bronze | `bronze/gdelt/{events,gkg}/ingest_date=YYYY-MM-DD/<mốc>.<loại>.gz` | File gốc GDELT, gzip, không sửa không xoá |
| Silver | `silver/gdelt/{events,gkg}/dt=YYYY-MM-DD/` | Parquet đã làm sạch, khử trùng lặp |
| Gold | `gold/<bảng>/dt=...` hoặc `run_id=...` | Parquet đã tổng hợp: `fct_events`, `agg_events_15m`, `bursts`, `gkg_*` |

---

# Ghi chú học

`pip install boto3 python-dotenv` → là thư viện chính thức cần dùng để Python thao tác với AWS.

**Kafka:**

- Broker: là một máy chủ Kafka, 1 broker chạy trong Docker.
- Topic: một "kênh" chứa message cùng loại, giống tên bảng (`gdelt.events`).
- Message: một bản ghi gồm key và value (value = một dòng sự kiện GDELT, key = GlobalEventID).
- Partition: topic chia thành nhiều phần để xử lý song song (3 partition).
- Offset: số thứ tự của message trong một partition, tăng dần từ 0 (Spark nhớ offset để biết đã đọc tới đâu).
- Producer / Consumer: bên ghi / bên đọc (Crawler / Spark).
- Consumer group: nhóm consumer chia nhau đọc một topic; Kafka nhớ offset theo từng nhóm (mỗi lần test dùng một nhóm mới).

Producer sẽ lấy dữ liệu crawl được từ website hoặc API rồi gửi vào topic smoke; consumer đọc message rồi lưu dữ liệu xuống S3.

- Nhiều producer có thể gửi cho một consumer group. VD: 3 crawler gửi vào topic smoke, một nhóm consumer đọc dữ liệu rồi ghi xuống S3.
- Một producer có thể có nhiều consumer group. VD: 1 luồng dữ liệu giá sản phẩm có thể được đọc độc lập bởi nhóm lưu S3 và nhóm phát hiện giá; 2 nhóm có offset riêng.

**`ingestion/gdelt_ingest`:**

- `__main__.py`: dòng lệnh init / events / gkg
- `config.py`: đọc cấu hình từ biến môi trường
- `source.py`: mọi thứ về nguồn GDELT
- `sinks.py`: hai nơi ghi, S3 và Kafka
