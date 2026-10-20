pip install boto3 python-dotenv -> là thư việc chính thức cần làm để python thao tác với AWS
broker: là một máy chủ kafka, 1 broker chạy trong Docker
Topic: Một "kênh" chứa messager cùng loại, giống tên bảng (gdelt.events)
Message: Một bản ghi gồm key và value (Value = một dòng sự kiện GDELT, key = GlobalEventID)
Partition: Topic chia thành nhiều phần để xử lý song song (3 partition)
Offset: Số thứ tự của message trong một partition, tăng dần từ 0 (Spark nhớ offset để biết đã đọc tới đâu)
Producer / Consumer: bên ghi bên đọc (Crawler / Spark)
Consumer group: Nhóm consumer chia nhau đọc một topic; Kafka nhớ offset theo từng nhóm (Mỗi lần test dùng một nhóm mới)


producer sẽ lấy dữ liệu crawler đc từ website hoặc API rồi gửi vào topic smock Consumer đọc message rồi lưu dữ liệu xuống S3 
- Nhiều producer có thể một consumer group | VD: 3 crawler gửi vào topic smock một nhóm consumer đọc dữ liệu có thể ghi xuống s3
- Một producer có thể nhiều consumer group | VD: 1 luồng data giá sản phẩm có thể đc đọc độc lập bởi nhóm lưu S3 và nhóm phát hiện giá 2 nhóm có offset riêng


ingestion\gdelt_ingest
- __main__.py  (dòng lệnh: init / events / gkg)
- config.py (đọc cấu hình từ biến môi trường)
- sinks.py  (mọi thứ về nguồn GDELT)
- source.py  (hai nơi ghi: S3 và Kafka)