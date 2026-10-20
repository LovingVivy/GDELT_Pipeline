pip install boto3 python-dotenv -> là thư việc chính thức cần làm để python thao tác với AWS
broker: là một máy chủ kafka, 1 broker chạy trong Docker
Topic: Một "kênh" chứa messager cùng loại, giống tên bảng (gdelt.events)
Message: Một bản ghi gồm key và value (Value = một dòng sự kiện GDELT, key = GlobalEventID)
Partition: Topic chia thành nhiều phần để xử lý song song (3 partition)
Offset: Số thứ tự của message trong một partition, tăng dần từ 0 (Spark nhớ offset để biết đã đọc tới đâu)
Producer / Consumer: bên ghi bên đọc (Crawler / Spark)
Consumer group: Nhóm consumer chia nhau đọc một topic; Kafka nhớ offset theo từng nhóm (Mỗi lần test dùng một nhóm mới)
