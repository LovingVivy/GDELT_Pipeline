"""Hai nơi crawler ghi dữ liệu: tầng bronze trên S3 và topic Kafka."""
import gzip

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from . import config


class BronzeStore:
    """Tầng bronze trên S3: chỉ thêm file mới, không sửa, không xoá."""

    def __init__(self, bucket: str):
        self.bucket = bucket
        self.s3 = boto3.client("s3")

    def exists(self, key: str) -> bool:
        try:
            self.s3.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as err:
            if err.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    def put_gzip(self, key: str, raw: bytes) -> int:
        data = gzip.compress(raw)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType="application/gzip")
        return len(data)


class EventProducer:
    """Gửi mỗi dòng của file Events thành một message; key là GlobalEventID."""

    def __init__(self):
        self.topic = config.KAFKA_EVENTS_TOPIC
        self.errors = 0
        self.producer = Producer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "enable.idempotence": True,  # broker tự bỏ message gửi lặp do thử lại mạng
                "acks": "all",               # chỉ coi là thành công khi broker đã ghi xong
                "compression.type": "lz4",
                "linger.ms": 50,             # chờ 50 ms để gom nhiều message gửi một lượt
            }
        )

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.errors += 1

    def send_lines(self, raw: bytes) -> int:
        sent = 0
        for line in raw.split(b"\n"):
            line = line.rstrip(b"\r")
            if not line:
                continue
            key = line.split(b"\t", 1)[0]  # cột đầu tiên là GlobalEventID
            while True:
                try:
                    self.producer.produce(self.topic, value=line, key=key, on_delivery=self._on_delivery)
                    break
                except BufferError:
                    self.producer.poll(0.5)  # bộ đệm đầy: chờ gửi bớt rồi thử lại
            sent += 1

        remaining = self.producer.flush(60)
        if remaining or self.errors:
            raise KafkaException(f"Gửi Kafka thất bại: {self.errors} lỗi, {remaining} message chưa gửi được")
        return sent


def ensure_topic() -> bool:
    """Tạo topic gdelt.events nếu chưa có. Trả về True nếu vừa tạo."""
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    if config.KAFKA_EVENTS_TOPIC in admin.list_topics(timeout=30).topics:
        return False
    topic = NewTopic(config.KAFKA_EVENTS_TOPIC, num_partitions=config.KAFKA_EVENTS_PARTITIONS, replication_factor=1)
    admin.create_topics([topic])[config.KAFKA_EVENTS_TOPIC].result(timeout=30)
    return True