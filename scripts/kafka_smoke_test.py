"""Gửi vài message vào topic smoke.test rồi đọc lại, để kiểm tra kết nối Kafka từ máy host."""
import os
import sys
import time
import uuid

from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

TOPIC = "smoke.test"


def produce(bootstrap: str) -> int:
    producer = Producer({"bootstrap.servers": bootstrap})
    delivered = 0

    def on_delivery(err, msg):
        nonlocal delivered
        if err is not None:
            print(f"  Gửi lỗi: {err}")
            return
        delivered += 1
        print(f"  Gửi key={msg.key().decode()} -> partition {msg.partition()}, offset {msg.offset()}")

    for i in range(6):
        key = f"event-{i % 3}"  # 3 key, mỗi key 2 message
        producer.produce(TOPIC, key=key, value=f"message {i}", on_delivery=on_delivery)

    producer.flush(10)
    return delivered


def consume(bootstrap: str, expected: int) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"smoke-{uuid.uuid4()}",  # nhóm mới mỗi lần chạy
            "auto.offset.reset": "earliest",       # nhóm mới thì đọc từ message đầu tiên
        }
    )
    consumer.subscribe([TOPIC])

    received = 0
    deadline = time.time() + 20
    while received < expected and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"  Đọc lỗi: {msg.error()}")
            continue
        received += 1
        print(f"  Đọc key={msg.key().decode()} value='{msg.value().decode()}' (partition {msg.partition()}, offset {msg.offset()})")

    consumer.close()
    return received


def main() -> int:
    load_dotenv()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

    print(f"Gửi vào {TOPIC} qua {bootstrap}:")
    sent = produce(bootstrap)
    if sent == 0:
        print("Không gửi được message nào. Kafka đã chạy và healthy chưa?")
        return 1

    print("Đọc lại:")
    got = consume(bootstrap, sent)
    if got < sent:
        print(f"Chỉ đọc được {got}/{sent} message")
        return 1

    print(f"Kafka hoạt động tốt: gửi {sent}, đọc {got}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())