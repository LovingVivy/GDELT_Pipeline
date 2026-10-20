"""Cấu hình của crawler, đọc từ biến môi trường (file .env khi chạy trên máy)."""
import os

from dotenv import load_dotenv

load_dotenv()


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường {name} (kiểm tra file .env)")
    return value


GDELT_BASE_URL = os.getenv("GDELT_BASE_URL", "http://data.gdeltproject.org/gdeltv2")

S3_BUCKET = required("S3_BUCKET")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_EVENTS_TOPIC = os.getenv("KAFKA_EVENTS_TOPIC", "gdelt.events")
KAFKA_EVENTS_PARTITIONS = int(os.getenv("KAFKA_EVENTS_PARTITIONS", "3"))