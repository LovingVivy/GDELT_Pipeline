"""Mỗi 15 phút: tải các file Events mới của GDELT và đẩy vào Kafka.

Tác vụ nhìn lại 3 giờ và chỉ xử lý những file chưa có ở bronze, nên file về trễ hoặc
lần chạy bị lỡ đều được tự động nạp bù ở lần sau. Job Spark streaming đọc Kafka và xử lý tiếp.
"""
from datetime import datetime

from airflow import DAG

from gdelt_common import DEFAULT_ARGS, ingest_task

with DAG(
    dag_id="gdelt_events_ingest",
    description="Crawler GDELT Events -> Kafka + bronze, mỗi 15 phút",
    schedule="3-59/15 * * * *",   # phút 3, 18, 33, 48: lệch 3 phút để chờ GDELT công bố file
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["gdelt", "streaming"],
    doc_md=__doc__,
) as dag:
    ingest_task("sync_events", "events --hours 3")