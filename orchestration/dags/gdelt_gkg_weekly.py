"""GKG -> TF-IDF + LDA: tải GKG của N ngày gần nhất rồi chạy Spark batch.

  sync_gkg_raw      crawler tải các file GKG còn thiếu về bronze trên S3
  spark_gkg_topics  Spark: bronze -> silver -> TF-IDF + LDA -> gold + ClickHouse

Cùng một quy trình được đăng ký thành hai DAG:
  gdelt_gkg_weekly  chạy 02:00 UTC thứ Hai hàng tuần; mặc định tạm dừng vì mỗi lần tải khá nhiều dữ liệu.
                    Khi bật lên, Airflow chạy ngay kỳ tuần gần nhất rồi tiếp tục theo lịch.
  gdelt_gkg_manual  không có lịch, dùng để bấm Trigger chạy ngay khi demo.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param

from gdelt_common import DEFAULT_ARGS, ingest_task, spark_task


def build_dag(dag_id: str, schedule: str | None, paused: bool) -> DAG:
    with DAG(
        dag_id=dag_id,
        description="GKG N ngày -> TF-IDF + LDA (Spark batch)",
        schedule=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=paused,
        default_args=DEFAULT_ARGS,
        params={
            "days": Param(7, type="integer", minimum=1, maximum=14),
            "step_minutes": Param(60, type="integer", enum=[15, 30, 60]),
            "topics": Param(12, type="integer", minimum=4, maximum=30),
        },
        tags=["gdelt", "batch"],
        doc_md=__doc__,
    ) as dag:
        sync_gkg_raw = ingest_task(
            "sync_gkg_raw",
            "gkg --days {{ params.days }} --step-minutes {{ params.step_minutes }}",
            execution_timeout=timedelta(hours=2),
        )
        spark_gkg_topics = spark_task(
            "spark_gkg_topics",
            "gkg_weekly.py",
            "--days {{ params.days }} --topics {{ params.topics }}",
            execution_timeout=timedelta(hours=3),
        )
        sync_gkg_raw >> spark_gkg_topics
    return dag


weekly = build_dag("gdelt_gkg_weekly", schedule="0 2 * * 1", paused=True)
manual = build_dag("gdelt_gkg_manual", schedule=None, paused=False)