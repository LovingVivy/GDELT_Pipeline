"""Phần dùng chung của các DAG: mỗi tác vụ chạy trong một container riêng trên mạng của pipeline.

Airflow chỉ điều phối (lập lịch, thử lại, theo dõi); mã xử lý nằm trong hai image
gdelt-ingest (crawler) và gdelt-spark (Spark job).
"""
import os
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator

PIPELINE_NETWORK = os.getenv("PIPELINE_NETWORK", "gdelt-pipeline_default")


def _env(names: tuple) -> dict:
    return {name: os.environ[name] for name in names if name in os.environ}


# Cấu hình thường: hiện được trên giao diện Airflow
PUBLIC_ENV = _env((
    "KAFKA_BOOTSTRAP_SERVERS", "AWS_DEFAULT_REGION", "S3_BUCKET", "CLICKHOUSE_HOST", "CLICKHOUSE_USER",
))
# Bí mật: truyền riêng để Airflow không hiển thị trên giao diện
SECRET_ENV = _env(("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "CLICKHOUSE_PASSWORD"))

DEFAULT_ARGS = {
    "owner": "gdelt",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def container_task(task_id: str, image: str, command: str, **kwargs) -> DockerOperator:
    return DockerOperator(
        task_id=task_id,
        image=image,
        command=command,
        environment=PUBLIC_ENV,
        private_environment=SECRET_ENV,
        network_mode=PIPELINE_NETWORK,
        docker_url="unix://var/run/docker.sock",
        auto_remove="force",
        mount_tmp_dir=False,
        **kwargs,
    )


def ingest_task(task_id: str, command: str, **kwargs) -> DockerOperator:
    return container_task(task_id, "gdelt-ingest:latest", command, **kwargs)


def spark_task(task_id: str, job: str, job_args: str = "", cores: int = 3,
               executor_memory: str = "2500m", **kwargs) -> DockerOperator:
    command = (
        "/opt/spark/bin/spark-submit --master spark://spark-master:7077 "
        f"--conf spark.cores.max={cores} --conf spark.executor.memory={executor_memory} "
        f"--conf spark.driver.memory=1g /opt/gdelt/jobs/{job} {job_args}"
    )
    return container_task(task_id, "gdelt-spark:latest", command, **kwargs)