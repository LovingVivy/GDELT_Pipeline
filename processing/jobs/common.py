"""Phần dùng chung của các Spark job: tạo SparkSession có cấu hình S3."""
import os

from pyspark.sql import SparkSession


def required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Thiếu biến môi trường {name}")
    return value


AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-1")
S3_BUCKET = required("S3_BUCKET")


def lake_path(path: str) -> str:
    """Đường dẫn trong data lake, ví dụ lake_path('silver/gdelt/events')."""
    return f"s3a://{S3_BUCKET}/{path}"


def build_spark(app_name: str, **extra_conf) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # --- Kết nối S3 qua S3A (thư viện hadoop-aws)
        .config("spark.hadoop.fs.s3a.access.key", required("AWS_ACCESS_KEY_ID"))
        .config("spark.hadoop.fs.s3a.secret.key", required("AWS_SECRET_ACCESS_KEY"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.endpoint.region", AWS_REGION)
    )
    for key, value in extra_conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark