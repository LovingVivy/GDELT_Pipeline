"""Phần dùng chung của các Spark job: tạo SparkSession có cấu hình S3."""
import os

from pyspark.sql import DataFrame, functions as F
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


CLICKHOUSE_OPTIONS = {
    "url": "jdbc:clickhouse://{}:{}/{}".format(
        os.getenv("CLICKHOUSE_HOST", "clickhouse"),
        os.getenv("CLICKHOUSE_PORT", "8123"),
        os.getenv("CLICKHOUSE_DB", "gdelt"),
    ),
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": os.getenv("CLICKHOUSE_USER", "gdelt"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", ""),
}


def for_clickhouse(df: DataFrame, keep_null: tuple = ()) -> DataFrame:
    """ClickHouse không nhận null ở cột không khai báo Nullable: chuỗi thành '', số thành 0."""
    strings = [f.name for f in df.schema.fields if f.dataType.simpleString() == "string"]
    numbers = [
        f.name for f in df.schema.fields
        if f.dataType.simpleString() in ("int", "bigint", "double") and f.name not in keep_null
    ]
    return df.na.fill("", strings).na.fill(0, numbers).withColumn("updated_at", F.current_timestamp())


def write_clickhouse(df: DataFrame, table: str) -> None:
    (
        df.write.format("jdbc")
        .options(**CLICKHOUSE_OPTIONS)
        .option("dbtable", table)
        .option("batchsize", 20000)
        .option("isolationLevel", "NONE")
        .mode("append")
        .save()
    )