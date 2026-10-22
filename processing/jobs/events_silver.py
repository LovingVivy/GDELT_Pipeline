"""Spark Structured Streaming: Kafka gdelt.events -> làm sạch -> tầng silver (Parquet trên S3)."""
import os

from pyspark.sql import DataFrame, SparkSession, functions as F

from common import build_spark, lake_path
from schemas import EVENT_SCHEMA

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_EVENTS_TOPIC", "gdelt.events")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/spark/checkpoints")
TRIGGER = os.getenv("STREAM_TRIGGER", "1 minute")
WATERMARK = os.getenv("STREAM_WATERMARK", "2 days")

SILVER_EVENTS = lake_path("silver/gdelt/events")


def read_kafka(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")   # chỉ áp dụng lần chạy đầu, khi chưa có checkpoint
        .option("maxOffsetsPerTrigger", 20000)   # mỗi batch đọc tối đa 20.000 message
        .option("failOnDataLoss", "false")
        .load()
    )


def parse(raw: DataFrame) -> DataFrame:
    """Tách dòng TSV thành 61 cột chuỗi; giữ lại vị trí trên Kafka để truy vết."""
    fields = F.split(F.col("value").cast("string"), "\t")
    rows = raw.select(
        fields.alias("f"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    ).where(F.size("f") == len(EVENT_SCHEMA))
    return rows.select(
        *[F.col("f")[i].alias(name) for i, (name, _) in enumerate(EVENT_SCHEMA)],
        "kafka_partition", "kafka_offset", "kafka_timestamp",
    )


def typed(name: str, dtype: str):
    """Cắt khoảng trắng, chuỗi rỗng thành null, rồi ép kiểu (giá trị sai kiểu cũng thành null)."""
    value = F.trim(F.col(name))
    value = F.when(value == "", None).otherwise(value)
    return (value if dtype == "string" else value.cast(dtype)).alias(name)


def clean(parsed: DataFrame) -> DataFrame:
    df = parsed.select(
        *[typed(name, dtype) for name, dtype in EVENT_SCHEMA],
        "kafka_partition", "kafka_offset", "kafka_timestamp",
    )

    lat, lon = F.col("action_geo_lat"), F.col("action_geo_long")
    valid_geo = lat.between(-90, 90) & lon.between(-180, 180) & ~((lat == 0) & (lon == 0))

    df = df.withColumns(
        {
            "event_time": F.to_timestamp("date_added", "yyyyMMddHHmmss"),
            "event_date": F.to_date("sql_date", "yyyyMMdd"),
            "action_geo_lat": F.when(valid_geo, lat),
            "action_geo_long": F.when(valid_geo, lon),
            "source_domain": F.expr("parse_url(source_url, 'HOST')"),
        }
    )

    valid = (
        F.col("global_event_id").isNotNull()
        # Mốc thời gian vô lý (nhất là ở tương lai) sẽ kéo watermark đi và làm rơi dữ liệu đúng.
        & F.col("event_time").between(
            F.lit("2015-02-18").cast("timestamp"),
            F.current_timestamp() + F.expr("INTERVAL 1 DAY"),
        )
        & F.col("event_root_code").rlike("^(0[1-9]|1[0-9]|20)$")
        & F.col("quad_class").between(1, 4)
    )
    return (
        df.where(valid)
        .drop("sql_date", "date_added")
        .withColumn("dt", F.to_date("event_time"))
    )


def dedupe(df: DataFrame) -> DataFrame:
    """Crawler giao at-least-once nên một sự kiện có thể đến hai lần."""
    return df.withWatermark("event_time", WATERMARK).dropDuplicates(["global_event_id", "event_time"])


def main() -> None:
    spark = build_spark(
        "gdelt-events-silver",
        **{
            "spark.sql.shuffle.partitions": "2",
            "spark.sql.streaming.stateStore.providerClass":
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        },
    )

    events = dedupe(clean(parse(read_kafka(spark))))

    query = (
        events.writeStream.queryName("events_silver")
        .format("parquet")
        .option("path", SILVER_EVENTS)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/events_silver")
        .partitionBy("dt")
        .outputMode("append")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    print(f"Đang ghi silver vào {SILVER_EVENTS}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()