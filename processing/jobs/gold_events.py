"""Silver -> gold: làm giàu và tổng hợp sự kiện, ghi Parquet lên S3 rồi đẩy sang ClickHouse.

Mỗi khi silver có file mới, tính lại toàn bộ các ngày (dt) bị ảnh hưởng và ghi đè đúng các ngày đó,
nên chạy lại bao nhiêu lần kết quả vẫn như nhau.
"""
import os

from pyspark.sql import DataFrame, functions as F

from common import build_spark, for_clickhouse, lake_path, write_clickhouse
from reference import countries, event_roots

from datetime import timedelta

from bursts import detect as detect_bursts
from common import build_spark, for_clickhouse, lake_path, replace_days_in_clickhouse, write_clickhouse

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/spark/checkpoints")
TRIGGER = os.getenv("GOLD_TRIGGER", "1 minute")

SILVER_EVENTS = lake_path("silver/gdelt/events")
GOLD_FCT_EVENTS = lake_path("gold/fct_events")
GOLD_AGG_EVENTS_15M = lake_path("gold/agg_events_15m")
GOLD_BURSTS = lake_path("gold/bursts")


def enrich(silver: DataFrame, countries_df: DataFrame, roots_df: DataFrame) -> DataFrame:
    """Chọn cột cần cho phân tích, ghép tên quốc gia (nơi xảy ra) và tên nhóm sự kiện."""
    events = silver.select(
        "global_event_id", "event_time", "event_date", "dt",
        "actor1_name", "actor2_name", "actor1_country_code", "actor2_country_code",
        "event_code", "event_root_code", "quad_class", "goldstein_scale",
        "num_mentions", "num_sources", "num_articles", "avg_tone",
        F.coalesce("action_geo_country_code", F.lit("")).alias("country_code"),
        F.col("action_geo_full_name").alias("geo_full_name"),
        F.col("action_geo_lat").alias("lat"),
        F.col("action_geo_long").alias("lon"),
        "source_url", "source_domain",
    )
    return (
        events.join(F.broadcast(countries_df), "country_code", "left")
        .join(F.broadcast(roots_df), "event_root_code", "left")
        .withColumn(
            "country_name",
            F.when(F.col("country_code") == "", "Unknown").otherwise(F.coalesce("country_name", "country_code")),
        )
        .withColumn("event_root_name", F.coalesce("event_root_name", "event_root_code"))
    )


def aggregate_15m(fct: DataFrame) -> DataFrame:
    return fct.groupBy(
        F.window("event_time", "15 minutes").getField("start").alias("window_start"),
        "dt", "country_code", "country_name", "event_root_code", "event_root_name", "quad_class",
    ).agg(
        F.count("*").alias("n_events"),
        F.countDistinct("source_url").alias("n_articles"),
        F.sum("num_mentions").alias("n_mentions"),
        F.sum("avg_tone").alias("sum_tone"),
        F.sum("goldstein_scale").alias("sum_goldstein"),
    )


def main() -> None:
    spark = build_spark(
        "gdelt-events-gold",
        **{
            "spark.sql.shuffle.partitions": "4",
            # mode("overwrite") chỉ thay các ngày có trong DataFrame, không xoá các ngày khác
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
        },
    )
    countries_df = countries(spark)
    roots_df = event_roots(spark)

    def publish(batch: DataFrame, batch_id: int) -> None:
        days = sorted(row.dt for row in batch.select("dt").distinct().collect())
        if not days:
            return
        # Đột biến cần 24 giờ nền, nên đọc thêm ngày liền trước của mỗi ngày bị ảnh hưởng.
        history_days = sorted(set(days) | {day - timedelta(days=1) for day in days})

        silver = spark.read.parquet(SILVER_EVENTS).where(F.col("dt").isin(history_days))
        history = enrich(silver, countries_df, roots_df).persist()
        fct = history.where(F.col("dt").isin(days))
        agg = aggregate_15m(fct).persist()
        bursts = detect_bursts(history, days).persist()
        try:
            # 1) S3 gold: nguồn chính thức
            fct.write.mode("overwrite").partitionBy("dt").parquet(GOLD_FCT_EVENTS)
            agg.write.mode("overwrite").partitionBy("dt").parquet(GOLD_AGG_EVENTS_15M)
            bursts.write.mode("overwrite").partitionBy("dt").parquet(GOLD_BURSTS)
            # 2) ClickHouse: bản sao để truy vấn nhanh
            write_clickhouse(for_clickhouse(fct, keep_null=("lat", "lon")), "fct_events")
            write_clickhouse(for_clickhouse(agg), "agg_events_15m")
            n_bursts = replace_days_in_clickhouse("gdelt.bursts", days, bursts)
            print(f"[gold] batch {batch_id}: ngày {[str(d) for d in days]}, "
                    f"{fct.count()} sự kiện, {agg.count()} dòng tổng hợp, {n_bursts} đột biến", flush=True)
        finally:
            for df in (history, agg, bursts):
                df.unpersist()

    # Đọc silver như một luồng chỉ để biết khi nào có file mới; dữ liệu thật đọc lại trong publish().
    silver_schema = spark.read.parquet(SILVER_EVENTS).schema
    changes = spark.readStream.schema(silver_schema).parquet(SILVER_EVENTS).select("dt")

    query = (
        changes.writeStream.queryName("events_gold")
        .foreachBatch(publish)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/events_gold")
        .trigger(processingTime=TRIGGER)
        .start()
    )
    print(f"Đang theo dõi {SILVER_EVENTS}", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main()