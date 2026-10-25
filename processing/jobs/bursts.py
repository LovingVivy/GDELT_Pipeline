"""Phát hiện đột biến: số bài báo của một đối tượng vọt lên bất thường so với 24 giờ trước đó.

    z = (n - mean) / max(std, sqrt(mean), 1)

n là số bài báo khác nhau trong cửa sổ 15 phút; mean, std tính trên các cửa sổ của 24 giờ liền trước.
"""
import os
from datetime import date, datetime, time, timezone

from pyspark.sql import DataFrame, Window, functions as F

WINDOW_MINUTES = 15
LOOKBACK_HOURS = int(os.getenv("BURST_LOOKBACK_HOURS", "24"))
MIN_BASELINE_HOURS = int(os.getenv("BURST_MIN_BASELINE_HOURS", "6"))
Z_THRESHOLD = float(os.getenv("BURST_Z", "4.0"))
MIN_SOURCES = int(os.getenv("BURST_MIN_SOURCES", "3"))
# Số bài tối thiểu trong cửa sổ, theo loại đối tượng (lọc nhiễu của chuỗi nhỏ)
MIN_ARTICLES = {"country": 8, "event_type": 20, "country_event": 8}


def build_series(events: DataFrame) -> DataFrame:
    """Chuỗi đếm theo 15 phút cho 3 loại đối tượng: (window_start, dimension, key, label, n, n_sources)."""
    articles = events.where(F.col("source_url").isNotNull()).select(
        F.window("event_time", f"{WINDOW_MINUTES} minutes").getField("start").alias("window_start"),
        "country_code", "country_name", "event_root_code", "event_root_name",
        "source_url", "source_domain",
    )
    counts = [
        F.countDistinct("source_url").alias("n"),
        F.countDistinct("source_domain").alias("n_sources"),
    ]
    located = articles.where(F.col("country_code") != "")

    by_country = (
        located.groupBy("window_start", F.col("country_code").alias("key"))
        .agg(F.first("country_name").alias("label"), *counts)
        .withColumn("dimension", F.lit("country"))
    )
    by_event_type = (
        articles.groupBy("window_start", F.col("event_root_code").alias("key"))
        .agg(F.first("event_root_name").alias("label"), *counts)
        .withColumn("dimension", F.lit("event_type"))
    )
    by_country_event = (
        located.groupBy("window_start", F.concat_ws("|", "country_code", "event_root_code").alias("key"))
        .agg(F.concat_ws(" · ", F.first("country_name"), F.first("event_root_name")).alias("label"), *counts)
        .withColumn("dimension", F.lit("country_event"))
    )
    return by_country.unionByName(by_event_type).unionByName(by_country_event)


def score(series: DataFrame, lo_ts: int, hi_ts: int) -> DataFrame:
    """Chấm z-score cho các cửa sổ trong [lo_ts, hi_ts] (epoch giây), giữ lại những cửa sổ vượt ngưỡng."""
    step = WINDOW_MINUTES * 60
    lookback = LOOKBACK_HOURS * 3600
    s = series.withColumn("ts", F.col("window_start").cast("long"))

    # Số cửa sổ hệ thống thực sự có dữ liệu trong 24 giờ trước mỗi mốc (dùng chung cho mọi chuỗi).
    active = s.select("ts").distinct().withColumn(
        "baseline_windows",
        F.count("*").over(Window.orderBy("ts").rangeBetween(-lookback, -step)),
    )

    # 24 giờ liền trước, KHÔNG gồm cửa sổ hiện tại
    prior = Window.partitionBy("dimension", "key").orderBy("ts").rangeBetween(-lookback, -step)
    n = F.col("n").cast("double")

    with_baseline = (
        s.withColumn("b_sum", F.coalesce(F.sum(n).over(prior), F.lit(0.0)))
        .withColumn("b_sumsq", F.coalesce(F.sum(n * n).over(prior), F.lit(0.0)))
        .where(F.col("ts").between(lo_ts, hi_ts))
        .join(F.broadcast(active), "ts")
        .where(F.col("baseline_windows") >= MIN_BASELINE_HOURS * 3600 // step)
    )

    mean = F.col("b_sum") / F.col("baseline_windows")
    variance = F.greatest(F.col("b_sumsq") / F.col("baseline_windows") - mean * mean, F.lit(0.0))
    min_articles = F.create_map(*[F.lit(x) for pair in MIN_ARTICLES.items() for x in pair])[F.col("dimension")]

    return (
        with_baseline.withColumn("baseline_mean", mean)
        .withColumn("baseline_std", F.sqrt(variance))
        .withColumn(
            "zscore",
            (n - F.col("baseline_mean"))
            / F.greatest(F.col("baseline_std"), F.sqrt(F.col("baseline_mean")), F.lit(1.0)),
        )
        .where(
            (F.col("n") >= min_articles)
            & (F.col("n_sources") >= MIN_SOURCES)
            & (F.col("zscore") >= Z_THRESHOLD)
        )
        .select(
            F.to_date("window_start").alias("dt"),
            "window_start",
            F.lit(WINDOW_MINUTES).alias("window_minutes"),
            "dimension", "key", "label",
            F.col("n").alias("n_articles"),
            "n_sources",
            F.round("baseline_mean", 3).alias("baseline_mean"),
            F.round("baseline_std", 3).alias("baseline_std"),
            F.round("zscore", 2).alias("zscore"),
            "baseline_windows",
            F.current_timestamp().alias("detected_at"),
        )
    )


def _epoch(day: date) -> int:
    return int(datetime.combine(day, time.min, tzinfo=timezone.utc).timestamp())


def detect(events_history: DataFrame, days: list[date]) -> DataFrame:
    """events_history phải phủ cả 24 giờ trước ngày sớm nhất trong `days`."""
    lo = _epoch(min(days))
    hi = _epoch(max(days)) + 86400 - WINDOW_MINUTES * 60
    return score(build_series(events_history), lo, hi)