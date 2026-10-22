"""Đọc tầng silver Events và in vài chỉ số kiểm tra chất lượng."""
from pyspark.sql import functions as F

from common import build_spark, lake_path

spark = build_spark("inspect-silver")
df = spark.read.parquet(lake_path("silver/gdelt/events"))

print("=== Schema")
df.printSchema()

total = df.count()
ids = df.select("global_event_id").distinct().count()
print(f"=== Tổng {total} dòng, {ids} GlobalEventID khác nhau, trùng: {total - ids}")

print("=== Số sự kiện theo giờ ghi nhận")
df.groupBy(F.date_trunc("hour", "event_time").alias("hour")).count().orderBy("hour").show(48, truncate=False)

print("=== Tỉ lệ thiếu dữ liệu ở vài cột")
checks = ["actor1_name", "actor2_name", "action_geo_country_code", "action_geo_lat", "source_url"]
df.select([F.round(F.avg(F.col(c).isNull().cast("int")), 3).alias(c) for c in checks]).show()

print("=== Mẫu dữ liệu")
df.select("global_event_id", "event_time", "event_date", "actor1_name", "actor2_name",
          "event_root_code", "quad_class", "action_geo_full_name", "source_domain").show(10, truncate=30)

spark.stop()