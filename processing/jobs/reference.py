"""Đọc bảng tra cứu (mã quốc gia FIPS, mã sự kiện CAMEO) thành DataFrame nhỏ."""
import csv
import os

from pyspark.sql import DataFrame, SparkSession

LOOKUP_DIR = os.getenv("LOOKUP_DIR", "/opt/gdelt/lookups")


def _read_tsv(name: str) -> list[dict]:
    with open(os.path.join(LOOKUP_DIR, name), encoding="utf-8") as f:
        return list(csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE))


def countries(spark: SparkSession) -> DataFrame:
    rows = [(r["code"], r["name"]) for r in _read_tsv("fips_country.tsv")]
    return spark.createDataFrame(rows, "country_code string, country_name string")


def event_roots(spark: SparkSession) -> DataFrame:
    """20 nhóm sự kiện gốc: các mã CAMEO đúng 2 chữ số."""
    rows = [
        (r["code"], r["description"].capitalize())
        for r in _read_tsv("cameo_eventcodes.tsv")
        if len(r["code"]) == 2
    ]
    return spark.createDataFrame(rows, "event_root_code string, event_root_name string")