"""Kiểm tra sức khoẻ toàn bộ pipeline, từ S3 tới dashboard. Chạy từ thư mục gốc dự án:

    python scripts/check_pipeline.py

Mỗi dòng là một tầng của hệ thống. Thoát với mã 1 nếu có tầng FAIL (dùng được trong CI hoặc Airflow).
"""
import os
import sys
import time

import boto3
import requests
from dotenv import load_dotenv

load_dotenv()
# Console Windows mặc định không phải UTF-8; ép UTF-8 để in được tiếng Việt.
sys.stdout.reconfigure(encoding="utf-8")

CLICKHOUSE = "http://localhost:8123"
CH_AUTH = (os.getenv("CLICKHOUSE_USER", "gdelt"), os.getenv("CLICKHOUSE_PASSWORD", ""))
results = []


def check(name: str):
    """Decorator: chạy một bước kiểm tra, ghi PASS / WARN / FAIL kèm chi tiết."""
    def wrap(fn):
        try:
            status, detail = fn()
        except Exception as err:  # một tầng hỏng không được làm dừng các bước kiểm tra khác
            status, detail = "FAIL", f"{type(err).__name__}: {err}"
        results.append((status, name, detail))
        return fn
    return wrap


def ch(sql: str) -> str:
    resp = requests.post(CLICKHOUSE, data=sql, auth=CH_AUTH, timeout=20)
    resp.raise_for_status()
    return resp.text.strip()


@check("S3 data lake (bronze / silver / gold)")
def s3_layers():
    s3 = boto3.client("s3")
    bucket = os.environ["S3_BUCKET"]
    counts = {}
    for layer in ("bronze", "silver", "gold"):
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=f"{layer}/", MaxKeys=1000)
        counts[layer] = resp.get("KeyCount", 0)
    missing = [layer for layer, n in counts.items() if n == 0]
    detail = ", ".join(f"{layer} {'>=' if n == 1000 else ''}{n} object" for layer, n in counts.items())
    return ("FAIL" if missing else "PASS"), detail


@check("Kafka (qua Kafka UI)")
def kafka():
    clusters = requests.get("http://localhost:8082/api/clusters", timeout=10).json()
    cluster = clusters[0]
    ok = cluster["status"] == "online"
    return ("PASS" if ok else "FAIL"), f"cụm {cluster['name']} {cluster['status']}, {cluster['brokerCount']} broker, {cluster['topicCount']} topic"


@check("Spark: worker và 2 job streaming")
def spark():
    state = requests.get("http://localhost:8090/json/", timeout=10).json()
    workers = [w for w in state["workers"] if w["state"] == "ALIVE"]
    apps = {a["name"] for a in state["activeapps"] if a["state"] == "RUNNING"}
    expected = {"gdelt-events-silver", "gdelt-events-gold"}
    missing = expected - apps
    detail = f"{len(workers)} worker sống, đang chạy: {', '.join(sorted(apps)) or 'không có'}"
    return ("FAIL" if not workers or missing else "PASS"), detail + (f"; THIẾU {', '.join(missing)}" if missing else "")


@check("ClickHouse: tầng gold có dữ liệu mới")
def clickhouse_freshness():
    events, lag_minutes = ch(
        "SELECT count(), dateDiff('minute', max(event_time), now()) FROM gdelt.fct_events FINAL FORMAT TSV"
    ).split("\t")
    bursts = ch("SELECT count() FROM gdelt.bursts")
    runs = ch("SELECT count() FROM gdelt.gkg_runs")
    lag = int(lag_minutes)
    # GDELT vốn trễ 45-60 phút; thêm 15 phút lịch crawler và vài phút xử lý.
    status = "PASS" if lag <= 120 else "WARN" if lag <= 360 else "FAIL"
    return status, f"{int(events):,} sự kiện, mới nhất cách đây {lag} phút, {bursts} đột biến, {runs} lần chạy GKG"


@check("Airflow: scheduler và webserver")
def airflow():
    health = requests.get("http://localhost:8089/health", timeout=10).json()
    scheduler = health["scheduler"]["status"]
    metadb = health["metadatabase"]["status"]
    ok = scheduler == "healthy" and metadb == "healthy"
    return ("PASS" if ok else "FAIL"), f"scheduler {scheduler}, metadatabase {metadb}"


@check("API (FastAPI)")
def api():
    start = time.perf_counter()
    health = requests.get("http://localhost:8000/api/health", timeout=10).json()
    overview = requests.get("http://localhost:8000/api/overview", params={"hours": 24}, timeout=30).json()
    ms = (time.perf_counter() - start) * 1000
    ok = health["ready"] and overview["ready"]
    return ("PASS" if ok else "FAIL"), f"/api/overview: {overview.get('events', 0):,} sự kiện trong 24 giờ, {ms:.0f} ms"


@check("Dashboard (nginx -> API)")
def dashboard():
    page = requests.get("http://localhost:3000/", timeout=10)
    proxied = requests.get("http://localhost:3000/api/health", timeout=10)
    ok = page.ok and "GDELT" in page.text and proxied.ok
    return ("PASS" if ok else "FAIL"), f"trang {page.status_code}, /api qua nginx {proxied.status_code}"


def main() -> int:
    width = max(len(name) for _, name, _ in results)
    for status, name, detail in results:
        print(f"[{status:4}] {name:<{width}}  {detail}")
    failed = [name for status, name, _ in results if status == "FAIL"]
    print()
    print("Kết quả:", "tất cả các tầng hoạt động" if not failed else f"{len(failed)} tầng lỗi: {', '.join(failed)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
