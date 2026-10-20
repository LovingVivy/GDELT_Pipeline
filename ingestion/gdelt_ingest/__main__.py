"""Dòng lệnh của crawler GDELT.

  python -m gdelt_ingest init                              tạo topic Kafka
  python -m gdelt_ingest events --hours 3                  đồng bộ Events còn thiếu -> Kafka + bronze
  python -m gdelt_ingest gkg --days 1 --step-minutes 60    đồng bộ GKG còn thiếu -> bronze
"""
import argparse
import json
import logging
import sys
from datetime import datetime

from . import config, source
from .sinks import BronzeStore, EventProducer, ensure_topic

log = logging.getLogger("gdelt_ingest")


def resolve_end(value: str | None, session) -> datetime:
    """Mốc kết thúc: do người dùng chỉ định, hoặc mốc mới nhất trong lastupdate.txt."""
    if value:
        return datetime.strptime(value.ljust(14, "0"), source.TS_FORMAT)
    return source.latest_timestamp(session)


def cmd_init(_args) -> dict:
    return {"topic_created": ensure_topic()}


def cmd_events(args) -> dict:
    session = source.new_session()
    store = BronzeStore(config.S3_BUCKET)
    producer = EventProducer()
    end = resolve_end(args.end, session)
    stats = {"dataset": "events", "end": end.strftime(source.TS_FORMAT),
             "ingested": 0, "rows": 0, "skipped": 0, "missing": []}

    for ts in source.timestamps(end, args.hours):
        key = source.bronze_key("events", ts)
        if not args.force and store.exists(key):
            stats["skipped"] += 1
            continue

        raw = source.download(session, "events", ts)
        if raw is None:
            stats["missing"].append(ts.strftime(source.TS_FORMAT))
            continue

        rows = producer.send_lines(raw)  # Kafka trước ...
        store.put_gzip(key, raw)         # ... S3 sau: file trên S3 là dấu "đã xong"
        stats["ingested"] += 1
        stats["rows"] += rows
        log.info("events %s: %d dòng -> Kafka, lưu %s", ts.strftime(source.TS_FORMAT), rows, key)

    if stats["missing"]:
        log.warning("%d file GDELT chưa có, lần chạy sau sẽ thử lại", len(stats["missing"]))
    return stats


def cmd_gkg(args) -> dict:
    session = source.new_session()
    store = BronzeStore(config.S3_BUCKET)
    end = resolve_end(args.end, session)
    stats = {"dataset": "gkg", "end": end.strftime(source.TS_FORMAT),
             "ingested": 0, "skipped": 0, "missing": [], "failed": []}

    for ts in source.timestamps(end, args.days * 24, args.step_minutes):
        key = source.bronze_key("gkg", ts)
        try:
            if not args.force and store.exists(key):
                stats["skipped"] += 1
                continue
            raw = source.download(session, "gkg", ts)
            if raw is None:
                stats["missing"].append(ts.strftime(source.TS_FORMAT))
                continue
            size = store.put_gzip(key, raw)
            stats["ingested"] += 1
            log.info("gkg %s: lưu %.1f MB", ts.strftime(source.TS_FORMAT), size / 1e6)
        except Exception as err:  # một file hỏng không được làm dừng cả đợt
            log.error("gkg %s: %s", ts.strftime(source.TS_FORMAT), err)
            stats["failed"].append(ts.strftime(source.TS_FORMAT))
    return stats


def main() -> int:
    parser = argparse.ArgumentParser(prog="gdelt_ingest", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="tạo topic Kafka").set_defaults(func=cmd_init)

    events = sub.add_parser("events", help="đồng bộ bảng Events")
    events.add_argument("--hours", type=float, default=3, help="nhìn lại bao nhiêu giờ")
    events.add_argument("--end", help="mốc kết thúc YYYYMMDD[HHMMSS], mặc định lấy từ lastupdate.txt")
    events.add_argument("--force", action="store_true", help="tải lại cả file đã có ở bronze")
    events.set_defaults(func=cmd_events)

    gkg = sub.add_parser("gkg", help="đồng bộ bảng GKG")
    gkg.add_argument("--days", type=float, default=7)
    gkg.add_argument("--step-minutes", type=int, default=60, help="15 = mọi file, 60 = mỗi giờ một file")
    gkg.add_argument("--end", help="mốc kết thúc YYYYMMDD[HHMMSS]")
    gkg.add_argument("--force", action="store_true")
    gkg.set_defaults(func=cmd_gkg)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    stats = args.func(args)
    print(json.dumps(stats, ensure_ascii=False))
    return 1 if stats.get("failed") else 0


if __name__ == "__main__":
    sys.exit(main())