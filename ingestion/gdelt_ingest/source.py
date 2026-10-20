"""Nguồn GDELT 2.0: tìm mốc mới nhất, sinh danh sách mốc 15 phút, tải và giải nén file."""
import io
import time
import zipfile
from datetime import datetime, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import config

TS_FORMAT = "%Y%m%d%H%M%S"

# Phần đuôi tên file của từng bảng trên máy chủ GDELT
SUFFIX = {"events": "export.CSV", "gkg": "gkg.csv"}


def new_session() -> requests.Session:
    """Phiên HTTP tự thử lại khi máy chủ GDELT lỗi tạm thời (5xx)."""
    session = requests.Session()
    retry = Retry(total=4, backoff_factor=1.5, status_forcelist=(500, 502, 503, 504), allowed_methods=("GET",))
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers["User-Agent"] = "gdelt-pipeline-student/1.0"
    return session


def latest_timestamp(session: requests.Session) -> datetime:
    """Đọc lastupdate.txt để biết mốc 15 phút mới nhất GDELT đã công bố."""
    resp = session.get(f"{config.GDELT_BASE_URL}/lastupdate.txt", timeout=30)
    resp.raise_for_status()
    for line in resp.text.splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2].endswith(".export.CSV.zip"):
            filename = parts[2].rsplit("/", 1)[1]  # 20261020073000.export.CSV.zip
            return datetime.strptime(filename[:14], TS_FORMAT)
    raise RuntimeError(f"Không tìm thấy file Events trong lastupdate.txt:\n{resp.text}")


def timestamps(end: datetime, hours: float, step_minutes: int = 15) -> list[datetime]:
    """Các mốc trong khoảng (end - hours, end], căn theo bội số step_minutes, xếp tăng dần."""
    minutes_of_day = end.hour * 60 + end.minute
    current = end.replace(second=0, microsecond=0) - timedelta(minutes=minutes_of_day % step_minutes)
    start = end - timedelta(hours=hours)
    result = []
    while current > start:
        result.append(current)
        current -= timedelta(minutes=step_minutes)
    return list(reversed(result))


def file_url(dataset: str, ts: datetime) -> str:
    return f"{config.GDELT_BASE_URL}/{ts.strftime(TS_FORMAT)}.{SUFFIX[dataset]}.zip"


def bronze_key(dataset: str, ts: datetime) -> str:
    """Đường dẫn trong tầng bronze, đúng quy ước ở docs/data-layout.md."""
    return f"bronze/gdelt/{dataset}/ingest_date={ts:%Y-%m-%d}/{ts.strftime(TS_FORMAT)}.{SUFFIX[dataset]}.gz"


def download(session: requests.Session, dataset: str, ts: datetime) -> bytes | None:
    """Tải và giải nén một file. Trả về None nếu GDELT chưa có file (404)."""
    for attempt in range(1, 4):
        try:
            resp = session.get(file_url(dataset, ts), timeout=180)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as archive:
                return archive.read(archive.namelist()[0])
        except (requests.RequestException, zipfile.BadZipFile):
            # Đứt mạng giữa chừng hoặc file tải về bị cụt: chờ một chút rồi thử lại.
            if attempt == 3:
                raise
            time.sleep(2 * attempt)