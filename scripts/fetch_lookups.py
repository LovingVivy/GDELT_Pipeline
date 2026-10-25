"""Tải bảng tra cứu mã sự kiện CAMEO và mã quốc gia FIPS của GDELT vào processing/lookups/."""
from pathlib import Path

import requests

OUT_DIR = Path(__file__).resolve().parent.parent / "processing" / "lookups"
BASE = "https://www.gdeltproject.org/data/lookups"

# Một số tên trong bảng FIPS của GDELT là tên cũ hoặc dài dòng; đổi sang tên thông dụng.
COUNTRY_RENAMES = {
    "VM": "Vietnam", "KS": "South Korea", "KN": "North Korea", "RS": "Russia",
    "CG": "DR Congo", "CF": "Congo", "MK": "North Macedonia", "IV": "Ivory Coast",
    "BF": "Bahamas", "EZ": "Czechia", "BM": "Myanmar",
}


def download_rows(name: str) -> list[list[str]]:
    resp = requests.get(f"{BASE}/{name}", timeout=60)
    resp.raise_for_status()
    lines = resp.text.replace("\r", "").splitlines()
    return [line.split("\t") for line in lines if line.strip()]


def write_tsv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write("\t".join(header) + "\n")
        for row in rows:
            f.write("\t".join(value.strip() for value in row) + "\n")
    print(f"Đã ghi {path} ({len(rows)} dòng)")


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cameo = download_rows("CAMEO.eventcodes.txt")[1:]  # dòng đầu là tiêu đề
    write_tsv(OUT_DIR / "cameo_eventcodes.tsv", ["code", "description"], cameo)

    fips = download_rows("FIPS.country.txt")           # file này không có tiêu đề
    fips = [[code, COUNTRY_RENAMES.get(code, name)] for code, name in fips]
    write_tsv(OUT_DIR / "fips_country.tsv", ["code", "name"], fips)


if __name__ == "__main__":
    main()