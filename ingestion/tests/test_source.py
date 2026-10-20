from datetime import datetime

from gdelt_ingest import source


def test_timestamps_15_minutes():
    result = source.timestamps(datetime(2026, 10, 20, 8, 15), hours=1)
    assert result == [
        datetime(2026, 10, 20, 7, 30),
        datetime(2026, 10, 20, 7, 45),
        datetime(2026, 10, 20, 8, 0),
        datetime(2026, 10, 20, 8, 15),
    ]


def test_timestamps_hourly_aligns_down():
    result = source.timestamps(datetime(2026, 10, 20, 8, 15), hours=3, step_minutes=60)
    assert result == [datetime(2026, 10, 20, 6), datetime(2026, 10, 20, 7), datetime(2026, 10, 20, 8)]


def test_bronze_key_follows_layout():
    key = source.bronze_key("events", datetime(2026, 10, 20, 8, 15))
    assert key == "bronze/gdelt/events/ingest_date=2026-10-20/20261020081500.export.CSV.gz"