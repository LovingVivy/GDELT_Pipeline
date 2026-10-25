-- Đột biến do Spark phát hiện: số bài báo của một đối tượng vọt lên so với 24 giờ trước đó.
CREATE TABLE IF NOT EXISTS gdelt.bursts
(
    dt               Date,
    window_start     DateTime,
    window_minutes   UInt16,
    dimension        LowCardinality(String),   -- country | event_type | country_event
    key              String,                   -- mã: 'VM', '14', 'VM|14'
    label            String,                   -- tên hiển thị: 'Vietnam · Protest'
    n_articles       UInt32,
    n_sources        UInt32,
    baseline_mean    Float64,
    baseline_std     Float64,
    zscore           Float64,
    baseline_windows UInt16,
    detected_at      DateTime
)
ENGINE = MergeTree
PARTITION BY dt
ORDER BY (window_start, dimension, key);