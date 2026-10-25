-- Bản sao tầng gold để phục vụ truy vấn. Nguồn chính thức là Parquet trên S3 (gold/).
-- Dùng DateTime thường (giờ UTC của server); Spark JDBC không nhận kiểu DateTime('UTC').

CREATE DATABASE IF NOT EXISTS gdelt;

-- Từng sự kiện, đã ghép tên quốc gia và tên loại sự kiện.
CREATE TABLE IF NOT EXISTS gdelt.fct_events
(
    global_event_id     UInt64,
    event_time          DateTime,
    event_date          Nullable(Date),
    dt                  Date,
    actor1_name         String,
    actor2_name         String,
    actor1_country_code LowCardinality(String),
    actor2_country_code LowCardinality(String),
    event_code          LowCardinality(String),
    event_root_code     LowCardinality(String),
    event_root_name     LowCardinality(String),
    quad_class          UInt8,
    goldstein_scale     Float32,
    num_mentions        UInt32,
    num_sources         UInt32,
    num_articles        UInt32,
    avg_tone            Float32,
    country_code        LowCardinality(String),
    country_name        LowCardinality(String),
    geo_full_name       String,
    lat                 Nullable(Float64),
    lon                 Nullable(Float64),
    source_url          String,
    source_domain       String,
    updated_at          DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY dt
ORDER BY (event_time, global_event_id);

-- Tổng hợp theo cửa sổ 15 phút x quốc gia x nhóm sự kiện.
CREATE TABLE IF NOT EXISTS gdelt.agg_events_15m
(
    window_start    DateTime,
    dt              Date,
    country_code    LowCardinality(String),
    country_name    LowCardinality(String),
    event_root_code LowCardinality(String),
    event_root_name LowCardinality(String),
    quad_class      UInt8,
    n_events        UInt64,
    n_articles      UInt64,     -- số bài khác nhau trong nhóm; KHÔNG cộng được giữa các nhóm
    n_mentions      UInt64,
    sum_tone        Float64,    -- lưu tổng để chia ra trung bình đúng ở mọi mức gộp
    sum_goldstein   Float64,
    updated_at      DateTime
)
ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY dt
ORDER BY (window_start, country_code, event_root_code, quad_class);