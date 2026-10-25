-- Kết quả batch GKG hàng tuần. Mỗi lần chạy xác định bằng run_id = "<ngày bắt đầu>_<ngày kết thúc>".

CREATE TABLE IF NOT EXISTS gdelt.gkg_runs
(
    run_id         String,
    period_start   Date,
    period_end     Date,
    n_docs         UInt64,
    n_titles       UInt64,
    n_topics       UInt16,
    vocab_size     UInt32,
    log_perplexity Float64,
    created_at     DateTime
)
ENGINE = MergeTree ORDER BY run_id;

-- Các từ đặc trưng nhất của mỗi chủ đề LDA
CREATE TABLE IF NOT EXISTS gdelt.gkg_topic_terms
(
    run_id     String,
    topic_id   UInt16,
    rank       UInt16,
    term       String,
    weight     Float64,
    created_at DateTime
)
ENGINE = MergeTree ORDER BY (run_id, topic_id, rank);

-- Số bài thuộc mỗi chủ đề theo từng ngày
CREATE TABLE IF NOT EXISTS gdelt.gkg_topic_daily
(
    run_id     String,
    date       Date,
    topic_id   UInt16,
    n_docs     UInt64,
    avg_tone   Float64,
    created_at DateTime
)
ENGINE = MergeTree ORDER BY (run_id, date, topic_id);

-- Bài tiêu biểu nhất của mỗi chủ đề
CREATE TABLE IF NOT EXISTS gdelt.gkg_topic_docs
(
    run_id     String,
    topic_id   UInt16,
    rank       UInt16,
    title      String,
    url        String,
    source     String,
    prob       Float64,
    created_at DateTime
)
ENGINE = MergeTree ORDER BY (run_id, topic_id, rank);

-- Thực thể nổi bật mỗi ngày, xếp hạng bằng TF-IDF
CREATE TABLE IF NOT EXISTS gdelt.gkg_entity_daily
(
    run_id     String,
    date       Date,
    kind       LowCardinality(String),   -- keyword | theme | person | org
    name       String,
    n_docs     UInt64,
    tfidf      Float64,
    rank       UInt16,
    created_at DateTime
)
ENGINE = MergeTree ORDER BY (run_id, kind, date, rank);