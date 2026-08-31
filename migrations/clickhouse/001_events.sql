CREATE DATABASE IF NOT EXISTS streamforge;

CREATE TABLE IF NOT EXISTS streamforge.events (
    event_id UUID,
    event_type LowCardinality(String),
    campaign_id UUID,
    user_id String,
    occurred_at DateTime64(3, 'UTC'),
    metadata String,
    processed_at DateTime64(3, 'UTC'),
    processing_attempt UInt8,
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (campaign_id, occurred_at, event_type, event_id);
