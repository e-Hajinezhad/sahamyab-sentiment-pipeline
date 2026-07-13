-- clickhouse-init.sql - Add to your existing init file
CREATE DATABASE IF NOT EXISTS analytics;

-- Main tweets table
CREATE TABLE IF NOT EXISTS analytics.tweets (
    id String,
    send_time DateTime,
    send_time_persian String,
    sender_name String,
    sender_username String,
    content String,
    type String,
    comment_count UInt32,
    has_parent UInt8,
    parent_id String,
    parent_content String,
    parent_sender_name String,
    extracted_at DateTime DEFAULT now(),
    sentiment_score Float32 DEFAULT 0.,
    sentiment_label String DEFAULT ''
) ENGINE = ReplacingMergeTree(extracted_at)
ORDER BY (send_time, id)
PARTITION BY toYYYYMM(send_time);



-- Create a materialized view for daily analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.tweets_daily_stats
ENGINE = SummingMergeTree()
ORDER BY (date, type)
POPULATE
AS
SELECT
    toDate(send_time) AS date,
    type,
    count() AS tweet_count,
    sum(comment_count) AS total_comments,
    countDistinct(sender_username) AS unique_authors
FROM analytics.tweets
GROUP BY date, type;

SELECT 'ClickHouse initialization complete!' AS message;
