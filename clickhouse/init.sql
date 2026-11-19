CREATE DATABASE IF NOT EXISTS default;

CREATE TABLE IF NOT EXISTS default.edit_counts (
    window_start DateTime,
    window_end DateTime,
    wiki String,
    namespace Int32,
    edit_count UInt64
) ENGINE = MergeTree()
ORDER BY (wiki, namespace, window_start);
