CREATE TABLE test_table (id UInt64) ENGINE = MergeTree() ORDER BY id SETTINGS index_granularity = 8192
