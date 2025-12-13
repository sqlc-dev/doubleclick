CREATE TABLE test_table (id UInt64, dt Date) ENGINE = MergeTree() PARTITION BY toYYYYMM(dt) ORDER BY id
