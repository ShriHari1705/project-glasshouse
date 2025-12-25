CREATE DATABASE IF NOT EXISTS glasshouse;

CREATE TABLE IF NOT EXISTS glasshouse.trades_raw (
    symbol String,
    price Float64,
    quantity Float64,
    timestamp DateTime64(3, 'UTC'),
    source String
) ENGINE = MergeTree()
ORDER BY (symbol, timestamp);
