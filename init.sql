CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS ohlcv_1m (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT        NOT NULL,
    open        DOUBLE PRECISION,
    high        DOUBLE PRECISION,
    low         DOUBLE PRECISION,
    close       DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    trade_count BIGINT,
    vwap        DOUBLE PRECISION
);

SELECT create_hypertable('ohlcv_1m', 'time');