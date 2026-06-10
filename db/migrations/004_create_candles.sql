-- Migration 004: Candles hypertable (TimescaleDB).
-- Global time-series store for OHLCV candles across all symbols.
-- Not tenant-scoped — candle data is market-wide, not per-tenant.

BEGIN;

CREATE TABLE IF NOT EXISTS candles (
    time      TIMESTAMPTZ    NOT NULL,
    symbol    VARCHAR(20)    NOT NULL,
    open      DECIMAL(18,8)  NOT NULL,
    high      DECIMAL(18,8)  NOT NULL,
    low       DECIMAL(18,8)  NOT NULL,
    close     DECIMAL(18,8)  NOT NULL,
    volume    DECIMAL(18,8)  NOT NULL,
    timeframe VARCHAR(5)     NOT NULL DEFAULT '5m'
);

-- Convert to TimescaleDB hypertable partitioned by time.
-- Requires the timescaledb extension (included in timescale/timescaledb image).
SELECT create_hypertable('candles', 'time', if_not_exists => TRUE);

-- Compress candle chunks older than 7 days to save storage.
SELECT add_compression_policy('candles', INTERVAL '7 days');

-- Primary query: fetch latest N candles for a symbol (used by signal_pipeline).
CREATE INDEX IF NOT EXISTS idx_candles_symbol_time
    ON candles (symbol, time DESC);

COMMIT;
