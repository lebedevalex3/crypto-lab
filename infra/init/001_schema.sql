CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS ohlcv (
  ts         TIMESTAMPTZ NOT NULL,
  exchange   TEXT NOT NULL,
  symbol     TEXT NOT NULL,
  tf         TEXT NOT NULL,
  open       DOUBLE PRECISION NOT NULL,
  high       DOUBLE PRECISION NOT NULL,
  low        DOUBLE PRECISION NOT NULL,
  close      DOUBLE PRECISION NOT NULL,
  volume     DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (ts, exchange, symbol, tf)
);

SELECT create_hypertable('ohlcv', by_range('ts'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS ix_ohlcv_symbol_tf_ts
  ON ohlcv(symbol, tf, ts DESC);
