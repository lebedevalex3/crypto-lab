# Data Contract (OHLCV, UTC)

- **Timezone**: UTC
- **Schema**: `timestamp (ns)`, `open`, `high`, `low`, `close`, `volume` (float64)
- **Timeframes**: `1d`, `1h`, `5m`
- **Filename template**: `{exchange}__{symbol_noslash}__{tf}__{from}__{to}.parquet`
  - Example: `binance__BTCUSDT__1h__2023-01-01__2024-12-31.parquet`

## Integrity Rules

- `low ≤ min(open, close) ≤ high`
- No duplicate rows on `(timestamp, symbol, tf)`
- Continuous timestamp index per timeframe (no gaps), or documented exceptions in QC report
- UTC only
