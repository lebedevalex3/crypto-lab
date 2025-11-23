from crypto_lab.lab_core.loader.ccxt_loader_v0 import download_ohlcv_to_parquet
from datetime import datetime, timezone

path = download_ohlcv_to_parquet(
    exchange_name="binance",
    symbol="BTC/USDT",
    timeframe="1m",
    since=datetime(2024, 1, 1, tzinfo=timezone.utc),
    limit=100
)

print("Saved to:", path)