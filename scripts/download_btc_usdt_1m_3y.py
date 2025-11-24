from datetime import datetime, timezone

from crypto_lab.lab_core.pipeline.history_v1 import download_ohlcv_history_v1


def to_ms(dt: datetime) -> int:
    """Перевод datetime в UNIX ms."""
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


if __name__ == "__main__":
    # Пример: с 1 января 2022 до текущего момента
    start_dt = datetime(2022, 1, 1)
    end_dt = datetime.now(timezone.utc)

    start_ms = to_ms(start_dt)
    end_ms = to_ms(end_dt)

    stats = download_ohlcv_history_v1(
        exchange="binance",
        symbol="BTC/USDT",
        timeframe="1m",
        start_ms=start_ms,
        end_ms=end_ms,
        limit=1500,
        root_dir="data/pipeline",
        resume=True,
    )

    print("Download stats:", stats)
