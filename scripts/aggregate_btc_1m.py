from pathlib import Path
from crypto_lab.lab_core.aggregator.aggregator_v0 import aggregate_ohlcv_v0

if __name__ == "__main__":
    result = aggregate_ohlcv_v0(
        exchange="binance",
        symbol="BTC/USDT",
        timeframe="1m",
        root_dir="data/pipeline",
        drop_dirty=True,
        recheck_qc=True
    )

    df = result["df"]

    print("Rows:", len(df))
    print(df.head())
    print(df.tail())

    # === Создаём директорию для итогового Parquet ===
    agg_dir = Path("data/agg/binance/BTC-USDT")
    agg_dir.mkdir(parents=True, exist_ok=True)

    output_path = agg_dir / "BTC-USDT_1m_2022-2025.parquet"

    # === Сохраняем единый Parquet ===
    df.to_parquet(output_path)
    print("Saved:", output_path)

