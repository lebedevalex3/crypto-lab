import pandas as pd
from pathlib import Path

from crypto_lab.lab_core.pipeline.pipeline_v0 import load_and_qc


# --------------------------
#  ТЕСТ 1. Позитивный сценарий (Без сохранения)
# --------------------------

def test_pipeline_v0_basic(monkeypatch):
    """Проверяем, что load_and_qc вызывает loader, normalize, QC
    и возвращает корректный словарь при save=False.
    """

    # ---- 1. Мокаем fetch_ohlcv_raw ----
    def mock_fetch_ohlcv_raw(exchange_name, symbol, timeframe, since, limit):
        return [
            [1700000000000, 100.0, 110.0, 90.0, 105.0, 10.0],
            [1700000060000, 101.0, 111.0, 91.0, 106.0, 11.0],
        ]

    monkeypatch.setattr(
        "crypto_lab.lab_core.loader.ccxt_loader_v0.fetch_ohlcv_raw",
        mock_fetch_ohlcv_raw,
    )

    # ---- 2. Мокаем normalize_to_contract ----
    def mock_normalize_to_contract(raw, exchange_name, symbol, timeframe):
        df = pd.DataFrame({
            "exchange": [exchange_name, exchange_name],
            "symbol": [symbol, symbol],
            "timeframe": [timeframe, timeframe],
            "timestamp": [1700000000000, 1700000060000],
            "open": [100.0, 101.0],
            "high": [110.0, 111.0],
            "low": [90.0, 91.0],
            "close": [105.0, 106.0],
            "volume": [10.0, 11.0],
        })

        df["timestamp"] = df["timestamp"].astype("int64")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype("float64")

        return df

    monkeypatch.setattr(
        "crypto_lab.lab_core.loader.ccxt_loader_v0.normalize_to_contract",
        mock_normalize_to_contract,
    )

    # ---- 3. Мокаем run_qc_v0 ----
    def mock_run_qc_v0(df):
        return {
            "structure_ok": True,
            "dtype_ok": True,
            "duplicates": 0,
            "missing_timestamps": [],
            "dirty_price_rows": [],
            "negative_values": [],
            "summary": "all good",
        }

    monkeypatch.setattr(
        "crypto_lab.lab_core.qc.qc_v0.run_qc_v0",
        mock_run_qc_v0,
    )

    # ---- 4. Вызываем load_and_qc ----
    result = load_and_qc(
        exchange_name="binance",
        symbol="BTC/USDT",
        timeframe="1m",
        save=False
    )

    df = result["df"]
    qc = result["qc"]
    saved_path = result["saved_path"]

    # ---- Проверки ----
    assert isinstance(df, pd.DataFrame)
    assert qc["structure_ok"] is True
    assert qc["duplicates"] == 0
    assert saved_path is None  # save=False → файл не создаём


# --------------------------
#  ТЕСТ 2. Сохранение файла (save=True)
# --------------------------

def test_pipeline_v0_save(monkeypatch, tmp_path):
    """Проверяем, что load_and_qc создаёт файл Parquet,
    когда save=True, и возвращает путь к нему.
    """

    # ---- 1. Мокаем fetch_ohlcv_raw ----
    def mock_fetch_ohlcv_raw(exchange_name, symbol, timeframe, since, limit):
        return [
            [1700000000000, 100.0, 110.0, 90.0, 105.0, 10.0],
        ]

    monkeypatch.setattr(
        "crypto_lab.lab_core.loader.ccxt_loader_v0.fetch_ohlcv_raw",
        mock_fetch_ohlcv_raw,
    )

    # ---- 2. Мокаем normalize_to_contract ----
    def mock_normalize_to_contract(raw, exchange_name, symbol, timeframe):
        df = pd.DataFrame({
            "exchange": [exchange_name],
            "symbol": [symbol],
            "timeframe": [timeframe],
            "timestamp": [1700000000000],
            "open": [100.0],
            "high": [110.0],
            "low": [90.0],
            "close": [105.0],
            "volume": [10.0],
        })
        df["timestamp"] = df["timestamp"].astype("int64")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype("float64")
        return df

    monkeypatch.setattr(
        "crypto_lab.lab_core.loader.ccxt_loader_v0.normalize_to_contract",
        mock_normalize_to_contract,
    )

    # ---- 3. Мокаем run_qc_v0 ----
    def mock_run_qc_v0(df):
        return {
            "structure_ok": True,
            "dtype_ok": True,
            "duplicates": 0,
            "missing_timestamps": [],
            "dirty_price_rows": [],
            "negative_values": [],
            "summary": "ok",
        }

    monkeypatch.setattr(
        "crypto_lab.lab_core.qc.qc_v0.run_qc_v0",
        mock_run_qc_v0,
    )

    # ---- 4. Запускаем pipeline с save=True ----
    result = load_and_qc(
        exchange_name="binance",
        symbol="BTC/USDT",
        timeframe="1m",
        save=True,
        output_dir=tmp_path
    )

    saved_path = result["saved_path"]

    # ---- Проверки ----
    assert saved_path is not None
    assert saved_path.exists()
    assert saved_path.suffix == ".parquet"

    # Структура пути:
    # tmp_path / binance / BTC-USDT / 1m / filename.parquet
    assert saved_path.parent.parent.parent.parent == tmp_path  # 1m → BTC-USDT → binance
    assert saved_path.parent.name == "1m"
    assert saved_path.parent.parent.name == "BTC-USDT"
    assert saved_path.parent.parent.parent.name == "binance"
