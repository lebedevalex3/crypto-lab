import pandas as pd

from crypto_lab.lab_core.qc.qc_v0 import run_qc_v0


def test_qc_v0_all_ok():
    # Готовим "идеальные" данные под контракт OHLCV
    data = {
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m", "1m"],
        # равномерная сетка по 60_000 мс
        "timestamp": [1700000000000, 1700000060000, 1700000120000],
        "open": [100.0, 101.0, 102.0],
        "high": [110.0, 111.0, 112.0],
        "low": [90.0, 91.0, 92.0],
        "close": [105.0, 106.0, 107.0],
        "volume": [10.0, 11.0, 12.0],
    }
    df = pd.DataFrame(data)

    # Явно приводим типы, как в normalize_to_contract
    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    result = run_qc_v0(df)

    # 1. Структура и типы должны быть корректны
    assert result["structure_ok"] is True
    assert result["dtype_ok"] is True

    # 2. Не должно быть дубликатов, пропусков и аномалий
    assert result["duplicates"] == 0
    assert result["missing_timestamps"] == []
    assert result["dirty_price_rows"] == []
    assert result["negative_values"] == []

    # 3. summary должен содержать ключевые части
    summary = result["summary"]
    assert "structure_ok=True" in summary
    assert "dtype_ok=True" in summary
    assert "duplicates=0" in summary

def test_qc_v0_missing_timestamps():
    # Сетка должна быть 60_000 мс (1m)
    # Нарочно делаем пропуск между второй и третьей свечой
    data = {
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m", "1m"],
        "timestamp": [
            1700000000000,   # t0
            1700000060000,   # t1
            1700000180000,   # t3 (t2 пропущено)
        ],
        "open": [100.0, 101.0, 102.0],
        "high": [110.0, 111.0, 112.0],
        "low": [90.0, 91.0, 92.0],
        "close": [105.0, 106.0, 107.0],
        "volume": [10.0, 11.0, 12.0],
    }

    df = pd.DataFrame(data)

    # Приводим dtype
    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    result = run_qc_v0(df)

    # Пропущенный timestamp (t2)
    expected_missing = [1700000120000]

    assert result["structure_ok"] is True
    assert result["dtype_ok"] is True

    # QC должен обнаружить ровно один пропуск
    assert result["missing_timestamps"] == expected_missing

    # Остальные проверки должны быть ок
    assert result["duplicates"] == 0
    assert result["dirty_price_rows"] == []
    assert result["negative_values"] == []

def test_qc_v0_detect_duplicates():
    # Создаём DataFrame, в котором есть два одинаковых timestamp (дубликат)
    data = {
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m", "1m"],

        # Дубликат: первая и вторая строки имеют одинаковый timestamp
        "timestamp": [
            1700000000000,
            1700000000000,  # <-- дубликат timestamp
            1700000060000,
        ],

        "open": [100.0, 100.0, 101.0],
        "high": [110.0, 110.0, 111.0],
        "low": [90.0, 90.0, 91.0],
        "close": [105.0, 105.0, 106.0],
        "volume": [10.0, 10.0, 11.0],
    }

    df = pd.DataFrame(data)

    # Приводим типы
    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    result = run_qc_v0(df)

    # QC должен определить, что дубликаты есть
    assert result["duplicates"] == 2  # обе строки считаются дубликатами

    # Остальные проверки должны быть положительными
    assert result["structure_ok"] is True
    assert result["dtype_ok"] is True
    assert result["missing_timestamps"] == []
    assert result["dirty_price_rows"] == []
    assert result["negative_values"] == []


def test_qc_v0_dirty_price_rows():
    # Делаем 3 строки:
    # 0 — нормальная
    # 1 — low > high
    # 2 — open < low (и close > high)
    data = {
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m", "1m"],
        "timestamp": [
            1700000000000,
            1700000060000,
            1700000120000,
        ],
        "open":  [100.0,  105.0,  50.0],
        "high":  [110.0,  100.0,  60.0],   # row1: high < low, row2: high < close
        "low":   [ 90.0,  120.0,  55.0],   # row1: low > high (грязная свеча)
        "close": [105.0,  102.0,  65.0],   # row2: close > high (грязная свеча)
        "volume":[ 10.0,   9.0,  12.0],
    }

    df = pd.DataFrame(data)

    # Приведение типов
    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    result = run_qc_v0(df)

    # row 1 и row 2 должны быть отмечены как грязные
    assert sorted(result["dirty_price_rows"]) == [1, 2]

    # Остальные проверки должны быть корректны
    assert result["duplicates"] == 0
    assert result["negative_values"] == []
    assert result["missing_timestamps"] == []
    assert result["structure_ok"] is True
    assert result["dtype_ok"] is True

def test_qc_v0_negative_values():
    # Создаём 3 строки:
    # 0 — корректная
    # 1 — отрицательный open
    # 2 — отрицательный volume
    data = {
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m", "1m"],
        "timestamp": [
            1700000000000,
            1700000060000,
            1700000120000,
        ],
        "open":  [100.0, -5.0, 102.0],   # row1: open < 0
        "high":  [110.0, 111.0, 112.0],
        "low":   [ 90.0,  91.0,  92.0],
        "close": [105.0, 106.0, 107.0],
        "volume":[ 10.0,  11.0, -3.0],   # row2: volume < 0
    }

    df = pd.DataFrame(data)

    # Приведение типов
    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    # Запуск QC
    result = run_qc_v0(df)

    # Должны быть обнаружены строки 1 и 2
    assert sorted(result["negative_values"]) == [1, 2]

    # Остальные проверки ок
    assert result["dirty_price_rows"] == []  # нет логических нарушений
    assert result["duplicates"] == 0
    assert result["missing_timestamps"] == []
    assert result["structure_ok"] is True
    assert result["dtype_ok"] is True