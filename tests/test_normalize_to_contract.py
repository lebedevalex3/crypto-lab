import pandas as pd

from crypto_lab.lab_core.loader.ccxt_loader_v0 import normalize_to_contract


def test_normalize_to_contract_basic():
    # Подготовим искусственные сырые данные (формат ccxt):
    # [timestamp_ms, open, high, low, close, volume]
    raw = [
        [1700000000000, 100.0, 110.0, 90.0, 105.0, 123.45],
        [1700000060000, 105.0, 120.0, 100.0, 115.0, 234.56],
    ]

    df = normalize_to_contract(
        raw_candles=raw,
        exchange_name="binance",
        symbol="BTC/USDT",
        timeframe="1m",
    )

    # 1. Проверяем порядок колонок по контракту
    expected_columns = [
        "exchange",
        "symbol",
        "timeframe",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert list(df.columns) == expected_columns

    # 2. Проверяем количество строк
    assert len(df) == 2

    # 3. Типы колонок
    assert df["timestamp"].dtype == "int64"
    for col in ["open", "high", "low", "close", "volume"]:
        assert df[col].dtype == "float64"

    # 4. Константные значения
    assert (df["exchange"] == "binance").all()
    assert (df["symbol"] == "BTC/USDT").all()
    assert (df["timeframe"] == "1m").all()

    # 5. Значения timestamp взяты именно из raw
    assert df.loc[0, "timestamp"] == 1700000000000
    assert df.loc[1, "timestamp"] == 1700000060000


def test_normalize_to_contract_empty_input():
    # Пустой список OHLCV
    raw = []

    df = normalize_to_contract(
        raw_candles=raw,
        exchange_name="binance",
        symbol="BTC/USDT",
        timeframe="1m",
    )

    # Проверяем, что это DataFrame
    assert isinstance(df, pd.DataFrame)

    # Проверяем, что строк нет
    assert len(df) == 0

    # Проверяем, что колонки соответствуют data-contract
    expected_columns = [
        "exchange",
        "symbol",
        "timeframe",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert list(df.columns) == expected_columns


    def test_normalize_to_contract_type_coercion():
    # Здесь timestamp как float, а цены/volume — как int
        raw = [
            [1700000000000.0, 100, 110, 90, 105, 1],
            [1700000060000.0, 105, 120, 100, 115, 2],
        ]

        df = normalize_to_contract(
            raw_candles=raw,
            exchange_name="binance",
            symbol="BTC/USDT",
            timeframe="1m",
        )

        # timestamp → int64
        assert df["timestamp"].dtype == "int64"
        assert df.loc[0, "timestamp"] == 1700000000000

        # Ценовые и объёмные поля должны привести к float64
        for col in ["open", "high", "low", "close", "volume"]:
            assert df[col].dtype == "float64"
def test_normalize_to_contract_allows_dirty_prices():
    # Нелогичная свеча: low > high
    raw = [
        [1700000000000, 100.0, 90.0, 110.0, 105.0, 10.0],
    ]

    df = normalize_to_contract(
        raw_candles=raw,
        exchange_name="binance",
        symbol="BTC/USDT",
        timeframe="1m",
    )

    # 1. DataFrame должен вернуться без ошибок
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1

    # 2. Колонки — по контракту
    expected_columns = [
        "exchange",
        "symbol",
        "timeframe",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert list(df.columns) == expected_columns

    # 3. Значения должны быть именно такими, как поданы (loader не исправляет грязь)
    assert df.loc[0, "high"] == 90.0
    assert df.loc[0, "low"] == 110.0

    # 4. Типы всё равно должны быть корректны
    assert df["timestamp"].dtype == "int64"
    for col in ["open", "high", "low", "close", "volume"]:
        assert df[col].dtype == "float64"

def test_normalize_string_fields_are_constant_and_str():
    raw = [
        [1700000000000, 100.0, 110.0, 90.0, 105.0, 10.0],
        [1700000060000, 105.0, 120.0, 100.0, 115.0, 20.0],
    ]

    df = normalize_to_contract(
        raw_candles=raw,
        exchange_name="binance",
        symbol="BTC/USDT",
        timeframe="1m",
    )

    # 1. Типы столбцов exchange, symbol, timeframe должны быть строками
    assert df["exchange"].dtype == object
    assert df["symbol"].dtype == object
    assert df["timeframe"].dtype == object

    # Проверяем, что значения реально строки
    assert isinstance(df.loc[0, "exchange"], str)
    assert isinstance(df.loc[0, "symbol"], str)
    assert isinstance(df.loc[0, "timeframe"], str)

    # 2. Они должны быть одинаковыми для всех строк
    assert (df["exchange"] == "binance").all()
    assert (df["symbol"] == "BTC/USDT").all()
    assert (df["timeframe"] == "1m").all()