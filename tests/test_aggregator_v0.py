from pathlib import Path

import pandas as pd
import pytest

from crypto_lab.lab_core.aggregator.aggregator_v0 import aggregate_ohlcv_v0


# ---------------------------------------------------------
# ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ: создание структуры каталогов и parquet
# ---------------------------------------------------------


def _write_parquet(tmp_root: Path, exchange: str, symbol: str, timeframe: str, df: pd.DataFrame, filename: str) -> Path:
    symbol_safe = symbol.replace("/", "-")
    base_dir = tmp_root / exchange / symbol_safe / timeframe
    base_dir.mkdir(parents=True, exist_ok=True)
    path = base_dir / filename
    df.to_parquet(path)
    return path


# ---------------------------------------------------------
# ТЕСТ 1. Базовая агрегация: concat + sort
# ---------------------------------------------------------


def test_aggregate_basic_concat_and_sort(tmp_path: Path):
    exchange = "binance"
    symbol = "BTC/USDT"
    timeframe = "1m"

    # Первый файл: две свечи
    df1 = pd.DataFrame(
        {
            "exchange": [exchange, exchange],
            "symbol": [symbol, symbol],
            "timeframe": [timeframe, timeframe],
            "timestamp": [1700000000000, 1700000060000],
            "open": [100.0, 101.0],
            "high": [110.0, 111.0],
            "low": [90.0, 91.0],
            "close": [105.0, 106.0],
            "volume": [10.0, 11.0],
        }
    )

    # Второй файл: ещё одна свеча, позднее по времени
    df2 = pd.DataFrame(
        {
            "exchange": [exchange],
            "symbol": [symbol],
            "timeframe": [timeframe],
            "timestamp": [1700000120000],
            "open": [102.0],
            "high": [112.0],
            "low": [92.0],
            "close": [107.0],
            "volume": [12.0],
        }
    )

    _write_parquet(tmp_path, exchange, symbol, timeframe, df1, "part1.parquet")
    _write_parquet(tmp_path, exchange, symbol, timeframe, df2, "part2.parquet")

    result = aggregate_ohlcv_v0(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        root_dir=tmp_path,
        drop_dirty=False,
        recheck_qc=False,
    )

    df = result["df"]
    files_read = result["files_read"]

    # Прочитано 2 файла
    assert len(files_read) == 2

    # Всего 3 свечи
    assert len(df) == 3

    # Отсортировано по timestamp
    assert list(df["timestamp"]) == [1700000000000, 1700000060000, 1700000120000]

    # QC в этом режиме не запускались
    assert result["qc_before_clean"] is None
    assert result["qc_after_clean"] is None


# ---------------------------------------------------------
# ТЕСТ 2. Удаление дубликатов по ключу (exchange, symbol, timeframe, timestamp)
# ---------------------------------------------------------


def test_aggregate_removes_duplicates(tmp_path: Path):
    exchange = "binance"
    symbol = "BTC/USDT"
    timeframe = "1m"

    # Первый файл: две свечи
    df1 = pd.DataFrame(
        {
            "exchange": [exchange, exchange],
            "symbol": [symbol, symbol],
            "timeframe": [timeframe, timeframe],
            "timestamp": [1700000000000, 1700000060000],
            "open": [100.0, 101.0],
            "high": [110.0, 111.0],
            "low": [90.0, 91.0],
            "close": [105.0, 106.0],
            "volume": [10.0, 11.0],
        }
    )

    # Второй файл: дубль второй свечи по ключу (exchange, symbol, timeframe, timestamp)
    df2 = pd.DataFrame(
        {
            "exchange": [exchange],
            "symbol": [symbol],
            "timeframe": [timeframe],
            "timestamp": [1700000060000],  # дубликат timestamp
            "open": [999.0],  # значения могли отличаться, но ключ тот же
            "high": [999.0],
            "low": [999.0],
            "close": [999.0],
            "volume": [999.0],
        }
    )

    _write_parquet(tmp_path, exchange, symbol, timeframe, df1, "part1.parquet")
    _write_parquet(tmp_path, exchange, symbol, timeframe, df2, "part2.parquet")

    result = aggregate_ohlcv_v0(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        root_dir=tmp_path,
        drop_dirty=False,
        recheck_qc=False,
    )

    df = result["df"]

    # Должны остаться уникальные timestamp'ы
    assert list(df["timestamp"]) == [1700000000000, 1700000060000]
    # Итого 2 строки
    assert len(df) == 2


# ---------------------------------------------------------
# ТЕСТ 3. drop_dirty=True: строки из dirty_price_rows/negative_values удаляются
# ---------------------------------------------------------


def test_aggregate_drop_dirty_true(tmp_path: Path, monkeypatch):
    exchange = "binance"
    symbol = "BTC/USDT"
    timeframe = "1m"

    # Один файл с тремя свечами
    df = pd.DataFrame(
        {
            "exchange": [exchange, exchange, exchange],
            "symbol": [symbol, symbol, symbol],
            "timeframe": [timeframe, timeframe, timeframe],
            "timestamp": [1700000000000, 1700000060000, 1700000120000],
            "open": [100.0, 101.0, 102.0],
            "high": [110.0, 111.0, 112.0],
            "low": [90.0, 91.0, 92.0],
            "close": [105.0, 106.0, 107.0],
            "volume": [10.0, 11.0, 12.0],
        }
    )

    _write_parquet(tmp_path, exchange, symbol, timeframe, df, "part.parquet")

    # Мокаем run_qc_v0 так, чтобы он говорил:
    # - строка с индексом 1 "грязная"
    # - отрицательных значений нет
    mock_qc = {
        "structure_ok": True,
        "dtype_ok": True,
        "duplicates": 0,
        "missing_timestamps": [],
        "dirty_price_rows": [1],
        "negative_values": [],
        "summary": "one dirty row",
    }

    def mock_run_qc_v0(df_arg: pd.DataFrame):
        return mock_qc

    monkeypatch.setattr(
        "crypto_lab.lab_core.aggregator.aggregator_v0.run_qc_v0",
        mock_run_qc_v0,
    )

    result = aggregate_ohlcv_v0(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        root_dir=tmp_path,
        drop_dirty=True,
        recheck_qc=False,
    )

    df_result = result["df"]
    qc_before = result["qc_before_clean"]
    qc_after = result["qc_after_clean"]

    # Строка с индексом 1 должна быть удалена → остаётся 2 строки
    assert len(df_result) == 2
    # Остались timestamp нулевой и третий
    assert list(df_result["timestamp"]) == [1700000000000, 1700000120000]

    # qc_before_clean должен содержать наш mock-отчёт
    assert qc_before == mock_qc

    # qc_after_clean в этом режиме не рассчитывается
    assert qc_after is None


# ---------------------------------------------------------
# ТЕСТ 4. recheck_qc=True: QC вызывается повторно
# ---------------------------------------------------------


def test_aggregate_recheck_qc(tmp_path: Path, monkeypatch):
    exchange = "binance"
    symbol = "BTC/USDT"
    timeframe = "1m"

    df = pd.DataFrame(
        {
            "exchange": [exchange, exchange],
            "symbol": [symbol, symbol],
            "timeframe": [timeframe, timeframe],
            "timestamp": [1700000000000, 1700000060000],
            "open": [100.0, 101.0],
            "high": [110.0, 111.0],
            "low": [90.0, 91.0],
            "close": [105.0, 106.0],
            "volume": [10.0, 11.0],
        }
    )

    _write_parquet(tmp_path, exchange, symbol, timeframe, df, "part.parquet")

    calls = {"count": 0}

    def mock_run_qc_v0(df_arg: pd.DataFrame):
        calls["count"] += 1
        return {
            "structure_ok": True,
            "dtype_ok": True,
            "duplicates": 0,
            "missing_timestamps": [],
            "dirty_price_rows": [],
            "negative_values": [],
            "summary": f"call {calls['count']}",
        }

    monkeypatch.setattr(
        "crypto_lab.lab_core.aggregator.aggregator_v0.run_qc_v0",
        mock_run_qc_v0,
    )

    result = aggregate_ohlcv_v0(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
        root_dir=tmp_path,
        drop_dirty=False,
        recheck_qc=True,
    )

    # run_qc_v0 должен быть вызван дважды:
    # - первый раз: qc_before_clean
    # - второй раз: qc_after_clean
    assert calls["count"] == 2

    assert result["qc_before_clean"] is not None
    assert result["qc_after_clean"] is not None

    # DataFrame не должен измениться, т.к. drop_dirty=False
    df_result = result["df"]
    assert len(df_result) == 2
    assert list(df_result["timestamp"]) == [1700000000000, 1700000060000]


# ---------------------------------------------------------
# ТЕСТ 5. Нет файлов → FileNotFoundError
# ---------------------------------------------------------


def test_aggregate_no_files(tmp_path: Path):
    exchange = "binance"
    symbol = "BTC/USDT"
    timeframe = "1m"

    # Создаём пустую структуру каталогов без файлов
    symbol_safe = symbol.replace("/", "-")
    base_dir = tmp_path / exchange / symbol_safe / timeframe
    base_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(FileNotFoundError):
        aggregate_ohlcv_v0(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            root_dir=tmp_path,
            drop_dirty=False,
            recheck_qc=False,
        )
