import pandas as pd
from pathlib import Path

from crypto_lab.lab_core.qc.dashboard_v0 import (
    render_qc_markdown,
    save_qc_markdown,
    pipeline_qc_dashboard,
)


# =============================================================================
# ТЕСТ 1. render_qc_markdown: базовый отчёт
# =============================================================================

def test_render_qc_markdown_basic():
    df = pd.DataFrame({
        "exchange": ["binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m"],
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

    qc = {
        "structure_ok": True,
        "dtype_ok": True,
        "duplicates": 0,
        "missing_timestamps": [],
        "dirty_price_rows": [],
        "negative_values": [],
        "summary": "all good",
    }

    md = render_qc_markdown(qc, df)

    # Основные разделы
    for section in [
        "QC Report",
        "Summary",
        "Missing Timestamps",
        "Dirty Price Rows",
        "Negative Values",
        "Dataset Overview",
    ]:
        assert section in md

    # Проверяем некоторые ключи
    assert "structure_ok" in md
    assert "duplicates" in md
    assert "0" in md  # значение в summary

    # Должны быть данные датасета
    assert "1700000000000" in md
    assert "1700000060000" in md


# =============================================================================
# ТЕСТ 2. Missing Timestamps
# =============================================================================

def test_render_qc_markdown_missing_timestamps():
    df = pd.DataFrame({
        "exchange": ["binance"],
        "symbol": ["BTC/USDT"],
        "timeframe": ["1m"],
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

    qc = {
        "structure_ok": True,
        "dtype_ok": True,
        "duplicates": 0,
        "missing_timestamps": [1700000120000, 1700000180000],
        "dirty_price_rows": [],
        "negative_values": [],
        "summary": "missing found",
    }

    md = render_qc_markdown(qc, df)

    assert "Missing Timestamps" in md
    assert "1700000120000" in md
    assert "1700000180000" in md


# =============================================================================
# ТЕСТ 3. Dirty Price Rows + превью
# =============================================================================

def test_render_qc_markdown_dirty_rows():
    df = pd.DataFrame({
        "exchange": ["binance", "binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m", "1m"],
        "timestamp": [1700000000000, 1700000060000, 1700000120000],
        "open": [100.0, 105.0, 102.0],
        "high": [110.0, 100.0, 60.0],
        "low": [90.0, 120.0, 55.0],
        "close": [105.0, 102.0, 65.0],
        "volume": [10.0, 9.0, 12.0],
    })

    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    qc = {
        "structure_ok": True,
        "dtype_ok": True,
        "duplicates": 0,
        "missing_timestamps": [],
        "dirty_price_rows": [1, 2],
        "negative_values": [],
        "summary": "dirty rows",
    }

    md = render_qc_markdown(qc, df)

    # Индексы проблемных строк должны быть в отчёте
    assert "1" in md
    assert "2" in md

    # Данные вокруг индексов тоже должны присутствовать
    assert "1700000060000" in md
    assert "1700000120000" in md

    # Колонки должны упоминаться
    for col in ["open", "high", "low", "close", "volume"]:
        assert col in md


# =============================================================================
# ТЕСТ 4. Negative values
# =============================================================================

def test_render_qc_markdown_negative_values():
    df = pd.DataFrame({
        "exchange": ["binance", "binance"],
        "symbol": ["BTC/USDT", "BTC/USDT"],
        "timeframe": ["1m", "1m"],
        "timestamp": [1700000000000, 1700000060000],
        "open":  [100.0, -5.0],  # negative open
        "high":  [110.0, 111.0],
        "low":   [90.0, 91.0],
        "close": [105.0, 106.0],
        "volume":[10.0, 11.0],
    })

    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    qc = {
        "structure_ok": True,
        "dtype_ok": True,
        "duplicates": 0,
        "missing_timestamps": [],
        "dirty_price_rows": [],
        "negative_values": [1],
        "summary": "neg val",
    }

    md = render_qc_markdown(qc, df)

    # Индекс должен быть
    assert "1" in md

    # Данные строки должны быть в превью
    assert "1700000060000" in md
    assert any(x in md for x in ["-5", "-5.0"])  # отрицательный open


# =============================================================================
# ТЕСТ 5. save_qc_markdown
# =============================================================================

def test_save_qc_markdown(tmp_path):
    text = "# Test Report\nHello"

    out_path = tmp_path / "report.md"
    saved = save_qc_markdown(text, out_path)

    assert saved.exists()
    assert saved.read_text(encoding="utf-8") == text


# =============================================================================
# ТЕСТ 6. pipeline_qc_dashboard: сохранение файла
# =============================================================================

def test_pipeline_qc_dashboard(tmp_path):
    df = pd.DataFrame({
        "exchange": ["binance"],
        "symbol": ["BTC/USDT"],
        "timeframe": ["1m"],
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

    qc = {
        "structure_ok": True,
        "dtype_ok": True,
        "duplicates": 0,
        "missing_timestamps": [],
        "dirty_price_rows": [],
        "negative_values": [],
        "summary": "ok",
    }

    result_path = pipeline_qc_dashboard(
        df=df,
        qc=qc,
        symbol="BTC/USDT",
        timeframe="1m",
        output_dir=tmp_path,
    )

    # Файл должен существовать
    assert result_path.exists()
    assert result_path.suffix == ".md"

    # Структура директорий:
    # tmp_path/BTC-USDT/1m/qc_report_xxx.md
    assert result_path.parent.name == "1m"
    assert result_path.parent.parent.name == "BTC-USDT"
    assert result_path.parent.parent.parent == tmp_path
