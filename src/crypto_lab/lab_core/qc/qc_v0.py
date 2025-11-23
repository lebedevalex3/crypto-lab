from __future__ import annotations

from typing import Dict, List

import pandas as pd


EXPECTED_COLUMNS = [
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

# Простое отображение таймфрейма в миллисекунды
TIMEFRAME_TO_MS: Dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def _check_structure(df: pd.DataFrame) -> bool:
    """Проверить, что структура DataFrame соответствует data-contract.

    Проверяем:
    - имена колонок,
    - порядок колонок,
    - количество колонок.
    """
    return list(df.columns) == EXPECTED_COLUMNS


def _check_dtypes(df: pd.DataFrame) -> bool:
    """Проверить базовые типы столбцов согласно контракту.

    Требования v0:
    - timestamp: int64
    - open/high/low/close/volume: float64
    - exchange/symbol/timeframe: object (строки)
    """
    try:
        if df["timestamp"].dtype != "int64":
            return False

        for col in ["open", "high", "low", "close", "volume"]:
            if df[col].dtype != "float64":
                return False

        for col in ["exchange", "symbol", "timeframe"]:
            if df[col].dtype != object:
                return False

        return True
    except KeyError:
        # Если каких-то колонок нет — структура уже не ок,
        # здесь просто считаем dtypes некорректными.
        return False


def _detect_duplicates(df: pd.DataFrame) -> List[int]:
    """Найти дубликаты по ключу (exchange, symbol, timeframe, timestamp).

    Возвращает список индексов строк, которые являются дубликатами.
    """
    if df.empty:
        return []

    dup_mask = df.duplicated(
        subset=["exchange", "symbol", "timeframe", "timestamp"], keep=False
    )
    return df.index[dup_mask].tolist()


def _infer_timeframe_ms(df: pd.DataFrame) -> int | None:
    """Определить размер шага таймфрейма в миллисекундах.

    В v0 предполагаем, что в DataFrame один таймфрейм.
    Если таймфрейм неизвестен словарю TIMEFRAME_TO_MS, возвращаем None.
    """
    if df.empty:
        return None

    unique_tfs = df["timeframe"].unique()
    if len(unique_tfs) != 1:
        # Для v0 не пытаемся поддерживать смешанные таймфреймы.
        return None

    tf = str(unique_tfs[0])
    return TIMEFRAME_TO_MS.get(tf)


def _detect_missing_timestamps(df: pd.DataFrame) -> List[int]:
    """Найти пропущенные timestamps в равномерной сетке.

    Логика v0:
    - сортируем по timestamp;
    - вычисляем шаг между соседними свечами;
    - если шаг > ожидаемого (по таймфрейму), считаем, что внутри есть пропуски;
    - генерируем список ожидаемых, но отсутствующих timestamp'ов.

    Если таймфрейм не удаётся определить (неизвестный или смешанный),
    возвращаем пустой список.
    """
    if df.empty:
        return []

    step_ms = _infer_timeframe_ms(df)
    if step_ms is None:
        return []

    ts = df["timestamp"].sort_values().to_list()
    missing: List[int] = []

    for prev, cur in zip(ts, ts[1:]):
        delta = cur - prev
        if delta > step_ms:
            # Генерируем все ожидаемые timestamps между prev и cur
            expected = prev + step_ms
            while expected < cur:
                missing.append(expected)
                expected += step_ms

    return missing


def _detect_dirty_prices(df: pd.DataFrame) -> List[int]:
    """Найти строки с нарушением базовой ценовой логики.

    Условия "грязной" свечи:
    - low > high
    - open > high
    - open < low
    - close > high
    - close < low

    Но:
    - строки с отрицательными значениями считаются
      *отдельной категорией ошибок* (negative_values)
      и НЕ должны попадать в dirty_price_rows.
    """
    if df.empty:
        return []

    # Исключаем строки с отрицательными значениями
    neg_mask = (
        (df["open"] < 0)
        | (df["high"] < 0)
        | (df["low"] < 0)
        | (df["close"] < 0)
        | (df["volume"] < 0)
    )

    # Маска грязных цен
    cond_low_gt_high = df["low"] > df["high"]
    cond_open_gt_high = df["open"] > df["high"]
    cond_open_lt_low = df["open"] < df["low"]
    cond_close_gt_high = df["close"] > df["high"]
    cond_close_lt_low = df["close"] < df["low"]

    dirty_mask = (
        cond_low_gt_high
        | cond_open_gt_high
        | cond_open_lt_low
        | cond_close_gt_high
        | cond_close_lt_low
    )

    # Исключаем строки, которые уже отмечены как negative_values
    final_mask = dirty_mask & (~neg_mask)

    return df.index[final_mask].tolist()


def _detect_negative_values(df: pd.DataFrame) -> List[int]:
    """Найти строки с отрицательными ценами или объёмом.

    Проверяем:
    - open, high, low, close, volume < 0
    """
    if df.empty:
        return []

    neg_mask = (
        (df["open"] < 0)
        | (df["high"] < 0)
        | (df["low"] < 0)
        | (df["close"] < 0)
        | (df["volume"] < 0)
    )

    return df.index[neg_mask].tolist()


def run_qc_v0(df: pd.DataFrame) -> Dict[str, object]:
    """Выполнить базовую QC-проверку данных OHLCV (версия v0).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame, полученный от normalize_to_contract и
        соответствующий data-contract OHLCV (или претендующий на это).

    Returns
    -------
    dict
        Словарь с ключами:
        - structure_ok: bool
        - dtype_ok: bool
        - duplicates: int
        - missing_timestamps: list[int]
        - dirty_price_rows: list[int]
        - negative_values: list[int]
        - summary: str

    Notes
    -----
    - v0 ТОЛЬКО обнаруживает проблемы, ничего не исправляет.
    - Все индексы возвращаются в терминах исходного df.index.
    """
    structure_ok = _check_structure(df)
    dtype_ok = _check_dtypes(df)

    duplicate_indices = _detect_duplicates(df)
    missing_timestamps = _detect_missing_timestamps(df)
    dirty_price_rows = _detect_dirty_prices(df)
    negative_values = _detect_negative_values(df)

    result: Dict[str, object] = {
        "structure_ok": structure_ok,
        "dtype_ok": dtype_ok,
        "duplicates": len(duplicate_indices),
        "missing_timestamps": missing_timestamps,
        "dirty_price_rows": dirty_price_rows,
        "negative_values": negative_values,
        "summary": "",
    }

    summary = (
        f"structure_ok={structure_ok}, "
        f"dtype_ok={dtype_ok}, "
        f"duplicates={len(duplicate_indices)}, "
        f"missing_timestamps={len(missing_timestamps)}, "
        f"dirty_price_rows={len(dirty_price_rows)}, "
        f"negative_values={len(negative_values)}"
    )
    result["summary"] = summary

    return result


__all__ = ["run_qc_v0"]
