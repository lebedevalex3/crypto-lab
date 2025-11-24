from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd

from crypto_lab.lab_core.qc.qc_v0 import run_qc_v0


def _discover_parquet_files(base_dir: Path) -> List[Path]:
    """Найти все parquet-файлы в каталоге для заданного инструмента.

    Parameters
    ----------
    base_dir : Path
        Каталог вида root_dir/exchange/symbol_safe/timeframe.

    Returns
    -------
    list[Path]
        Отсортированный список путей к .parquet файлам.

    Raises
    ------
    FileNotFoundError
        Если каталог не существует или в нём нет parquet-файлов.
    """
    if not base_dir.exists() or not base_dir.is_dir():
        raise FileNotFoundError(f"Base directory does not exist: {base_dir}")

    files = sorted(base_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under: {base_dir}")

    return files


def _load_all_parquet(files: Iterable[Path]) -> pd.DataFrame:
    """Прочитать все parquet-файлы и объединить в один DataFrame.

    Если все файлы пустые, возвращается пустой DataFrame.
    """
    dfs: List[pd.DataFrame] = []
    for f in files:
        df = pd.read_parquet(f)
        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    return combined


def _sort_by_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Отсортировать DataFrame по timestamp и сбросить индекс."""
    if df.empty or "timestamp" not in df.columns:
        return df

    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def _deduplicate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Удалить дубликаты по ключу (exchange, symbol, timeframe, timestamp)."""
    if df.empty:
        return df

    key_cols = ["exchange", "symbol", "timeframe", "timestamp"]
    missing = [c for c in key_cols if c not in df.columns]
    if missing:
        # Если структура неожиданная, не трогаем (v0).
        return df

    df = df.drop_duplicates(subset=key_cols, keep="first").reset_index(drop=True)
    return df


def _drop_dirty_rows(df: pd.DataFrame, qc_report: Dict[str, Any]) -> pd.DataFrame:
    """Удалить строки с грязными ценами и отрицательными значениями.

    Использует поля:
    - 'dirty_price_rows'
    - 'negative_values'
    из отчёта QC.
    """
    if df.empty:
        return df

    dirty = qc_report.get("dirty_price_rows", []) or []
    negative = qc_report.get("negative_values", []) or []

    if not dirty and not negative:
        return df

    bad_indices = sorted(set(dirty + negative))
    # Индексы в отчёте QC даны в терминах исходного df.index
    df_clean = df.drop(index=bad_indices, errors="ignore").reset_index(drop=True)
    return df_clean


def aggregate_ohlcv_v0(
    exchange: str,
    symbol: str,
    timeframe: str,
    root_dir: str | Path = "data/pipeline",
    drop_dirty: bool = True,
    recheck_qc: bool = False,
) -> Dict[str, Any]:
    """Агрегировать OHLCV-данные из набора Parquet-файлов (v0).

    Шаги:
    1. Найти все .parquet файлы для (exchange, symbol, timeframe) под root_dir.
    2. Прочитать и объединить данные в один DataFrame.
    3. Отсортировать по timestamp.
    4. Удалить дубликаты по (exchange, symbol, timeframe, timestamp).
    5. (Опционально) прогнать QC и удалить грязные строки (dirty + negative).
    6. (Опционально) повторно прогнать QC после очистки.

    Parameters
    ----------
    exchange : str
        Имя биржи, например "binance".
    symbol : str
        Символ в формате Crypto Lab (в v0 совпадает с ccxt, например "BTC/USDT").
    timeframe : str
        Таймфрейм, например "1m", "5m", "1h".
    root_dir : str | Path
        Корневая папка, где лежат данные pipeline_v0 (по умолчанию "data/pipeline").
    drop_dirty : bool
        Если True, строки с грязными ценами и отрицательными значениями будут удалены
        на основе отчёта QC.
    recheck_qc : bool
        Если True, после всех модификаций DataFrame будет повторно прогнан QC,
        и результат вернётся в поле 'qc_after_clean'.

    Returns
    -------
    dict
        {
            "df": pd.DataFrame,          # финальный агрегированный DataFrame
            "files_read": list[Path],    # список прочитанных файлов
            "qc_before_clean": dict | None,
            "qc_after_clean": dict | None,
        }

    Raises
    ------
    FileNotFoundError
        Если не найден каталог с данными или в нём отсутствуют parquet-файлы.
    """
    root_dir = Path(root_dir)
    symbol_safe = symbol.replace("/", "-")
    base_dir = root_dir / exchange / symbol_safe / timeframe

    # 1. Поиск файлов
    files = _discover_parquet_files(base_dir)

    # 2. Чтение и объединение
    df = _load_all_parquet(files)

    # 3. Сортировка и удаление дубликатов
    df = _sort_by_timestamp(df)
    df = _deduplicate_df(df)

    qc_before_clean: Optional[Dict[str, Any]] = None
    qc_after_clean: Optional[Dict[str, Any]] = None

    # 4–5. QC и опциональная очистка
    if drop_dirty or recheck_qc:
        qc_before_clean = run_qc_v0(df)

        if drop_dirty:
            df = _drop_dirty_rows(df, qc_before_clean)

    # 6. Повторный QC после очистки (по желанию)
    if recheck_qc:
        qc_after_clean = run_qc_v0(df)

    return {
        "df": df,
        "files_read": files,
        "qc_before_clean": qc_before_clean,
        "qc_after_clean": qc_after_clean,
    }


__all__ = [
    "aggregate_ohlcv_v0",
]
