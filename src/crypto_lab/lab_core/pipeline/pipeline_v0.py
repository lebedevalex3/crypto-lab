from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import pandas as pd

from crypto_lab.lab_core.loader.ccxt_loader_v0 import (
    fetch_ohlcv_raw,
    normalize_to_contract,
)
from crypto_lab.lab_core.qc.qc_v0 import run_qc_v0


def load_and_qc(
    exchange_name: str,
    symbol: str,
    timeframe: str,
    since: Optional[int | float | datetime] = None,
    limit: int = 1000,
    save: bool = False,
    output_dir: Path | str = "data/pipeline",
) -> Dict[str, Any]:
    """Сквозной конвейер (v0): загрузка → нормализация → QC → (опционально) сохранение.

    Parameters
    ----------
    exchange_name : str
        Биржа (например, "binance").
    symbol : str
        Символ в формате Crypto Lab (в v0 совпадает с ccxt: "BTC/USDT").
    timeframe : str
        Таймфрейм, например "1m", "5m", "1h".
    since : datetime | int | float | None
        Начальная точка загрузки данных. Может быть datetime или timestamp ms.
    limit : int
        Количество свечей в одном запросе.
    save : bool
        Если True — сохраняет результат в Parquet.
    output_dir : Path | str
        Корневая папка для сохранения данных.

    Returns
    -------
    dict
        {
            "df": pandas.DataFrame,
            "qc": dict,
            "saved_path": Path | None,
        }
    """
    # 1. Загрузка сырых данных
    raw = fetch_ohlcv_raw(
        exchange_name=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
        since=since,
        limit=limit,
    )

    # 2. Нормализация под контракт
    df = normalize_to_contract(
        raw_candles=raw,
        exchange_name=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
    )

    # 3. QC
    qc_report = run_qc_v0(df)

    saved_path: Optional[Path] = None

    # 4. Сохранение по желанию
    if save:
        out_dir = Path(output_dir) / exchange_name / symbol.replace("/", "-") / timeframe
        out_dir.mkdir(parents=True, exist_ok=True)

        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        file_path = out_dir / f"{timeframe}_{now_ms}.parquet"

        df.to_parquet(file_path)
        saved_path = file_path

    return {
        "df": df,
        "qc": qc_report,
        "saved_path": saved_path,
    }


__all__ = ["load_and_qc"]
