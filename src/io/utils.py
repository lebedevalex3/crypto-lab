# src/io/utils.py
from __future__ import annotations

import datetime as dt
import random
import time
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import pandas as pd

TF = Literal["5m", "1h", "1d"]

TF_SECONDS: dict[TF, int] = {"5m": 300, "1h": 3600, "1d": 86400}
TF_PANDAS: dict[TF, str] = {"5m": "5T", "1h": "1H", "1d": "1D"}


def tf_to_seconds(tf: TF) -> int:
    """Возвращает количество секунд в одном баре для данного таймфрейма."""
    if tf not in TF_SECONDS:
        raise ValueError(f"Unsupported tf: {tf}")
    return TF_SECONDS[tf]


def floor_utc_to_closed_bar(now_utc: dt.datetime, tf: TF) -> dt.datetime:
    """Возвращает конец последнего ЗАКРЫТОГО бара в UTC (включительно)."""
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=dt.UTC)
    seconds: int = tf_to_seconds(tf)
    ts: int = int(now_utc.timestamp())
    closed: int = (ts // seconds) * seconds
    closed -= seconds  # смещаемся на предыдущий закрытый бар
    return dt.datetime.fromtimestamp(closed, tz=dt.UTC)


def trim_partial_tail(df: pd.DataFrame, tf: TF, now_utc: dt.datetime | None = None) -> pd.DataFrame:
    """Удаляет последнюю незакрытую свечу из DataFrame согласно таймфрейму."""
    if df.empty:
        return df
    if now_utc is None:
        now_utc = dt.datetime.now(dt.UTC)
    last_closed: dt.datetime = floor_utc_to_closed_bar(now_utc, tf)
    return df.loc[:last_closed]


def sleep_with_jitter(base_seconds: float, jitter_ratio: float = 0.2) -> None:
    """Пауза с небольшим случайным разбросом, чтобы избежать одновременных запросов."""
    jitter: float = base_seconds * jitter_ratio
    time.sleep(base_seconds + random.uniform(-jitter, jitter))


def exp_backoff(attempt: int, base: float = 1.0, cap: float = 30.0) -> float:
    """Возвращает время задержки (секунды) для экспоненциального бэкоффа."""
    return min(cap, base * (2 ** max(0, attempt - 1)))
