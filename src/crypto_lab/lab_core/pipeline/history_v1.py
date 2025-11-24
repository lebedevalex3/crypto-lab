from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, Any, Union

import pandas as pd

from crypto_lab.lab_core.pipeline.pipeline_v0 import load_and_qc


@dataclass
class HistoryDownloadStats:
    """Краткая статистика по загрузке истории."""
    batches: int
    rows_total: int
    start_ms_effective: int
    end_ms_effective: int


def _find_last_timestamp_in_pipeline_dir(
    exchange: str,
    symbol: str,
    timeframe: str,
    root_dir: Union[str, Path] = "data/pipeline",
) -> Optional[int]:
    """Найти максимальный timestamp среди уже сохранённых parquet-файлов.

    Это нужно для режима resume: продолжать загрузку с последнего места.
    """
    root_dir = Path(root_dir)
    symbol_dir = root_dir / exchange / symbol.replace("/", "-") / timeframe

    if not symbol_dir.exists():
        return None

    files = sorted(symbol_dir.glob("*.parquet"))
    if not files:
        return None

    # Берём последний файл и вытаскиваем max(timestamp) из него
    last_file = files[-1]
    try:
        df_last = pd.read_parquet(last_file, columns=["timestamp"])
    except Exception:
        return None

    if df_last.empty:
        return None

    return int(df_last["timestamp"].max())


def download_ohlcv_history_v1(
    exchange: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms: Optional[int] = None,
    limit: int = 1500,
    root_dir: Union[str, Path] = "data/pipeline",
    resume: bool = True,
    max_batches: Optional[int] = None,
) -> HistoryDownloadStats:
    """Загрузить историю OHLCV кусками через существующий load_and_qc.

    Параметры
    ---------
    exchange : str
        Имя биржи, например "binance".
    symbol : str
        Торговая пара, например "BTC/USDT".
    timeframe : str
        Таймфрейм ccxt, например "1m", "5m", "1h".
    start_ms : int
        Стартовый timestamp в миллисекундах (UNIX ms).
    end_ms : int | None
        Конечный timestamp (не включительно). Если None — берём max из данных,
        которые вернёт биржа (по факту).
    limit : int
        Размер чанка для одного вызова load_and_qc (передаётся как limit).
    root_dir : str | Path
        Корень для сохранения данных pipeline (тот же, что у load_and_qc).
    resume : bool
        Если True — пытаемся продолжить загрузку с последнего timestamp
        уже сохранённых файлов, если он позже start_ms.
    max_batches : int | None
        Защита от случайного бесконечного цикла: если не None и число чанков
        превысило max_batches — остановиться.

    Возвращает
    ----------
    HistoryDownloadStats
        Статистика по загрузке (кол-во чанков, строк и фактический диапазон).
    """
    root_dir = Path(root_dir)

    # Если end_ms не задан, оставляем None — будем ограничены только наличием данных с биржи.
    # На уровне архитектуры это окей: downloader просто идёт "вперёд", пока биржа отдаёт данные.

    # Режим resume: если есть уже сохранённые файлы — можно продолжить с последнего timestamp.
    effective_start_ms = int(start_ms)
    if resume:
        last_ts = _find_last_timestamp_in_pipeline_dir(exchange, symbol, timeframe, root_dir)
        if last_ts is not None and last_ts > effective_start_ms:
            effective_start_ms = last_ts + 1  # +1 ms, чтобы не брать тот же бар ещё раз

    current_since = effective_start_ms
    batches = 0
    rows_total = 0
    last_ts_overall: Optional[int] = None

    print(
        f"[history_v1] Start download: {exchange} {symbol} {timeframe}, "
        f"since={current_since}, end_ms={end_ms}, limit={limit}, resume={resume}"
    )

    while True:
        if end_ms is not None and current_since >= end_ms:
            print("[history_v1] Reached end_ms, stop.")
            break

        if max_batches is not None and batches >= max_batches:
            print("[history_v1] Reached max_batches, stop.")
            break

        print(f"[history_v1] Batch {batches + 1}: since={current_since}")

        # Ключевой момент: здесь мы используем уже существующий pipeline v0
        # как "чанк-лоадер" с QC и сохранением.
        result: Dict[str, Any] = load_and_qc(
            exchange_name=exchange,
            symbol=symbol,
            timeframe=timeframe,
            limit=limit,
            since=current_since,
            save=True,
            output_dir=root_dir,
        )

        df_chunk: pd.DataFrame = result["df"]

        if df_chunk.empty:
            print("[history_v1] Empty chunk received, stop.")
            break

        rows_chunk = int(len(df_chunk))
        rows_total += rows_chunk
        batches += 1

        last_ts_chunk = int(df_chunk["timestamp"].max())
        last_ts_overall = last_ts_chunk if last_ts_overall is None else max(last_ts_overall, last_ts_chunk)

        print(
            f"[history_v1] Batch {batches} done: rows={rows_chunk}, "
            f"last_ts_chunk={last_ts_chunk}"
        )

        # Если end_ms задан и мы уже за него вышли — выходим
        if end_ms is not None and last_ts_chunk >= end_ms:
            print("[history_v1] Last chunk reached end_ms, stop.")
            break

        # Защита от зависания: если last_ts не увеличился — выходим
        if last_ts_chunk <= current_since:
            print(
                "[history_v1] last_ts_chunk <= current_since, "
                "possible API issue. Stop to avoid infinite loop."
            )
            break

        # Сдвигаем окно вперёд. +1 ms, чтобы не захватывать ту же свечу.
        current_since = last_ts_chunk + 1

    if last_ts_overall is None:
        # Ничего не загрузили
        last_ts_overall = effective_start_ms

    stats = HistoryDownloadStats(
        batches=batches,
        rows_total=rows_total,
        start_ms_effective=effective_start_ms,
        end_ms_effective=last_ts_overall,
    )

    print(
        f"[history_v1] Completed: batches={stats.batches}, "
        f"rows_total={stats.rows_total}, "
        f"range={stats.start_ms_effective} → {stats.end_ms_effective}"
    )

    return stats
