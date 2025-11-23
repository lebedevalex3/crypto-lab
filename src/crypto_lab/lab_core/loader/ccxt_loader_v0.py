from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Sequence

import ccxt  # type: ignore
import pandas as pd


def _to_millis(ts: Optional[datetime | int | float]) -> Optional[int]:
    """Преобразовать datetime или числовой timestamp в миллисекунды с эпохи (UTC)."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return int(ts)
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            # На v0 считаем, что naive-datetime уже в UTC
            ts = ts.replace(tzinfo=timezone.utc)
        return int(ts.timestamp() * 1000)
    raise TypeError(f"Unsupported timestamp type: {type(ts)}")


def get_exchange_client(exchange_name: str):
    """Вернуть экземпляр клиента ccxt для указанной биржи (v0: только binance).

    Parameters
    ----------
    exchange_name:
        Имя биржи, например "binance".

    Returns
    -------
    ccxt.Exchange
        Инициализированный клиент ccxt.

    Raises
    ------
    ValueError
        Если биржа не поддерживается в реализации v0.
    """
    name = exchange_name.lower()
    if name == "binance":
        return ccxt.binance()
    raise ValueError(f"Unsupported exchange for loader_v0: {exchange_name!r}")


def fetch_ohlcv_raw(
    exchange_name: str,
    symbol: str,
    timeframe: str,
    since: Optional[datetime | int | float] = None,
    limit: int = 1000,
) -> List[List[float]]:
    """Загрузить сырые свечи OHLCV с биржи через ccxt (один запрос, v0).

    Это минимальная обёртка над ``ccxt.Exchange.fetch_ohlcv`` без:
    - ретраев,
    - учёта rate-limit,
    - расширенной обработки ошибок.

    Parameters
    ----------
    exchange_name:
        Идентификатор биржи, в v0 поддерживается только "binance".
    symbol:
        Торговый символ в формате ccxt, например "BTC/USDT".
        В v0 мы приравниваем этот формат к внутреннему формату Crypto Lab.
    timeframe:
        Таймфрейм, поддерживаемый ccxt (например "1m", "5m", "1h").
    since:
        Начало периода. Может быть:
        - ``datetime`` (naive считаем UTC, tz-aware приводим к UTC),
        - timestamp в миллисекундах (int/float),
        - ``None`` (дефолтное поведение биржи).
    limit:
        Максимальное количество свечей в одном запросе (зависит от ограничений биржи).

    Returns
    -------
    list[list]
        Список OHLCV-строк в формате ccxt:
        ``[timestamp_ms, open, high, low, close, volume]`` для каждой свечи.

    Notes
    -----
    * Это версия v0: делается один запрос и не происходит автоматического
      разбиения по времени для длинных периодов.
    * Ошибки ccxt пробрасываются вызывающему коду.
    """
    client = get_exchange_client(exchange_name)
    since_ms = _to_millis(since)
    raw = client.fetch_ohlcv(symbol=symbol, timeframe=timeframe, since=since_ms, limit=limit)
    return raw


def normalize_to_contract(
    raw_candles: Sequence[Sequence[float]],
    exchange_name: str,
    symbol: str,
    timeframe: str,
) -> pd.DataFrame:
    """Нормализовать сырые свечи ccxt до контракта OHLCV Crypto Lab.

    Parameters
    ----------
    raw_candles:
        Последовательность OHLCV-строк в формате ccxt:
        ``[timestamp_ms, open, high, low, close, volume]``.
    exchange_name:
        Имя биржи (например, "binance").
    symbol:
        Символ в едином формате Crypto Lab.
        В v0 он совпадает с форматом ccxt, например "BTC/USDT".
    timeframe:
        Таймфрейм, например "1m", "5m", "1h".

    Returns
    -------
    pandas.DataFrame
        DataFrame со столбцами:
        ``["exchange", "symbol", "timeframe", "timestamp",
           "open", "high", "low", "close", "volume"]``

        * ``timestamp`` — int64, миллисекунды (UTC);
        * ценовые и объёмные столбцы — float64;
        * строковые столбцы — object/string.

    Notes
    -----
    На этапе v0 здесь нет QC-проверок. Функция только приводит формат
    к единому data-contract.
    """
    if not raw_candles:
        # Пустой набор, возвращаем пустой DataFrame с нужной схемой
        return pd.DataFrame(
            columns=[
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
        )

    df = pd.DataFrame(
        raw_candles,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )

    # Приведение типов
    df["timestamp"] = df["timestamp"].astype("int64")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype("float64")

    # Добавляем константные столбцы сверху
    df.insert(0, "exchange", exchange_name)
    df.insert(1, "symbol", symbol)
    df.insert(2, "timeframe", timeframe)

    # Фиксируем порядок столбцов по контракту
    df = df[
        [
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
    ]
    return df


def download_ohlcv_to_parquet(
    exchange_name: str,
    symbol: str,
    timeframe: str,
    since: Optional[datetime | int | float] = None,
    limit: int = 1000,
    output_dir: Path | str = Path("data/raw"),
) -> Path:
    """Высокоуровневая функция: скачать OHLCV и сохранить в Parquet (v0).

    Parameters
    ----------
    exchange_name:
        Идентификатор биржи, например "binance".
    symbol:
        Символ в формате Crypto Lab. В v0 он совпадает с форматом ccxt ("BTC/USDT").
    timeframe:
        Таймфрейм, например "1m", "5m", "1h".
    since:
        Необязательное время начала (datetime или миллисекунды).
    limit:
        Количество свечей в одном запросе.
    output_dir:
        Корневой каталог для сырых данных. По умолчанию ``data/raw``.

    Returns
    -------
    pathlib.Path
        Путь к созданному файлу Parquet.

    Notes
    -----
    В v0:
    * Запрашивается только одна порция данных (без пагинации).
    * Имя файла включает ``timeframe`` и текущий UTC timestamp для уникальности.
    * Каталоги создаются автоматически.
    """
    output_root = Path(output_dir)

    raw = fetch_ohlcv_raw(
        exchange_name=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
        since=since,
        limit=limit,
    )
    df = normalize_to_contract(
        raw_candles=raw,
        exchange_name=exchange_name,
        symbol=symbol,
        timeframe=timeframe,
    )

    # Структура: data/raw/{exchange}/{symbol}/{timeframe}/...
    now_ms = _to_millis(datetime.now(timezone.utc))
    file_name = f"{timeframe}_{now_ms}.parquet"

    # Для имени папки заменяем '/' в символе
    file_path = output_root / exchange_name / symbol.replace("/", "-") / timeframe / file_name
    file_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_parquet(file_path)

    return file_path


__all__ = [
    "get_exchange_client",
    "fetch_ohlcv_raw",
    "normalize_to_contract",
    "download_ohlcv_to_parquet",
]
