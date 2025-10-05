import datetime as dt
import os
import time
from pathlib import Path

import ccxt
import pandas as pd
from ccxt.base.exchange import Exchange
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

TIMEFRAMES = {"1d": "1d", "1h": "1h", "5m": "5m"}

"""Получение экземпляра биржи """


def get_exchange(name: str) -> Exchange:
    if not hasattr(ccxt, name):
        raise AttributeError(f"Exchange {name} not found in ccxt module")
    cls = getattr(ccxt, name)
    exchange = cls({"enableRateLimit": True})
    return exchange


""" Универсальный преобразователь даты"""


def to_utc_datetime(value: str | dt.datetime) -> dt.datetime:
    """Преобразует строку '2023-12-23' или datetime без tz -> datetime с UTC"""
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.UTC)
        return value.astimezone(dt.UTC)
    elif isinstance(value, str):
        return dt.datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=dt.UTC)
    else:
        raise TypeError(f"Неподдерживаемый тип: {type(value)} (ожидалась str или datetime)")


print(to_utc_datetime("2024-12-11"))


"""Основная функция загрузки """


def fetch_ohlcv(
    exchange_name: str,
    symbol: str,
    timeframe: str,
    since: str | dt.datetime,
    until: str | dt.datetime,
    limit: int = 1000,
) -> pd.DataFrame:
    """Загружает свечи OHLCV через ccxt кусками по limit."""

    # --- Преобразование входных параметров ---
    since_dt = to_utc_datetime(since)
    until_dt = to_utc_datetime(until)

    if since_dt >= until_dt:
        raise ValueError(f"since ({since_dt}) должно быть раньше 'until' ({until_dt}) ")

    since_ms = int(since_dt.timestamp() * 1000)
    until_ms = int(until_dt.timestamp() * 1000)
    all_data = []

    exchange = get_exchange(exchange_name)

    logger.info(f"{exchange_name} | {symbol}| {timeframe} | {since_dt.date()} -> {until_dt.date()}")
    # --- основной цикл загрузки ---
    while since_ms < until_ms:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since_ms, limit)
        except Exception as e:
            logger.error(f"Fetch error:{e}")
            time.sleep(5)
            continue
        if not ohlcv:
            break

        all_data.extend(ohlcv)

        # передвигаем окно
        since_ms = ohlcv[-1][0] + exchange.parse_timeframe(timeframe) * 1000
        time.sleep(exchange.rateLimit / 1000)

    if not all_data:
        logger.warning("Нет данных. Проверь даты или символ.")
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        # --- преобразование в DataFrame ---
    df: pd.DataFrame = pd.DataFrame(
        all_data, columns=["timestamp", "open", "high", "low", "close", "volume"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.drop_duplicates(subset="timestamp").set_index("timestamp")

    logger.success(f"Загружено {len(df)} строк за период {since_dt.date()} → {until_dt.date()}")
    return df


print(fetch_ohlcv("binance", "BTCUSDT", "1h", "2025-09-20", "2025-10-01"))
