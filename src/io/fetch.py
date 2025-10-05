import datetime as dt
import json
import os
import time
from pathlib import Path

import ccxt
import pandas as pd
from ccxt.base.exchange import Exchange
from dotenv import load_dotenv
from loguru import logger
from tqdm import tqdm

# ---------------- Инициализация ----------------

load_dotenv()

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

TIMEFRAMES = {"1d": "1d", "1h": "1h", "5m": "5m"}

# ---------------- Вспомогательные функции ----------------


def get_exchange(name: str) -> Exchange:
    """Возвращает экземпляр биржи ccxt по имени."""
    if not hasattr(ccxt, name):
        raise AttributeError(f"Exchange {name} not found in ccxt module")
    cls = getattr(ccxt, name)
    return cls({"enableRateLimit": True})


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


# ---------------- Проверка качества данных ----------------


def qc_summary(df: pd.DataFrame, tf: str) -> float:
    """
    Проверяет полноту данных: нет ли пропущенных свечей.
    Возвращает процент пропусков.
    """
    if df.empty:
        logger.warning("⚠️ DataFrame пуст — проверка пропущена.")
        return 100.0

    freq_map = {"1d": "1D", "1h": "1H", "5m": "5T"}
    if tf not in freq_map:
        logger.warning(f"Неизвестный таймфрейм '{tf}', QC пропущен.")
        return 0.0

    expected = pd.date_range(df.index.min(), df.index.max(), freq=freq_map[tf], tz="UTC")
    missing = expected.difference(df.index)

    pct_missing = len(missing) / len(expected) * 100 if len(expected) else 0
    if pct_missing > 0:
        logger.warning(f"⚠️ Пропущено {len(missing)} свечей ({pct_missing:.2f}%) для {tf}")
    else:
        logger.info(f"✅ Все свечи на месте для {tf}")
    return pct_missing


# ---------------- Запись метаданных ----------------


def write_metadata(
    parquet_path: Path,
    exchange: str,
    symbol: str,
    tf: str,
    start: dt.datetime,
    end: dt.datetime,
    rows: int,
    missing_pct: float,
) -> None:
    """Создаёт JSON-файл с метаданными рядом с parquet."""
    meta = {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": tf,
        "rows": rows,
        "pct_missing": round(missing_pct, 3),
        "from": str(start),
        "to": str(end),
        "utc_saved": dt.datetime.now(dt.UTC).isoformat(),
    }
    json_path = parquet_path.with_suffix(".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    logger.info(f"🧾 Метаданные сохранены → {json_path.name}")


# ---------------- Основная функция загрузки ----------------


def fetch_ohlcv(
    exchange_name: str,
    symbol: str,
    timeframe: str,
    since: str | dt.datetime,
    until: str | dt.datetime,
    limit: int = 1000,
) -> pd.DataFrame:
    """Загружает свечи OHLCV через ccxt кусками по limit.
    Добавлен tqdm-прогресс и autosave каждые 10 000 строк.
    """

    since_dt = to_utc_datetime(since)
    until_dt = to_utc_datetime(until)

    if since_dt >= until_dt:
        raise ValueError(f"'since' ({since_dt}) должно быть раньше 'until' ({until_dt})")

    exchange = get_exchange(exchange_name)
    since_ms = int(since_dt.timestamp() * 1000)
    until_ms = int(until_dt.timestamp() * 1000)
    all_data: list[list] = []

    logger.info(f"{exchange_name} | {symbol} | {timeframe} | {since_dt.date()} → {until_dt.date()}")

    # определяем шаг таймфрейма (в секундах)
    try:
        delta = exchange.parse_timeframe(timeframe)
    except Exception:
        delta = {"1d": 86400, "1h": 3600, "5m": 300}.get(timeframe, 3600)

    max_attempts = 5
    attempt = 0

    total_est = int((until_dt - since_dt).total_seconds() / delta)
    pbar = tqdm(total=total_est, desc=f"{exchange_name} {symbol} {timeframe}", ncols=90)

    while since_ms < until_ms:
        try:
            ohlcv = exchange.fetch_ohlcv(symbol, timeframe, since_ms, limit)
            attempt = 0
        except Exception as e:
            attempt += 1
            if attempt >= max_attempts:
                logger.error(f"🚨 Превышено число попыток ({max_attempts}). Прерывание загрузки.")
                break
            logger.warning(f"Ошибка загрузки ({attempt}/{max_attempts}): {e}")
            time.sleep(5 * attempt)
            continue

        if not ohlcv:
            logger.info("Биржа вернула пустой ответ — достигнут конец диапазона.")
            break

        if all_data and ohlcv[0][0] <= all_data[-1][0]:
            ohlcv = [row for row in ohlcv if row[0] > all_data[-1][0]]

        all_data.extend(ohlcv)
        pbar.update(len(ohlcv))

        since_ms = ohlcv[-1][0] + delta * 1000
        time.sleep(getattr(exchange, "rateLimit", 500) / 1000)

        if len(all_data) % 10000 < limit:
            tmp_df = pd.DataFrame(
                all_data, columns=["timestamp", "open", "high", "low", "close", "volume"]
            )
            tmp_path = (
                RAW_DIR / f"autosave_{exchange_name}_{symbol.replace('/', '')}_{timeframe}.parquet"
            )
            tmp_df.to_parquet(tmp_path)
            logger.info(f"💾 Autosave: {len(tmp_df)} строк → {tmp_path}")

    pbar.close()

    if not all_data:
        logger.warning("Нет данных. Проверь символ, таймфрейм или даты.")
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    df = pd.DataFrame(all_data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df = df.drop_duplicates(subset="timestamp").sort_values("timestamp").set_index("timestamp")

    mask = (df.index >= since_dt) & (df.index <= until_dt)
    df = df.loc[mask].astype(float)

    logger.success(
        f"✅ Загружено {len(df)} строк: "
        f"{df.index.min().date()} → {df.index.max().date()} "
        f"(ограничено {until_dt.date()})"
    )
    return df


# ---------------- Сохранение в Parquet ----------------


def save_parquet(
    df: pd.DataFrame,
    exchange: str,
    symbol: str,
    tf: str,
    start: dt.datetime,
    end: dt.datetime,
    out_dir: Path = RAW_DIR,
) -> Path:
    """Сохраняет DataFrame в Parquet по контракту."""
    if df.empty:
        logger.warning("⚠️ Пустой DataFrame — сохранение пропущено.")
        return Path()

    symbol_clean = symbol.replace("/", "")
    filename = f"{exchange}__{symbol_clean}__{tf}__{start.date()}__{end.date()}.parquet"
    path = out_dir / filename

    df = df.copy()
    df.index = pd.to_datetime(df.index, utc=True)
    df = df.sort_index()

    df.to_parquet(path, index=True)
    logger.success(f"💾 Сохранено {len(df)} строк в {path}")
    return path


# ---------------- Обёртка для нескольких таймфреймов ----------------


def collect_and_save(
    exchange_name: str,
    symbol: str,
    timeframes: list[str],
    start: str | dt.datetime,
    end: str | dt.datetime,
) -> None:
    """Загружает и сохраняет данные сразу по нескольким таймфреймам."""
    start_dt = to_utc_datetime(start)
    end_dt = to_utc_datetime(end)

    for tf in timeframes:
        df = fetch_ohlcv(exchange_name, symbol, tf, start_dt, end_dt)
        missing_pct = qc_summary(df, tf)
        parquet_path = save_parquet(df, exchange_name, symbol, tf, start_dt, end_dt)
        if parquet_path.exists():
            write_metadata(
                parquet_path,
                exchange_name,
                symbol,
                tf,
                start_dt,
                end_dt,
                len(df),
                missing_pct,
            )
        logger.info(f"QC завершён: {missing_pct:.2f}% пропусков для {tf}")


# ---------------- CLI-тест ----------------

if __name__ == "__main__":
    exch = "binance"
    sym = "BTC/USDT"
    tfs = ["5m"]
    start = "2025-04-30"
    end = "2025-10-01"

    collect_and_save(exch, sym, tfs, start, end)
