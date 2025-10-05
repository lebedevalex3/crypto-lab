# src/io/state.py
from __future__ import annotations

import datetime as dt
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

load_dotenv()
DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
CATALOG_DIR = DATA_DIR / "catalog"
CATALOG_DIR.mkdir(parents=True, exist_ok=True)
STATE_PATH = CATALOG_DIR / "state.json"


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    """
    Атомарно записывает JSON-файл.

    Функция создаёт временный файл в той же директории, записывает в него
    переданные данные `payload` в формате JSON, а затем заменяет им целевой файл `path`.
    Это гарантирует целостность: при сбое записи старый файл остаётся нетронутым.

    Args:
        path (Path): Путь к целевому JSON-файлу, который нужно создать или перезаписать.
        payload (Dict[str, Any]): Словарь с данными, которые будут сериализованы в JSON.

    Returns:
        None: Функция ничего не возвращает. Выполняет запись на диск с атомарной заменой файла.

    Notes:
        - Используется `tempfile.mkstemp()` для безопасного создания временного файла.
        - Замена выполняется с помощью `os.replace()`, что обеспечивает атомарность
          на уровне файловой системы (старый файл заменяется новым одной операцией).
    """
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="state_", suffix=".json", dir=str(path.parent))
    os.close(tmp_fd)
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, path)  # атомарная замена


def _load_state() -> dict[str, Any]:
    """
    Загружает текущее состояние сборщика данных из JSON-файла `state.json`.

    Если файл существует — читает его и возвращает содержимое как словарь.
    Если файла нет — возвращает пустую структуру с ключом "items".

    Returns:
        Dict[str, Any]: Словарь состояния в формате:
            {
                "items": {
                    "<exchange>::<symbol>::<tf>": {
                        "last_closed_ts": "<ISO-строка времени>",
                        "runs": [ ... ]  # журнал запусков (опционально)
                    },
                    ...
                }
            }
    """
    if not STATE_PATH.exists():
        return {"items": {}}
    with open(STATE_PATH, encoding="utf-8") as f:
        return json.load(f)


def _save_state(state: dict[str, Any]) -> None:
    """
    Сохраняет текущее состояние загрузчика (курсор и метаданные) в файл `state.json`.

    Функция выполняет атомарную запись: сначала создаёт временный JSON-файл
    в той же директории, затем заменяет основной файл `state.json` новой версией.
    Это гарантирует целостность состояния даже при сбоях записи
    или одновременных процессах обновления.

    Параметры
    ----------
    state : dict[str, Any]
        Словарь, содержащий текущее состояние всех наборов данных.
        Пример структуры:
        {
            "items": {
                "binance::BTC/USDT::5m": {
                    "last_closed_ts": "2025-10-05T14:00:00+00:00",
                    "runs": [
                        {"utc": "...", "rows_added": 1200, "from": "...", "to": "..."}
                    ]
                },
                ...
            }
        }

    Возвращает
    ----------
    None
        Функция ничего не возвращает. После выполнения файл `state.json`
        обновляется на диске.
    """
    _atomic_write_json(STATE_PATH, state)


def _key(exchange: str, symbol: str, tf: str) -> str:
    """
    Формирует уникальный ключ идентификации набора данных.

    Ключ используется для обращения к состоянию (state) конкретного источника данных
    в файле `state.json`. Он объединяет название биржи, торговый символ и таймфрейм
    в единый строковый идентификатор формата:

        "<exchange>::<symbol>::<tf>"

    Пример:
        >>> _key("binance", "BTC/USDT", "1h")
        "binance::BTC/USDT::1h"

    Args:
        exchange (str): Название биржи (например, "binance" или "bybit").
        symbol (str): Торговая пара (например, "BTC/USDT").
        tf (str): Таймфрейм данных (например, "5m", "1h", "1d").

    Returns:
        str: Уникальный строковый ключ для идентификации набора данных.
    """
    return f"{exchange}::{symbol}::{tf}"


def get_cursor(exchange: str, symbol: str, tf: str) -> dt.datetime | None:
    """
    Возвращает курсор (время последнего закрытого бара) для указанной биржи, символа и таймфрейма.

    Курсор используется в режиме APPEND для определения точки,
    с которой нужно продолжить загрузку новых данных без повторного сбора всей истории.

    Args:
        exchange (str): Название биржи (например, "binance" или "bybit").
        symbol (str): Торговая пара в формате "BTC/USDT".
        tf (str): Таймфрейм, например "5m", "1h" или "1d".

    Returns:
        datetime | None: Объект datetime с таймзоной UTC,
        соответствующий последнему успешно сохранённому закрытому бару.
        Возвращает None, если курсор ещё не установлен (первая загрузка).

    Пример:
        >>> get_cursor("binance", "BTC/USDT", "1h")
        datetime.datetime(2025, 10, 4, 23, 0, tzinfo=datetime.UTC)
    """
    state = _load_state()
    item = state["items"].get(_key(exchange, symbol, tf))
    if not item or not item.get("last_closed_ts"):
        return None
    return dt.datetime.fromisoformat(item["last_closed_ts"])


def set_cursor(exchange: str, symbol: str, tf: str, last_closed_ts: dt.datetime) -> None:
    """
    Обновляет «курсор» последнего загруженного бара для указанной биржи, символа и таймфрейма.

    Используется для режима догрузки (APPEND), чтобы при следующем запуске
    загрузка начиналась не с нуля, а с момента, где сбор данных был завершён ранее.

    Параметры:
        exchange (str): Название биржи (например, 'binance', 'bybit').
        symbol (str): Торговая пара (например, 'BTC/USDT').
        tf (str): Таймфрейм данных ('5m', '1h', '1d').
        last_closed_ts (datetime): Время конца последнего полностью закрытого бара (UTC).

    Поведение:
        - Загружает текущий state.json.
        - Обновляет или создаёт запись по ключу "exchange::symbol::tf".
        - Сохраняет ISO-время последнего закрытого бара (в UTC) в поле "last_closed_ts".
        - Перезаписывает state.json атомарно, чтобы избежать повреждения файла.

    Пример:
        >>> set_cursor("binance", "BTC/USDT", "1h", datetime.datetime(2025, 10, 5, 12,
        tzinfo=datetime.UTC))
        # state.json теперь содержит отметку 2025-10-05T12:00:00+00:00
    """
    state = _load_state()
    k = _key(exchange, symbol, tf)
    state["items"].setdefault(k, {})
    state["items"][k]["last_closed_ts"] = last_closed_ts.replace(tzinfo=dt.UTC).isoformat()
    _save_state(state)


def record_run(
    exchange: str,
    symbol: str,
    tf: str,
    rows_added: int,
    from_ts: dt.datetime,
    to_ts: dt.datetime,
    notes: str = "",
) -> None:
    """
    Сохраняет запись о запуске задачи загрузки (append/backfill/full)
    в файл state.json.

    Parameters
    ----------
    exchange : str
        Название биржи (например, "binance" или "bybit").
    symbol : str
        Торговый инструмент (например, "BTC/USDT").
    tf : str
        Таймфрейм данных ("5m", "1h", "1d").
    rows_added : int
        Количество строк (свечей), добавленных в результате загрузки.
    from_ts : datetime
        Временная метка начала диапазона (UTC).
    to_ts : datetime
        Временная метка конца диапазона (UTC).
    notes : str, optional
        Дополнительное описание запуска (например, "append-latest", "backfill-gap").

    Returns
    -------
    None
        Функция ничего не возвращает. Она обновляет файл state.json, добавляя
        новую запись в историю запусков для указанного `(exchange, symbol, tf)`.
    """

    state = _load_state()
    key = _key(exchange, symbol, tf)

    # создаём при необходимости пустой блок
    item = state["items"].setdefault(key, {})
    runs = item.setdefault("runs", [])

    runs.append(
        {
            "utc": dt.datetime.now(dt.UTC).isoformat(),
            "rows_added": int(rows_added),
            "from": from_ts.replace(tzinfo=dt.UTC).isoformat(),
            "to": to_ts.replace(tzinfo=dt.UTC).isoformat(),
            "notes": notes,
        }
    )

    _save_state(state)
