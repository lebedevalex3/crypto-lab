from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Optional


# Набор стандартных полей лог-записи, чтобы отделять их от extra
_STANDARD_LOG_KEYS = {
    "name",
    "msg",
    "args",
    "levelname",
    "levelno",
    "pathname",
    "filename",
    "module",
    "exc_info",
    "exc_text",
    "stack_info",
    "lineno",
    "funcName",
    "created",
    "msecs",
    "relativeCreated",
    "thread",
    "threadName",
    "processName",
    "process",
}


class JsonFormatter(logging.Formatter):
    """Форматер, который выводит записи логов как JSON-строки.

    Пример записи:
    {
        "time": "2025-11-23T16:00:12.345Z",
        "level": "info",
        "logger": "crypto_lab.cli",
        "msg": "pipeline_start",
        "event": "pipeline_start",
        "exchange": "binance",
        "symbol": "BTC/USDT",
        ...
    }
    """

    def format(self, record: logging.LogRecord) -> str:
        # Базовая структура
        log_record = {
            "time": datetime.utcnow().isoformat(timespec="milliseconds") + "Z",
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }

        # Дополнительные поля (extra), переданные через logger.*(..., extra={...})
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_KEYS:
                # Не перезаписываем базовые ключи, если вдруг совпали
                if key not in log_record:
                    log_record[key] = value

        return json.dumps(log_record, ensure_ascii=False)


def setup_logging(level: str = "INFO") -> logging.Logger:
    """Настроить корневой логгер проекта crypto_lab со структурированным JSON-логированием.

    Parameters
    ----------
    level : str
        Минимальный уровень логирования: "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL".

    Returns
    -------
    logging.Logger
        Логгер верхнего уровня "crypto_lab".
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    logger = logging.getLogger("crypto_lab")

    # Если уже настроен (хендлеры есть) — не дублируем
    if logger.handlers:
        logger.setLevel(numeric_level)
        return logger

    logger.setLevel(numeric_level)
    logger.propagate = False  # чтобы не дублировать в root-логгер

    handler = logging.StreamHandler()
    handler.setLevel(numeric_level)
    handler.setFormatter(JsonFormatter())

    logger.addHandler(handler)
    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """Получить дочерний логгер внутри пространства crypto_lab.

    Пример:
    - get_logger() → "crypto_lab"
    - get_logger("cli") → "crypto_lab.cli"
    - get_logger(__name__.split("crypto_lab.", 1)[-1]) → "crypto_lab.cli.cli_v0"
    """
    base = "crypto_lab"
    if name:
        full_name = f"{base}.{name}"
    else:
        full_name = base
    return logging.getLogger(full_name)
