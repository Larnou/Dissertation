"""
Единая конфигурация логирования и прогресс-баров.
"""

import sys
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any, TypeVar

from loguru import logger
from tqdm.auto import tqdm

T = TypeVar("T")

_IS_CONFIGURED = False

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <4}</level> | "
    "<level>{message}</level>"
)

TQDM_DEFAULTS: dict[str, Any] = {
    "ncols": 100,
    "dynamic_ncols": True,
    "leave": True,
    "mininterval": 0.1,
    "file": sys.stdout,
}


def tqdm_like_log_desc(desc: str) -> str:
    timestamp = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"{timestamp} | INFO | {desc}"


def setup_logging(level: str = "INFO") -> None:
    """
    Настраивает единый формат loguru для всего проекта.
    """

    global _IS_CONFIGURED
    if _IS_CONFIGURED:
        return

    logger.remove()
    logger.add(sys.stdout, level=level, format=LOG_FORMAT, backtrace=False, diagnose=False)

    _IS_CONFIGURED = True


def get_logger():
    """
    Возвращает сконфигурированный logger.
    """

    setup_logging()
    return logger


def progress(iterable: Iterable[T], desc: str, **kwargs: Any):
    """
    Единый progress-bar для долгих операций.

    Использование:
        for item in progress(items, desc="Загрузка"):
            ...
    """
    setup_logging()
    options = {**TQDM_DEFAULTS, **kwargs}
    options.setdefault(
        "bar_format",
        "{desc} - {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    )
    return tqdm(iterable, desc=tqdm_like_log_desc(desc), **options)
