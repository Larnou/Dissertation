"""
Единая настройка логирования и прогресс-баров.
"""

from collections.abc import Iterable
from datetime import UTC, datetime
import sys
from typing import Any, TypeVar

from loguru import logger
from tqdm.auto import tqdm

T = TypeVar("T")

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <7}</level> | "
    "<level>{message}</level>"
)

TQDM_DEFAULTS: dict[str, Any] = {
    "ncols": 100,
    "dynamic_ncols": True,
    "leave": True,
    "mininterval": 0.1,
    "file": sys.stdout,
}

_configured = False


def format_progress_description(desc: str) -> str:
    """
    Формирует описание progress-бара в стиле строки лога.
    """

    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"{timestamp} | INFO | {desc}"


def setup_logging(level: str = "INFO") -> None:
    """
    Настраивает единый формат loguru для всего проекта.
    """

    global _configured

    normalized_level = level.strip().upper()
    logger.remove()
    logger.add(
        sys.stdout,
        level=normalized_level,
        format=LOG_FORMAT,
        backtrace=False,
        diagnose=False,
    )
    _configured = True


def _ensure_logging() -> None:
    if not _configured:
        setup_logging()


def get_logger():
    """
    Возвращает настроенный logger loguru.
    """

    _ensure_logging()
    return logger


def progress_bar(iterable: Iterable[T], desc: str, **kwargs: Any) -> tqdm:
    """
    Единый progress-bar для долгих операций.

    Использование:
        for item in progress_bar(items, desc="Загрузка"):
            ...
    """

    _ensure_logging()
    options = {**TQDM_DEFAULTS, **kwargs}
    options.setdefault(
        "bar_format",
        "{desc} - {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    )
    return tqdm(iterable, desc=format_progress_description(desc), **options)
