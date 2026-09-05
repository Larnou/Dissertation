"""
Настройка loguru для всего приложения.
"""

import sys

from loguru import logger

LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
    "<level>{level: <7}</level> | "
    "<level>{message}</level>"
)

_configured = False


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


def ensure_logging() -> None:
    """
    Включает логирование при первом обращении, если его ещё не настраивали.
    """

    if not _configured:
        setup_logging()


def get_logger():
    """
    Возвращает настроенный logger loguru.
    """

    ensure_logging()
    return logger
