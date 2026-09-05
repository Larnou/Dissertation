"""
Единый progress-bar в стиле строк лога.
"""

from collections.abc import Iterable
from datetime import UTC, datetime
import sys
from typing import Any, TypeVar

from tqdm.auto import tqdm

from backend.src.log.setup import ensure_logging

T = TypeVar("T")

TQDM_DEFAULTS: dict[str, Any] = {
    "ncols": 100,
    "dynamic_ncols": True,
    "leave": True,
    "mininterval": 0.1,
    "file": sys.stdout,
}


def format_progress_description(desc: str) -> str:
    """
    Формирует описание progress-бара в стиле строки лога.
    """

    timestamp = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return f"{timestamp} | INFO | {desc}"


def progress_bar(iterable: Iterable[T], desc: str, **kwargs: Any) -> tqdm:
    """
    Единый progress-bar для долгих операций.

    Использование:
        for item in progress_bar(items, desc="Загрузка"):
            ...
    """

    ensure_logging()
    options = {**TQDM_DEFAULTS, **kwargs}
    options.setdefault(
        "bar_format",
        "{desc} - {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
    )
    return tqdm(iterable, desc=format_progress_description(desc), **options)
