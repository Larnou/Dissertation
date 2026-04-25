"""Конфигурация проекта: схемы и загрузка JSON."""

from backend.src.config.loader import load_app_config
from backend.src.config.logging import get_logger, progress, setup_logging
from backend.src.config.schemas import (
    AppConfig,
    FrequencyFilterConfig,
    HParameterConfig,
    PathsConfig,
    ReadingConfig,
    WindowFilterConfig,
)

setup_logging()
config = load_app_config()

__all__ = [
    "AppConfig",
    "FrequencyFilterConfig",
    "HParameterConfig",
    "PathsConfig",
    "ReadingConfig",
    "WindowFilterConfig",
    "config",
    "get_logger",
    "load_app_config",
    "progress",
    "setup_logging",
]
