"""
Конфигурация проекта: схемы и загрузка JSON.
"""
from functools import lru_cache

from backend.src.config.loader import load_app_config
from backend.src.config.logging import get_logger, progress_bar, setup_logging
from backend.src.config.schemas import (
    AppConfig,
    FrequencyFilterConfig,
    HParameterConfig,
    PathsConfig,
    ReadingConfig,
    WindowFilterConfig,
)


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    return load_app_config()

__all__ = [
    "AppConfig",
    "FrequencyFilterConfig",
    "HParameterConfig",
    "PathsConfig",
    "ReadingConfig",
    "WindowFilterConfig",
    "get_logger",
    "load_app_config",
	"progress_bar",
    "setup_logging",
]
