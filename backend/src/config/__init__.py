"""Конфигурация проекта: схемы и загрузка JSON."""

from backend.src.config.loader import load_app_config
from backend.src.config.schemas import (
    AppConfig,
    FrequencyFilterConfig,
    HParameterConfig,
    PathsConfig,
    ReadingConfig,
    WindowFilterConfig,
)

config = load_app_config()

__all__ = [
    "AppConfig",
    "FrequencyFilterConfig",
    "HParameterConfig",
    "PathsConfig",
    "ReadingConfig",
    "WindowFilterConfig",
    "config",
    "load_app_config",
]
