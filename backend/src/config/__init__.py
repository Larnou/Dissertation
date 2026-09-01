"""
Конфигурация приложения: схема JSON и загрузка.
"""

from functools import lru_cache

from backend.src.config.loader import load_app_config
from backend.src.config.root import default_config_path, project_root
from backend.src.config.schemas import (
    AppConfig,
    HParameterConfig,
    PathsConfig,
    ReadingConfig,
    WindowFilterConfig,
)


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """
    Возвращает конфиг из config.json в корне репозитория.

    Результат кэшируется на процесс. Библиотечный код должен принимать
    AppConfig аргументом; эта функция — для пайплайнов и скриптов.
    Для произвольного файла используйте load_app_config(path).
    В тестах сбрасывайте кэш через get_config.cache_clear().
    """

    return load_app_config()


__all__ = [
    "AppConfig",
    "HParameterConfig",
    "PathsConfig",
    "ReadingConfig",
    "WindowFilterConfig",
    "default_config_path",
    "get_config",
    "load_app_config",
    "project_root",
]
