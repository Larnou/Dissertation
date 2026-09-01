from pathlib import Path

CONFIG_FILENAME = "config.json"
_ROOT_MARKERS = ("pyproject.toml", CONFIG_FILENAME)


def project_root(start: Path | None = None) -> Path:
    """
    Находит корень репозитория по маркерам pyproject.toml и config.json.

    Поиск идёт вверх от start. Если start не задан, начинается от этого модуля,
    поэтому перенос клона в другую папку путь не ломает.

    Args:
        start: файл или каталог, от которого идти вверх. По умолчанию — этот модуль.

    Returns:
        Абсолютный путь корня репозитория.

    Raises:
        FileNotFoundError: ни один маркер не найден выше start.
    """

    current = (start or Path(__file__)).resolve()
    if current.is_file():
        current = current.parent

    for candidate in (current, *current.parents):
        if any((candidate / marker).is_file() for marker in _ROOT_MARKERS):
            return candidate

    raise FileNotFoundError(f"Корень репозитория не найден (нет pyproject.toml/{CONFIG_FILENAME}), start={current}")


def default_config_path(start: Path | None = None) -> Path:
    """
    Путь к config.json в корне репозитория.

    Args:
        start: точка старта для поиска корня; как у project_root.
    """

    return project_root(start) / CONFIG_FILENAME
