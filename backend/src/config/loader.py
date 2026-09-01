import json
from pathlib import Path

from pydantic import ValidationError

from backend.src.config.root import default_config_path
from backend.src.config.schemas import AppConfig


def load_app_config(path: Path | str | None = None) -> AppConfig:
    """
    Читает JSON-конфиг и валидирует его через Pydantic-модели.

    Args:
        path: путь к JSON. Если не задан, берётся config.json в корне репозитория.

    Returns:
        Иммутабельный AppConfig. Даты и шаг скачивания уже разобраны в datetime/timedelta.

    Raises:
        FileNotFoundError: файл не найден.
        ValueError: JSON повреждён или не проходит схему.
    """

    config_path = Path(path) if path is not None else default_config_path()

    if not config_path.is_file():
        raise FileNotFoundError(f"Файл конфигурации не найден: {config_path}")

    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Некорректный JSON в файле конфигурации: {config_path}") from exc

    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        raise ValueError(f"Конфигурация не прошла валидацию: {config_path}") from exc
