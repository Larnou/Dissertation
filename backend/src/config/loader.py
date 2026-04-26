import json
from pathlib import Path

from pydantic import ValidationError

from backend.src.config.schemas import AppConfig


def load_app_config(config_path: Path | None = None) -> AppConfig:
    """
    Читает `config.json` и валидирует его через Pydantic-модели.

    Возвращает `AppConfig`, чтобы обращаться к параметрам через точку
    (например, `config.window_filter.low_pass` / `high_pass` — длительности периодов в секундах).
    """
    path = config_path or (Path(__file__).resolve().parent / "config.json")
    if not path.is_file():
        msg = f"Config file not found: {path}"
        raise FileNotFoundError(msg)

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        msg = f"Invalid JSON in config file: {path}"
        raise ValueError(msg) from exc

    try:
        return AppConfig.model_validate(data)
    except ValidationError as exc:
        msg = f"Config validation failed for {path}"
        raise ValueError(msg) from exc
