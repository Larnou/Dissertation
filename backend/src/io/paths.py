import re
from datetime import datetime
from pathlib import Path

from backend.src.config.schemas import AppConfig, TIME_FORMAT


EVENTS_DIRNAME = "events"
THEMIS_PREFIX = "THEMIS"
AVAILABLE_DATA_PERIODS_DIRNAME = "available_data_periods"


def project_root() -> Path:
    """Корень репозитория (родитель каталога `backend`)."""
    return Path(__file__).resolve().parents[3]


def event_interval_folder_name(config: AppConfig) -> str:
    """
    Сегмент папки по интервалу из `reading.time_start` / `time_end`: только даты, формат
    `YYYY-MM-DD_YYYY-MM-DD` (как `2017-01-01_2017-01-04`).
    """
    start = datetime.strptime(config.reading.time_start, TIME_FORMAT)
    end = datetime.strptime(config.reading.time_end, TIME_FORMAT)
    return f"{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}"


def themis_satellite_folder_name(config: AppConfig) -> str:
    """Подкаталог спутника, например `THEMIS-A` (буква из `reading.satellite`)."""
    letter = config.reading.satellite.strip().upper()
    return f"{THEMIS_PREFIX}-{letter}"


def events_root(config: AppConfig) -> Path:
    """Каталог событий: `<data_root>/events` относительно корня репозитория."""
    return (project_root() / config.paths.data_root / EVENTS_DIRNAME).resolve()


def dataset_events_dir(config: AppConfig) -> Path:
    """
    Папка одного «события»: `.../events/<интервал>/<THEMIS-X>/`.
    """
    return (
        events_root(config)
        / event_interval_folder_name(config)
        / themis_satellite_folder_name(config)
    ).resolve()


def normalize_dataset_stem(stem: str) -> str:
    """Имя набора без `.parquet`; безопасное для одного сегмента пути."""
    name = stem.strip().removesuffix(".parquet")
    if not name or re.search(r"[\\/]", name):
        msg = f"Invalid dataset stem: {stem!r}"
        raise ValueError(msg)
    return name


def parquet_dataset_path(config: AppConfig, dataset_stem: str) -> Path:
    """
    Полный путь к parquet для набора данных, например::

        backend/data/events/2017-01-01_2017-01-04/THEMIS-A/magnetic_field.parquet

    `dataset_stem` — логическое имя без путей (например ``magnetic_field``).
    """
    stem = normalize_dataset_stem(dataset_stem)
    return (dataset_events_dir(config) / f"{stem}.parquet").resolve()


def availability_periods_dir(config: AppConfig) -> Path:
    """
    Каталог CSV с периодами доступности:
    `.../events/<интервал>/available_data_periods/`.
    """
    return (
        events_root(config)
        / event_interval_folder_name(config)
        / AVAILABLE_DATA_PERIODS_DIRNAME
    ).resolve()


def availability_periods_csv_path(config: AppConfig, source_stem: str) -> Path:
    """
    Полный путь к CSV с периодами доступности источника, например::

        backend/data/events/2017-01-01_2017-01-04/available_data_periods/fgm_availability_periods.csv
    """
    stem = normalize_dataset_stem(source_stem)
    return (availability_periods_dir(config) / f"{stem}_availability_periods.csv").resolve()
