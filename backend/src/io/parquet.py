from pathlib import Path

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.paths import parquet_dataset_path


def data_file_path(config: AppConfig, dataset_stem: str) -> Path:
    """Абсолютный путь к `.../events/<даты>/THEMIS-X/<dataset_stem>.parquet`."""
    return parquet_dataset_path(config, dataset_stem)


def read_data_from_parquet(config: AppConfig, dataset_stem: str) -> pd.DataFrame:
    path = parquet_dataset_path(config, dataset_stem)
    return pd.read_parquet(path)


def save_data_to_parquet(config: AppConfig, dataframe: pd.DataFrame, dataset_stem: str) -> None:
    """
    Сохраняет DataFrame в parquet под `backend/data/events/...`.
    """
    path = parquet_dataset_path(config, dataset_stem)
    path.parent.mkdir(parents=True, exist_ok=True)

    dataframe.reset_index(drop=True, inplace=True)
    dataframe.to_parquet(path)
