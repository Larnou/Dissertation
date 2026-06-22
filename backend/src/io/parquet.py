from pathlib import Path

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.paths import PathResolver


def data_file_path(config: AppConfig, dataset_stem: str) -> Path:
    """
    Абсолютный путь к `.../events/<даты>/THEMIS-X/<dataset_stem>.parquet`.
    """

    return PathResolver(config).data_file(dataset_stem)


def split_dataframe_by_time_gaps(dataframe: pd.DataFrame, time_column: str = "Time", gap_seconds: int = 12) -> list[pd.DataFrame]:
    """
    Разбивает DataFrame на сегменты по разрывам во времени.
    """

    if dataframe.empty:
        return []

    time_series = pd.to_datetime(dataframe[time_column])
    masks = (time_series.diff() > pd.to_timedelta(gap_seconds, unit="s")).cumsum()

    return [
        dataframe[masks == mask_id].reset_index(drop=True)
        for mask_id in masks.unique()
    ]


def read_data_from_parquet(config: AppConfig, dataset_stem: str, read_as_list: bool = False) -> pd.DataFrame | list[pd.DataFrame]:
    """
    Читает parquet-датасет; опционально разбивает его по временным разрывам.
    """

    path = PathResolver(config).data_file(dataset_stem)
    dataframe = pd.read_parquet(path)

    if read_as_list:
        return split_dataframe_by_time_gaps(dataframe)

    return dataframe


def save_data_to_parquet(config: AppConfig, dataframe: pd.DataFrame, dataset_stem: str) -> None:
    """
    Сохраняет DataFrame в parquet под `backend/data/events/...`.
    """

    path = PathResolver(config).data_file(dataset_stem)
    path.parent.mkdir(parents=True, exist_ok=True)

    saved_dataframe = dataframe.reset_index(drop=True)
    saved_dataframe.to_parquet(path)


def kyoto_file_path(config: AppConfig, dataset_stem: str) -> Path:
    """
    Абсолютный путь к `backend/data/Kyoto/<dataset_stem>.parquet`.
    """

    return PathResolver(config).kyoto_file(dataset_stem)


def read_kyoto_from_parquet(config: AppConfig, dataset_stem: str) -> pd.DataFrame:
    """
    Читает parquet-датасет из ``backend/data/Kyoto``.
    """

    return pd.read_parquet(kyoto_file_path(config, dataset_stem))


def save_kyoto_to_parquet(config: AppConfig, dataframe: pd.DataFrame, dataset_stem: str) -> Path:
    """
    Сохраняет DataFrame в parquet под ``backend/data/Kyoto``.
    """

    path = PathResolver(config).kyoto_file(dataset_stem)
    path.parent.mkdir(parents=True, exist_ok=True)

    saved_dataframe = dataframe.reset_index(drop=True)
    saved_dataframe.to_parquet(path)
    return path
