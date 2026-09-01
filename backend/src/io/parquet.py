from pathlib import Path

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.paths import KyotoIndex, paths


def event_parquet_path(
    config: AppConfig,
    source: str,
) -> Path:
    """
    Абсолютный путь к parquet-файлу в каталоге data/ события.
    """

    return paths(config).dataset(source)


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


def read_data_from_parquet(
    config: AppConfig,
    source: str,
    *,
    read_as_list: bool = False,
) -> pd.DataFrame | list[pd.DataFrame]:
    """
    Читает parquet датасета события; опционально разбивает по временным разрывам.
    """

    path = event_parquet_path(config, source)
    dataframe = pd.read_parquet(path)

    if read_as_list:
        return split_dataframe_by_time_gaps(dataframe)

    return dataframe


def save_data_to_parquet(
    config: AppConfig,
    dataframe: pd.DataFrame,
    source: str,
) -> Path:
    """
    Сохраняет DataFrame в parquet под backend/data/events/.../data/.
    """

    path = event_parquet_path(config, source)
    path.parent.mkdir(parents=True, exist_ok=True)

    saved_dataframe = dataframe.reset_index(drop=True)
    saved_dataframe.to_parquet(path)
    return path


def kyoto_index_path(config: AppConfig, index: KyotoIndex) -> Path:
    """
    Абсолютный путь к parquet индекса Kyoto WDC.
    """

    return paths(config).kyoto.index_parquet(index)


def read_kyoto_index(config: AppConfig, index: KyotoIndex) -> pd.DataFrame:
    """
    Читает parquet индекса Kyoto WDC.
    """

    return pd.read_parquet(kyoto_index_path(config, index))


def save_kyoto_index(config: AppConfig, dataframe: pd.DataFrame, index: KyotoIndex) -> Path:
    """
    Сохраняет parquet индекса Kyoto WDC.
    """

    path = kyoto_index_path(config, index)
    path.parent.mkdir(parents=True, exist_ok=True)

    saved_dataframe = dataframe.reset_index(drop=True)
    saved_dataframe.to_parquet(path)
    return path
