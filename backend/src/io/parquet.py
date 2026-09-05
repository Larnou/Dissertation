from pathlib import Path

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.paths import paths


def read_data_from_parquet(config: AppConfig, source: str) -> pd.DataFrame:
    """
    Читает parquet датасета события.
    """

    return pd.read_parquet(paths(config).dataset(source))


def save_data_to_parquet(
    config: AppConfig,
    dataframe: pd.DataFrame,
    source: str,
) -> Path:
    """
    Сохраняет DataFrame в parquet под data/ события.
    """

    path = paths(config).dataset(source)
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.reset_index(drop=True).to_parquet(path)
    return path
