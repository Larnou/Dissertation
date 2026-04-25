from datetime import timedelta
from typing import List

import pandas as pd
from loguru import logger


def get_or_interpolate_data(
        interpolate: bool = False,
        include_electron: bool = False,
        save_to_disk: bool = True,
        parameters: dict | None = None,
        raw_datasets: List[pd.DataFrame] | None = None,
        overlaps: list | None = None,
) -> List[pd.DataFrame]:
    filename = "available_electron_data.parquet" if include_electron else "available_data.parquet"

    if interpolate:
        if not raw_datasets:
            raise ValueError("Для интерполяции необходим список датафреймов `raw_datasets`")

        data = DFInterpolator(dataframes=raw_datasets).interpolate_lists(overlaps=overlaps)
        logger.info(f"Интерполяция завершена. Получено датасетов: {len(data)}")

        if save_to_disk and data:
            combined = pd.concat(data).reset_index(drop=True)
            save_data_to_parquet(parameters, combined, filename, title=True)
        return data

    else:
        loaded = read_data_from_parquet(parameters, filename, read_as_list=True)
        logger.info(f"Данные загружены с диска. Количество датасетов: {len(loaded)}")
        return loaded


def filter_datasets_by_min_duration(
        datasets: List[pd.DataFrame],
        min_minutes: float = 25.0,
) -> List[pd.DataFrame]:
    min_delta = timedelta(minutes=min_minutes)
    filtered = []

    for df in datasets:
        if df.empty:
            continue
        duration = df["Time"].max() - df["Time"].min()
        if duration >= min_delta:
            filtered.append(df)

    logger.info(f"Фильтрация завершена. Осталось датасетов: {len(filtered)}")
    return filtered