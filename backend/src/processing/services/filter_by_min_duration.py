from datetime import timedelta

import pandas as pd
from backend.src.config import get_logger, progress_bar, AppConfig

logger = get_logger()


def filter_datasets_by_min_duration(datasets: list[pd.DataFrame], min_minutes: float = 25.0) -> list[pd.DataFrame]:
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