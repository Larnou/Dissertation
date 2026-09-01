import pandas as pd

from backend.src.config import AppConfig
from backend.src.config import get_logger
from backend.src.io.parquet import read_data_from_parquet, save_data_to_parquet
from backend.src.io.paths import EventDataset
from backend.src.processing.interpolation.df_interpolator import DFInterpolator
from backend.src.processing.services.filter_by_min_duration import filter_datasets_by_min_duration

logger = get_logger()


def get_or_interpolate_data(
    interpolate: bool,
    parameters: AppConfig,
    raw_datasets: list[pd.DataFrame] | None = None,
    overlaps: list | None = None,
    min_minutes: float = 25.0,
) -> list[pd.DataFrame]:

    dataset = EventDataset.AVAILABLE

    if interpolate:
        if not raw_datasets:
            raise ValueError("Для интерполяции необходим список датафреймов `raw_datasets`")
        if not overlaps:
            raise ValueError("Для интерполяции необходим список интервалов `overlaps`")

        data = DFInterpolator(dataframes=raw_datasets).interpolate_many(overlaps=overlaps)
        logger.info(f"Интерполяция завершена. Получено датасетов: {len(data)}")
        data = filter_datasets_by_min_duration(data, min_minutes=min_minutes)
        logger.info(
            f"Фильтрация интерполированных данных завершена. "
            f"Порог: {min_minutes} минут. Осталось датасетов: {len(data)}"
        )

        if data:
            combined = pd.concat(data).reset_index(drop=True)
            save_data_to_parquet(parameters, combined, dataset)
        return data

    loaded = read_data_from_parquet(parameters, dataset, read_as_list=True)
    logger.info("Данные загружены с диска без доп. фильтрации по длительности.")
    return loaded
