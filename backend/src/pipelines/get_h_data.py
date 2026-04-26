from datetime import timedelta

import pandas as pd

from backend.src.config import config, get_logger
from backend.src.io import DataDownloading
from backend.src.io.parquet import data_file_path, save_data_to_parquet
from backend.src.processing import AvailabilityIntervals, build_prepared_datasets
from backend.src.processing.interpolate import get_or_interpolate_data
from backend.src.processing.intersections import intersect_many, summarize_intervals

logger = get_logger()

INTERPOLATE_DATA = False
LOAD_FROM_CDAWEB = False
USE_NOISE_MASK = False
PREPARED_DATASET_STEM = "prepared_data"


logger.info(f"{'Загрузка интерполированных' if not INTERPOLATE_DATA else 'Интерполирование'} данных:")
available_data = get_or_interpolate_data(
    interpolate=INTERPOLATE_DATA,
    parameters=config,
    raw_datasets=None,
    overlaps=None,
    min_minutes=25.0,
)
logger.info(f"Количество доступных датасетов: {len(available_data)}")

prepared_datasets = build_prepared_datasets(
    available_data,
    config,
    use_noise=USE_NOISE_MASK,
)
logger.info(f"Подготовлено H-ready датасетов: {len(prepared_datasets)}")

combined_prepared = pd.concat(prepared_datasets, ignore_index=True)
save_data_to_parquet(config, combined_prepared, PREPARED_DATASET_STEM)
logger.info(f"Готовый датасет сохранён: {data_file_path(config, PREPARED_DATASET_STEM)}")
logger.info(f"Колонки готового датасета: {list(combined_prepared.columns)}")
