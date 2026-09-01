import pandas as pd

from backend.src.config import get_config
from backend.src.io.parquet import save_data_to_parquet
from backend.src.io.paths import EventDataset
from backend.src.log import get_logger
from backend.src.physics.ion_current_density import IonCurrentDensityModel
from backend.src.processing import build_prepared_datasets
from backend.src.processing.interpolation import get_or_interpolate_data

logger = get_logger()
config = get_config()

INTERPOLATE_DATA = False
LOAD_FROM_CDAWEB = False
USE_NOISE_MASK = False

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
output_path = save_data_to_parquet(config, combined_prepared, EventDataset.PREPARED)
logger.info(f"Готовый датасет сохранён: {output_path}")
logger.info(f"Колонки готового датасета: {list(combined_prepared.columns)}")
