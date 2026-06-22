from pathlib import Path

from backend.src.config import get_config, get_logger
from backend.src.io.parquet import data_file_path
from backend.src.processing.interpolation import add_symh_index_to_available_data

logger = get_logger()
config = get_config()

PROJECT_ROOT = Path(__file__).resolve().parents[3]
KYOTO_SYMH_PATH = PROJECT_ROOT / "backend" / "data" / "kyoto" / "sym_index" / "symh.parquet"
AVAILABLE_DATASET_STEM = "available_data"
available_data_path = data_file_path(config, AVAILABLE_DATASET_STEM)


logger.info(f"Загрузка Kyoto SYM-H index: {KYOTO_SYMH_PATH}")
logger.info(f"Дополнение available_data: {available_data_path}")

updated = add_symh_index_to_available_data(
    symh_path=KYOTO_SYMH_PATH,
    available_data_path=available_data_path,
)

logger.info(f"available_data перезаписан: {available_data_path}")
logger.info(f"Количество строк: {len(updated)}")
logger.info(f"Диапазон Time: {updated['Time'].min()} — {updated['Time'].max()}")
logger.info(f"Пропуски SYMH: {int(updated['SYMH'].isna().sum())}")
