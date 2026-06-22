import logging
from pathlib import Path

from backend.src.io.kyoto_symh import read_kyoto_symh_directory, save_kyoto_symh_to_parquet

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
KYOTO_SYMH_INPUT_DIR = PROJECT_ROOT / "backend" / "data" / "kyoto" / "sym_index"
KYOTO_SYMH_OUTPUT_PATH = KYOTO_SYMH_INPUT_DIR / "symh.parquet"


logger.info(f"Чтение Kyoto SYM-H index из {KYOTO_SYMH_INPUT_DIR}")
symh_data = read_kyoto_symh_directory(KYOTO_SYMH_INPUT_DIR)

save_kyoto_symh_to_parquet(symh_data, KYOTO_SYMH_OUTPUT_PATH)

if symh_data.empty:
    logger.info(f"Kyoto SYM-H index пустой. Файл сохранён: {KYOTO_SYMH_OUTPUT_PATH}")
else:
    logger.info(f"Сохранён Kyoto SYM-H index: {KYOTO_SYMH_OUTPUT_PATH}")
    logger.info(f"Количество строк: {len(symh_data)}")
    logger.info(f"Диапазон Time: {symh_data['Time'].min()} — {symh_data['Time'].max()}")
    logger.info(f"Колонки: {list(symh_data.columns)}")
