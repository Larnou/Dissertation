import logging
from pathlib import Path

from backend.src.io.kyoto_ae import read_kyoto_ae_directory, save_kyoto_ae_to_parquet

logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
KYOTO_AE_INPUT_DIR = PROJECT_ROOT / "backend" / "data" / "kyoto" / "ae_index"
KYOTO_AE_OUTPUT_PATH = KYOTO_AE_INPUT_DIR / "ae.parquet"


logger.info(f"Чтение Kyoto AE index из {KYOTO_AE_INPUT_DIR}")
ae_data = read_kyoto_ae_directory(KYOTO_AE_INPUT_DIR)

save_kyoto_ae_to_parquet(ae_data, KYOTO_AE_OUTPUT_PATH)

if ae_data.empty:
    logger.info(f"Kyoto AE index пустой. Файл сохранён: {KYOTO_AE_OUTPUT_PATH}")
else:
    logger.info(f"Сохранён Kyoto AE index: {KYOTO_AE_OUTPUT_PATH}")
    logger.info(f"Количество строк: {len(ae_data)}")
    logger.info(f"Диапазон Time: {ae_data['Time'].min()} — {ae_data['Time'].max()}")
    logger.info(f"Колонки: {list(ae_data.columns)}")
