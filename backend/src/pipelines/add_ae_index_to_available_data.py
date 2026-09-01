from backend.src.config import get_config, get_logger
from backend.src.io.paths import EventDataset, KyotoIndex, paths
from backend.src.processing.interpolation import add_ae_index_to_available_data

logger = get_logger()
config = get_config()
resolver = paths(config)

kyoto_ae_path = resolver.kyoto.index_parquet(KyotoIndex.AE)
available_data_path = resolver.dataset(EventDataset.AVAILABLE)

logger.info(f"Загрузка Kyoto AE index: {kyoto_ae_path}")
logger.info(f"Дополнение available_data: {available_data_path}")

updated = add_ae_index_to_available_data(
    ae_path=kyoto_ae_path,
    available_data_path=available_data_path,
)

logger.info(f"available_data перезаписан: {available_data_path}")
logger.info(f"Количество строк: {len(updated)}")
logger.info(f"Диапазон Time: {updated['Time'].min()} — {updated['Time'].max()}")
logger.info(f"Пропуски AE: {int(updated['AE'].isna().sum())}")
