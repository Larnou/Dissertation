from backend.src.config import get_config
from backend.src.io.paths import EventDataset, KyotoIndex, paths
from backend.src.log import get_logger
from backend.src.processing.interpolation import add_symh_index_to_available_data

logger = get_logger()
config = get_config()
resolver = paths(config)

kyoto_symh_path = resolver.kyoto.index_parquet(KyotoIndex.SYMH)
available_data_path = resolver.dataset(EventDataset.AVAILABLE)

logger.info(f"Загрузка Kyoto SYM-H index: {kyoto_symh_path}")
logger.info(f"Дополнение available_data: {available_data_path}")

updated = add_symh_index_to_available_data(
    symh_path=kyoto_symh_path,
    available_data_path=available_data_path,
)

logger.info(f"available_data перезаписан: {available_data_path}")
logger.info(f"Количество строк: {len(updated)}")
logger.info(f"Диапазон Time: {updated['Time'].min()} — {updated['Time'].max()}")
logger.info(f"Пропуски SYMH: {int(updated['SYMH'].isna().sum())}")
