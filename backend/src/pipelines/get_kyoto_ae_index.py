from backend.src.config import get_config, get_logger
from backend.src.io.kyoto_ae import read_kyoto_ae_directory, save_kyoto_ae_to_parquet
from backend.src.io.paths import KyotoIndex, paths

logger = get_logger()
config = get_config()
resolver = paths(config)

kyoto_ae_input_dir = resolver.kyoto.source_dir(KyotoIndex.AE)
kyoto_ae_output_path = resolver.kyoto.index_parquet(KyotoIndex.AE)

logger.info(f"Чтение Kyoto AE index из {kyoto_ae_input_dir}")
ae_data = read_kyoto_ae_directory(kyoto_ae_input_dir)

save_kyoto_ae_to_parquet(ae_data, kyoto_ae_output_path)

if ae_data.empty:
    logger.info(f"Kyoto AE index пустой. Файл сохранён: {kyoto_ae_output_path}")
else:
    logger.info(f"Сохранён Kyoto AE index: {kyoto_ae_output_path}")
    logger.info(f"Количество строк: {len(ae_data)}")
    logger.info(f"Диапазон Time: {ae_data['Time'].min()} — {ae_data['Time'].max()}")
    logger.info(f"Колонки: {list(ae_data.columns)}")
