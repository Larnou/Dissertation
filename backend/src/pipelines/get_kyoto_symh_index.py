from backend.src.config import get_config
from backend.src.io.kyoto_symh import read_kyoto_symh_directory, save_kyoto_symh_to_parquet
from backend.src.io.paths import KyotoIndex, paths
from backend.src.log import get_logger

logger = get_logger()
config = get_config()
resolver = paths(config)

kyoto_symh_input_dir = resolver.kyoto.source_dir(KyotoIndex.SYMH)
kyoto_symh_output_path = resolver.kyoto.index_parquet(KyotoIndex.SYMH)

logger.info(f"Чтение Kyoto SYM-H index из {kyoto_symh_input_dir}")
symh_data = read_kyoto_symh_directory(kyoto_symh_input_dir)

save_kyoto_symh_to_parquet(symh_data, kyoto_symh_output_path)

if symh_data.empty:
    logger.info(f"Kyoto SYM-H index пустой. Файл сохранён: {kyoto_symh_output_path}")
else:
    logger.info(f"Сохранён Kyoto SYM-H index: {kyoto_symh_output_path}")
    logger.info(f"Количество строк: {len(symh_data)}")
    logger.info(f"Диапазон Time: {symh_data['Time'].min()} — {symh_data['Time'].max()}")
    logger.info(f"Колонки: {list(symh_data.columns)}")
