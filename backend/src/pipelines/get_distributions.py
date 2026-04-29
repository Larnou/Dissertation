from backend.src.config import get_config, get_logger
from backend.src.io.matrices import save_distribution_matrices
from backend.src.io.parquet import read_data_from_parquet
from backend.src.processing.distribution import Distributions

logger = get_logger()
config = get_config()


logger.info("Загрузка prepared_data.parquet как списка датасетов.")
prepared_datasets = read_data_from_parquet(
    config=config,
    dataset_stem="prepared_data",
    read_as_list=True,
)
logger.info(f"Загружено датасетов: {len(prepared_datasets)}")


distribution_calculator = Distributions(config)
param_distr = distribution_calculator.collect(prepared_datasets)

reducer = "mean"
distribution_maps = distribution_calculator.build_maps(param_distr, reducer=reducer)
saved_paths = save_distribution_matrices(config, distribution_maps, reducer=reducer)

for parameter_name, path in saved_paths.items():
    logger.info(f"Сохранено распределение {parameter_name}: {path}")

