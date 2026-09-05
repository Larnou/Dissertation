from backend.src.config import get_config
from backend.src.io.matrices import save_distribution_matrices, save_raw_distribution_long
from backend.src.io.names import Reducer
from backend.src.io.parquet import read_data_from_parquet
from backend.src.io.paths import EventDataset
from backend.src.log import get_logger
from backend.src.processing.distribution import Distributions
from backend.src.processing.utils.time_gaps import split_dataframe_by_time_gaps

logger = get_logger()
config = get_config()

logger.info("Загрузка prepared_data.parquet как списка датасетов.")
prepared_datasets = split_dataframe_by_time_gaps(
    read_data_from_parquet(config=config, source=EventDataset.PREPARED)
)
logger.info(f"Загружено датасетов: {len(prepared_datasets)}")

distribution_calculator = Distributions()
param_distr = distribution_calculator.collect(prepared_datasets)
raw_long_paths = save_raw_distribution_long(config, param_distr.as_grids())
for parameter_name, raw_long_path in raw_long_paths.items():
    logger.info(f"Сохранены сырые распределения (long) для {parameter_name}: {raw_long_path}")

reducers = [Reducer.MEAN, Reducer.MEDIAN, Reducer.Q25, Reducer.Q75]
for reducer in reducers:
    distribution_maps = distribution_calculator.build_maps(param_distr, reducer=reducer)
    saved_paths = save_distribution_matrices(config, distribution_maps, reducer=reducer)

    for parameter_name, path in saved_paths.items():
        logger.info(f"Сохранено распределение {parameter_name}: {path}")
