from backend.src.config import get_config, get_logger
from backend.src.io.parquet import data_file_path, read_data_from_parquet, save_data_to_parquet
from backend.src.physics.ion_current_density import IonCurrentDensityModel

logger = get_logger()
config = get_config()

PREPARED_DATASET_STEM = "prepared_data"
VELOCITY_KEYS = ("V_a_meas", "V_r_meas", "V_f_meas")
DENSITY_KEY = "density"

logger.info("Загрузка prepared_data.parquet.")
prepared = read_data_from_parquet(config, PREPARED_DATASET_STEM)
if prepared.empty:
    logger.info("prepared_data пустой. Нечего дополнять.")
else:
    required_columns = {DENSITY_KEY, *VELOCITY_KEYS}
    missing_columns = sorted(required_columns.difference(prepared.columns))
    if missing_columns:
        raise KeyError(
            "В prepared_data не хватает колонок для расчета плотности тока: "
            f"{missing_columns}. Доступные колонки: {list(prepared.columns)}"
        )

    current_density = IonCurrentDensityModel(
        prepared,
        density_key=DENSITY_KEY,
        velocity_keys=VELOCITY_KEYS,
        density_in_cm3=True,
        velocity_in_km_s=True,
    ).model()

    output = prepared.copy()
    output[["J_a", "J_r", "J_f"]] = current_density[["J_a", "J_r", "J_f"]]

    save_data_to_parquet(config, output, PREPARED_DATASET_STEM)
    logger.info(f"Готовый датасет сохранён: {data_file_path(config, PREPARED_DATASET_STEM)}")
    logger.info(f"Добавлены колонки: {['J_a', 'J_r', 'J_f']}")
