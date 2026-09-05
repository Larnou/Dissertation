import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.parquet import save_data_to_parquet
from backend.src.io.paths import EventDataset
from backend.src.log import get_logger
from backend.src.processing import build_prepared_datasets
from backend.src.processing.interpolation import get_or_interpolate_data

logger = get_logger()


def run_get_h_data(
    config: AppConfig,
    *,
    interpolate: bool = False,
    use_noise_mask: bool = False,
    min_minutes: float = 25.0,
) -> pd.DataFrame:
    """
    Собирает H-ready таблицу из available_data и пишет prepared parquet.

    Args:
        config: интервал, спутник, каталоги.
        interpolate: True — заново склеить available_data; для этого нужны сырьё
            и пересечения, поэтому здесь обычно False (читает готовый parquet).
        use_noise_mask: маска шума при сборке prepared.
        min_minutes: порог длительности при интерполяции.
    """

    action = "Интерполирование" if interpolate else "Загрузка интерполированных"
    logger.info(f"{action} данных:")
    available_data = get_or_interpolate_data(
        interpolate=interpolate,
        parameters=config,
        raw_datasets=None,
        overlaps=None,
        min_minutes=min_minutes,
    )
    logger.info(f"Количество доступных датасетов: {len(available_data)}")

    prepared_datasets = build_prepared_datasets(
        available_data,
        config,
        use_noise=use_noise_mask,
    )
    logger.info(f"Подготовлено H-ready датасетов: {len(prepared_datasets)}")

    combined_prepared = pd.concat(prepared_datasets, ignore_index=True)
    output_path = save_data_to_parquet(config, combined_prepared, EventDataset.PREPARED)
    logger.info(f"Готовый датасет сохранён: {output_path}")
    logger.info(f"Колонки готового датасета: {list(combined_prepared.columns)}")
    return combined_prepared


def main() -> None:
    run_get_h_data(
        get_config(),
        interpolate=False,
        use_noise_mask=False,
    )


if __name__ == "__main__":
    main()
