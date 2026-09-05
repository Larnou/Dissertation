from datetime import timedelta

import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io import DataDownloading
from backend.src.io.paths import DerivedDataset, Instrument
from backend.src.log import get_logger
from backend.src.processing import AvailabilityIntervals
from backend.src.processing.core import intersect_many, summarize_intervals
from backend.src.processing.interpolation import get_or_interpolate_data, interpolate_omn_dataset
from backend.src.processing.services.build_beta_dataset import build_beta_dataset
from backend.src.processing.services.build_shue_dataset import build_shue_dataset

logger = get_logger()


def run_get_data(
    config: AppConfig,
    *,
    load_from_cdaweb: bool = False,
    interpolate: bool = True,
    min_overlap: timedelta = timedelta(hours=1),
    min_minutes: float = 25.0,
) -> list[pd.DataFrame]:
    """
    Собирает available_data: сырьё → Shue/β → интервалы → пересечение → интерполяция.

    β считается и пишется в таблицу, но пересечение окон не режет:
    дыр у β больше, чем у остальных источников.

    Args:
        config: интервал, спутник, каталоги.
        load_from_cdaweb: True — качать с CDAWeb; False — читать parquet события.
        interpolate: True — заново склеить и записать available_data;
            False — прочитать уже готовый parquet.
        min_overlap: минимальная длина общего окна источников.
        min_minutes: отбросить интерполированные куски короче этого порога.
    """

    loader = DataDownloading(config, load_from_cdaweb=load_from_cdaweb)
    source = "CDAweb" if load_from_cdaweb else "диска"
    logger.info(f"Загрузка данных с {source}:")

    ssc_data = loader.get_ssc_data()
    fgm_data = loader.get_fgm_data()
    esa_ion_data = loader.get_esa_data(particle="ion")
    efi_data = loader.get_efi_data()
    sta_data = loader.get_sta_data()
    omn_data = interpolate_omn_dataset(omn_data=loader.get_omn_data())
    mom_data = loader.get_mom_data()
    shue_data = build_shue_dataset(ssc_data=ssc_data, omn_data=omn_data)
    beta_data = build_beta_dataset(fgm_data=fgm_data, mom_data=mom_data)

    availability = AvailabilityIntervals(config, show_progress=True)
    logger.info("Получение интервалов доступности:")
    ssc_intervals = availability.from_dataframe(ssc_data, Instrument.SSC)
    fgm_intervals = availability.from_dataframe(fgm_data, Instrument.FGM)
    esa_ion_intervals = availability.from_dataframe(esa_ion_data, Instrument.ESA_ION)
    efi_intervals = availability.from_dataframe(efi_data, Instrument.EFI)
    sta_intervals = availability.from_dataframe(sta_data, Instrument.STA)
    shue_intervals = availability.from_dataframe(shue_data, DerivedDataset.SHUE)
    availability.from_dataframe(beta_data, DerivedDataset.BETA)

    logger.info("Получение общего набора доступных периодов:")
    overlaps = intersect_many(
        interval_groups=[
            ssc_intervals,
            sta_intervals,
            efi_intervals,
            fgm_intervals,
            esa_ion_intervals,
            shue_intervals,
        ],
        min_duration=min_overlap,
    )
    logger.info(f"Итог по пересечениям: {summarize_intervals(overlaps)}")

    action = "Интерполирование" if interpolate else "Загрузка интерполированных"
    logger.info(f"{action} данных:")
    available_data = get_or_interpolate_data(
        interpolate=interpolate,
        parameters=config,
        raw_datasets=[
            ssc_data,
            fgm_data,
            esa_ion_data,
            efi_data,
            sta_data,
            shue_data,
            beta_data,
        ],
        overlaps=overlaps,
        min_minutes=min_minutes,
    )

    logger.info(f"Количество доступных датасетов: {len(available_data)}")
    if available_data:
        logger.info(f"Колонки первого датасета: {list(available_data[0].columns)}")
    return available_data


def main() -> None:
    run_get_data(
        get_config(),
        load_from_cdaweb=False,
        interpolate=True,
    )


if __name__ == "__main__":
    main()
