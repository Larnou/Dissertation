from datetime import timedelta

from backend.src.config import get_config
from backend.src.io import DataDownloading
from backend.src.io.paths import DerivedDataset, Instrument
from backend.src.log import get_logger
from backend.src.processing import AvailabilityIntervals
from backend.src.processing.core import intersect_many, summarize_intervals
from backend.src.processing.interpolation import get_or_interpolate_data, interpolate_omn_dataset
from backend.src.processing.services.build_beta_dataset import build_beta_dataset
from backend.src.processing.services.build_shue_dataset import build_shue_dataset

logger = get_logger()
config = get_config()

load_from_cdaweb = False
loader = DataDownloading(config, load_from_cdaweb=load_from_cdaweb)

# Загрузка данных
logger.info(f"Загрузка данных с {'CDAweb' if load_from_cdaweb else 'диска'}:")
ssc_data = loader.get_ssc_data()
fgm_data = loader.get_fgm_data()
esa_ion_data = loader.get_esa_data(particle="ion")
efi_data = loader.get_efi_data()
sta_data = loader.get_sta_data()
omn_data = interpolate_omn_dataset(omn_data=loader.get_omn_data())
mom_data = loader.get_mom_data()
shue_data = build_shue_dataset(ssc_data=ssc_data, omn_data=omn_data)
beta_data = build_beta_dataset(fgm_data=fgm_data, mom_data=mom_data)


# Доступность данных
availability = AvailabilityIntervals(config, show_progress=True)

logger.info("Получение интервалов доступности:")
ssc_intervals = availability.from_dataframe(ssc_data, Instrument.SSC)
fgm_intervals = availability.from_dataframe(fgm_data, Instrument.FGM)
esa_ion_intervals = availability.from_dataframe(esa_ion_data, Instrument.ESA_ION)
efi_intervals = availability.from_dataframe(efi_data, Instrument.EFI)
sta_intervals = availability.from_dataframe(sta_data, Instrument.STA)
shue_intervals = availability.from_dataframe(shue_data, DerivedDataset.SHUE)
beta_intervals = availability.from_dataframe(beta_data, DerivedDataset.BETA)


# Intersections
logger.info("Получение общего набора доступных периодов:")
interval_intersections = intersect_many(
    interval_groups=[
        ssc_intervals,
        sta_intervals,
        efi_intervals,
        fgm_intervals,
        esa_ion_intervals,
        shue_intervals
    ],
    min_duration=timedelta(hours=1),
)

logger.info(f"Итог по пересечениям: {summarize_intervals(interval_intersections)}")


# Интерполяция данных
# Интерполяция, сохранение и загрузка получившихся данных
# Если INTERPOLATE_DATA = False загружаем данные с гуглДиска
# Если INTERPOLATE_DATA = True выполняем новую интерполяцию по данным

INTERPOLATE_DATA = True

logger.info(f"{'Загрузка интерполированных' if not INTERPOLATE_DATA else 'Интерполирование'} данных:")
available_data = get_or_interpolate_data(
    interpolate=INTERPOLATE_DATA,
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
    overlaps=interval_intersections,
    min_minutes=25.0,
)

logger.info(f"Количество доступных датасетов: {len(available_data)}")
if available_data:
    logger.info(f"Колонки первого датасета: {list(available_data[0].columns)}")
