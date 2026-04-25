from datetime import timedelta

from backend.src.config import config
from backend.src.config import get_logger
from backend.src.io import DataDownloading
from backend.src.processing import AvailabilityIntervals
from backend.src.processing.interpolate import get_or_interpolate_data
from backend.src.processing.intersections import intersect_many, summarize_intervals

logger = get_logger()

load_from_cdaweb = False
loader = DataDownloading(config, load_from_cdaweb=load_from_cdaweb)

# Загрузка данных
logger.info(f"Загрузка данных с {'CDAweb' if load_from_cdaweb else 'диска'}:")
ssc_data = loader.get_ssc_data()
fgm_data = loader.get_fgm_data()
esa_ion_data = loader.get_esa_data(particle="ion")
# esa_electron_data = loader.get_esa_data(particle="electron")
efi_data = loader.get_efi_data()
sta_data = loader.get_sta_data()
omn_data = loader.get_omn_data()
shue_data = loader.get_shue_data()


# Доступность данных
availability = AvailabilityIntervals(show_progress=True)

logger.info("Получение интервалов доступности:")
ssc_intervals = availability.from_dataframe(ssc_data, "ssc")
fgm_intervals = availability.from_dataframe(fgm_data, "fgm")
esa_ion_intervals = availability.from_dataframe(esa_ion_data, "esa_ion")
# esa_electron_intervals = availability.from_dataframe(esa_electron_data, "esa_electron")
efi_intervals = availability.from_dataframe(efi_data, "efi")
sta_intervals = availability.from_dataframe(sta_data, "sta")
shue_intervals = availability.from_dataframe(shue_data, "shue")


# Intersections
logger.info(f"Получение общего набора доступных периодов:")
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



intervals_list = [
    {'intervals': ssc_intervals, "data_type": "ssc"},
    {'intervals': fgm_intervals, "data_type": "fgm"},
    {'intervals': esa_ion_intervals, "data_type": "esa_ion"},
    {'intervals': efi_intervals, "data_type": "efi"},
    {'intervals': sta_intervals, "data_type": "sta"},
    {'intervals': interval_intersections, "data_type": "intersections"},
]

availability.show_intervals(ssc_data, intervals_list)