from datetime import timedelta

from matplotlib import pyplot as plt

from backend.src.config import config
from backend.src.io import DataDownloading
from backend.src.processing import AvailabilityIntervals
from backend.src.processing.interpolate import get_or_interpolate_data
from backend.src.processing.intersections import intersect_many, save_intervals_csv, summarize_intervals

load_from_cdaweb = False
loader = DataDownloading(config, load_from_cdaweb=load_from_cdaweb)

# Загрузка данных
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

ssc_intervals = availability.from_dataframe(ssc_data, "ssc")
fgm_intervals = availability.from_dataframe(fgm_data, "fgm")
esa_ion_intervals = availability.from_dataframe(esa_ion_data, "esa_ion")
# esa_electron_intervals = availability.from_dataframe(esa_electron_data, "esa_electron")
efi_intervals = availability.from_dataframe(efi_data, "efi")
sta_intervals = availability.from_dataframe(sta_data, "sta")
shue_intervals = availability.from_dataframe(shue_data, "shue")


# Intersections
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

save_intervals_csv(
    intervals=interval_intersections,
    output_path=availability.csv_output_dir / "intersections_availability_periods.csv",
)

print(summarize_intervals(interval_intersections))


# Интерполяция данных
# Интерполяция, сохранение и загрузка получившихся данных
# Если INTERPOLATE_DATA = False загружаем данные с гуглДиска
# Если INTERPOLATE_DATA = True выполняем новую интерпоялцию по данным

INTERPOLATE_DATA = False

available_data = get_or_interpolate_data(
    interpolate=INTERPOLATE_DATA,
    save_to_disk=True,
    parameters=config,
    raw_datasets=[
        ssc_data,
        fgm_data,
        esa_ion_data,
        efi_data,
        sta_data,
        shue_data,
    ],
    overlaps=interval_intersections,
    min_minutes=25.0,
)

print(len(available_data))
print(available_data[0].columns)