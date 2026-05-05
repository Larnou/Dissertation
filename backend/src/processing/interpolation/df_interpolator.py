import numpy as np
import pandas as pd

from backend.src.config import get_logger, progress_bar

logger = get_logger()

class DFInterpolator:

    def __init__(self, dataframes: list[pd.DataFrame]):
        self.dataframes = dataframes

    @staticmethod
    def to_utc_time(series: pd.Series) -> pd.Series:
        """
        Приводит datetime-колонку к UTC tz-naive для безопасных сравнений.
        """

        return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)


    @staticmethod
    def normalize_overlap(overlap: tuple) -> tuple[pd.Timestamp, pd.Timestamp]:
        start = pd.to_datetime(overlap[0], utc=True, errors="coerce")
        end = pd.to_datetime(overlap[1], utc=True, errors="coerce")

        if pd.isna(start) or pd.isna(end):
            raise ValueError(f"Некорректный интервал overlap: {overlap!r}")
        return start.tz_convert("UTC").tz_localize(None), end.tz_convert("UTC").tz_localize(None)


    def slice_by_overlap(self, overlap):
        # Корректировка данных на период времени
        corrected_datasets = []
        overlap_start, overlap_end = self.normalize_overlap(overlap)

        # Ограничение по времени для каждого датасета
        for dataframe in self.dataframes:
            working = dataframe.copy()
            working["Time"] = self.to_utc_time(working["Time"])
            working = working.dropna(subset=["Time"])

            chunk = working[(working["Time"] >= overlap_start) & (working["Time"] <= overlap_end)].reset_index(drop=True)
            corrected_datasets.append(chunk)

        return corrected_datasets


    def build_time_axis(self, datasets: list[pd.DataFrame]) -> pd.DataFrame:
        time_key = "Time"
        # Получение столбцов со временем в каждом датафрейме
        time_series = [data[time_key] for data in datasets]

        # Объединение, сортировка и удаление повторяющихся значений
        axis = pd.DataFrame(pd.concat(time_series))
        axis = axis.sort_values(by=time_key, ascending=True).drop_duplicates(subset=[time_key])

        return axis


    def merge_on_time_axis(self, overlap) -> pd.DataFrame:
        sliced = self.slice_by_overlap(overlap)
        data = self.build_time_axis(sliced)

        # Объединяем датафреймы к колонке со временем
        for index, dataset in enumerate(sliced):
            working_dataset = dataset
            if index > 0 and "L" in working_dataset.columns:
                # Сохраняем L только из первого датасета (ssc_data), чтобы не получать L_x/L_y.
                working_dataset = working_dataset.drop(columns=["L"])
            data = pd.merge(left=data, right=working_dataset, on="Time", how="left")

        return data


    @staticmethod
    def update_longitude_and_mlt(data: pd.DataFrame) -> pd.DataFrame:
        longitude = np.deg2rad(data["Longitude"])
        longitude[~np.isnan(longitude)] = np.unwrap(longitude[~np.isnan(longitude)])
        data["Longitude"] = np.rad2deg(longitude) % 360
        data["MLT"] = ((data["Longitude"].to_numpy() + 180.0) % 360.0) / 15.0
        return data


    def interpolate(self, overlap) -> pd.DataFrame:
        data = self.merge_on_time_axis(overlap=overlap)
        data = self.update_longitude_and_mlt(data)

        # Проводим интерполяцию всех данных (необходимо обновить индекс из-за особенностей работы с колонкой времени)
        data = data.set_index("Time")
        data = data.interpolate(method="linear", limit_direction="both")
        data = data.reset_index()

        return data.reset_index(drop=True)


    def interpolate_many(self, overlaps):
        # Обработка интерполяции датасетов в случае обработки нескольких промежутков одновременно
        data = []

        for overlap in progress_bar(overlaps, desc="[interpolate] интерполяция по интервалам доступности"):
            interpolated_data = self.interpolate(overlap=overlap)
            data.append(interpolated_data)

        return data
