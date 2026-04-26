from datetime import timedelta
from typing import List

import numpy as np
import pandas as pd

from backend.src.config import get_logger, progress, AppConfig
from backend.src.io.parquet import read_data_from_parquet, save_data_to_parquet

logger = get_logger()

class DFInterpolator:

    def __init__(self, dataframes: list):
        self.dataframes = dataframes

    @staticmethod
    def _to_utc_naive_time(series: pd.Series) -> pd.Series:
        """Приводит datetime-колонку к UTC tz-naive для безопасных сравнений."""
        return (
            pd.to_datetime(series, utc=True, errors="coerce")
            .dt.tz_convert("UTC")
            .dt.tz_localize(None)
        )

    @staticmethod
    def _normalize_overlap(overlap: tuple) -> tuple[pd.Timestamp, pd.Timestamp]:
        start = pd.to_datetime(overlap[0], utc=True, errors="coerce")
        end = pd.to_datetime(overlap[1], utc=True, errors="coerce")
        if pd.isna(start) or pd.isna(end):
            raise ValueError(f"Некорректный интервал overlap: {overlap!r}")
        return start.tz_convert("UTC").tz_localize(None), end.tz_convert("UTC").tz_localize(None)

    def update_dataframes_by_date(self, overlap):
        # Корректировка данных на период времени
        corrected_datasets = []
        overlap_start, overlap_end = self._normalize_overlap(overlap)
        # Ограничение по времени для каждого датасета
        for dataframe in self.dataframes:
            working = dataframe.copy()
            working["Time"] = self._to_utc_naive_time(working["Time"])
            working = working.dropna(subset=["Time"])

            dataset = working[
                (working["Time"] >= overlap_start) & (working["Time"] <= overlap_end)
            ].reset_index(drop=True)
            corrected_datasets.append(dataset)

        return corrected_datasets




    def set_time_column(self, datasets):
        time_key = 'Time'
        # Получение столбцов со временем в каждом датафрейме
        dataframes = [data[time_key] for data in datasets]

        # Объединение, сортировка и удаление повторяющихся значений
        time_column = pd.DataFrame(pd.concat(dataframes))
        time_column = time_column.sort_values(by = time_key, ascending = True)
        time_column = time_column.drop_duplicates(subset = [time_key])

        return time_column



    def get_merged_data(self, overlap):
        corrected_datasets = self.update_dataframes_by_date(overlap)
        data = self.set_time_column(corrected_datasets)

        # Объединяем датафреймы к колонке со временем
        for index, dataset in enumerate(corrected_datasets):
            working_dataset = dataset
            if index > 0 and "L" in working_dataset.columns:
                # Сохраняем L только из первого датасета (ssc_data), чтобы не получать L_x/L_y.
                working_dataset = working_dataset.drop(columns=["L"])
            data = pd.merge(left=data, right=working_dataset, on='Time', how='left')

        return data



    def interpolate(self, overlap):
        data = self.get_merged_data(overlap = overlap)
        # Проводим операции над столбцом с долготой\широтой (с периодичностью в 2pi)
        # Переводим градусы в радианы
        longitude = np.deg2rad(data['Longitude'])

        # Делаем анврап (сохранить направление изменения градусов, то есть сохраняем переходы 359-0-1 и 1-0-359)
        longitude[~np.isnan(longitude)] = np.unwrap(longitude[~np.isnan(longitude)])

        # После анврапа переводим радианы обратно в градусы
        data['Longitude'] = np.rad2deg(longitude)

        # Проводим интерполяцию всех данных (необходимо обновить индекс из-за особенностей работы с колонкой времени)
        data = data.set_index('Time')
        data = data.interpolate(method='linear', limit_direction='both')
        data = data.reset_index()

        # Доводим лонгитьюд до нужных значением путём нахождения остатка от деления на 360
        data['Longitude'] %= 360
        # Считаем значение МЛТ, для упрощения подсчётов в будущем
        data['MLT'] = ((np.array(list(data['Longitude'])) + 180) % 360) / 15

        return data.reset_index(drop=True)


    def interpolate_lists(self, overlaps):
        # Обработка интерполяции датасетов в случае обработки нескольких промежутков одновременно
        data = []

        for overlap in progress(overlaps, desc="[interpolate] интерполяция по интервалам доступности"):
            interpolated_data = self.interpolate(overlap=overlap)
            data.append(interpolated_data)

        return data


def get_or_interpolate_data(
    interpolate: bool,
    parameters: AppConfig,
    raw_datasets: List[pd.DataFrame] | None = None,
    overlaps: list | None = None,
    min_minutes: float = 25.0,
) -> List[pd.DataFrame]:
    filename = "available_data.parquet"

    if interpolate:
        if not raw_datasets:
            raise ValueError("Для интерполяции необходим список датафреймов `raw_datasets`")
        if not overlaps:
            raise ValueError("Для интерполяции необходим список интервалов `overlaps`")

        data = DFInterpolator(dataframes=raw_datasets).interpolate_lists(overlaps=overlaps)
        logger.info(f"Интерполяция завершена. Получено датасетов: {len(data)}")
        data = filter_datasets_by_min_duration(data, min_minutes=min_minutes)
        logger.info(
            f"Фильтрация интерполированных данных завершена. "
            f"Порог: {min_minutes} минут. Осталось датасетов: {len(data)}"
        )

        if data:
            combined = pd.concat(data).reset_index(drop=True)
            save_data_to_parquet(parameters, combined, filename)
        return data

    else:
        loaded = read_data_from_parquet(parameters, filename, read_as_list=True)
        logger.info("Данные загружены с диска без доп. фильтрации по длительности.")
        return loaded


def filter_datasets_by_min_duration(
    datasets: List[pd.DataFrame],
    min_minutes: float = 25.0,
) -> List[pd.DataFrame]:
    min_delta = timedelta(minutes=min_minutes)
    filtered = []

    for df in datasets:
        if df.empty:
            continue
        duration = df["Time"].max() - df["Time"].min()
        if duration >= min_delta:
            filtered.append(df)

    logger.info(f"Фильтрация завершена. Осталось датасетов: {len(filtered)}")
    return filtered