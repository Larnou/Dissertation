from datetime import timedelta
import sys
from typing import List

import numpy as np
import pandas as pd
from loguru import logger
from tqdm import tqdm

from backend.src.io.parquet import read_data_from_parquet, save_data_to_parquet


class DFInterpolator:

    def __init__(self, dataframes: list):
        self.dataframes = dataframes



    def update_dataframes_by_date(self, overlap):
        # Корректировка данных на период времени
        corrected_datasets = []
        # Ограничение по времени для каждого датасета
        for dataframe in self.dataframes:
            dataset = dataframe[(dataframe['Time'] >= overlap[0]) & (dataframe['Time'] <= overlap[1])].reset_index(drop=True)
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
        for dataset in corrected_datasets:
            data = pd.merge(left=data, right=dataset, on='Time', how='left')

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


        # ===============
        # Уменьшаем количество индексов, так как они негативно влияют на данные
        # listy = list(fgm_data['Time'])
        # data = data.loc[data['Time'].isin(listy)]

        return data.reset_index(drop=True)


    def interpolate_lists(self, overlaps):
        # Обработка интерполяции датасетов в случае обработки нескольких промежутков одновременно
        data = []

        for overlap in tqdm(overlaps, desc='Интерполяция по интервалам доступности', file = sys.stdout):
            interpolated_data = self.interpolate(overlap=overlap)
            data.append(interpolated_data)

        return data


def get_or_interpolate_data(
        interpolate: bool = False,
        save_to_disk: bool = True,
        parameters: dict | None = None,
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

        if save_to_disk and data:
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