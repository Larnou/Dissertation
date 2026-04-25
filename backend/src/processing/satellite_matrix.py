import math

import numpy as np
import pandas as pd
from backend.src.config import progress


class SatelliteMatrix:

    def __init__(self, satellite_dataframe: pd.DataFrame, for_instrument: str):
        self.satellite_dataframe = satellite_dataframe
        self.for_instrument = for_instrument
        self.lshell_range = 16
        self.radian_range = 24


    @staticmethod
    def get_time_diff(dataframe: pd.DataFrame) -> list:
        time_diff = [item.total_seconds() for item in dataframe['Time'].diff()]
        time_diff[0] = time_diff[1]
        return time_diff

    # TODO: Написать сохранение матриц в файл

    def get_availability_matrix(self, periods: list, corrector = True):
        # Создаём матрицу для заполнения
        availability_matrix = np.zeros((self.lshell_range, self.radian_range), dtype = int)

        # Обход по каждой паре start-end в periods
        for period in progress(
            periods,
            desc=f"[matrix] обработка периодов доступности ({self.for_instrument})",
        ):
            start = period[0]
            end = period[1]

            time_column = self.satellite_dataframe['Time']
            time_range = (time_column >= start) & (time_column <= end)
            availability_data = self.satellite_dataframe.loc[time_range].reset_index(drop = True)

            # список разности времени нахождения между значениями
            satellite_time_diff = self.get_time_diff(availability_data)

            # Получение координат на плоскости L-MLT
            for i in range(len(availability_data)):
                # перевод широты в правильные значения (отличается на pi)
                longitude = (availability_data['Longitude'][i] + 180) % 360

                # определение координат ячейки для заполнения данных
                radian_ind = math.floor(longitude // 15)
                lshell_ind = math.floor(availability_data['L'][i])
                availability_matrix[lshell_ind][radian_ind] += satellite_time_diff[i]



        # перевод полученного времени в часы
        availability_matrix = np.array(availability_matrix)
        availability_matrix = (availability_matrix / 3600).astype(float)

        # Нормализация для отображения графиков
        if corrector:
            availability_matrix[availability_matrix == 0] = -1

        return availability_matrix


    def get_relative_availability_matrix(self, periods, satellite_matrix: np.array):
        availability_matrix = self.get_availability_matrix(periods, False)

        # Получение относительной матрицы доступности данных
        with np.errstate(divide='ignore', invalid='ignore'):
            relative_matrix = availability_matrix / satellite_matrix

        # Замена inf/nan на 0 (опционально)
        relative_matrix = np.nan_to_num(relative_matrix, nan=0.0, posinf=0.0, neginf=0.0)
        relative_matrix[relative_matrix == 0] = -1

        return relative_matrix



    def get_satellite_matrix(self):

        # создание матрицы нужной размерности под полярный график
        satellite_matrix = np.zeros((self.lshell_range, self.radian_range), dtype = int)
        # список разности времени нахождения между значениями
        satellite_time_diff = self.get_time_diff(self.satellite_dataframe)


        for i in progress(
            range(len(self.satellite_dataframe)),
            desc="[matrix] подсчёт матрицы орбитального времени",
        ):
            # перевод широты в правильные значения (отличается на pi)
            longitude = (self.satellite_dataframe['Longitude'][i] + 180) % 360

            # определение координат ячейки для заполнения данных
            radian_ind = math.floor(longitude // 15)
            lshell_ind = math.floor(self.satellite_dataframe['L'][i])
            satellite_matrix[lshell_ind][radian_ind] += satellite_time_diff[i]


        # перевод полученного времени в часы
        satellite_matrix = np.array(satellite_matrix)
        satellite_matrix = (satellite_matrix / 3600).astype(float)
        satellite_matrix[satellite_matrix == 0] = -1

        return satellite_matrix