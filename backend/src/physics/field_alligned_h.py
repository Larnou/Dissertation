import numpy as np
import pandas as pd
from datetime import timedelta

from backend.src.config import AppConfig


class FAHCalculator:
    """
    Класс для вычисления H-параметра в field-aligned системе координат,
    построенной по скользящему среднему магнитного поля (тренду).
    """

    def __init__(self, dataframe: pd.DataFrame, parameters: AppConfig):
        """
        :param dataframe: pd.DataFrame с колонками:
            'Time' - время (datetime)
            'GSM_X', 'GSM_Y', 'GSM_Z' - координаты спутника (км)
            'GSM_Bx', 'GSM_By', 'GSM_Bz' - магнитное поле (нТл)
            'GSM_Ex', 'GSM_Ey', 'GSM_Ez' - электрическое поле (мВ/м)
            'GSM_Vix', 'GSM_Viy', 'GSM_Viz' - скорость ионов (км/с)
        :param parameters: AppConfig c секциями window_filter и h_parameter
            (окно тренда поля — window_filter.high_pass, короткий период в секундах).
        """
        self.data = dataframe
        self.window_sec = self._extract_window_sec(parameters)
        self.noise_e, self.noise_vb = self._extract_noise_levels(parameters)
        self.time_key = 'Time'

    @staticmethod
    def _extract_window_sec(parameters) -> int:
        """Размер окна скользящего среднего для тренда B (сек): короткий период из конфига."""
        return int(parameters.window_filter.high_pass)

    @staticmethod
    def _extract_noise_levels(parameters) -> tuple[float, float]:
        """Извлекает уровни шума для E и -VxB из AppConfig."""
        return float(parameters.h_parameter.noise_e), float(parameters.h_parameter.noise_vb)

    @staticmethod
    def vector_length(comp_list):
        """Евклидова длина вектора по трём компонентам (поэлементно)."""
        return np.sqrt(comp_list[0]**2 + comp_list[1]**2 + comp_list[2]**2)

    def get_interval_borders(self):
        """Определяет размер окна (в точках) и половину окна."""
        start_time = self.data[self.time_key].iloc[0]
        end_window = start_time + timedelta(seconds=self.window_sec)
        df_window = self.data.loc[self.data[self.time_key] < end_window]
        interval = len(df_window)
        half_interval = int(interval / 2)
        return interval, half_interval

    def sliding_mean(self, series):
        """Скользящее среднее с паддингом NaN на краях."""
        interval, _ = self.get_interval_borders()
        conv = np.convolve(series, np.ones(interval), 'valid') / interval
        pad_left = (interval - 1) // 2
        pad_right = interval - 1 - pad_left
        padded = np.pad(conv, (pad_left, pad_right), mode='constant', constant_values=np.nan)
        return padded

    def compute_trend_field(self):
        """Возвращает DataFrame со скользящим средним магнитного поля (трендом)."""
        Bx_mean = self.sliding_mean(self.data['GSM_Bx'])
        By_mean = self.sliding_mean(self.data['GSM_By'])
        Bz_mean = self.sliding_mean(self.data['GSM_Bz'])
        trend_df = pd.DataFrame({
            'GSM_Bx': Bx_mean,
            'GSM_By': By_mean,
            'GSM_Bz': Bz_mean
        })
        return trend_df

    def compute_fa_basis(self, b_field_df, coords_df):
        """
        По заданному полю (тренд или полное) и координатам возвращает базисные векторы ef, ea, er.
        b_field_df: DataFrame с колонками GSM_Bx, GSM_By, GSM_Bz (той же длины, что и coords_df)
        coords_df: DataFrame с колонками GSM_X, GSM_Y, GSM_Z
        """
        Bx = b_field_df['GSM_Bx']
        By = b_field_df['GSM_By']
        Bz = b_field_df['GSM_Bz']
        Bnorm = self.vector_length([Bx, By, Bz])
        ef_x = Bx / Bnorm
        ef_y = By / Bnorm
        ef_z = Bz / Bnorm
        ef = pd.DataFrame({'x': ef_x, 'y': ef_y, 'z': ef_z})

        X = coords_df['GSM_X']
        Y = coords_df['GSM_Y']
        Z = coords_df['GSM_Z']
        ea_x = Y * ef_z - Z * ef_y
        ea_y = Z * ef_x - X * ef_z
        ea_z = X * ef_y - Y * ef_x
        eanorm = self.vector_length([ea_x, ea_y, ea_z])
        ea_x = ea_x / eanorm
        ea_y = ea_y / eanorm
        ea_z = ea_z / eanorm
        ea = pd.DataFrame({'x': ea_x, 'y': ea_y, 'z': ea_z})

        er_x = ea_y * ef_z - ea_z * ef_y
        er_y = ea_z * ef_x - ea_x * ef_z
        er_z = ea_x * ef_y - ea_y * ef_x
        er = pd.DataFrame({'x': er_x, 'y': er_y, 'z': er_z})

        return ef, ea, er

    def compute_econv(self):
        """Вычисляет E_conv = -V × B (в мВ/м)."""
        Vx = self.data['GSM_Vix']
        Vy = self.data['GSM_Viy']
        Vz = self.data['GSM_Viz']
        Bx = self.data['GSM_Bx']
        By = self.data['GSM_By']
        Bz = self.data['GSM_Bz']

        cross_x = Vy * Bz - Vz * By
        cross_y = Vz * Bx - Vx * Bz
        cross_z = Vx * By - Vy * Bx

        Econv_x = -cross_x / 1000.0
        Econv_y = -cross_y / 1000.0
        Econv_z = -cross_z / 1000.0

        return pd.DataFrame({
            'GSM_Ex_conv': Econv_x,
            'GSM_Ey_conv': Econv_y,
            'GSM_Ez_conv': Econv_z
        })


    def compute_g_parameter(self, E_meas, VxB_conv):
        """
        Вычисление G-параметра по формуле:
        G = |E_n| / sqrt(E_n^2 + (VxB)_n^2)

        Параметры:
        - E_meas: измеренное электрическое поле (E_f_meas, E_a_meas, E_r_meas)
        - VxB_conv: конвективное поле (X_f_conv, X_a_conv, X_r_conv)

        Возвращает:
        - G: массив значений G-параметра (от 0 до 1)
        """
        # Добавляем малую константу для избежания деления на ноль
        eps = 1e-10

        # Вычисляем знаменатель
        denominator = np.sqrt(E_meas**2 + VxB_conv**2 + eps)

        # Вычисляем G = |E_meas| / denominator
        G = np.abs(E_meas) / denominator

        # Ограничиваем значения диапазоном [0, 1] (на всякий случай)
        G = np.clip(G, 0.0, 1.0)

        return G

    @staticmethod
    def _project(vec_df, basis_df):
        return vec_df['x']*basis_df['x'] + vec_df['y']*basis_df['y'] + vec_df['z']*basis_df['z']

    def _prepare_fa_components(self):
        interval, half_interval = self.get_interval_borders()
        if half_interval == 0:
            raise ValueError("Окно слишком мало для расчёта скользящего среднего.")

        # Обрезаем исходные данные до внутренней области (удаляем края)
        df_cut = self.data.iloc[half_interval:-half_interval].reset_index(drop=True)
        # Тренд (скользящее среднее) – также обрезаем до той же длины
        trend_df = self.compute_trend_field()          # длина = len(self.data)
        trend_cut = trend_df.iloc[half_interval:-half_interval].reset_index(drop=True)
        # Координаты спутника – обрезаем
        coords_cut = self.data[['GSM_X', 'GSM_Y', 'GSM_Z']].iloc[half_interval:-half_interval].reset_index(drop=True)

        # Базис по тренду на обрезанных данных
        ef, ea, er = self.compute_fa_basis(trend_cut, coords_cut)

        # Измеренное электрическое поле (обрезанное)
        E_meas = df_cut[['GSM_Ex', 'GSM_Ey', 'GSM_Ez']]
        E_meas.columns = ['x', 'y', 'z']

        # Конвективное поле (обрезанное)
        Econv_full = self.compute_econv()
        Econv_cut = Econv_full.iloc[half_interval:-half_interval].reset_index(drop=True)
        Econv_cut.columns = ['x', 'y', 'z']

        # Проекции
        E_f = self._project(E_meas, ef)
        E_a = self._project(E_meas, ea)
        E_r = self._project(E_meas, er)
        X_f = self._project(Econv_cut, ef)
        X_a = self._project(Econv_cut, ea)
        X_r = self._project(Econv_cut, er)

        # Компоненты тренда в FA (обрезанные) – для справки
        # B_f_trend = (trend_cut['GSM_Bx']*ef['x'] + trend_cut['GSM_By']*ef['y'] + trend_cut['GSM_Bz']*ef['z'])
        # B_a_trend = (trend_cut['GSM_Bx']*ea['x'] + trend_cut['GSM_By']*ea['y'] + trend_cut['GSM_Bz']*ea['z'])
        # B_r_trend = (trend_cut['GSM_Bx']*er['x'] + trend_cut['GSM_By']*er['y'] + trend_cut['GSM_Bz']*er['z'])

        return {
            'time': df_cut['Time'],
            'E_f': E_f,
            'E_a': E_a,
            'E_r': E_r,
            'X_f': X_f,
            'X_a': X_a,
            'X_r': X_r,
        }

    def _build_result(self, components, use_noise_mask: bool):
        E_f = components['E_f']
        E_a = components['E_a']
        E_r = components['E_r']
        X_f = components['X_f']
        X_a = components['X_a']
        X_r = components['X_r']

        G_f = self.compute_g_parameter(E_f, X_f)
        G_a = self.compute_g_parameter(E_a, X_a)
        G_r = self.compute_g_parameter(E_r, X_r)

        eps = 1e-10
        if use_noise_mask:
            delta = np.sqrt(self.noise_e**2 + self.noise_vb**2)
            amp_f = np.sqrt(E_f**2 + X_f**2)
            amp_a = np.sqrt(E_a**2 + X_a**2)
            amp_r = np.sqrt(E_r**2 + X_r**2)
            mask_f = amp_f > delta
            mask_a = amp_a > delta
            mask_r = amp_r > delta

            H_f = np.full_like(E_f, np.nan, dtype=float)
            H_a = np.full_like(E_a, np.nan, dtype=float)
            H_r = np.full_like(E_r, np.nan, dtype=float)

            H_f[mask_f] = (E_f[mask_f] - X_f[mask_f])**2 / (E_f[mask_f]**2 + X_f[mask_f]**2 + eps)
            H_a[mask_a] = (E_a[mask_a] - X_a[mask_a])**2 / (E_a[mask_a]**2 + X_a[mask_a]**2 + eps)
            H_r[mask_r] = (E_r[mask_r] - X_r[mask_r])**2 / (E_r[mask_r]**2 + X_r[mask_r]**2 + eps)
        else:
            H_f = (E_f - X_f)**2 / (E_f**2 + X_f**2 + eps)
            H_a = (E_a - X_a)**2 / (E_a**2 + X_a**2 + eps)
            H_r = (E_r - X_r)**2 / (E_r**2 + X_r**2 + eps)

        return pd.DataFrame({
            'Time': components['time'],
            'H_f': H_f,
            'H_a': H_a,
            'H_r': H_r,
            'G_f': G_f,
            'G_a': G_a,
            'G_r': G_r,
            'E_f_meas': E_f,
            'E_a_meas': E_a,
            'E_r_meas': E_r,
            'X_f_conv': X_f,
            'X_a_conv': X_a,
            'X_r_conv': X_r,
        })

    def calculate_h_fa_noise(self):
        components = self._prepare_fa_components()
        return self._build_result(components, use_noise_mask=True)


    def calculate_h_fa(self):
        components = self._prepare_fa_components()
        return self._build_result(components, use_noise_mask=False)