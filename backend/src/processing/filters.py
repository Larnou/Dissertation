from dataclasses import dataclass

import numpy as np
import pandas as pd

from backend.src.config import AppConfig


@dataclass(frozen=True, slots=True)
class DataFiltration:
    """
    Двухступенчатая фильтрация E/V через скользящие средние.

    В `AppConfig.window_filter` поля — длительности периода колебаний в секундах:
    `low_pass` — длинный период (широкое окно, первая ветвь: вычитание сглаженного),
    `high_pass` — короткий период (узкое окно, вторая ветвь: сглаживание остатка).
    """

    config: AppConfig

    FILTER_COLUMNS: tuple[str, ...] = (
        "GSM_Ex",
        "GSM_Ey",
        "GSM_Ez",
        "GSM_Vix",
        "GSM_Viy",
        "GSM_Viz",
    )
    REQUIRED_META_COLUMNS: tuple[str, ...] = (
        "Time",
        "GSM_Bx",
        "GSM_By",
        "GSM_Bz",
        "GSM_X",
        "GSM_Y",
        "GSM_Z",
    )
    OPTIONAL_META_COLUMNS: tuple[str, ...] = (
        "L",
        "MLT",
    )
    WIDTH_MODIFIER: float = 0.20

    @staticmethod
    def _ensure_odd(window_size: int) -> int:
        size = max(window_size, 3)
        return size if size % 2 == 1 else size + 1

    @staticmethod
    def _cut_by_half_window(data: pd.DataFrame, half_window_size: int) -> pd.DataFrame:
        if len(data) <= 2 * half_window_size:
            return data.iloc[0:0].copy()
        return data.iloc[half_window_size:-half_window_size].reset_index(drop=True)

    def _smooth(self, values: pd.Series, window_size: int) -> np.ndarray:
        window = np.ones(window_size, dtype=float) / float(window_size)
        return np.convolve(values.to_numpy(dtype=float), window, mode="same")

    def _apply(self, data: pd.DataFrame, window_size: int, detrend: bool) -> pd.DataFrame:
        filtered_data: dict[str, pd.Series | np.ndarray] = {
            column: data[column].to_numpy() for column in self.REQUIRED_META_COLUMNS
        }
        for column in self.OPTIONAL_META_COLUMNS:
            filtered_data[column] = (
                data[column].to_numpy() if column in data.columns else np.full(len(data), np.nan)
            )

        for column in self.FILTER_COLUMNS:
            smoothed = self._smooth(data[column], window_size)
            source = data[column].to_numpy(dtype=float)
            filtered_data[column] = source - smoothed if detrend else smoothed

        return pd.DataFrame(filtered_data)

    def _window_sizes(self, data: pd.DataFrame) -> tuple[int, int]:
        step_seconds = (
            pd.to_datetime(data["Time"], utc=True, errors="coerce")
            .sort_values()
            .diff()
            .dt.total_seconds()
            .dropna()
            .mean()
        )
        if pd.isna(step_seconds) or step_seconds <= 0:
            step_seconds = 1.0

        period_long_sec = float(self.config.window_filter.low_pass)
        period_short_sec = float(self.config.window_filter.high_pass)

        wide_window = self._ensure_odd(int((period_long_sec * (1 + self.WIDTH_MODIFIER)) / step_seconds))
        narrow_window = self._ensure_odd(int((period_short_sec * (1 - self.WIDTH_MODIFIER)) / step_seconds))
        narrow_window = min(narrow_window, wide_window)

        return wide_window, narrow_window

    def window_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        if data.empty:
            return data.copy()

        required_columns = set(self.REQUIRED_META_COLUMNS).union(self.FILTER_COLUMNS)
        missing = required_columns.difference(data.columns)
        if missing:
            missing_sorted = ", ".join(sorted(missing))
            raise ValueError(f"Для фильтрации не хватает колонок: {missing_sorted}")

        working = data.reset_index(drop=True)
        wide_window, narrow_window = self._window_sizes(working)

        wide_filtered = self._apply(working, wide_window, detrend=True)
        wide_cut = self._cut_by_half_window(wide_filtered, wide_window // 2)
        if wide_cut.empty:
            return wide_cut

        narrow_filtered = self._apply(wide_cut, narrow_window, detrend=False)
        return self._cut_by_half_window(narrow_filtered, narrow_window // 2)
