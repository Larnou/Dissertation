from dataclasses import dataclass

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.processing.services.availability import AvailabilityService
from backend.src.processing.utils.intervals_view import IntervalsView
from frontend.plot.availability_plot import (
    show_availability,
    show_combined_intervals,
)


@dataclass(frozen=True, slots=True)
class AvailabilityIntervals:
    """
    Совместимый фасад поверх разнесённых слоёв:
    - processing.services для вычисления интервалов
    - visualization для отрисовки
    - processing.io для сохранения CSV
    """

    config: AppConfig
    time_col: str = "Time"
    show_progress: bool = True

    def from_dataframe(self, dataframe: pd.DataFrame, data_type: str) -> IntervalsView:
        service = AvailabilityService(time_col=self.time_col, show_progress=self.show_progress)
        return service.from_dataframe(dataframe, data_type, config=self.config, save_csv=True)

    def show(self, dataframe: pd.DataFrame, intervals: IntervalsView, data_type: str) -> None:
        show_availability(dataframe, intervals, data_type)

    def show_intervals(self, dataframe: pd.DataFrame, intervals_list: list[dict]) -> None:
        show_combined_intervals(dataframe, intervals_list)
