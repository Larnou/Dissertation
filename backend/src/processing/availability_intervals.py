from dataclasses import dataclass

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.processing.services.availability import AvailabilityService
from backend.src.processing.utils.intervals_view import IntervalsView


@dataclass(frozen=True, slots=True)
class AvailabilityIntervals:
    """
    Считает интервалы доступности и пишет CSV в periods/ события.
    """

    config: AppConfig
    time_col: str = "Time"
    show_progress: bool = True

    def from_dataframe(self, dataframe: pd.DataFrame, data_type: str) -> IntervalsView:
        """
        Ищет непрерывные куски по правилу источника и сохраняет CSV.

        Args:
            dataframe: таблица с колонкой Time и полем из RULES.
            data_type: стем источника, например Instrument.FGM или DerivedDataset.SHUE.
        """

        service = AvailabilityService(time_col=self.time_col, show_progress=self.show_progress)
        return service.from_dataframe(dataframe, data_type, config=self.config, save_csv=True)
