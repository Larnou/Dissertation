from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd

from backend.src.config import progress_bar
from backend.src.config.schemas import AppConfig
from backend.src.processing.io.intervals_storage import save_source_periods
from backend.src.io.paths import AvailabilitySource
from backend.src.processing.utils.intervals_view import (
    IntervalsView,
    RULES,
    TimeInterval,
)


@dataclass(frozen=True, slots=True)
class AvailabilityService:
    time_col: str = "Time"
    show_progress: bool = True

    def from_dataframe(
        self,
        dataframe: pd.DataFrame,
        data_type: AvailabilitySource,
        *,
        config: AppConfig | None = None,
        save_csv: bool = False,
    ) -> IntervalsView:
        rule = RULES.get(data_type)

        working = dataframe.copy()
        working[self.time_col] = (
            pd.to_datetime(working[self.time_col], utc=True, errors="coerce")
            .dt.tz_convert("UTC")
            .dt.tz_localize(None)
        )
        working = working.dropna(subset=[rule.required_col])
        working = working.dropna(subset=[self.time_col])

        min_hole_s = float(rule.min_hole_seconds)
        min_interval_s = float(rule.min_interval_seconds)

        working = working.sort_values(self.time_col).reset_index(drop=True)
        time_diff_s = working[self.time_col].diff().dt.total_seconds()

        break_inner = np.flatnonzero(time_diff_s.to_numpy() >= min_hole_s)
        break_points = np.concatenate(([0], break_inner, [len(working)]))

        min_delta = timedelta(seconds=min_interval_s)
        intervals: list[TimeInterval] = []

        for i in progress_bar(
            range(len(break_points) - 1),
            desc=f"[availability] {data_type}: определение интервалов",
            disable=not self.show_progress,
        ):
            start_idx = int(break_points[i])
            end_exclusive = int(break_points[i + 1])

            start = working.at[start_idx, self.time_col]
            end = working.at[end_exclusive - 1, self.time_col]
            interval_duration = end - start

            if interval_duration >= min_delta:
                intervals.append((start, end))

        if save_csv:
            if config is None:
                raise ValueError("config is required when save_csv=True")
            save_source_periods(intervals=intervals, config=config, source=data_type)

        return IntervalsView(tuple(intervals))
