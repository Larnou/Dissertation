"""
Интервалы непрерывной доступности данных по времени и их пересечения.

Пользовательский сценарий (Jupyter):

- `ssc_intervals = availability.from_dataframe(ssc_data, "ssc")`
- В ячейке достаточно написать `ssc_intervals`, чтобы получить вывод вида:
  `1. 2017-01-01 00:00:00+00:00 - 2017-01-04 00:00:00+00:00`
"""

import sys
from datetime import timedelta
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from tqdm import tqdm

from backend.src.config import config
from backend.src.io.paths import availability_periods_csv_path, availability_periods_dir
from backend.src.processing.utils.intervals_view import (
    DataSourceKind,
    IntervalsView,
    RULES,
    TimeInterval,
)
from backend.src.processing.utils.is_show_intervals import is_show_intervals
from backend.src.processing.utils.plot_interval_settings import set_plot_interval_settings

from backend.src.processing.utils.show_overlaps import (
    get_availability_color,
    show_interval_spans,
    show_overlaps,
)
from backend.src.processing.intersections import save_intervals_csv


@dataclass(frozen=True, slots=True)
class AvailabilityIntervals:

    time_col: str = "Time"
    show_progress: bool = True
    csv_output_dir: Path = field(default_factory=lambda: availability_periods_dir(config))

    def from_dataframe(self, dataframe: pd.DataFrame, data_type: DataSourceKind) -> IntervalsView:
        rule = RULES.get(data_type)

        working = dataframe.copy()
        # Нормализуем время: все интервалы делаем tz-naive (UTC), иначе пересечения падают
        # с "Cannot compare tz-naive and tz-aware timestamps".
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

        for i in tqdm(range(len(break_points) - 1), desc=f"Определение {data_type} интервалов", file=sys.stdout, disable=False):
            start_idx = int(break_points[i])
            end_exclusive = int(break_points[i + 1])

            start = working.at[start_idx, self.time_col]
            end = working.at[end_exclusive - 1, self.time_col]
            interval_duration = end - start

            if interval_duration >= min_delta:
                intervals.append((start, end))

        save_intervals_csv(
            intervals=intervals,
            output_path=availability_periods_csv_path(config=config, source_stem=data_type),
        )

        return IntervalsView(tuple(intervals))


    def show(self, dataframe: pd.DataFrame, intervals: IntervalsView, data_type: DataSourceKind):
        if is_show_intervals:
            fig, ax = plt.subplots(1, 1, figsize=(18, 7), layout="constrained", sharex=False)

            show_overlaps(ax, intervals, dataframe, data_type, 1)

            set_plot_interval_settings(ax, x_label="Time", y_label="Relative level units")
            plt.show()


    def show_intervals(self, dataframe: pd.DataFrame, intervals_list: list[dict]):
        if is_show_intervals:
            fig, ax = plt.subplots(1, 1, figsize=(18, 7), layout="constrained", sharex=False)

            for index, interval_data in enumerate(intervals_list, start=1):
                intervals = interval_data["intervals"]
                data_type = interval_data["data_type"]
                show_overlaps(ax, intervals, dataframe, data_type, index)

                if data_type == "intersections":
                    show_interval_spans(
                        ax=ax,
                        intervals=intervals,
                        color=get_availability_color(data_type=data_type),
                        alpha=0.5,
                        zorder=0,
                    )

            set_plot_interval_settings(ax, x_label="Time", y_label="Relative level units")
            plt.show()
