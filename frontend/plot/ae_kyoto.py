from datetime import datetime
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from frontend.plot.kyoto_daily import (
    INDEX_LINE_COLOR,
    daily_image_name,
    finite_values,
    mark_extremum,
    save_daily_figure,
    slice_index_day,
    style_daily_axis,
)


AE_FILL_COLOR = "#FF8000"
OFFICIAL_Y_LIMITS = (-500.0, 2000.0)


def slice_ae_day(
    data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    value_column: str = "AE",
) -> pd.DataFrame:
    """
    Берёт одни сутки AE и приводит ряд к минутной сетке 00–24 UT.
    """

    return slice_index_day(data, day, value_column=value_column)


def draw_ae_daily(
    axis: plt.Axes,
    data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    value_column: str = "AE",
    show_xlabel: bool = True,
) -> pd.Timestamp:
    """
    Рисует суточную панель AE на готовой оси.

    Returns:
        Начало выбранных суток.
    """

    day_frame = slice_ae_day(data, day, value_column=value_column)
    day_start = day_frame.index[0].normalize()
    day_end = day_start + pd.Timedelta(days=1)
    values = day_frame[value_column].to_numpy(dtype=float)
    finite = finite_values(values)
    peak_idx = int(np.nanargmax(values))
    peak_time = day_frame.index[peak_idx]
    peak_value = float(finite.max())

    y_min, y_max = OFFICIAL_Y_LIMITS
    if finite.max() > y_max:
        y_max = float(np.ceil(finite.max() / 500.0) * 500.0)
    if finite.min() < y_min:
        y_min = float(np.floor(finite.min() / 500.0) * 500.0)

    axis.fill_between(day_frame.index, 0.0, values, color=AE_FILL_COLOR, linewidth=0)
    axis.plot(day_frame.index, values, color=INDEX_LINE_COLOR, linewidth=0.7)
    axis.axhline(0.0, color="#444444", linewidth=0.6)
    style_daily_axis(
        axis,
        day_start=day_start,
        day_end=day_end,
        y_min=y_min,
        y_max=y_max,
        y_step=500.0,
        index_label="AE",
        show_xlabel=show_xlabel,
    )
    mark_extremum(axis, peak_time, peak_value)
    axis.set_title(
        f"AE index  {day_start:%Y-%m-%d}  UTC    "
        f"max {peak_value:.0f} nT at {peak_time:%H:%M}",
        fontsize=12,
    )
    return day_start


def plot_ae_daily(
    data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    value_column: str = "AE",
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    """
    Суточный график AE в компоновке Kyoto WDC: 00–24 UT, оранжевая заливка.

    Args:
        data: таблица с Time и AE.
        day: сутки UTC, например ``2017-09-08``.
        value_column: колонка индекса.
        output_path: куда сохранить PNG; ``None`` — не писать файл.
        show: показать окно matplotlib.
    """

    fig, axis = plt.subplots(figsize=(12.4, 5.0), layout="constrained")
    day_start = draw_ae_daily(axis, data, day, value_column=value_column)
    return save_daily_figure(fig, output_path, show, daily_image_name("ae", day_start))
