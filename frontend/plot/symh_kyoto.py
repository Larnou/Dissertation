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


SYMH_Y_STEP = 50.0


def slice_symh_day(
    data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    value_column: str = "SYMH",
) -> pd.DataFrame:
    """
    Берёт одни сутки SYM-H и приводит ряд к минутной сетке 00–24 UT.
    """

    return slice_index_day(data, day, value_column=value_column)


def draw_symh_daily(
    axis: plt.Axes,
    data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    value_column: str = "SYMH",
    show_xlabel: bool = True,
) -> pd.Timestamp:
    """
    Рисует суточную панель SYM-H на готовой оси.

    Returns:
        Начало выбранных суток.
    """

    day_frame = slice_symh_day(data, day, value_column=value_column)
    day_start = day_frame.index[0].normalize()
    day_end = day_start + pd.Timedelta(days=1)
    values = day_frame[value_column].to_numpy(dtype=float)
    finite = finite_values(values)
    peak_idx = int(np.nanargmin(values))
    peak_time = day_frame.index[peak_idx]
    peak_value = float(finite.min())
    y_min, y_max = _symh_limits(finite)

    axis.plot(day_frame.index, values, color=INDEX_LINE_COLOR, linewidth=0.8)
    axis.axhline(0.0, color="#444444", linewidth=0.6)
    style_daily_axis(
        axis,
        day_start=day_start,
        day_end=day_end,
        y_min=y_min,
        y_max=y_max,
        y_step=SYMH_Y_STEP,
        index_label="SYM-H",
        show_xlabel=show_xlabel,
    )
    mark_extremum(axis, peak_time, peak_value)
    axis.set_title(
        f"SYM-H index  {day_start:%Y-%m-%d}  UTC    "
        f"min {peak_value:.0f} nT at {peak_time:%H:%M}",
        fontsize=12,
    )
    return day_start


def plot_symh_daily(
    data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    value_column: str = "SYMH",
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    """
    Суточный график SYM-H в той же компоновке, что AE, без заливки.

    Args:
        data: таблица с Time и SYMH.
        day: сутки UTC, например ``2017-09-08``.
        value_column: колонка индекса.
        output_path: куда сохранить PNG; ``None`` — не писать файл.
        show: показать окно matplotlib.
    """

    fig, axis = plt.subplots(figsize=(12.4, 5.0), layout="constrained")
    day_start = draw_symh_daily(axis, data, day, value_column=value_column)
    return save_daily_figure(fig, output_path, show, daily_image_name("symh", day_start))


def _symh_limits(finite: np.ndarray) -> tuple[float, float]:
    y_min = min(0.0, float(finite.min()))
    y_max = max(0.0, float(finite.max()))
    y_min = float(np.floor((y_min - 20.0) / SYMH_Y_STEP) * SYMH_Y_STEP)
    y_max = float(np.ceil((y_max + 20.0) / SYMH_Y_STEP) * SYMH_Y_STEP)
    if y_min >= 0.0:
        y_min = -SYMH_Y_STEP
    if y_max <= 0.0:
        y_max = SYMH_Y_STEP
    return y_min, y_max
