from enum import Enum
from typing import Any

import pandas as pd


class AvailabilityColor(str, Enum):
    # Контрастная палитра (близкая к Tableau 10 / colorblind-friendly)
    fgm = "#1f77b4"  # blue
    esa_ion = "#ff7f0e"  # orange
    esa_electron = "#2ca02c"  # green
    efi = "#d62728"  # red
    ssc = "#9467bd"  # purple
    sta = "#8c564b"  # brown
    omn = "#17becf"  # cyan
    shue = "#e377c2"  # magenta
    intersections = "black"


def get_availability_color(data_type: str) -> str:
    return AvailabilityColor[data_type].value


def show_interval_spans(
    ax: Any,
    intervals: Any,
    color: str,
    alpha: float = 0.5,
    line_style: str = "--",
    line_width: float = 1.5,
    zorder: int = 0,
):
    """
    Draw interval boundaries as dashed vertical lines and fill between them.

    Intended for Matplotlib datetime x-axis.
    """
    if not intervals:
        return

    if alpha < 0 or alpha > 1:
        raise ValueError("alpha must be in [0, 1]")

    for start_dt, end_dt in intervals:
        ax.axvline(start_dt, color=color, linestyle=line_style, linewidth=line_width, alpha=alpha, zorder=zorder)
        ax.axvline(end_dt, color=color, linestyle=line_style, linewidth=line_width, alpha=alpha, zorder=zorder)
        ax.axvspan(start_dt, end_dt, color=color, alpha=alpha, zorder=zorder)



def show_overlaps(
    ax: Any,
    intervals: Any,
    data: pd.DataFrame,
    data_type: str,
    plot_level: int,
):

    time_column: str = "Time"
    resolved_color = get_availability_color(data_type=data_type)

    for i, interval in enumerate(intervals):
        dat = data[(data[time_column] >= interval[0]) & (data[time_column] <= interval[1])].reset_index(drop=True)
        if dat.empty:
            continue

        if i == 0:
            ax.hlines(
                plot_level,
                dat[time_column].iloc[0],
                dat[time_column].iloc[-1],
                label=data_type,
                color=resolved_color,
                linewidth=8,
            )
            continue

        ax.hlines(
            plot_level,
            dat[time_column].iloc[0],
            dat[time_column].iloc[-1],
            color=resolved_color,
            linewidth=8,
        )