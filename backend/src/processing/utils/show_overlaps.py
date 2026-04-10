from enum import Enum
from typing import Any

import pandas as pd


class AvailabilityColor(str, Enum):
    fgm = "tab:blue"
    esa_ion = "tab:orange"
    esa_electron = "tab:green"
    efi = "tab:red"
    ssc = "tab:purple"
    sta = "tab:brown"
    omn = "tab:pink"


def _get_color(data_type: str) -> str:
    return AvailabilityColor[data_type].value



def show_overlaps(
    ax: Any,
    intervals: Any,
    data: pd.DataFrame,
    data_type: str,
    plot_level: int,
):

    time_column: str = "Time"
    resolved_color = _get_color(data_type=data_type)

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