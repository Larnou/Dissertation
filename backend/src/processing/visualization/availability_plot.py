import pandas as pd
from matplotlib import pyplot as plt

from backend.src.processing.utils.plot_interval_settings import set_plot_interval_settings
from backend.src.processing.utils.show_overlaps import (
    get_availability_color,
    show_interval_spans,
    show_overlaps,
)


def show_availability(dataframe: pd.DataFrame, intervals, data_type: str) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(18, 7), layout="constrained", sharex=False)
    show_overlaps(ax, intervals, dataframe, data_type, 1)
    set_plot_interval_settings(ax, x_label="Time", y_label="Relative level units")
    plt.show()


def show_combined_intervals(dataframe: pd.DataFrame, intervals_list: list[dict]) -> None:
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
