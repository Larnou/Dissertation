from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def read_time_series(path: str | Path, time_column: str = "Time") -> pd.DataFrame:
    dataframe = pd.read_parquet(path)
    dataframe[time_column] = pd.to_datetime(dataframe[time_column], utc=True, errors="coerce").dt.tz_localize(None)
    return dataframe.dropna(subset=[time_column]).sort_values(time_column).reset_index(drop=True)


def read_periods(path: str | Path) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    periods = pd.read_csv(path)
    starts = pd.to_datetime(periods["start"], utc=True, errors="coerce").dt.tz_localize(None)
    ends = pd.to_datetime(periods["end"], utc=True, errors="coerce").dt.tz_localize(None)
    return [(start, end) for start, end in zip(starts, ends, strict=False) if pd.notna(start) and pd.notna(end)]


def plot_availability_periods(
    dataframe: pd.DataFrame,
    periods: list[tuple[pd.Timestamp, pd.Timestamp]],
    *,
    time_column: str = "Time",
    level: int = 1,
    label: str = "availability",
) -> None:
    fig, ax = plt.subplots(1, 1, figsize=(18, 6), layout="constrained")

    for start, end in periods:
        clipped = dataframe[(dataframe[time_column] >= start) & (dataframe[time_column] <= end)]
        if clipped.empty:
            continue
        ax.hlines(level, clipped[time_column].iloc[0], clipped[time_column].iloc[-1], linewidth=8, label=label)
        label = ""

    ax.set_xlabel("Time")
    ax.set_ylabel("Availability")
    ax.grid(alpha=0.25)
    if ax.get_legend_handles_labels()[1]:
        ax.legend()
    plt.show()