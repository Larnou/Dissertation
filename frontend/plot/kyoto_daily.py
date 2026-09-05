from datetime import datetime
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter, MultipleLocator


INDEX_LINE_COLOR = "#111111"
PEAK_LINE_COLOR = "#222222"


def slice_index_day(
    data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    value_column: str,
) -> pd.DataFrame:
    """
    Берёт одни сутки индекса и приводит ряд к минутной сетке 00–24 UT.

    Пропуски остаются NaN: на графике будет разрыв.

    Args:
        data: таблица с колонками Time и индекса.
        day: календарные сутки в UTC.
        value_column: колонка индекса.

    Returns:
        Индекс Time на каждую минуту суток, колонка значения.

    Raises:
        KeyError: нет Time или колонки индекса.
        ValueError: за эти сутки нет ни одного значения.
    """

    if "Time" not in data.columns:
        raise KeyError(f"Column 'Time' not found. Available: {list(data.columns)}")
    if value_column not in data.columns:
        raise KeyError(f"Column {value_column!r} not found. Available: {list(data.columns)}")

    stamp = pd.Timestamp(day)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    day_start = stamp.normalize()
    day_end = day_start + pd.Timedelta(days=1)

    work = data.loc[:, ["Time", value_column]].copy()
    work["Time"] = _to_utc_naive(work["Time"])
    work[value_column] = pd.to_numeric(work[value_column], errors="coerce")
    work = work.dropna(subset=["Time"])
    work = work[(work["Time"] >= day_start) & (work["Time"] < day_end)]
    if work.dropna(subset=[value_column]).empty:
        raise ValueError(f"No {value_column} values found for {day_start:%Y-%m-%d}.")

    minute_index = pd.date_range(day_start, day_end - pd.Timedelta(minutes=1), freq="1min")
    indexed = work.drop_duplicates(subset=["Time"]).set_index("Time").sort_index()
    return indexed.reindex(minute_index).rename_axis("Time")


def daily_image_name(index: str, day: str | datetime | pd.Timestamp) -> str:
    """
    Имя PNG: ``{index}_for_{year}_{month}_{day}.png``.

    Args:
        index: стем индекса, например ``ae`` или ``symh``.
        day: календарные сутки.
    """

    stamp = pd.Timestamp(day)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert("UTC").tz_localize(None)
    return f"{index}_for_{stamp:%Y_%m_%d}.png"


def style_daily_axis(
    axis: plt.Axes,
    *,
    day_start: pd.Timestamp,
    day_end: pd.Timestamp,
    y_min: float,
    y_max: float,
    y_step: float,
    index_label: str,
    show_xlabel: bool = True,
) -> None:
    """
    Ось 00–24 UT, подписи каждые 3 часа, серая сетка.
    """

    axis.set_xlim(day_start, day_end)
    axis.set_ylim(y_min, y_max)
    axis.yaxis.set_major_locator(MultipleLocator(y_step))
    axis.set_ylabel("(nT)")
    axis.set_xlabel("U T" if show_xlabel else "")
    axis.text(-0.045, 0.62, index_label, transform=axis.transAxes, ha="right", va="center", fontsize=11)
    hour_ticks = [day_start + pd.Timedelta(hours=hour) for hour in range(0, 25, 3)]
    axis.set_xticks(hour_ticks)
    axis.xaxis.set_minor_locator(mdates.HourLocator(interval=1))
    axis.xaxis.set_major_formatter(FuncFormatter(_hour_tick_formatter(day_start)))
    axis.grid(which="major", color="0.75", linewidth=0.6)
    axis.grid(which="minor", axis="x", color="0.85", linewidth=0.4)
    axis.set_axisbelow(True)


def mark_extremum(
    axis: plt.Axes,
    peak_time: pd.Timestamp,
    peak_value: float,
) -> None:
    """
    Пунктир от 0 нТл до экстремума.
    """

    axis.vlines(peak_time, 0.0, peak_value, color=PEAK_LINE_COLOR, linewidth=1.1, linestyle="--", zorder=3)


def save_daily_figure(
    fig: plt.Figure,
    output_path: str | Path | None,
    show: bool,
    default_name: str,
) -> Path:
    resolved = Path(output_path) if output_path is not None else Path(default_name)
    if output_path is not None:
        resolved.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved, dpi=200)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved


def finite_values(values: np.ndarray) -> np.ndarray:
    finite = values[np.isfinite(values)]
    if finite.size == 0:
        raise ValueError("No finite index values in the selected day.")
    return finite


def _to_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def _hour_tick_formatter(day_start: pd.Timestamp):
    day_end = day_start + pd.Timedelta(days=1)

    def formatter(value: float, _pos: int) -> str:
        stamp = pd.Timestamp(mdates.num2date(value)).tz_localize(None)
        if stamp >= day_end - pd.Timedelta(seconds=30):
            return "24"
        return f"{stamp.hour:02d}"

    return formatter
