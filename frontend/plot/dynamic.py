from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


ROLLING_WINDOW = "1D"


def _to_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def _minute_means(data: pd.DataFrame, value_columns: list[str]) -> pd.DataFrame:
    work = data.loc[:, ["Time", *value_columns]].copy()
    work["Time"] = _to_utc_naive(work["Time"])
    work = work.dropna(subset=["Time"]).sort_values("Time")
    for column in value_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    indexed = work.set_index("Time")
    return indexed.resample("1min").mean()


def _daily_means(data: pd.DataFrame, value_columns: list[str]) -> pd.DataFrame:
    work = data.loc[:, ["Time", *value_columns]].copy()
    work["Time"] = _to_utc_naive(work["Time"])
    work = work.dropna(subset=["Time"]).sort_values("Time")
    for column in value_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")
    indexed = work.set_index("Time")
    return indexed.resample("1D").mean()


def _rolling_daily_mean(series: pd.Series) -> pd.Series:
    return series.rolling(window=ROLLING_WINDOW, min_periods=1).mean()


def _save_figure(fig: plt.Figure, output_path: str | Path | None, show: bool) -> Path:
    resolved_output = Path(output_path) if output_path else Path("dynamic_plot.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def _format_time_axis(axis: plt.Axes) -> None:
    axis.set_xlabel("Time (UTC)")
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))


def _filter_time_range(
    data: pd.DataFrame,
    *,
    start_time: str | None = None,
    end_time: str | None = None,
) -> pd.DataFrame:
    if start_time is None and end_time is None:
        return data

    filtered = data
    if start_time is not None:
        start_ts = pd.to_datetime(start_time, utc=True).tz_convert("UTC").tz_localize(None)
        filtered = filtered.loc[filtered.index >= start_ts]
    if end_time is not None:
        end_ts = pd.to_datetime(end_time, utc=True).tz_convert("UTC").tz_localize(None)
        filtered = filtered.loc[filtered.index < end_ts]
    return filtered


def plot_component_dynamics(
    data: pd.DataFrame,
    component: str,
    start_time: str,
    end_time: str,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    normalized_component = component.lower()
    if normalized_component not in {"f", "a", "r"}:
        raise ValueError("Component must be one of: 'f', 'a', 'r'.")

    e_column = f"E_{normalized_component}_meas"
    v_column = f"V_{normalized_component}_meas"
    h_column = f"H_{normalized_component}"
    x_column = f"X_{normalized_component}_conv"
    required_columns = ("Time", e_column, v_column, x_column, h_column)
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    start_ts = pd.to_datetime(start_time, utc=True, errors="raise").tz_convert("UTC").tz_localize(None)
    end_ts = pd.to_datetime(end_time, utc=True, errors="raise").tz_convert("UTC").tz_localize(None)
    if end_ts <= start_ts:
        raise ValueError("end_time must be later than start_time.")

    work = data.loc[:, required_columns].copy()
    work["Time"] = _to_utc_naive(work["Time"])
    work = work.dropna(subset=["Time"]).sort_values("Time")
    work = work[(work["Time"] >= start_ts) & (work["Time"] <= end_ts)]
    work = work.dropna(subset=[e_column, v_column, x_column, h_column])
    if work.empty:
        raise ValueError("No data found in selected time range for the required columns.")

    fig, axes = plt.subplots(3, 1, figsize=(14, 9), sharex=True, layout="constrained")

    axes[0].plot(work["Time"], work[v_column], color="#ff7f0e", linewidth=1.2)
    axes[0].set_ylabel(v_column)
    axes[0].grid(alpha=0.35)

    axes[1].plot(work["Time"], work[x_column], color="#2ca02c", linewidth=1.2, label=x_column)
    axes[1].plot(work["Time"], work[e_column], color="#1f77b4", linewidth=1.1, alpha=0.9, label=e_column)
    axes[1].set_ylabel(f"{x_column} / {e_column}")
    axes[1].grid(alpha=0.35)
    axes[1].legend(loc="upper right", fontsize=9)

    h_values = work[h_column].to_numpy(dtype=float)
    h_mean = float(h_values.mean())
    h_sigma = float(h_values.std())
    h_minus_sigma = h_mean - h_sigma
    h_plus_sigma = h_mean + h_sigma

    axes[2].plot(work["Time"], work[h_column], color="#d62728", linewidth=1.2, label=h_column)
    axes[2].axhline(h_mean, color="#111111", linestyle="--", linewidth=1.3, label=f"mean={h_mean:.2f}")
    axes[2].axhline(h_minus_sigma, color="#7f7f7f", linestyle=":", linewidth=1.2, label=f"mean-1σ={h_minus_sigma:.2f}")
    axes[2].axhline(h_plus_sigma, color="#7f7f7f", linestyle=":", linewidth=1.2, label=f"mean+1σ={h_plus_sigma:.2f}")
    axes[2].fill_between(
        work["Time"],
        h_minus_sigma,
        h_plus_sigma,
        color="#7f7f7f",
        alpha=0.3,
        label="mean ± 1σ",
    )
    axes[2].set_ylabel(h_column)
    axes[2].set_xlabel("Time (UTC)")
    axes[2].grid(alpha=0.35)
    axes[2].legend(loc="upper right", fontsize=9)

    axes[0].set_title(
        f"Dynamic parameters for component '{normalized_component}'"
        f"\n{start_ts} — {end_ts}",
        fontsize=13,
    )
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    fig.autofmt_xdate(rotation=25)

    resolved_output = Path(output_path) if output_path else Path(
        f"dynamic_{normalized_component}_{start_ts:%Y%m%d_%H%M%S}_{end_ts:%Y%m%d_%H%M%S}.png"
    )
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def plot_ae_dynamics(
    data: pd.DataFrame,
    *,
    value_column: str = "AE",
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    if value_column not in data.columns:
        raise KeyError(f"Column {value_column!r} not found. Available: {list(data.columns)}")

    minute_data = _minute_means(data, [value_column]).dropna(subset=[value_column])
    if minute_data.empty:
        raise ValueError(f"No data available for column {value_column!r}.")

    rolling_mean = _rolling_daily_mean(minute_data[value_column])
    start_ts = minute_data.index[0]
    end_ts = minute_data.index[-1]

    fig, axis = plt.subplots(figsize=(14, 5), layout="constrained")
    axis.plot(
        minute_data.index,
        minute_data[value_column],
        color="#9467bd",
        linewidth=0.6,
        alpha=0.55,
        label=value_column,
    )
    axis.plot(
        rolling_mean.index,
        rolling_mean,
        color="#4c1d95",
        linewidth=1.8,
        label=f"{value_column} ({ROLLING_WINDOW} mean)",
    )
    axis.set_ylabel(f"{value_column} [nT]")
    axis.grid(alpha=0.35)
    axis.legend(loc="upper right", fontsize=9)
    axis.set_title(
        f"AE index dynamics\n{start_ts:%Y-%m-%d %H:%M} — {end_ts:%Y-%m-%d %H:%M} UTC",
        fontsize=13,
    )
    _format_time_axis(axis)
    fig.autofmt_xdate(rotation=25)

    return _save_figure(fig, output_path or Path("dynamic_ae.png"), show)


def plot_ae_hg_component_daily(
    ae_data: pd.DataFrame,
    parameter_data: pd.DataFrame,
    *,
    component: str,
    start_time: str | None = None,
    end_time: str | None = None,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    normalized_component = component.lower()
    if normalized_component not in {"a", "r"}:
        raise ValueError("Component must be one of: 'a', 'r'.")

    h_column = f"H_{normalized_component}"
    g_column = f"G_{normalized_component}"
    missing = [column for column in (h_column, g_column) if column not in parameter_data.columns]
    if missing:
        raise KeyError(
            f"Parameter columns are missing: {missing}. Available: {list(parameter_data.columns)}"
        )
    if "AE" not in ae_data.columns:
        raise KeyError("AE column is missing in ae_data.")

    ae_daily = _daily_means(ae_data, ["AE"])
    parameter_daily = _daily_means(parameter_data, [h_column, g_column])
    merged = ae_daily.join(parameter_daily, how="inner").dropna()
    merged = _filter_time_range(merged, start_time=start_time, end_time=end_time)
    if merged.empty:
        raise ValueError("No overlapping AE and H/G data found in the selected time range.")

    start_ts = merged.index[0]
    end_ts = merged.index[-1]

    fig, axis_left = plt.subplots(figsize=(14, 5), layout="constrained")
    axis_right = axis_left.twinx()

    axis_left.plot(
        merged.index,
        merged["AE"],
        color="#4c1d95",
        linewidth=1.8,
        marker="o",
        markersize=3,
        label="AE (daily mean)",
    )
    axis_right.plot(
        merged.index,
        merged[h_column],
        color="#d62728",
        linewidth=1.8,
        marker="o",
        markersize=3,
        label=f"{h_column} (daily mean)",
    )
    axis_right.plot(
        merged.index,
        merged[g_column],
        color="#2ca02c",
        linewidth=1.8,
        linestyle="--",
        marker="s",
        markersize=3,
        label=f"{g_column} (daily mean)",
    )

    axis_left.set_ylabel("AE [nT]")
    axis_right.set_ylabel("H / G")
    axis_left.grid(alpha=0.35)
    axis_left.set_title(
        f"AE, {h_column}, {g_column}: daily means\n"
        f"{start_ts:%Y-%m-%d} — {end_ts:%Y-%m-%d} UTC",
        fontsize=13,
    )
    _format_time_axis(axis_left)

    left_handles, left_labels = axis_left.get_legend_handles_labels()
    right_handles, right_labels = axis_right.get_legend_handles_labels()
    axis_left.legend(
        left_handles + right_handles,
        left_labels + right_labels,
        loc="upper right",
        fontsize=9,
    )
    fig.autofmt_xdate(rotation=25)

    default_name = f"dynamic_ae_hg_{normalized_component}.png"
    return _save_figure(fig, output_path or Path(default_name), show)


def plot_ae_hg_component_rolling(
    ae_data: pd.DataFrame,
    parameter_data: pd.DataFrame,
    **kwargs,
) -> Path:
    """Совместимость: использует дневные средние вместо скользящего окна."""
    return plot_ae_hg_component_daily(ae_data, parameter_data, **kwargs)


def plot_ae_parameter_comparison(
    ae_data: pd.DataFrame,
    parameter_data: pd.DataFrame,
    *,
    parameter_columns: tuple[str, str],
    parameter_label: str,
    parameter_colors: tuple[str, str],
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    missing_parameters = [column for column in parameter_columns if column not in parameter_data.columns]
    if missing_parameters:
        raise KeyError(
            f"Parameter columns are missing: {missing_parameters}. "
            f"Available: {list(parameter_data.columns)}"
        )
    if "AE" not in ae_data.columns:
        raise KeyError("AE column is missing in ae_data.")

    ae_minute = _minute_means(ae_data, ["AE"])
    parameter_minute = _minute_means(parameter_data, list(parameter_columns))
    merged = ae_minute.join(parameter_minute, how="inner").dropna()
    if merged.empty:
        raise ValueError("No overlapping AE and parameter data found for comparison.")

    ae_rolling = _rolling_daily_mean(merged["AE"])
    parameter_rolling = {
        column: _rolling_daily_mean(merged[column])
        for column in parameter_columns
    }

    start_ts = merged.index[0]
    end_ts = merged.index[-1]
    fig, axis_left = plt.subplots(figsize=(14, 5), layout="constrained")
    axis_right = axis_left.twinx()

    axis_left.plot(
        merged.index,
        merged["AE"],
        color="#9467bd",
        linewidth=0.6,
        alpha=0.55,
        label="AE",
    )
    axis_left.plot(
        ae_rolling.index,
        ae_rolling,
        color="#4c1d95",
        linewidth=1.8,
        label=f"AE ({ROLLING_WINDOW} mean)",
    )

    for column, color in zip(parameter_columns, parameter_colors, strict=True):
        axis_right.plot(
            merged.index,
            merged[column],
            color=color,
            linewidth=0.6,
            alpha=0.55,
            label=column,
        )
        axis_right.plot(
            parameter_rolling[column].index,
            parameter_rolling[column],
            color=color,
            linewidth=1.8,
            linestyle="--",
            label=f"{column} ({ROLLING_WINDOW} mean)",
        )

    axis_left.set_ylabel("AE [nT]")
    axis_right.set_ylabel(parameter_label)
    axis_left.grid(alpha=0.35)
    axis_left.set_title(
        f"AE vs {parameter_label} ({parameter_columns[0]}, {parameter_columns[1]})\n"
        f"{start_ts:%Y-%m-%d %H:%M} — {end_ts:%Y-%m-%d %H:%M} UTC",
        fontsize=13,
    )
    _format_time_axis(axis_left)

    left_handles, left_labels = axis_left.get_legend_handles_labels()
    right_handles, right_labels = axis_right.get_legend_handles_labels()
    axis_left.legend(
        left_handles + right_handles,
        left_labels + right_labels,
        loc="upper right",
        fontsize=8,
    )
    fig.autofmt_xdate(rotation=25)

    default_name = f"dynamic_ae_{parameter_label.lower()}_{parameter_columns[0]}_{parameter_columns[1]}.png"
    return _save_figure(fig, output_path or Path(default_name), show)
