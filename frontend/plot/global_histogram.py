from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import AutoMinorLocator


def set_bins_parameters(values: np.ndarray, bin_step: float = 0.05) -> tuple[np.ndarray, float]:
    hist_min = float(np.floor(np.min(values) * 10) / 10)
    hist_max = float(np.ceil(np.max(values) * 10) / 10)
    if np.isclose(hist_min, hist_max):
        hist_max = hist_min + bin_step
    bins_count = max(1, int(round((hist_max - hist_min) / bin_step)))
    bins = np.linspace(hist_min, hist_max, bins_count + 1)
    return bins, bin_step


def _extract_sector_values(hdata: np.ndarray, l_range: tuple[int, int], mlt_range: tuple[int, int]) -> np.ndarray:
    l_start, l_end = l_range
    mlt_start, mlt_end = mlt_range
    selected_rows = hdata[l_start:l_end]

    if mlt_start > mlt_end:
        selected = np.concatenate((selected_rows[:, mlt_start:24], selected_rows[:, 0:mlt_end]), axis=1)
    else:
        selected = selected_rows[:, mlt_start:mlt_end]

    values = selected.astype(float).ravel()
    return values[~np.isnan(values)]


def show_hist_by_lmlt_range(
    ax: plt.Axes,
    hdata: np.ndarray,
    l_range: tuple[int, int],
    mlt_range: tuple[int, int],
    parameter: str,
    component: str,
) -> None:
    data = _extract_sector_values(hdata, l_range, mlt_range)
    if data.size == 0:
        ax.set_title(f"{parameter}{component} parameter for MLT={mlt_range[0]}-{mlt_range[1]} (no data)", size=16)
        return

    data_mean = float(np.mean(data))
    data_std = float(np.std(data))
    data_q25, data_q50, data_q75 = np.quantile(data, [0.25, 0.50, 0.75])

    bins, bin_step = set_bins_parameters(data)
    histogram = ax.hist(data, bins=bins, edgecolor="black", color="#F48849", rwidth=0.8)
    max_hist = float(np.max(histogram[0])) if histogram[0].size else 0.0

    ax.axvspan(data_mean - data_std, data_mean + data_std, 0, max_hist, color="red", alpha=0.2, label="Average ±σ")
    line_width = 0.004
    ax.axvspan(data_mean - line_width, data_mean + line_width, 0, max_hist, color="#5b5b5b", label="Average")
    ax.axvspan(data_q25 - line_width, data_q25 + line_width, 0, max_hist, color="#1369b7", label="Percentile 25")
    ax.axvspan(data_q50 - line_width, data_q50 + line_width, 0, max_hist, color="#3fb60c", label="Median")
    ax.axvspan(data_q75 - line_width, data_q75 + line_width, 0, max_hist, color="#b11b1b", label="Percentile 75")

    ax.grid(alpha=0.6)
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))
    ax.xaxis.set_minor_locator(AutoMinorLocator(max(1, int(0.5 / bin_step))))
    ax.tick_params(axis="y", which="major", color="black", length=8, width=2, labelsize=14)
    ax.tick_params(axis="y", which="minor", color="black", length=4, width=1, labelsize=14)
    ax.tick_params(axis="x", which="major", color="black", length=8, width=2, labelsize=14)
    ax.tick_params(axis="x", which="minor", color="black", length=4, width=1, labelsize=14)
    ax.set_title(f"H parameter for MLT={mlt_range[0]}-{mlt_range[1]} ({component})", size=16)
    ax.legend(fontsize=12)


def show_hist_sectors(
    data: np.ndarray,
    component: str,
    lfrom: int,
    lto: int,
    output_path: str | Path | None = None,
    show: bool = False,
) -> Path:
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(10, 10), layout="constrained")
    l_range = (lfrom, lto)

    show_hist_by_lmlt_range(ax1, data, l_range, (3, 9), component)
    show_hist_by_lmlt_range(ax2, data, l_range, (9, 15), component)
    show_hist_by_lmlt_range(ax3, data, l_range, (15, 21), component)
    show_hist_by_lmlt_range(ax4, data, l_range, (21, 3), component)

    resolved_output = Path(output_path) if output_path else Path(f"h{component}_per_mlt_l{lfrom}-l{lto}.svg")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def _extract_sector_values_from_long(
    raw_long: pd.DataFrame,
    parameter_name: str,
    l_range: tuple[int, int],
    mlt_range: tuple[int, int],
) -> np.ndarray:
    l_start, l_end = l_range
    mlt_start, mlt_end = mlt_range
    filtered = raw_long[
        (raw_long["parameter"] == parameter_name)
        & (raw_long["L_bin"] >= l_start)
        & (raw_long["L_bin"] < l_end)
    ]
    if mlt_start > mlt_end:
        filtered = filtered[(filtered["MLT_bin"] >= mlt_start) | (filtered["MLT_bin"] < mlt_end)]
    else:
        filtered = filtered[(filtered["MLT_bin"] >= mlt_start) & (filtered["MLT_bin"] < mlt_end)]

    values = filtered["value"].to_numpy(dtype=float)
    return values[~np.isnan(values)]


def show_hist_sectors_from_long(
    raw_long: pd.DataFrame,
    parameter_name: str,
    parameter: str,
    component: str,
    lfrom: int,
    lto: int,
    show: bool = True,
) -> None:
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(10, 10), layout="constrained")
    l_range = (lfrom, lto)

    for ax, mlt_range in zip((ax1, ax2, ax3, ax4), ((3, 9), (9, 15), (15, 21), (21, 3))):
        data = _extract_sector_values_from_long(raw_long, parameter_name, l_range, mlt_range)
        if data.size == 0:
            ax.set_title(f"{parameter}{component} parameter for MLT={mlt_range[0]}-{mlt_range[1]} (no data)", size=16)
            continue

        data_mean = float(np.mean(data))
        data_std = float(np.std(data))
        data_q25, data_q50, data_q75 = np.quantile(data, [0.25, 0.50, 0.75])
        bins, bin_step = set_bins_parameters(data)
        histogram = ax.hist(data, bins=bins, edgecolor="black", color="#F48849", rwidth=0.8)
        max_hist = float(np.max(histogram[0])) if histogram[0].size else 0.0

        ax.axvspan(data_mean - data_std, data_mean + data_std, 0, max_hist, color="red", alpha=0.2, label="Average ±σ")
        line_width = 0.004
        ax.axvspan(data_mean - line_width, data_mean + line_width, 0, max_hist, color="#5b5b5b", label="Average")
        ax.axvspan(data_q25 - line_width, data_q25 + line_width, 0, max_hist, color="#1369b7", label="Percentile 25")
        ax.axvspan(data_q50 - line_width, data_q50 + line_width, 0, max_hist, color="#3fb60c", label="Median")
        ax.axvspan(data_q75 - line_width, data_q75 + line_width, 0, max_hist, color="#b11b1b", label="Percentile 75")
        ax.grid(alpha=0.6)
        ax.yaxis.set_minor_locator(AutoMinorLocator(5))
        ax.xaxis.set_minor_locator(AutoMinorLocator(max(1, int(0.5 / bin_step))))
        ax.tick_params(axis="y", which="major", color="black", length=8, width=2, labelsize=14)
        ax.tick_params(axis="y", which="minor", color="black", length=4, width=1, labelsize=14)
        ax.tick_params(axis="x", which="major", color="black", length=8, width=2, labelsize=14)
        ax.tick_params(axis="x", which="minor", color="black", length=4, width=1, labelsize=14)
        ax.set_title(f"{parameter}{component} parameter for MLT={mlt_range[0]}-{mlt_range[1]} ({component})", size=16)
        ax.legend(fontsize=12)

    if show:
        plt.show()
    else:
        plt.close(fig)