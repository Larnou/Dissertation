from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import AutoMinorLocator


VALID_COMPONENTS: set[str] = {"f", "a", "r"}


def _smooth_counts(counts: np.ndarray, smooth_window: int) -> np.ndarray:
    if smooth_window <= 1 or counts.size < 2:
        return counts.astype(float)

    window_size = min(int(smooth_window), counts.size)
    if window_size % 2 == 0:
        window_size -= 1
    if window_size <= 1:
        return counts.astype(float)

    kernel = np.ones(window_size, dtype=float) / window_size
    return np.convolve(counts, kernel, mode="same")


def plot_ae_histogram_outside_h_range(
    data: pd.DataFrame,
    component: str,
    h_value: float,
    delta: float,
    bins: int | np.ndarray = 40,
    smooth_window: int = 5,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    normalized_component = component.lower()
    if normalized_component not in VALID_COMPONENTS:
        raise ValueError("Component must be one of: 'f', 'a', 'r'.")
    if delta < 0:
        raise ValueError("delta must be greater than or equal to 0.")
    if smooth_window < 1:
        raise ValueError("smooth_window must be greater than or equal to 1.")

    h_column = f"H_{normalized_component}"
    ae_column = "AE"
    required_columns = (h_column, ae_column)
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    lower = h_value - delta
    upper = h_value + delta
    work = data.loc[:, required_columns].copy()
    work[h_column] = pd.to_numeric(work[h_column], errors="coerce")
    work[ae_column] = pd.to_numeric(work[ae_column], errors="coerce")
    work = work.dropna(subset=required_columns)
    work = work[(work[h_column] < lower) | (work[h_column] > upper)]
    if work.empty:
        raise ValueError(f"No AE data found where {h_column} is outside [{lower}, {upper}].")

    ae_values = work[ae_column].to_numpy(dtype=float)
    counts, bin_edges = np.histogram(ae_values, bins=bins)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    smoothed_counts = _smooth_counts(counts, smooth_window=smooth_window)

    fig, ax = plt.subplots(figsize=(11, 7), layout="constrained")
    ax.hist(ae_values, bins=bin_edges, edgecolor="black", color="#F48849", rwidth=0.85, label="AE counts")
    ax.plot(bin_centers, smoothed_counts, color="#1369b7", linewidth=2.2, label="Smoothed envelope")

    ax.set_title(f"AE distribution outside {h_column} in [{lower:.3g}, {upper:.3g}]", fontsize=14)
    ax.set_xlabel("AE index", fontsize=12)
    ax.set_ylabel("Counts", fontsize=12)
    ax.grid(alpha=0.35)
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(axis="both", which="major", color="black", length=8, width=1.5, labelsize=11)
    ax.tick_params(axis="both", which="minor", color="black", length=4, width=1)
    ax.legend(fontsize=11)

    resolved_output = Path(output_path) if output_path else Path(
        f"hist_ae_outside_H_{normalized_component}_h{h_value:g}_d{delta:g}.png"
    )
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output
