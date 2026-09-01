from collections.abc import Sequence
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import AutoMinorLocator


VALID_COMPONENTS: set[str] = {"f", "a", "r"}
SYMH_COLUMN = "SYMH"
SYMH_WEIGHT_MODES: set[str] = {"none", "abs_value", "log_abs_value"}


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


def _build_symh_weights(values: np.ndarray, weight_mode: str) -> np.ndarray | None:
    if weight_mode not in SYMH_WEIGHT_MODES:
        raise ValueError(f"weight_mode must be one of: {sorted(SYMH_WEIGHT_MODES)}.")
    if weight_mode == "none":
        return None
    magnitudes = np.abs(values)
    if weight_mode == "abs_value":
        weights = magnitudes
    else:
        weights = np.log10(magnitudes + 1.0)
    if not np.any(weights > 0):
        raise ValueError("Weighted histogram requires at least one positive weight.")
    return weights


def _resolve_h_range(h_range: Sequence[float]) -> tuple[float, float]:
    if len(h_range) != 2:
        raise ValueError("h_range must contain exactly two values: [lower, upper].")

    lower = float(h_range[0])
    upper = float(h_range[1])
    if not np.isfinite(lower) or not np.isfinite(upper):
        raise ValueError("H range bounds must be finite numbers.")
    if upper < lower:
        raise ValueError("H range upper bound must be greater than or equal to lower bound.")

    return lower, upper


def _prepare_symh_values(data: pd.DataFrame, required_columns: Sequence[str]) -> pd.DataFrame:
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    work = data.loc[:, required_columns].copy()
    for column in required_columns:
        work[column] = pd.to_numeric(work[column], errors="coerce")

    return work.dropna(subset=required_columns)


def _plot_symh_values(
    symh_values: np.ndarray,
    title: str,
    bins: int | np.ndarray,
    smooth_window: int,
    normalize: bool,
    y_log_scale: bool,
    weight_mode: str,
    output_path: str | Path | None,
    show: bool,
    default_filename: str,
) -> Path:
    if symh_values.size == 0:
        raise ValueError("No SYM-H data found for histogram.")

    weights = _build_symh_weights(symh_values, weight_mode)
    counts, bin_edges = np.histogram(symh_values, bins=bins, weights=weights, density=normalize)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    smoothed_counts = _smooth_counts(counts, smooth_window=smooth_window)

    fig, ax = plt.subplots(figsize=(11, 7), layout="constrained")
    is_weighted = weights is not None
    ax.hist(
        symh_values,
        bins=bin_edges,
        weights=weights,
        density=normalize,
        edgecolor="black",
        color="#F48849",
        rwidth=0.85,
        label=("Weighted SYM-H density" if normalize else "Weighted SYM-H counts") if is_weighted else ("SYM-H density" if normalize else "SYM-H counts"),
    )
    ax.plot(bin_centers, smoothed_counts, color="#1369b7", linewidth=2.2, label="Smoothed envelope")
    if y_log_scale:
        positive_counts = smoothed_counts[smoothed_counts > 0]
        if positive_counts.size == 0:
            raise ValueError("No positive histogram values found for logarithmic Y scale.")
        ax.set_yscale("log")
        ax.set_ylim(bottom=float(np.min(positive_counts)) * 0.5)

    title_suffix = []
    if normalize:
        title_suffix.append("normalized")
    if is_weighted:
        title_suffix.append(f"weighted: {weight_mode}")
    if y_log_scale:
        title_suffix.append("log Y")
    ax.set_title(title + (f" ({', '.join(title_suffix)})" if title_suffix else ""), fontsize=14)
    ax.set_xlabel("SYM-H index", fontsize=12)
    ax.set_ylabel(("Weighted probability density" if is_weighted else "Probability density") if normalize else ("Weighted counts" if is_weighted else "Counts"), fontsize=12)
    ax.grid(alpha=0.35)
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))
    ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(axis="both", which="major", color="black", length=8, width=1.5, labelsize=11)
    ax.tick_params(axis="both", which="minor", color="black", length=4, width=1)
    ax.legend(fontsize=11)

    resolved_output = Path(output_path) if output_path else Path(default_filename)
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def plot_symh_histogram(
    data: pd.DataFrame,
    bins: int | np.ndarray = 40,
    smooth_window: int = 5,
    normalize: bool = False,
    y_log_scale: bool = False,
    weight_mode: str = "none",
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    if smooth_window < 1:
        raise ValueError("smooth_window must be greater than or equal to 1.")

    work = _prepare_symh_values(data, [SYMH_COLUMN])
    weight_suffix = f"_weighted_{weight_mode}" if weight_mode != "none" else ""
    return _plot_symh_values(
        symh_values=work[SYMH_COLUMN].to_numpy(dtype=float),
        title="SYM-H index distribution",
        bins=bins,
        smooth_window=smooth_window,
        normalize=normalize,
        y_log_scale=y_log_scale,
        weight_mode=weight_mode,
        output_path=output_path,
        show=show,
        default_filename=(
            f"hist_symh_index"
            f"{'_normalized' if normalize else ''}"
            f"{weight_suffix}"
            f"{'_log_y' if y_log_scale else ''}.png"
        ),
    )


def plot_symh_histogram_inside_h_range(
    data: pd.DataFrame,
    component: str,
    h_range: Sequence[float],
    bins: int | np.ndarray = 40,
    smooth_window: int = 5,
    normalize: bool = False,
    y_log_scale: bool = False,
    weight_mode: str = "none",
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    normalized_component = component.lower()
    if normalized_component not in VALID_COMPONENTS:
        raise ValueError("Component must be one of: 'f', 'a', 'r'.")
    if smooth_window < 1:
        raise ValueError("smooth_window must be greater than or equal to 1.")

    h_column = f"H_{normalized_component}"
    lower, upper = _resolve_h_range(h_range)
    work = _prepare_symh_values(data, [h_column, SYMH_COLUMN])
    work = work[(work[h_column] >= lower) & (work[h_column] <= upper)]
    if work.empty:
        raise ValueError(f"No SYM-H data found where {h_column} is inside [{lower}, {upper}].")

    weight_suffix = f"_weighted_{weight_mode}" if weight_mode != "none" else ""
    return _plot_symh_values(
        symh_values=work[SYMH_COLUMN].to_numpy(dtype=float),
        title=f"SYM-H distribution inside {h_column} in [{lower:.3g}, {upper:.3g}]",
        bins=bins,
        smooth_window=smooth_window,
        normalize=normalize,
        y_log_scale=y_log_scale,
        weight_mode=weight_mode,
        output_path=output_path,
        show=show,
        default_filename=(
            f"hist_symh_inside_H_{normalized_component}_{lower:g}_{upper:g}"
            f"{'_normalized' if normalize else ''}"
            f"{weight_suffix}"
            f"{'_log_y' if y_log_scale else ''}.png"
        ),
    )
