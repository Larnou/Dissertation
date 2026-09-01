from collections.abc import Sequence
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import AutoMinorLocator


VALID_COMPONENTS: set[str] = {"f", "a", "r"}
AE_WEIGHT_MODES: set[str] = {"none", "value", "log_value"}


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


def _build_ae_weights(values: np.ndarray, weight_mode: str) -> np.ndarray | None:
    if weight_mode not in AE_WEIGHT_MODES:
        raise ValueError(f"weight_mode must be one of: {sorted(AE_WEIGHT_MODES)}.")
    if weight_mode == "none":
        return None
    if np.any(values < 0):
        raise ValueError("AE values must be non-negative when weight_mode is enabled.")
    if weight_mode == "value":
        weights = values
    else:
        weights = np.log10(values + 1.0)
    if not np.any(weights > 0):
        raise ValueError("Weighted histogram requires at least one positive weight.")
    return weights


def _resolve_h_range(
    h_range: Sequence[float] | None,
    h_value: float | None,
    delta: float | None,
) -> tuple[float, float]:
    if h_range is not None:
        if len(h_range) != 2:
            raise ValueError("h_range must contain exactly two values: [lower, upper].")
        lower = float(h_range[0])
        upper = float(h_range[1])
    else:
        if h_value is None or delta is None:
            raise ValueError("Either h_range or both h_value and delta must be provided.")
        if delta < 0:
            raise ValueError("delta must be greater than or equal to 0.")
        lower = h_value - delta
        upper = h_value + delta

    if not np.isfinite(lower) or not np.isfinite(upper):
        raise ValueError("H range bounds must be finite numbers.")
    if upper < lower:
        raise ValueError("H range upper bound must be greater than or equal to lower bound.")

    return lower, upper


def _prepare_ae_histogram(
    ae_values: np.ndarray,
    bins: int | np.ndarray,
    log_scale: bool,
    normalize: bool,
    weight_mode: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray | None]:
    values = ae_values[np.isfinite(ae_values)]
    if log_scale:
        values = values[values > 0]
        if values.size == 0:
            raise ValueError("No positive AE values found for logarithmic histogram.")

        if isinstance(bins, int):
            if bins < 1:
                raise ValueError("bins must be greater than or equal to 1.")
            ae_min = float(np.min(values))
            ae_max = float(np.max(values))
            if np.isclose(ae_min, ae_max):
                ae_min = ae_min / 10
                ae_max = ae_max * 10
            bin_edges = np.logspace(np.log10(ae_min), np.log10(ae_max), bins + 1)
        else:
            bin_edges = np.asarray(bins, dtype=float)
            if np.any(bin_edges <= 0):
                raise ValueError("Custom bins must be positive when log_scale=True.")
        bin_centers = np.sqrt(bin_edges[:-1] * bin_edges[1:])
    else:
        weights = _build_ae_weights(values, weight_mode)
        counts, bin_edges = np.histogram(values, bins=bins, weights=weights, density=normalize)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        return values, counts, bin_edges, bin_centers, weights

    weights = _build_ae_weights(values, weight_mode)
    counts, _ = np.histogram(values, bins=bin_edges, weights=weights, density=normalize)
    return values, counts, bin_edges, bin_centers, weights


def plot_ae_histogram(
    data: pd.DataFrame,
    bins: int | np.ndarray = 40,
    smooth_window: int = 5,
    log_scale: bool = False,
    normalize: bool = False,
    y_log_scale: bool = False,
    weight_mode: str = "none",
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    ae_column = "AE"
    if ae_column not in data.columns:
        raise KeyError(f"Required column is missing: {ae_column}")
    if smooth_window < 1:
        raise ValueError("smooth_window must be greater than or equal to 1.")

    work = data.loc[:, [ae_column]].copy()
    work[ae_column] = pd.to_numeric(work[ae_column], errors="coerce")
    work = work.dropna(subset=[ae_column])
    if work.empty:
        raise ValueError("No AE data found for histogram.")

    ae_values, counts, bin_edges, bin_centers, weights = _prepare_ae_histogram(
        work[ae_column].to_numpy(dtype=float),
        bins=bins,
        log_scale=log_scale,
        normalize=normalize,
        weight_mode=weight_mode,
    )
    smoothed_counts = _smooth_counts(counts, smooth_window=smooth_window)

    fig, ax = plt.subplots(figsize=(11, 7), layout="constrained")
    is_weighted = weights is not None
    ax.hist(
        ae_values,
        bins=bin_edges,
        weights=weights,
        density=normalize,
        edgecolor="black",
        color="#F48849",
        rwidth=0.85,
        label=("Weighted AE density" if normalize else "Weighted AE counts") if is_weighted else ("AE density" if normalize else "AE counts"),
    )
    ax.plot(bin_centers, smoothed_counts, color="#1369b7", linewidth=2.2, label="Smoothed envelope")
    if log_scale:
        ax.set_xscale("log")
    if y_log_scale:
        positive_counts = smoothed_counts[smoothed_counts > 0]
        if positive_counts.size == 0:
            raise ValueError("No positive histogram values found for logarithmic Y scale.")
        ax.set_yscale("log")
        ax.set_ylim(bottom=float(np.min(positive_counts)) * 0.5)

    title_parts = ["AE index distribution"]
    if normalize:
        title_parts.append("normalized")
    if is_weighted:
        title_parts.append(f"weighted: {weight_mode}")
    if log_scale:
        title_parts.append("log scale")
    if y_log_scale:
        title_parts.append("log Y")
    title_suffix = f" ({', '.join(title_parts[1:])})" if len(title_parts) > 1 else ""
    ax.set_title(title_parts[0] + title_suffix, fontsize=14)
    ax.set_xlabel("AE index" + (" (log scale)" if log_scale else ""), fontsize=12)
    ax.set_ylabel(("Weighted probability density" if is_weighted else "Probability density") if normalize else ("Weighted counts" if is_weighted else "Counts"), fontsize=12)
    ax.grid(alpha=0.35)
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))
    if not log_scale:
        ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(axis="both", which="major", color="black", length=8, width=1.5, labelsize=11)
    ax.tick_params(axis="both", which="minor", color="black", length=4, width=1)
    ax.legend(fontsize=11)

    suffix = (
        ("_normalized" if normalize else "")
        + (f"_weighted_{weight_mode}" if is_weighted else "")
        + ("_log" if log_scale else "")
        + ("_log_y" if y_log_scale else "")
    )
    resolved_output = Path(output_path) if output_path else Path(f"hist_ae_index{suffix}.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def plot_ae_histogram_inside_h_range(
    data: pd.DataFrame,
    component: str,
    h_value: float | None = None,
    delta: float | None = None,
    bins: int | np.ndarray = 40,
    smooth_window: int = 5,
    log_scale: bool = False,
    normalize: bool = False,
    y_log_scale: bool = False,
    weight_mode: str = "none",
    output_path: str | Path | None = None,
    show: bool = True,
    h_range: Sequence[float] | None = None,
) -> Path:
    normalized_component = component.lower()
    if normalized_component not in VALID_COMPONENTS:
        raise ValueError("Component must be one of: 'f', 'a', 'r'.")
    if smooth_window < 1:
        raise ValueError("smooth_window must be greater than or equal to 1.")

    h_column = f"H_{normalized_component}"
    ae_column = "AE"
    required_columns = (h_column, ae_column)
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    lower, upper = _resolve_h_range(h_range, h_value, delta)
    work = data.loc[:, required_columns].copy()
    work[h_column] = pd.to_numeric(work[h_column], errors="coerce")
    work[ae_column] = pd.to_numeric(work[ae_column], errors="coerce")
    work = work.dropna(subset=required_columns)
    work = work[(work[h_column] >= lower) & (work[h_column] <= upper)]
    if work.empty:
        raise ValueError(f"No AE data found where {h_column} is inside [{lower}, {upper}].")

    ae_values, counts, bin_edges, bin_centers, weights = _prepare_ae_histogram(
        work[ae_column].to_numpy(dtype=float),
        bins=bins,
        log_scale=log_scale,
        normalize=normalize,
        weight_mode=weight_mode,
    )
    smoothed_counts = _smooth_counts(counts, smooth_window=smooth_window)

    fig, ax = plt.subplots(figsize=(11, 7), layout="constrained")
    is_weighted = weights is not None
    ax.hist(
        ae_values,
        bins=bin_edges,
        weights=weights,
        density=normalize,
        edgecolor="black",
        color="#F48849",
        rwidth=0.85,
        label=("Weighted AE density" if normalize else "Weighted AE counts") if is_weighted else ("AE density" if normalize else "AE counts"),
    )
    ax.plot(bin_centers, smoothed_counts, color="#1369b7", linewidth=2.2, label="Smoothed envelope")
    if log_scale:
        ax.set_xscale("log")
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
    if log_scale:
        title_suffix.append("log scale")
    if y_log_scale:
        title_suffix.append("log Y")
    ax.set_title(
        f"AE distribution inside {h_column} in [{lower:.3g}, {upper:.3g}]"
        + (f" ({', '.join(title_suffix)})" if title_suffix else ""),
        fontsize=14,
    )
    ax.set_xlabel("AE index" + (" (log scale)" if log_scale else ""), fontsize=12)
    ax.set_ylabel(("Weighted probability density" if is_weighted else "Probability density") if normalize else ("Weighted counts" if is_weighted else "Counts"), fontsize=12)
    ax.grid(alpha=0.35)
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))
    if not log_scale:
        ax.xaxis.set_minor_locator(AutoMinorLocator())
    ax.tick_params(axis="both", which="major", color="black", length=8, width=1.5, labelsize=11)
    ax.tick_params(axis="both", which="minor", color="black", length=4, width=1)
    ax.legend(fontsize=11)

    suffix = (
        ("_normalized" if normalize else "")
        + (f"_weighted_{weight_mode}" if is_weighted else "")
        + ("_log" if log_scale else "")
        + ("_log_y" if y_log_scale else "")
    )
    resolved_output = Path(output_path) if output_path else Path(f"hist_ae_inside_H_{normalized_component}_{lower:g}_{upper:g}{suffix}.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output
