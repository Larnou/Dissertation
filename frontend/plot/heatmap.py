from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


REQUIRED_COLUMNS: tuple[str, ...] = ("L", "MLT", "beta")


def _prepare_beta_bins(data: pd.DataFrame, lfrom: int, lto: int) -> pd.DataFrame:
    missing = [column for column in REQUIRED_COLUMNS if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    work = data.loc[:, REQUIRED_COLUMNS].copy()
    work = work.dropna(subset=["L", "MLT", "beta"])

    work["L_bin"] = np.floor(work["L"]).astype(int)
    work["MLT_bin"] = np.floor(work["MLT"]).astype(int) % 24

    return work[(work["L_bin"] >= lfrom) & (work["L_bin"] < lto)].copy()


def plot_beta_heatmap(
    data: pd.DataFrame,
    lfrom: int,
    lto: int,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    prepared = _prepare_beta_bins(data, lfrom, lto)
    if prepared.empty:
        raise ValueError("No beta data found for the selected L range.")

    aggregated = (
        prepared.groupby(["L_bin", "MLT_bin"], as_index=False)["beta"]
        .mean()
        .pivot(index="L_bin", columns="MLT_bin", values="beta")
        .reindex(index=range(lfrom, lto), columns=range(24))
    )

    fig, ax = plt.subplots(figsize=(12, 6), layout="constrained")
    image = ax.imshow(
        aggregated.values,
        origin="lower",
        aspect="auto",
        interpolation="nearest",
        extent=[0, 24, lfrom, lto],
        cmap="viridis",
    )

    ax.set_title(f"Average beta distribution by MLT and L (L={lfrom}-{lto})", fontsize=14)
    ax.set_xlabel("MLT", fontsize=12)
    ax.set_ylabel("L-shell", fontsize=12)
    ax.set_xticks(np.arange(0, 25, 3))
    ax.set_xticks(np.arange(0, 25, 1), minor=True)
    ax.set_yticks(np.arange(lfrom, lto + 1, 1))
    ax.grid(color="white", alpha=0.6, linewidth=0.5)
    ax.tick_params(axis="x", which="minor", color="black", length=4, width=1)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("Average beta", fontsize=11)

    resolved_output = Path(output_path) if output_path else Path(f"beta_heatmap_l{lfrom}-l{lto}.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def plot_beta_h_count_heatmap(
    data: pd.DataFrame,
    h_parameter: str,
    beta_bin_step: float = 0.1,
    h_bin_step: float = 0.1,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    required = ("beta", h_parameter)
    missing = [column for column in required if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    work = data.loc[:, required].dropna()
    work = work[(work["beta"] >= 0.0) & (work["beta"] <= 1.5)]
    if work.empty:
        raise ValueError("No paired beta and H data found for heatmap in 0 <= beta <= 10 range.")

    beta_values = work["beta"].to_numpy(dtype=float)
    h_values = work[h_parameter].to_numpy(dtype=float)

    beta_min = float(np.floor(np.min(beta_values) / beta_bin_step) * beta_bin_step)
    beta_max = float(np.ceil(np.max(beta_values) / beta_bin_step) * beta_bin_step)
    h_min = float(np.floor(np.min(h_values) / h_bin_step) * h_bin_step)
    h_max = float(np.ceil(np.max(h_values) / h_bin_step) * h_bin_step)

    if np.isclose(beta_min, beta_max):
        beta_max = beta_min + beta_bin_step
    if np.isclose(h_min, h_max):
        h_max = h_min + h_bin_step

    beta_edges = np.arange(beta_min, beta_max + beta_bin_step, beta_bin_step)
    h_edges = np.arange(h_min, h_max + h_bin_step, h_bin_step)

    counts, _, _ = np.histogram2d(beta_values, h_values, bins=(beta_edges, h_edges))

    fig, ax = plt.subplots(figsize=(10, 7), layout="constrained")
    image = ax.imshow(
        counts.T,
        origin="lower",
        aspect="auto",
        interpolation="nearest",
        extent=[beta_edges[0], beta_edges[-1], h_edges[0], h_edges[-1]],
        cmap="plasma",
    )

    ax.set_title(f"Counts per beta-{h_parameter} cell", fontsize=14)
    ax.set_xlabel("beta", fontsize=12)
    ax.set_ylabel(h_parameter, fontsize=12)
    ax.grid(color="white", alpha=0.6, linewidth=0.4)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("Counts", fontsize=11)

    resolved_output = Path(output_path) if output_path else Path(f"beta_{h_parameter}_count_heatmap.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def plot_h_g_count_heatmap(
    data: pd.DataFrame,
    component: str,
    g_bin_step: float = 0.1,
    h_bin_step: float = 0.1,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    g_parameter = f"G_{component}"
    h_parameter = f"H_{component}"
    required = (g_parameter, h_parameter)
    missing = [column for column in required if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    work = data.loc[:, required].dropna()
    if work.empty:
        raise ValueError(f"No paired {h_parameter} and {g_parameter} data found for heatmap.")

    g_values = work[g_parameter].to_numpy(dtype=float)
    h_values = work[h_parameter].to_numpy(dtype=float)

    g_min = float(np.floor(np.min(g_values) / g_bin_step) * g_bin_step)
    g_max = float(np.ceil(np.max(g_values) / g_bin_step) * g_bin_step)
    h_min = float(np.floor(np.min(h_values) / h_bin_step) * h_bin_step)
    h_max = float(np.ceil(np.max(h_values) / h_bin_step) * h_bin_step)

    if np.isclose(g_min, g_max):
        g_max = g_min + g_bin_step
    if np.isclose(h_min, h_max):
        h_max = h_min + h_bin_step

    g_edges = np.arange(g_min, g_max + g_bin_step, g_bin_step)
    h_edges = np.arange(h_min, h_max + h_bin_step, h_bin_step)

    counts, _, _ = np.histogram2d(g_values, h_values, bins=(g_edges, h_edges))

    fig, ax = plt.subplots(figsize=(10, 7), layout="constrained")
    image = ax.imshow(
        counts.T,
        origin="lower",
        aspect="auto",
        interpolation="nearest",
        extent=[g_edges[0], g_edges[-1], h_edges[0], h_edges[-1]],
        cmap="magma",
    )

    ax.set_title(f"Counts per {h_parameter}-{g_parameter} cell", fontsize=14)
    ax.set_xlabel(g_parameter, fontsize=12)
    ax.set_ylabel(h_parameter, fontsize=12)
    ax.grid(color="white", alpha=0.6, linewidth=0.4)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("Counts", fontsize=11)

    resolved_output = Path(output_path) if output_path else Path(f"{h_parameter}_{g_parameter}_count_heatmap.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def plot_j_h_count_heatmap(
    data: pd.DataFrame,
    component: str,
    j_bin_step: float = 1e-10,
    h_bin_step: float = 0.1,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    j_parameter = f"J_{component}"
    h_parameter = f"H_{component}"
    required = (j_parameter, h_parameter)
    missing = [column for column in required if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    work = data.loc[:, required].dropna()
    if work.empty:
        raise ValueError(f"No paired {h_parameter} and {j_parameter} data found for heatmap.")

    j_values = work[j_parameter].to_numpy(dtype=float)
    h_values = work[h_parameter].to_numpy(dtype=float)

    j_min = float(np.floor(np.min(j_values) / j_bin_step) * j_bin_step)
    j_max = float(np.ceil(np.max(j_values) / j_bin_step) * j_bin_step)
    h_min = float(np.floor(np.min(h_values) / h_bin_step) * h_bin_step)
    h_max = float(np.ceil(np.max(h_values) / h_bin_step) * h_bin_step)

    if np.isclose(j_min, j_max):
        j_max = j_min + j_bin_step
    if np.isclose(h_min, h_max):
        h_max = h_min + h_bin_step

    j_edges = np.arange(j_min, j_max + j_bin_step, j_bin_step)
    h_edges = np.arange(h_min, h_max + h_bin_step, h_bin_step)

    counts, _, _ = np.histogram2d(j_values, h_values, bins=(j_edges, h_edges))

    fig, ax = plt.subplots(figsize=(10, 7), layout="constrained")
    image = ax.imshow(
        counts.T,
        origin="lower",
        aspect="auto",
        interpolation="nearest",
        extent=[j_edges[0], j_edges[-1], h_edges[0], h_edges[-1]],
        cmap="cividis",
    )

    ax.set_title(f"Counts per {h_parameter}-{j_parameter} cell", fontsize=14)
    ax.set_xlabel(j_parameter, fontsize=12)
    ax.set_ylabel(h_parameter, fontsize=12)
    ax.grid(color="white", alpha=0.6, linewidth=0.4)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("Counts", fontsize=11)

    resolved_output = Path(output_path) if output_path else Path(f"{h_parameter}_{j_parameter}_count_heatmap.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output


def plot_j_g_count_heatmap(
    data: pd.DataFrame,
    component: str,
    j_bin_step: float = 1e-10,
    g_bin_step: float = 0.1,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    j_parameter = f"J_{component}"
    g_parameter = f"G_{component}"
    required = (j_parameter, g_parameter)
    missing = [column for column in required if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    work = data.loc[:, required].dropna()
    if work.empty:
        raise ValueError(f"No paired {g_parameter} and {j_parameter} data found for heatmap.")

    j_values = work[j_parameter].to_numpy(dtype=float)
    g_values = work[g_parameter].to_numpy(dtype=float)

    j_min = float(np.floor(np.min(j_values) / j_bin_step) * j_bin_step)
    j_max = float(np.ceil(np.max(j_values) / j_bin_step) * j_bin_step)
    g_min = float(np.floor(np.min(g_values) / g_bin_step) * g_bin_step)
    g_max = float(np.ceil(np.max(g_values) / g_bin_step) * g_bin_step)

    if np.isclose(j_min, j_max):
        j_max = j_min + j_bin_step
    if np.isclose(g_min, g_max):
        g_max = g_min + g_bin_step

    j_edges = np.arange(j_min, j_max + j_bin_step, j_bin_step)
    g_edges = np.arange(g_min, g_max + g_bin_step, g_bin_step)

    counts, _, _ = np.histogram2d(j_values, g_values, bins=(j_edges, g_edges))

    fig, ax = plt.subplots(figsize=(10, 7), layout="constrained")
    image = ax.imshow(
        counts.T,
        origin="lower",
        aspect="auto",
        interpolation="nearest",
        extent=[j_edges[0], j_edges[-1], g_edges[0], g_edges[-1]],
        cmap="inferno",
    )

    ax.set_title(f"Counts per {g_parameter}-{j_parameter} cell", fontsize=14)
    ax.set_xlabel(j_parameter, fontsize=12)
    ax.set_ylabel(g_parameter, fontsize=12)
    ax.grid(color="white", alpha=0.6, linewidth=0.4)

    colorbar = fig.colorbar(image, ax=ax, pad=0.02)
    colorbar.set_label("Counts", fontsize=11)

    resolved_output = Path(output_path) if output_path else Path(f"{g_parameter}_{j_parameter}_count_heatmap.png")
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output
