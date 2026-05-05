from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_h_vs_g_components_scatter(
    parquet_path: str | Path,
    show: bool = True,
    output_path: str | Path | None = None,
) -> Path:
    resolved_parquet_path = Path(parquet_path).expanduser().resolve()
    if not resolved_parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {resolved_parquet_path}")

    dataframe = pd.read_parquet(resolved_parquet_path)
    components = ("f", "a", "r")
    colors = {"f": "#1f77b4", "a": "#2ca02c", "r": "#d62728"}

    fig, ax = plt.subplots(figsize=(10, 8))

    for component in components:
        g_column = f"G_{component}"
        h_column = f"H_{component}"
        if g_column not in dataframe.columns or h_column not in dataframe.columns:
            raise KeyError(f"Required columns are missing: {g_column}, {h_column}")

        pair_data = dataframe[[g_column, h_column]].dropna()
        if pair_data.empty:
            continue

        x_values = pair_data[g_column].to_numpy(dtype=float)
        y_values = pair_data[h_column].to_numpy(dtype=float)
        ax.scatter(
            x_values,
            y_values,
            s=14,
            alpha=0.45,
            color=colors[component],
            edgecolors="none",
            label=f"{component}: H_{component} vs G_{component} (n={len(pair_data)})",
        )

    ax.set_xlabel("G component", size=12)
    ax.set_ylabel("H component", size=12)
    ax.set_title("Scatter: H vs G for all components", size=14)
    ax.grid(alpha=0.4)
    ax.axhline(0, color="black", linewidth=0.8, alpha=0.7)
    ax.axvline(0, color="black", linewidth=0.8, alpha=0.7)
    ax.legend(fontsize=9)

    resolved_output_path = Path(output_path) if output_path else Path("scatter_h_vs_g_components.png")
    if output_path is not None:
        resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output_path, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output_path
