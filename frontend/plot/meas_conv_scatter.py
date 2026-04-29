from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def plot_meas_vs_conv_with_regression(
    parquet_path: str | Path,
    component: str,
    show: bool = True,
    output_path: str | Path | None = None,
) -> float:
    resolved_parquet_path = Path(parquet_path).expanduser().resolve()
    if not resolved_parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {resolved_parquet_path}")

    column_x = f"X_{component}_conv"
    column_y = f"E_{component}_meas"

    dataframe = pd.read_parquet(resolved_parquet_path)
    pair_data = dataframe[[column_x, column_y]].dropna()

    x_values = np.abs(pair_data[column_x].to_numpy(dtype=float))
    y_values = np.abs(pair_data[column_y].to_numpy(dtype=float))

    fig, ax = plt.subplots(figsize=(10, 8))
    ax.scatter(x_values, y_values, s=16, alpha=0.55, color="#1f77b4", edgecolors="none", label="Data points")

    slope, intercept = np.polyfit(x_values, y_values, 1)
    x_line = np.linspace(np.min(x_values), np.max(x_values), 200)
    y_regression = slope * x_line + intercept
    regression_angle_deg = float(np.degrees(np.arctan(slope)))

    ax.plot(
        x_line,
        y_regression,
        color="#d62728",
        linewidth=2.0,
        label=f"Regression: y={slope:.3f}x+{intercept:.3f}, angle={regression_angle_deg:.2f}°",
    )

    x_max_abs = max(float(np.max(np.abs(x_values))), 1.0)
    y_max_abs = max(float(np.max(np.abs(y_values))), 1.0)
    guide_span = max(x_max_abs, y_max_abs)
    guide_x = np.linspace(0.0, guide_span, 2)

    for angle in (30, 45, 60):
        guide_slope = np.tan(np.radians(angle))
        ax.plot(
            guide_x,
            guide_slope * guide_x,
            linestyle="--",
            linewidth=1.4,
            alpha=0.9,
            label=f"{angle}° line",
        )

    ax.set_xlabel(f"|{column_x}|", size=12)
    ax.set_ylabel(f"|{column_y}|", size=12)
    ax.set_title(f"Scatter |{column_y}| vs |{column_x}|", size=14)
    ax.grid(alpha=0.35)
    ax.set_xlim(left=-0.1)
    ax.set_ylim(bottom=-0.1)
    ax.axhline(0, color="black", linewidth=0.8, alpha=0.6)
    ax.axvline(0, color="black", linewidth=0.8, alpha=0.6)
    ax.set_aspect("equal", adjustable="box")
    ax.legend(fontsize=9)

    if output_path is not None:
        resolved_output_path = Path(output_path).expanduser().resolve()
        resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output_path, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return regression_angle_deg
