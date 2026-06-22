from pathlib import Path
import sys

import matplotlib
import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import PathResolver

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot import SatellitePlot

def read_distribution_matrix(path: str | Path):
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Distribution file not found: {resolved_path}")
    return pd.read_csv(resolved_path).to_numpy(dtype=float)


def build_path(config: AppConfig, parameter: str, component: str, reducer: str) -> Path:
    if parameter != "Beta":
        return PathResolver(config).distribution_file(f"distribution_{parameter}_{component}_{reducer}.csv")
    else:
        return PathResolver(config).distribution_file(f"distribution_{parameter}_{reducer}.csv")


def show_plot(
    config: AppConfig,
    parameter: str,
    component: str,
    reducer: str,
    save_image: bool = False,
) -> None:
    resolver = PathResolver(config)
    path = build_path(config, parameter, component, reducer)
    matrix = read_distribution_matrix(path)
    print(f"Using matplotlib backend: {matplotlib.get_backend()}")
    print("Opening interactive plot window...")

    output_path = None
    if save_image and parameter != "Beta":
        output_path = resolver.image_file(f"polar_{parameter}_{component}_{reducer}.png")
    elif parameter == "Beta":
        output_path = resolver.image_file(f"polar_{parameter}_{reducer}.png")

    value_scale = 1e9 if parameter == "J" else 1.0
    color_norm = "asinh" if parameter == "J" else "linear"
    unit_suffix = " [nA/m²]" if parameter == "J" else ""
    plotter = SatellitePlot(min_lshell=4)
    plotter.draw_polar_plot(
        matrix,
        max_lshell=min(16, matrix.shape[0]),
        title=f"{resolver.satellite_id} {parameter}_{component}_{reducer}{unit_suffix}",
        output_path=output_path,
        show=True,
        value_scale=value_scale,
        color_norm=color_norm,
    )


def main() -> None:
    # Доступные parameter: G, H, Beta, J
    # Доступные компоненты: f, r, a
    # Доступные reducer: mean, median, q25, q75

    config = get_config()
    parameter = "J"
    component = "a"
    reducer = "mean"
    show_plot(config, parameter, component, reducer, save_image=True)




if __name__ == "__main__":
    main()
