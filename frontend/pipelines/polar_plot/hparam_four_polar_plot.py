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


def read_matrix(path: str | Path):
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Matrix file not found: {resolved}")
    return pd.read_csv(resolved).to_numpy(dtype=float)


def build_paths(
    config: AppConfig,
    parameter: str,
    component: str,
    reducers: list[str],
) -> tuple[Path, Path, Path, Path]:
    if len(reducers) != 4:
        raise ValueError("reducers must contain exactly four values, for example ['mean', 'median', 'q25', 'q75'].")

    resolver = PathResolver(config)
    mean_path = resolver.distribution_file(f"distribution_{parameter}_{component}_{reducers[0]}.csv")
    median_path = resolver.distribution_file(f"distribution_{parameter}_{component}_{reducers[1]}.csv")
    q1_path = resolver.distribution_file(f"distribution_{parameter}_{component}_{reducers[2]}.csv")
    q3_path = resolver.distribution_file(f"distribution_{parameter}_{component}_{reducers[3]}.csv")
    return mean_path, median_path, q1_path, q3_path


def show_plot(
    config: AppConfig,
    parameter: str,
    component: str,
    reducers: list[str],
    save_image: bool = False,
) -> None:
    resolver = PathResolver(config)
    mean_path, median_path, q1_path, q3_path = build_paths(config, parameter, component, reducers)

    mean_data = read_matrix(mean_path)
    median_data = read_matrix(median_path)
    q1_data = read_matrix(q1_path)
    q3_data = read_matrix(q3_path)

    output_path = None
    if save_image:
        output_path = resolver.image_file(f"hparam_four_{parameter}_{component}_{reducers[0]}_{reducers[1]}_{reducers[2]}_{reducers[3]}.png")
    SatellitePlot().draw_hparam_four_plots(
        mean_matrix=mean_data,
        median_matrix=median_data,
        q1_matrix=q1_data,
        q3_matrix=q3_data,
        component=component,
        save_image=save_image,
        mean_title=reducers[0],
        median_title=reducers[1],
        q1_title=reducers[2],
        q3_title=reducers[3],
        output_path=output_path,
        show=True,
    )


def main() -> None:
    # Доступные parameter: G, H
    # Доступные компоненты: f, r, a
    # Доступные reducer: mean, median, q25, q75

    config = get_config()
    parameter = "G"
    component = "f"
    reducers = ["mean", "median", "q25", "q75"]
    show_plot(config, parameter, component, reducers, save_image=False)


if __name__ == "__main__":
    main()
