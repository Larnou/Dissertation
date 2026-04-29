from pathlib import Path
import sys
import matplotlib
matplotlib.use("TkAgg", force=True)

import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import PathResolver

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from frontend.plot import SatellitePlot




def read_matrix(path: str | Path):
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Matrix file not found: {resolved}")
    return pd.read_csv(resolved).to_numpy(dtype=float)

def build_paths(config: AppConfig, parameter: str, component: str, reducer: list[str]) -> tuple[Path, Path]:
    resolver = PathResolver(config)
    path_1 = resolver.distribution_file(f"distribution_{parameter}_{component}_{reducer[0]}.csv")
    path_2 = resolver.distribution_file(f"distribution_{parameter}_{component}_{reducer[1]}.csv")
    return path_1, path_2


def show_plot(config: AppConfig, parameter, component, reducer, save_image: bool = False) -> None:
    resolver = PathResolver(config)
    path_1, path_2 = build_paths(config, parameter, component, reducer)

    data_1 = read_matrix(path_1)
    data_2 = read_matrix(path_2)

    output_path = None
    if save_image:
        output_path = resolver.image_file(f"hparam_two_{parameter}_{component}_{reducer[0]}_{reducer[1]}.png")
    SatellitePlot().draw_hparam_plots(
        data_1,
        data_2,
        component=component,
        save_image=save_image,
        first_title=reducer[0],
        second_title=reducer[1],
        output_path=output_path,
        show=True,
    )


if __name__ == "__main__":

    # Доступные parameter: G, H
    # Доступные компоненты: f, r, a
    # Доступные reducer: mean, median, q25, q75

    config = get_config()
    parameter = 'H'
    component = 'f'
    reducer = ['mean', 'q25']

    show_plot(config, parameter, component, reducer, save_image=False)
