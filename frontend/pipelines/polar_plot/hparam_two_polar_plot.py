from pathlib import Path
import sys

import matplotlib
import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import Component, DistributionParameter, Reducer, paths

PROJECT_ROOT = Path(__file__).resolve().parents[3]
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
) -> tuple[Path, Path]:
    resolver = paths(config)
    distribution_parameter = DistributionParameter(parameter)
    return (
        resolver.distribution_map(
            distribution_parameter,
            Reducer(reducers[0]),
            component=Component(component),
        ),
        resolver.distribution_map(
            distribution_parameter,
            Reducer(reducers[1]),
            component=Component(component),
        ),
    )


def show_plot(
    config: AppConfig,
    parameter: str,
    component: str,
    reducers: list[str],
    save_image: bool = False,
) -> None:
    resolver = paths(config)
    path_1, path_2 = build_paths(config, parameter, component, reducers)

    data_1 = read_matrix(path_1)
    data_2 = read_matrix(path_2)

    output_path = None
    if save_image:
        output_path = resolver.image(f"hparam_two_{parameter}_{component}_{reducers[0]}_{reducers[1]}")
    SatellitePlot().draw_hparam_plots(
        data_1,
        data_2,
        component=component,
        save_image=save_image,
        first_title=reducers[0],
        second_title=reducers[1],
        output_path=output_path,
        show=True,
    )


if __name__ == "__main__":
    config = get_config()
    show_plot(
        config=config,
        parameter="H",
        component="a",
        reducers=["mean", "q25"],
        save_image=True,
    )
