from pathlib import Path
import sys

import matplotlib
import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import Component, DistributionParameter, paths

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot.global_histogram import show_hist_sectors_from_long


def show_plot(
    config: AppConfig,
    parameter: str,
    component: str,
    lfrom: int,
    lto: int,
    bin_step: float = 0.05,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = paths(config)
    matrix_path = resolver.distribution_raw_long(
        DistributionParameter(parameter),
        Component(component),
    )
    distribution_data = pd.read_parquet(matrix_path)

    output_path = None
    if save_image:
        output_path = resolver.image(f"hist_{parameter}_{component}_l{lfrom}-l{lto}")

    return show_hist_sectors_from_long(
        data=distribution_data,
        parameter=parameter,
        component=component,
        lfrom=lfrom,
        lto=lto,
        bin_step=bin_step,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    show_plot(
        config=config,
        parameter="H",
        component="a",
        lfrom=4,
        lto=12,
        bin_step=0.05,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
