from pathlib import Path
import sys

import matplotlib
import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import PathResolver

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot.global_histogram import show_hist_sectors_from_long


def read_distribution_raw_long(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Raw distribution file not found: {resolved_path}")
    return pd.read_parquet(resolved_path)


def build_raw_long_path(config: AppConfig, parameter: str, component: str) -> Path:
    return PathResolver(config).matrix_file(f"distribution_raw_long_{parameter}_{component}.parquet")


def show_plot(
    config: AppConfig,
    parameter: str,
    component: str,
    lfrom: int,
    lto: int,
    save_image: bool = False,
    show: bool = True,
) -> None:
    resolver = PathResolver(config)
    raw_long_path = build_raw_long_path(config, parameter, component)
    raw_long = read_distribution_raw_long(raw_long_path)
    parameter_name = f"{parameter}_{component}"
    output_path = None
    if save_image:
        output_path = resolver.image_file(f"hist_{parameter}_{component}_l{lfrom}-l{lto}.png")
    show_hist_sectors_from_long(
        raw_long=raw_long,
        parameter_name=parameter_name,
        parameter=parameter,
        component=component,
        lfrom=lfrom,
        lto=lto,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    # Доступные parameter: G, H
    # Доступные компоненты: f, r, a
    config = get_config()
    parameter = "J"
    component = "a"
    lfrom = 4
    lto = 16

    show_plot(config, parameter, component, lfrom, lto, save_image=True, show=True)


if __name__ == "__main__":
    main()
