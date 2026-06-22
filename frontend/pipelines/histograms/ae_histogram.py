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

from frontend.plot import plot_ae_histogram


def read_available_data(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Available dataset not found: {resolved_path}")
    return pd.read_parquet(resolved_path)


def build_available_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("available_data")


def show_plot(
    config: AppConfig,
    bins: int = 40,
    smooth_window: int = 5,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = PathResolver(config)
    available_data = read_available_data(build_available_data_path(config))

    output_path = None
    if save_image:
        output_path = resolver.image_file("hist_ae_index.png")

    return plot_ae_histogram(
        data=available_data,
        bins=bins,
        smooth_window=smooth_window,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    bins = 40
    smooth_window = 5

    show_plot(
        config=config,
        bins=bins,
        smooth_window=smooth_window,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
