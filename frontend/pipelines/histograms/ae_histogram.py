from pathlib import Path
import sys

import matplotlib
import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import EventDataset, paths

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot import plot_ae_histogram


def show_plot(
    config: AppConfig,
    bins: int = 40,
    smooth_window: int = 5,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = paths(config)
    available_data = pd.read_parquet(resolver.dataset(EventDataset.AVAILABLE))

    output_path = None
    if save_image:
        output_path = resolver.image("hist_ae_index")

    return plot_ae_histogram(
        data=available_data,
        bins=bins,
        smooth_window=smooth_window,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    show_plot(
        config=config,
        bins=40,
        smooth_window=5,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
