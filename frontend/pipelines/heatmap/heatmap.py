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

from frontend.plot import plot_beta_heatmap


def show_plot(
    config: AppConfig,
    lfrom: int,
    lto: int,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = paths(config)
    available_data = pd.read_parquet(resolver.dataset(EventDataset.AVAILABLE))

    output_path = None
    if save_image:
        output_path = resolver.image(f"heatmap_beta_l{lfrom}-l{lto}")

    return plot_beta_heatmap(
        data=available_data,
        lfrom=lfrom,
        lto=lto,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    show_plot(
        config=config,
        lfrom=4,
        lto=12,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
