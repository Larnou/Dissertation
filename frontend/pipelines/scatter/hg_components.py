from pathlib import Path
import sys

import matplotlib

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import PathResolver

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot import plot_h_vs_g_components_scatter


def build_prepared_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("prepared_data")


def show_plot(
    config: AppConfig,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = PathResolver(config)
    parquet_path = build_prepared_data_path(config)
    output_path = None
    if save_image:
        output_path = resolver.image_file("scatter_h_vs_g_components.png")
    return plot_h_vs_g_components_scatter(
        parquet_path=parquet_path,
        show=show,
        output_path=output_path,
    )


def main() -> None:
    config = get_config()
    show_plot(
        config=config,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
