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

from frontend.plot import plot_meas_vs_conv_with_regression


def build_prepared_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("prepared_data")


def show_plot(
    config: AppConfig,
    component: str,
    save_image: bool = False,
    show: bool = True,
) -> float:
    resolver = PathResolver(config)
    parquet_path = build_prepared_data_path(config)
    output_path = None
    if save_image:
        output_path = resolver.image_file(f"scatter_{component}.png")
    angle = plot_meas_vs_conv_with_regression(
        parquet_path=parquet_path,
        component=component,
        show=show,
        output_path=output_path,
    )
    print(f"Regression angle ({component}): {angle:.2f}°")
    return angle


def main() -> None:
    # Доступные компоненты: f, r, a
    config = get_config()
    component = "r"

    show_plot(
        config=config,
        component=component,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()