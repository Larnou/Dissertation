from pathlib import Path
import sys

import matplotlib

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import EventDataset, paths

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot import plot_meas_vs_conv_with_regression


def show_plot(
    config: AppConfig,
    component: str,
    save_image: bool = False,
    show: bool = True,
) -> float:
    resolver = paths(config)
    parquet_path = resolver.dataset(EventDataset.PREPARED)
    output_path = None
    if save_image:
        output_path = resolver.image(f"scatter_{component}")
    angle = plot_meas_vs_conv_with_regression(
        parquet_path=parquet_path,
        component=component,
        show=show,
        output_path=output_path,
    )
    print(f"Regression angle ({component}): {angle:.2f}°")
    return angle


def main() -> None:
    config = get_config()
    show_plot(
        config=config,
        component="r",
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
