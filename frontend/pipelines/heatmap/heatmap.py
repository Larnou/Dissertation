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

from frontend.plot import plot_beta_heatmap


def read_prepared_data(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Prepared dataset not found: {resolved_path}")
    return pd.read_parquet(resolved_path)


def build_prepared_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("available_data")


def show_plot(
    config: AppConfig,
    lfrom: int,
    lto: int,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = PathResolver(config)
    prepared_data_path = build_prepared_data_path(config)
    prepared_data = read_prepared_data(prepared_data_path)

    output_path = None
    if save_image:
        output_path = resolver.image_file(f"heatmap_beta_l{lfrom}-l{lto}.png")

    return plot_beta_heatmap(
        data=prepared_data,
        lfrom=lfrom,
        lto=lto,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    lfrom = 4
    lto = 16

    show_plot(
        config=config,
        lfrom=lfrom,
        lto=lto,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
