from pathlib import Path
import sys

import matplotlib
import pandas as pd

from backend.src.config import get_config
from backend.src.config.schemas import AppConfig
from backend.src.io.paths import KyotoIndex, paths

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot import plot_ae_symh_daily
from frontend.plot.kyoto_daily import daily_image_name


def show_plot(
    config: AppConfig,
    day: str,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = paths(config)
    ae_data = pd.read_parquet(resolver.kyoto.index_parquet(KyotoIndex.AE))
    symh_data = pd.read_parquet(resolver.kyoto.index_parquet(KyotoIndex.SYMH))

    output_path = None
    if save_image:
        output_path = resolver.kyoto.dynamics_file(daily_image_name("ae_symh", day))

    return plot_ae_symh_daily(
        ae_data=ae_data,
        symh_data=symh_data,
        day=day,
        output_path=output_path,
        show=show,
    )


def show_range(
    config: AppConfig,
    start_day: str,
    end_day: str,
    save_image: bool = False,
    show: bool = False,
) -> list[Path]:
    resolver = paths(config)
    ae_data = pd.read_parquet(resolver.kyoto.index_parquet(KyotoIndex.AE))
    symh_data = pd.read_parquet(resolver.kyoto.index_parquet(KyotoIndex.SYMH))
    days = pd.date_range(start=start_day, end=end_day, freq="D")
    paths_out: list[Path] = []

    for day in days:
        output_path = None
        if save_image:
            output_path = resolver.kyoto.dynamics_file(daily_image_name("ae_symh", day))
        paths_out.append(
            plot_ae_symh_daily(
                ae_data=ae_data,
                symh_data=symh_data,
                day=day,
                output_path=output_path,
                show=show,
            )
        )
    return paths_out


def main() -> None:
    config = get_config()
    show_plot(
        config=config,
        day="2017-03-31",
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
