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

from frontend.plot import plot_component_dynamics


def read_prepared_data(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Prepared dataset not found: {resolved_path}")
    return pd.read_parquet(resolved_path)


def build_prepared_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("prepared_data")


def show_plot(
    config: AppConfig,
    component: str,
    start_time: str,
    end_time: str,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = PathResolver(config)
    prepared_data = read_prepared_data(build_prepared_data_path(config))

    output_path = None
    if save_image:
        output_path = resolver.image_file(
            f"dynamic_{component.lower()}_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}.png"
        )

    return plot_component_dynamics(
        data=prepared_data,
        component=component,
        start_time=start_time,
        end_time=end_time,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    component = "a"
    start_time = "2017-12-20 09:39:16"
    end_time = "2017-12-20 09:41:16"

    show_plot(
        config=config,
        component=component,
        start_time=start_time,
        end_time=end_time,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
