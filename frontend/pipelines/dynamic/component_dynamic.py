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

from frontend.plot import plot_component_dynamics


def show_plot(
    config: AppConfig,
    component: str,
    start_time: str,
    end_time: str,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = paths(config)
    prepared_data = pd.read_parquet(resolver.dataset(EventDataset.PREPARED))

    output_path = None
    if save_image:
        output_path = resolver.image(
            f"dynamic_{component.lower()}_{start_time.replace(' ', '_').replace(':', '-')}_{end_time.replace(' ', '_').replace(':', '-')}"
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
    show_plot(
        config=config,
        component="a",
        start_time="2017-12-20 09:39:16",
        end_time="2017-12-20 09:41:16",
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
