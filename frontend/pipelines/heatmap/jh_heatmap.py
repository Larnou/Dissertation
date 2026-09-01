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

from frontend.plot import plot_j_h_count_heatmap


def show_plots(
    config: AppConfig,
    components: tuple[str, ...],
    j_bin_step: float = 0.1,
    h_bin_step: float = 0.1,
    save_image: bool = False,
    show: bool = True,
) -> list[Path]:
    resolver = paths(config)
    prepared_data = pd.read_parquet(resolver.dataset(EventDataset.PREPARED))
    output_paths: list[Path] = []

    for component in components:
        output_path = None
        if save_image:
            output_path = resolver.image(f"heatmap_jh_{component}_counts")

        result_path = plot_j_h_count_heatmap(
            data=prepared_data,
            component=component,
            j_bin_step=j_bin_step,
            h_bin_step=h_bin_step,
            output_path=output_path,
            show=show,
        )
        output_paths.append(result_path)

    return output_paths


def main() -> None:
    config = get_config()
    show_plots(
        config=config,
        components=("f", "a", "r"),
        j_bin_step=0.05,
        h_bin_step=0.05,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
