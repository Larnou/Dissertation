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

from frontend.plot import plot_j_g_count_heatmap


def read_prepared_data(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Prepared dataset not found: {resolved_path}")
    return pd.read_parquet(resolved_path)


def build_prepared_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("prepared_data")


def show_plots(
    config: AppConfig,
    components: tuple[str, ...],
    j_bin_step: float = 1e-10,
    g_bin_step: float = 0.1,
    save_image: bool = False,
    show: bool = True,
) -> list[Path]:
    resolver = PathResolver(config)
    prepared_data = read_prepared_data(build_prepared_data_path(config))
    output_paths: list[Path] = []

    for component in components:
        output_path = None
        if save_image:
            output_path = resolver.image_file(f"heatmap_jg_{component}_counts.png")

        result_path = plot_j_g_count_heatmap(
            data=prepared_data,
            component=component,
            j_bin_step=j_bin_step,
            g_bin_step=g_bin_step,
            output_path=output_path,
            show=show,
        )
        output_paths.append(result_path)

    return output_paths


def main() -> None:
    config = get_config()
    components = ("f", "a", "r")

    show_plots(
        config=config,
        components=components,
        j_bin_step=1e-10,
        g_bin_step=0.05,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
