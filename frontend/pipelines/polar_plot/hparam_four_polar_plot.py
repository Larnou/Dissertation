from pathlib import Path
import sys

import matplotlib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot import SatellitePlot


def read_matrix(path: str | Path):
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Matrix file not found: {resolved}")
    return pd.read_csv(resolved).to_numpy(dtype=float)


def build_paths(
    event_data: str,
    satellite: str,
    parameter: str,
    component: str,
    reducers: list[str],
) -> tuple[Path, Path, Path, Path]:
    if len(reducers) != 4:
        raise ValueError("reducers must contain exactly four values, for example ['mean', 'median', 'q25', 'q75'].")

    distributions_dir = PROJECT_ROOT / "backend" / "data" / "events" / event_data / satellite / "distributions"
    mean_path = distributions_dir / f"distribution_{parameter}_{component}_{reducers[0]}.csv"
    median_path = distributions_dir / f"distribution_{parameter}_{component}_{reducers[1]}.csv"
    q1_path = distributions_dir / f"distribution_{parameter}_{component}_{reducers[2]}.csv"
    q3_path = distributions_dir / f"distribution_{parameter}_{component}_{reducers[3]}.csv"
    return mean_path, median_path, q1_path, q3_path


def show_plot(event_data: str, satellite: str, parameter: str, component: str, reducers: list[str]) -> None:
    mean_path, median_path, q1_path, q3_path = build_paths(event_data, satellite, parameter, component, reducers)

    mean_data = read_matrix(mean_path)
    median_data = read_matrix(median_path)
    q1_data = read_matrix(q1_path)
    q3_data = read_matrix(q3_path)

    SatellitePlot().draw_hparam_four_plots(
        mean_matrix=mean_data,
        median_matrix=median_data,
        q1_matrix=q1_data,
        q3_matrix=q3_data,
        component=component,
        save_image=False,
        mean_title=reducers[0],
        median_title=reducers[1],
        q1_title=reducers[2],
        q3_title=reducers[3],
        show=True,
    )


def main() -> None:
    # Доступные parameter: G, H
    # Доступные компоненты: f, r, a
    # Доступные reducer: mean, median, q25, q75

    event_data = "2017-01-01_2017-01-04"
    satellite = "THEMIS-A"
    parameter = "G"
    component = "f"
    reducers = ["mean", "median", "q25", "q75"]
    show_plot(event_data, satellite, parameter, component, reducers)


if __name__ == "__main__":
    main()
