from pathlib import Path
import sys

import matplotlib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot import SatellitePlot

def read_distribution_matrix(path: str | Path):
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Distribution file not found: {resolved_path}")
    return pd.read_csv(resolved_path).to_numpy(dtype=float)


def build_path(event_data: str, satellite: str, parameter: str, component: str, reducer: str) -> Path:
    distributions_dir = PROJECT_ROOT / "backend" / "data" / "events" / event_data / satellite / "distributions"
    return distributions_dir / f"distribution_{parameter}_{component}_{reducer}.csv"


def show_plot(event_data: str, satellite: str, parameter: str, component: str, reducer: str) -> None:
    path = build_path(event_data, satellite, parameter, component, reducer)
    matrix = read_distribution_matrix(path)
    print(f"Using matplotlib backend: {matplotlib.get_backend()}")
    print("Opening interactive plot window...")

    plotter = SatellitePlot(min_lshell=4)
    plotter.draw_polar_plot(
        matrix,
        max_lshell=min(16, matrix.shape[0]),
        title=f"{satellite} {parameter}_{component}_{reducer}",
        show=True,
    )


def main() -> None:
    # Доступные parameter: G, H
    # Доступные компоненты: f, r, a
    # Доступные reducer: mean, median, q25, q75

    event_data = "2017-01-01_2017-01-04"
    satellite = "THEMIS-A"
    parameter = "H"
    component = "a"
    reducer = "mean"
    show_plot(event_data, satellite, parameter, component, reducer)




if __name__ == "__main__":
    main()
