from pathlib import Path
import sys
import matplotlib
matplotlib.use("TkAgg", force=True)

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from frontend.plot import SatellitePlot




def read_matrix(path: str | Path):
    resolved = Path(path).expanduser().resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"Matrix file not found: {resolved}")
    return pd.read_csv(resolved).to_numpy(dtype=float)

def build_paths(event_data: str, satellite: str, parameter: str, component: str, reducer: list[str]) -> tuple[Path, Path]:

    distributions_dir = PROJECT_ROOT / "backend" / "data" / "events" / event_data / satellite / "distributions"
    path_1 = distributions_dir / f"distribution_{parameter}_{component}_{reducer[0]}.csv"
    path_2 = distributions_dir / f"distribution_{parameter}_{component}_{reducer[1]}.csv"
    return path_1, path_2


def show_plot(event_data, satellite, parameter, component, reducer) -> None:

    path_1, path_2 = build_paths(event_data, satellite, parameter, component, reducer)

    data_1 = read_matrix(path_1)
    data_2 = read_matrix(path_2)

    SatellitePlot().draw_hparam_plots(
        data_1,
        data_2,
        component=component,
        save_image=False,
        first_title=reducer[0],
        second_title=reducer[1],
        show=True,
    )


if __name__ == "__main__":

    # Доступные parameter: G, H
    # Доступные компоненты: f, r, a
    # Доступные reducer: mean, median, q25, q75

    event_data = '2017-01-01_2017-01-04'
    satellite = 'THEMIS-A'
    parameter = 'H'
    component = 'f'
    reducer = ['mean', 'q25']

    show_plot(event_data, satellite, parameter, component, reducer)
