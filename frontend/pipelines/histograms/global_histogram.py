from pathlib import Path
import sys

import matplotlib
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

matplotlib.use("TkAgg", force=True)

from frontend.plot.global_histogram import show_hist_sectors_from_long


def read_distribution_raw_long(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Raw distribution file not found: {resolved_path}")
    return pd.read_csv(resolved_path)


def build_raw_long_path(event_data: str, satellite: str) -> Path:
    matrices_dir = PROJECT_ROOT / "backend" / "data" / "events" / event_data / satellite / "matrices"
    return matrices_dir / "distribution_raw_long.csv"


def show_plot(
    event_data: str,
    satellite: str,
    parameter: str,
    component: str,
    lfrom: int,
    lto: int,
    show: bool = True,
) -> None:
    raw_long_path = build_raw_long_path(event_data, satellite)
    raw_long = read_distribution_raw_long(raw_long_path)
    parameter_name = f"{parameter}_{component}"
    show_hist_sectors_from_long(
        raw_long=raw_long,
        parameter_name=parameter_name,
        parameter=parameter,
        component=component,
        lfrom=lfrom,
        lto=lto,
        show=show,
    )


def main() -> None:
    # Доступные parameter: G, H
    # Доступные компоненты: f, r, a
    event_data = "2017-01-01_2017-01-04"
    satellite = "THEMIS-A"
    parameter = "G"
    component = "r"
    lfrom = 4
    lto = 16

    show_plot(event_data, satellite, parameter, component, lfrom, lto, show=True)


if __name__ == "__main__":
    main()
