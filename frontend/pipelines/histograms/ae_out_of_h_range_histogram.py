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

from frontend.plot import plot_ae_histogram_outside_h_range


def read_available_data(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Available dataset not found: {resolved_path}")
    return pd.read_parquet(resolved_path)


def read_prepared_data(path: str | Path) -> pd.DataFrame:
    resolved_path = Path(path).expanduser().resolve()
    if not resolved_path.exists():
        raise FileNotFoundError(f"Prepared dataset not found: {resolved_path}")
    return pd.read_parquet(resolved_path)


def build_available_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("available_data")


def build_prepared_data_path(config: AppConfig) -> Path:
    return PathResolver(config).data_file("prepared_data")


def _to_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def build_ae_h_dataset(config: AppConfig, component: str) -> pd.DataFrame:
    normalized_component = component.lower()
    h_column = f"H_{normalized_component}"
    available_data = read_available_data(build_available_data_path(config))
    if h_column in available_data.columns:
        return available_data

    required_available_columns = ("Time", "AE")
    missing_available = [column for column in required_available_columns if column not in available_data.columns]
    if missing_available:
        raise KeyError(f"Required columns are missing in available_data: {missing_available}")

    prepared_data = read_prepared_data(build_prepared_data_path(config))
    required_prepared_columns = ("Time", h_column)
    missing_prepared = [column for column in required_prepared_columns if column not in prepared_data.columns]
    if missing_prepared:
        raise KeyError(f"Required columns are missing in prepared_data: {missing_prepared}")

    ae_frame = available_data.loc[:, required_available_columns].copy()
    ae_frame["Time"] = _to_utc_naive(ae_frame["Time"])
    ae_frame = ae_frame.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    h_frame = prepared_data.loc[:, required_prepared_columns].copy()
    h_frame["Time"] = _to_utc_naive(h_frame["Time"])
    h_frame = h_frame.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    merged = ae_frame.merge(h_frame, on="Time", how="inner").sort_values("Time").reset_index(drop=True)
    if merged.empty:
        raise ValueError("No common timestamps found between available_data and prepared_data.")
    return merged


def _format_number_for_filename(value: float) -> str:
    return f"{value:g}".replace("-", "m").replace(".", "p")


def show_plot(
    config: AppConfig,
    component: str,
    h_value: float,
    delta: float,
    bins: int = 40,
    smooth_window: int = 5,
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = PathResolver(config)
    ae_h_data = build_ae_h_dataset(config, component=component)

    output_path = None
    if save_image:
        normalized_component = component.lower()
        h_value_part = _format_number_for_filename(h_value)
        delta_part = _format_number_for_filename(delta)
        output_path = resolver.image_file(f"hist_ae_outside_H_{normalized_component}_h{h_value_part}_d{delta_part}.png")

    return plot_ae_histogram_outside_h_range(
        data=ae_h_data,
        component=component,
        h_value=h_value,
        delta=delta,
        bins=bins,
        smooth_window=smooth_window,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    component = "a"
    h_value = 1
    delta = 0.2
    bins = 40
    smooth_window = 5

    show_plot(
        config=config,
        component=component,
        h_value=h_value,
        delta=delta,
        bins=bins,
        smooth_window=smooth_window,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
