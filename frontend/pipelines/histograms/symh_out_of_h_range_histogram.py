from collections.abc import Sequence
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

from frontend.plot import plot_symh_histogram_inside_h_range


def _to_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def build_symh_h_dataset(config: AppConfig, component: str) -> pd.DataFrame:
    normalized_component = component.lower()
    h_column = f"H_{normalized_component}"
    resolver = paths(config)
    available_data = pd.read_parquet(resolver.dataset(EventDataset.AVAILABLE))
    if h_column in available_data.columns:
        return available_data

    required_available_columns = ("Time", "SYMH")
    missing_available = [column for column in required_available_columns if column not in available_data.columns]
    if missing_available:
        raise KeyError(f"Required columns are missing in available_data: {missing_available}")

    prepared_data = pd.read_parquet(resolver.dataset(EventDataset.PREPARED))
    required_prepared_columns = ("Time", h_column)
    missing_prepared = [column for column in required_prepared_columns if column not in prepared_data.columns]
    if missing_prepared:
        raise KeyError(f"Required columns are missing in prepared_data: {missing_prepared}")

    symh_frame = available_data.loc[:, required_available_columns].copy()
    symh_frame["Time"] = _to_utc_naive(symh_frame["Time"])
    symh_frame = symh_frame.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    h_frame = prepared_data.loc[:, required_prepared_columns].copy()
    h_frame["Time"] = _to_utc_naive(h_frame["Time"])
    h_frame = h_frame.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    merged = symh_frame.merge(h_frame, on="Time", how="inner").sort_values("Time").reset_index(drop=True)
    if merged.empty:
        raise ValueError("No common timestamps found between available_data and prepared_data.")
    return merged


def _format_number_for_filename(value: float) -> str:
    return f"{value:g}".replace("-", "m").replace(".", "-")


def show_plot(
    config: AppConfig,
    component: str,
    h_range: Sequence[float],
    bins: int = 40,
    smooth_window: int = 5,
    log_scale: bool = False,
    normalize: bool = True,
    y_log_scale: bool = True,
    weight_mode: str = "log_value",
    save_image: bool = False,
    show: bool = True,
) -> Path:
    resolver = paths(config)
    symh_h_data = build_symh_h_dataset(config, component=component)

    output_path = None
    if save_image:
        normalized_component = component.lower()
        lower_part = _format_number_for_filename(float(h_range[0]))
        upper_part = _format_number_for_filename(float(h_range[1]))
        suffix = (
            ("_normalized" if normalize else "")
            + (f"_weighted_{weight_mode}" if weight_mode != "none" else "")
            + ("_log" if log_scale else "")
            + ("_log_y" if y_log_scale else "")
        )
        output_path = resolver.image(f"hist_symh_inside_H_{normalized_component}_{lower_part}_{upper_part}{suffix}")

    return plot_symh_histogram_inside_h_range(
        data=symh_h_data,
        component=component,
        h_range=h_range,
        bins=bins,
        smooth_window=smooth_window,
        log_scale=log_scale,
        normalize=normalize,
        y_log_scale=y_log_scale,
        weight_mode=weight_mode,
        output_path=output_path,
        show=show,
    )


def main() -> None:
    config = get_config()
    show_plot(
        config=config,
        component="a",
        h_range=[0.8, 1.2],
        bins=40,
        smooth_window=5,
        log_scale=True,
        normalize=False,
        y_log_scale=True,
        weight_mode="log_value",
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
