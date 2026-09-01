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

from frontend.plot import plot_beta_h_count_heatmap


def _to_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def build_beta_h_dataset(config: AppConfig, h_columns: tuple[str, ...]) -> pd.DataFrame:
    resolver = paths(config)
    available_data = pd.read_parquet(resolver.dataset(EventDataset.AVAILABLE))
    prepared_data = pd.read_parquet(resolver.dataset(EventDataset.PREPARED))

    beta_frame = available_data[["Time", "beta"]].copy()
    beta_frame["Time"] = _to_utc_naive(beta_frame["Time"])
    beta_frame = beta_frame.dropna(subset=["Time", "beta"]).drop_duplicates(subset=["Time"])

    required_prepared_columns = ("Time", *h_columns)
    missing = [column for column in required_prepared_columns if column not in prepared_data.columns]
    if missing:
        raise KeyError(f"Required H columns are missing in prepared_data: {missing}")

    h_frame = prepared_data.loc[:, required_prepared_columns].copy()
    h_frame["Time"] = _to_utc_naive(h_frame["Time"])
    h_frame = h_frame.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    merged = (
        beta_frame.merge(h_frame, on="Time", how="inner")
        .sort_values("Time")
        .reset_index(drop=True)
    )
    if merged.empty:
        raise ValueError("No common timestamps found between available_data and prepared_data.")
    return merged


def show_plots(
    config: AppConfig,
    h_parameters: tuple[str, ...],
    beta_bin_step: float = 0.1,
    h_bin_step: float = 0.1,
    save_image: bool = False,
    show: bool = True,
) -> list[Path]:
    resolver = paths(config)
    beta_h_data = build_beta_h_dataset(config, h_columns=h_parameters)
    output_paths: list[Path] = []

    for h_parameter in h_parameters:
        output_path = None
        if save_image:
            output_path = resolver.image(f"heatmap_beta_{h_parameter}_counts")

        result_path = plot_beta_h_count_heatmap(
            data=beta_h_data,
            h_parameter=h_parameter,
            beta_bin_step=beta_bin_step,
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
        h_parameters=("H_f", "H_a", "H_r"),
        beta_bin_step=0.05,
        h_bin_step=0.05,
        save_image=True,
        show=True,
    )


if __name__ == "__main__":
    main()
