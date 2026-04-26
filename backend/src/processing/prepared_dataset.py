from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from backend.src.config import AppConfig, progress
from backend.src.physics.field_alligned_h import FAHCalculator
from backend.src.processing.filters import DataFiltration


FA_COLUMNS: tuple[str, ...] = (
    "Time",
    "H_f",
    "H_a",
    "H_r",
    "G_f",
    "G_a",
    "G_r",
    "E_f_meas",
    "E_a_meas",
    "E_r_meas",
    "X_f_conv",
    "X_a_conv",
    "X_r_conv",
    "B_f_trend",
    "B_a_trend",
    "B_r_trend",
)


def _to_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def _align_filtered_and_fa(filtered: pd.DataFrame, fa_data: pd.DataFrame) -> pd.DataFrame:
    prepared_filtered = filtered.copy()
    prepared_filtered["Time"] = _to_utc_naive(prepared_filtered["Time"])
    prepared_filtered = prepared_filtered.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    available_fa_columns = [column for column in FA_COLUMNS if column in fa_data.columns]
    prepared_fa = fa_data[available_fa_columns].copy()
    prepared_fa["Time"] = _to_utc_naive(prepared_fa["Time"])
    prepared_fa = prepared_fa.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    return (
        prepared_filtered.merge(prepared_fa, on="Time", how="inner")
        .sort_values("Time")
        .reset_index(drop=True)
    )


def build_prepared_dataset(
    dataset: pd.DataFrame,
    config: AppConfig,
    *,
    use_noise: bool = True,
) -> pd.DataFrame:
    """
    Build one H-ready dataset from interpolated data.

    Output always contains at least `Time`, `L`, `MLT`, `H_*`, `G_*`.
    It also keeps all filtered input columns plus FA projections/components (`E_*`, `X_*`, `B_*`).
    """
    if dataset.empty:
        return dataset.copy()

    filtered = DataFiltration(config).window_filter(dataset)
    if filtered.empty:
        return filtered

    calculator = FAHCalculator(filtered.reset_index(drop=True), config)
    fa_data = calculator.calculate_h_fa_noise() if use_noise else calculator.calculate_h_fa()
    if fa_data.empty:
        return filtered.iloc[0:0].copy()

    return _align_filtered_and_fa(filtered, fa_data)


def build_prepared_datasets(
    datasets: Iterable[pd.DataFrame],
    config: AppConfig,
    *,
    use_noise: bool = True,
    desc: str = "[prepare] расчёт H/G/X по интерполированным данным",
) -> list[pd.DataFrame]:
    prepared_datasets: list[pd.DataFrame] = []

    for dataset in progress(datasets, desc=desc):
        prepared = build_prepared_dataset(dataset, config, use_noise=use_noise)
        if prepared.empty:
            continue
        prepared_datasets.append(prepared)

    return prepared_datasets
