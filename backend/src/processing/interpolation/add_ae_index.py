from pathlib import Path

import pandas as pd


def add_ae_index_to_available_data(
    *,
    ae_path: Path,
    available_data_path: Path,
    ae_column: str = "AE",
) -> pd.DataFrame:
    """
    Adds Kyoto AE index values to available_data and overwrites available_data parquet.

    The AE series is limited to the time range of available_data, interpolated onto
    available_data["Time"], and saved back to `available_data_path`.
    """

    ae_path = Path(ae_path)
    available_data_path = Path(available_data_path)

    ae_data = pd.read_parquet(ae_path)
    available_data = pd.read_parquet(available_data_path)

    result = interpolate_ae_index(available_data=available_data, ae_data=ae_data, ae_column=ae_column)
    result.reset_index(drop=True).to_parquet(available_data_path)

    return result


def interpolate_ae_index(
    *,
    available_data: pd.DataFrame,
    ae_data: pd.DataFrame,
    ae_column: str = "AE",
) -> pd.DataFrame:
    """
    Interpolates AE values by available_data["Time"] without changing available_data rows.
    """

    _validate_columns(available_data, required_columns={"Time"}, dataframe_name="available_data")
    _validate_columns(ae_data, required_columns={"Time", ae_column}, dataframe_name="ae_data")

    result = available_data.copy()
    result["Time"] = _normalize_time(result["Time"])
    if result["Time"].isna().any():
        raise ValueError("available_data contains invalid Time values")
    if result.empty:
        result[ae_column] = pd.Series(dtype="float64")
        return result

    ae_working = ae_data.loc[:, ["Time", ae_column]].copy()
    ae_working["Time"] = _normalize_time(ae_working["Time"])
    ae_working[ae_column] = pd.to_numeric(ae_working[ae_column], errors="coerce")
    ae_working = ae_working.dropna(subset=["Time", ae_column]).sort_values("Time").drop_duplicates(subset=["Time"])

    time_start = result["Time"].min()
    time_end = result["Time"].max()
    ae_working = ae_working[(ae_working["Time"] >= time_start) & (ae_working["Time"] <= time_end)]
    if ae_working.empty:
        raise ValueError(f"AE data does not overlap available_data time range: {time_start} — {time_end}")

    interpolation_axis = pd.DataFrame(
        {
            "Time": pd.concat([result["Time"], ae_working["Time"]], ignore_index=True)
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        }
    )
    interpolation_axis = interpolation_axis.merge(ae_working, on="Time", how="left").set_index("Time")
    interpolation_axis[ae_column] = interpolation_axis[ae_column].interpolate(method="time", limit_direction="both")

    result[ae_column] = interpolation_axis.reindex(result["Time"])[ae_column].to_numpy()
    return result


def _normalize_time(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)


def _validate_columns(dataframe: pd.DataFrame, *, required_columns: set[str], dataframe_name: str) -> None:
    missing_columns = sorted(required_columns.difference(dataframe.columns))
    if missing_columns:
        raise KeyError(f"{dataframe_name} does not contain required columns: {missing_columns}")
