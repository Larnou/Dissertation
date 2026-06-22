from pathlib import Path

import pandas as pd


def add_symh_index_to_available_data(
    *,
    symh_path: Path,
    available_data_path: Path,
    symh_column: str = "SYMH",
) -> pd.DataFrame:
    """
    Adds Kyoto SYM-H index values to available_data and overwrites available_data parquet.

    The SYM-H series is limited to the time range of available_data, interpolated onto
    available_data["Time"], and saved back to `available_data_path`.
    """

    symh_path = Path(symh_path)
    available_data_path = Path(available_data_path)

    symh_data = pd.read_parquet(symh_path)
    available_data = pd.read_parquet(available_data_path)

    result = interpolate_symh_index(available_data=available_data, symh_data=symh_data, symh_column=symh_column)
    result.reset_index(drop=True).to_parquet(available_data_path)

    return result


def interpolate_symh_index(
    *,
    available_data: pd.DataFrame,
    symh_data: pd.DataFrame,
    symh_column: str = "SYMH",
) -> pd.DataFrame:
    """
    Interpolates SYM-H values by available_data["Time"] without changing available_data rows.
    """

    _validate_columns(available_data, required_columns={"Time"}, dataframe_name="available_data")
    _validate_columns(symh_data, required_columns={"Time", symh_column}, dataframe_name="symh_data")

    result = available_data.copy()
    result["Time"] = _normalize_time(result["Time"])
    if result["Time"].isna().any():
        raise ValueError("available_data contains invalid Time values")
    if result.empty:
        result[symh_column] = pd.Series(dtype="float64")
        return result

    symh_working = symh_data.loc[:, ["Time", symh_column]].copy()
    symh_working["Time"] = _normalize_time(symh_working["Time"])
    symh_working[symh_column] = pd.to_numeric(symh_working[symh_column], errors="coerce")
    symh_working = symh_working.dropna(subset=["Time", symh_column]).sort_values("Time").drop_duplicates(subset=["Time"])

    time_start = result["Time"].min()
    time_end = result["Time"].max()
    symh_working = symh_working[(symh_working["Time"] >= time_start) & (symh_working["Time"] <= time_end)]
    if symh_working.empty:
        raise ValueError(f"SYM-H data does not overlap available_data time range: {time_start} — {time_end}")

    interpolation_axis = pd.DataFrame(
        {
            "Time": pd.concat([result["Time"], symh_working["Time"]], ignore_index=True)
            .drop_duplicates()
            .sort_values()
            .reset_index(drop=True)
        }
    )
    interpolation_axis = interpolation_axis.merge(symh_working, on="Time", how="left").set_index("Time")
    interpolation_axis[symh_column] = interpolation_axis[symh_column].interpolate(method="time", limit_direction="both")

    result[symh_column] = interpolation_axis.reindex(result["Time"])[symh_column].to_numpy()
    return result


def _normalize_time(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_convert("UTC").dt.tz_localize(None)


def _validate_columns(dataframe: pd.DataFrame, *, required_columns: set[str], dataframe_name: str) -> None:
    missing_columns = sorted(required_columns.difference(dataframe.columns))
    if missing_columns:
        raise KeyError(f"{dataframe_name} does not contain required columns: {missing_columns}")
