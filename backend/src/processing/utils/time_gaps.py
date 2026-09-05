import pandas as pd


def split_dataframe_by_time_gaps(
    dataframe: pd.DataFrame,
    time_column: str = "Time",
    gap_seconds: int = 12,
) -> list[pd.DataFrame]:
    """
    Режет таблицу на сегменты по разрывам во времени.
    """

    if dataframe.empty:
        return []

    time_series = pd.to_datetime(dataframe[time_column])
    masks = (time_series.diff() > pd.to_timedelta(gap_seconds, unit="s")).cumsum()
    return [
        dataframe[masks == mask_id].reset_index(drop=True)
        for mask_id in masks.unique()
    ]
