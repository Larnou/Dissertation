from datetime import timedelta

import pandas as pd


def is_show_intervals(dataframe: pd.DataFrame) -> bool:
    start = dataframe.index[0]
    end = dataframe.index[-1]

    duration = end - start

    if duration > timedelta(days=14):
        return False
    else:
        return True