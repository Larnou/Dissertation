import pandas as pd


def interpolate_omn_dataset(*, omn_data: pd.DataFrame) -> pd.DataFrame:
    """
    OMNI часто содержит None/NaN в FP и Bz_GSM. Это ломает интервалы доступности.

    Здесь делаем "простую" интерполяцию как в ноутбуке: заполняем пропуски в обе стороны.
    Допущение: для вашей задачи не критично, что интерполяция может заполнить большие разрывы.
    """

    if omn_data.empty:
        return omn_data

    if "Time" not in omn_data.columns:
        return omn_data

    working = omn_data.copy()
    working["Time"] = pd.to_datetime(working["Time"], utc=True, errors="coerce")
    working = working.dropna(subset=["Time"]).sort_values("Time").drop_duplicates(subset=["Time"]).reset_index(drop=True)

    if working.empty:
        return working

    for col in ("FP", "Bz_GSM"):
        if col in working.columns:
            working[col] = pd.to_numeric(working[col], errors="coerce")

    working[["FP", "Bz_GSM"]] = working[["FP", "Bz_GSM"]].interpolate(
        method="linear",
        limit_direction="both",
    )

    return working