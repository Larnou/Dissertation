import numpy as np
import pandas as pd

from backend.src.physics.shue import ShueModel

OMNI_MATCH_TOLERANCE = pd.Timedelta(minutes=1)
SHUE_MIN_L = 4.0


def prepare_for_time_merge(dataframe: pd.DataFrame) -> pd.DataFrame:
    prepared = dataframe.copy()
    prepared["Time"] = pd.to_datetime(prepared["Time"], utc=True, errors="coerce")
    return prepared.dropna(subset=["Time"]).sort_values("Time").reset_index(drop=True)


def compute_mlt(longitude: pd.Series) -> np.ndarray:
    return ((np.asarray(longitude, dtype="float64") + 180.0) % 360.0) / 15.0


def build_shue_dataset(*, ssc_data: pd.DataFrame, omn_data: pd.DataFrame) -> pd.DataFrame:
    """
    Собирает датасет Shue из SSC + OMNI и возвращает DataFrame с колонками:
    Time, L, MLT, r.

    Важно: для дальнейшего расчёта интервалов по Shue применяем ограничения:
      (L >= 4) & (L <= r)

    Источники:
    - SSC: Time, L, Longitude, GSM_X/Y/Z
    - OMNI: Time, FP, Bz_GSM
    """

    ssc_working = prepare_for_time_merge(ssc_data)
    omn_working = prepare_for_time_merge(omn_data)

    merged = pd.merge_asof(
        left=ssc_working,
        right=omn_working[["Time", "FP", "Bz_GSM"]],
        on="Time",
        direction="nearest",
        tolerance=OMNI_MATCH_TOLERANCE,
    )

    merged = merged.rename(columns={"Bz_GSM": "Bz"})
    merged = merged.dropna(subset=["FP", "Bz", "GSM_X", "GSM_Y", "GSM_Z", "L", "Longitude"]).reset_index(drop=True)

    dataset = pd.DataFrame(
        {
            "Time": merged["Time"],
            "L": merged["L"],
            "MLT": compute_mlt(merged["Longitude"]),
            "r": ShueModel(merged).model(),
        }
    )

    bordered_dataset = dataset[(dataset["L"] >= SHUE_MIN_L) & (dataset["L"] <= dataset["r"])].reset_index(drop=True)
    return bordered_dataset
