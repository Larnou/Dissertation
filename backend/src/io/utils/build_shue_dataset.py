from __future__ import annotations

import numpy as np
import pandas as pd

from backend.src.physics.shue import ShueModel


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

    ssc_working = ssc_data.copy()
    omn_working = omn_data.copy()

    ssc_working["Time"] = pd.to_datetime(ssc_working["Time"], utc=True, errors="coerce")
    omn_working["Time"] = pd.to_datetime(omn_working["Time"], utc=True, errors="coerce")

    ssc_working = ssc_working.dropna(subset=["Time"]).sort_values("Time").reset_index(drop=True)
    omn_working = omn_working.dropna(subset=["Time"]).sort_values("Time").reset_index(drop=True)

    merged = pd.merge_asof(
        left=ssc_working,
        right=omn_working[["Time", "FP", "Bz_GSM"]],
        on="Time",
        direction="nearest",
        tolerance=pd.Timedelta(minutes=1),
    )

    merged = merged.rename(columns={"Bz_GSM": "Bz"})
    merged = merged.dropna(subset=["FP", "Bz", "GSM_X", "GSM_Y", "GSM_Z", "L", "Longitude"]).reset_index(drop=True)

    mlt = ((np.asarray(merged["Longitude"], dtype="float64") + 180.0) % 360.0) / 15.0
    r = ShueModel(merged).model()

    dataset = pd.DataFrame(
        {
            "Time": merged["Time"].to_list(),
            "L": merged["L"].to_list(),
            "MLT": mlt,
            "r": r,
        }
    )

    bordered_dataset = dataset[(dataset["L"] >= 4) & (dataset["L"] <= dataset["r"])].reset_index(drop=True)
    return bordered_dataset

