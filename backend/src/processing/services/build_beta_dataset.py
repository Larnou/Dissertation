import numpy as np
import pandas as pd

from backend.src.physics.beta import BetaModel
from backend.src.processing.services.build_shue_dataset import prepare_for_time_merge

# Согласование FGM и MOM по времени (разная каденция на разных модах).
BETA_MATCH_TOLERANCE = pd.Timedelta(seconds=45)


def build_beta_dataset(
    *,
    fgm_data: pd.DataFrame,
    mom_data: pd.DataFrame,
    match_tolerance: pd.Timedelta | None = None,
    pressure_column: str = "Ion_pressure",
) -> pd.DataFrame:
    """
    Собирает датазет только с временем и β из двух источников: FGM и MOM.

    В ``mom_data`` нужны **давление и время** (колонка ``Ion_pressure`` или ``pressure_column``),
    не готовое β: β здесь считается заново из давления и |B|. Передать только ``Time`` и ``beta``
    нельзя — для восстановления давления недостаточно данных.

    Пайплайн ``get_data`` подает результат сюда в список сырых датасетов (рядом с ``shue_data`` с
    колонкой ``r``): в merge попадает уже колонка ``beta``, без ``Ion_pressure``.
    """

    tolerance = BETA_MATCH_TOLERANCE if match_tolerance is None else match_tolerance

    fgm = prepare_for_time_merge(fgm_data)
    mom = prepare_for_time_merge(mom_data)

    if pressure_column not in mom.columns:
        raise KeyError(
            f"В MOM-данных нет столбца {pressure_column!r}. Доступные: {list(mom.columns)}"
        )

    mom_sub = mom[["Time", pressure_column]].copy()
    if pressure_column != "Ion_pressure":
        mom_sub = mom_sub.rename(columns={pressure_column: "Ion_pressure"})

    merged = pd.merge_asof(
        left=fgm,
        right=mom_sub,
        on="Time",
        direction="nearest",
        tolerance=tolerance,
    )

    required_b = ["GSM_Bx", "GSM_By", "GSM_Bz"]
    merged = merged.dropna(subset=["Ion_pressure", *required_b]).reset_index(drop=True)

    return pd.DataFrame(
        {
            "Time": merged["Time"],
            "beta": BetaModel(merged, pressure_column="Ion_pressure").model(),
        }
    )
