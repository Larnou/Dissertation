from collections.abc import Mapping
from pathlib import Path

import numpy as np
import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.names import Reducer
from backend.src.io.paths import paths


def save_distribution_matrix(
    config: AppConfig,
    matrix: np.ndarray,
    parameter_key: str,
    reducer: Reducer | str,
) -> Path:
    """
    Сохраняет одну матрицу распределения в CSV в каталоге distributions/.
    """

    resolved_reducer = Reducer(reducer) if isinstance(reducer, str) else reducer
    path = paths(config).distribution_map_by_key(parameter_key, resolved_reducer)
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(matrix).to_csv(path, index=False)
    return path


def save_distribution_matrices(
    config: AppConfig,
    distributions: dict[str, np.ndarray],
    reducer: Reducer | str,
) -> dict[str, Path]:
    """
    Сохраняет набор матриц распределений и возвращает пути к файлам.
    """

    saved_paths: dict[str, Path] = {}
    for parameter_key, matrix in distributions.items():
        saved_paths[parameter_key] = save_distribution_matrix(
            config=config,
            matrix=matrix,
            parameter_key=parameter_key,
            reducer=reducer,
        )
    return saved_paths


def save_raw_distribution_long(
    config: AppConfig,
    grids: Mapping[str, list],
) -> dict[str, Path]:
    """
    Сохраняет исходные значения распределений в long-формате.

    Args:
        config: конфиг с путями события.
        grids: сетки по ключу колонки (H_f, Beta, J_a, ...).
    """

    resolver = paths(config)
    saved_paths: dict[str, Path] = {}
    for parameter_key, grid in grids.items():
        path = resolver.distribution_raw_long_by_key(parameter_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        rows: list[dict[str, float | int | str]] = []
        for l_index, l_row in enumerate(grid):
            for mlt_index, values in enumerate(l_row):
                for value in values:
                    rows.append(
                        {
                            "parameter": parameter_key,
                            "L_bin": l_index,
                            "MLT_bin": mlt_index,
                            "value": float(value),
                        }
                    )
        pd.DataFrame(rows, columns=["parameter", "L_bin", "MLT_bin", "value"]).to_parquet(path, index=False)
        saved_paths[parameter_key] = path
    return saved_paths
