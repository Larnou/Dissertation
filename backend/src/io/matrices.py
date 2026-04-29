from pathlib import Path

import numpy as np
import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.paths import PathResolver
from backend.src.processing.distribution import DistributionBuckets


def save_distribution_matrix(config: AppConfig, matrix: np.ndarray, parameter_name: str, reducer: str) -> Path:
    """
    Сохраняет одну матрицу распределения в CSV в каталоге matrices.
    """

    file_name = f"distribution_{parameter_name}_{reducer}.csv"
    path = PathResolver(config).distribution_file(file_name)
    path.parent.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(matrix).to_csv(path, index=False)
    return path


def save_distribution_matrices(config: AppConfig, distributions: dict[str, np.ndarray], reducer: str) -> dict[str, Path]:
    """
    Сохраняет набор матриц распределений и возвращает пути к файлам.
    """

    saved_paths: dict[str, Path] = {}
    for parameter_name, matrix in distributions.items():
        saved_paths[parameter_name] = save_distribution_matrix(
            config=config,
            matrix=matrix,
            parameter_name=parameter_name,
            reducer=reducer,
        )
    return saved_paths


def save_raw_distribution_long(
    config: AppConfig,
    buckets: DistributionBuckets,
    file_name: str = "distribution_raw_long.csv",
) -> Path:
    """
    Сохраняет исходные значения распределений в long-формате в каталоге matrices.
    """

    path = PathResolver(config).matrix_file(file_name)
    path.parent.mkdir(parents=True, exist_ok=True)

    rows: list[dict[str, float | int | str]] = []
    parameter_grids = {
        "H_f": buckets.hf,
        "H_a": buckets.ha,
        "H_r": buckets.hr,
        "G_f": buckets.gf,
        "G_a": buckets.ga,
        "G_r": buckets.gr,
    }

    for parameter_name, grid in parameter_grids.items():
        for l_index, l_row in enumerate(grid):
            for mlt_index, values in enumerate(l_row):
                for value in values:
                    rows.append(
                        {
                            "parameter": parameter_name,
                            "L_bin": l_index,
                            "MLT_bin": mlt_index,
                            "value": float(value),
                        }
                    )

    pd.DataFrame(rows, columns=["parameter", "L_bin", "MLT_bin", "value"]).to_csv(path, index=False)
    return path
