from dataclasses import dataclass
from math import floor
from typing import Callable, Iterable, Literal

import numpy as np
import pandas as pd

from backend.src.config import AppConfig, progress

Reducer = Literal["mean", "std", "median", "q25", "q75"]
Grid = list[list[list[float]]]


@dataclass(frozen=True, slots=True)
class DistributionBuckets:
    hf: Grid
    ha: Grid
    hr: Grid
    gf: Grid
    ga: Grid
    gr: Grid


@dataclass(frozen=True, slots=True)
class Distributions:
    config: AppConfig
    lshell_range: int = 16
    radian_range: int = 24
    REQUIRED_COLUMNS: tuple[str, ...] = (
        "L",
        "MLT",
        "H_f",
        "H_a",
        "H_r",
        "G_f",
        "G_a",
        "G_r",
    )

    def _empty_grid(self) -> Grid:
        return [[[] for _ in range(self.radian_range)] for _ in range(self.lshell_range)]

    def _validate_required_columns(self, dataset: pd.DataFrame) -> None:
        missing = sorted(set(self.REQUIRED_COLUMNS).difference(dataset.columns))
        if missing:
            missing_str = ", ".join(missing)
            raise ValueError(f"Prepared dataset missing required columns: {missing_str}")

    def collect(self, datasets: Iterable[pd.DataFrame]) -> DistributionBuckets:
        hf = self._empty_grid()
        ha = self._empty_grid()
        hr = self._empty_grid()
        gf = self._empty_grid()
        ga = self._empty_grid()
        gr = self._empty_grid()

        for dataset in progress(datasets, desc="[distribution] расчёт H/G распределений"):
            if dataset.empty:
                continue

            self._validate_required_columns(dataset)
            prepared = dataset.dropna(subset=["L", "MLT"]).reset_index(drop=True)
            if prepared.empty:
                continue

            for row in prepared.itertuples(index=False):
                l_index = floor(row.L)
                r_index = floor(row.MLT)
                if not (0 <= l_index < self.lshell_range and 0 <= r_index < self.radian_range):
                    continue

                hf[l_index][r_index].append(float(row.H_f))
                ha[l_index][r_index].append(float(row.H_a))
                hr[l_index][r_index].append(float(row.H_r))
                gf[l_index][r_index].append(float(row.G_f))
                ga[l_index][r_index].append(float(row.G_a))
                gr[l_index][r_index].append(float(row.G_r))

        return DistributionBuckets(hf=hf, ha=ha, hr=hr, gf=gf, ga=ga, gr=gr)

    def reduce(self, grid: Grid, reducer: Reducer) -> np.ndarray:
        distribution = np.full((self.lshell_range, self.radian_range), -1.0, dtype=float)

        reducers: dict[Reducer, Callable[[list[float]], float]] = {
            "mean": np.mean,
            "std": np.std,
            "median": np.median,
            "q25": lambda values: float(np.quantile(values, 0.25)),
            "q75": lambda values: float(np.quantile(values, 0.75)),
        }
        reducer_fn = reducers[reducer]

        for l_index in range(self.lshell_range):
            for r_index in range(self.radian_range):
                values = grid[l_index][r_index]
                if not values:
                    continue
                distribution[l_index, r_index] = float(reducer_fn(values))

        return distribution

    def build_maps(self, buckets: DistributionBuckets, reducer: Reducer = "mean") -> dict[str, np.ndarray]:
        return {
            "H_f": self.reduce(buckets.hf, reducer),
            "H_a": self.reduce(buckets.ha, reducer),
            "H_r": self.reduce(buckets.hr, reducer),
            "G_f": self.reduce(buckets.gf, reducer),
            "G_a": self.reduce(buckets.ga, reducer),
            "G_r": self.reduce(buckets.gr, reducer),
        }
