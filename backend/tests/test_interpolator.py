"""
Склейка таблиц на общей оси времени.
"""

from __future__ import annotations

import unittest

import pandas as pd

from backend.src.processing.interpolation.df_interpolator import DFInterpolator


class TestMergeOnTimeAxis(unittest.TestCase):
    def test_duplicate_columns_keep_first_source(self) -> None:
        times = pd.to_datetime(["2017-03-31 00:00", "2017-03-31 00:01"])
        ssc = pd.DataFrame({"Time": times, "L": [6.0, 6.1], "X": [1.0, 1.1]})
        shue = pd.DataFrame({"Time": times, "L": [99.0, 99.0], "r": [8.0, 8.1]})

        merged = DFInterpolator([ssc, shue]).merge_on_time_axis((times[0], times[1]))

        self.assertEqual(list(merged.columns), ["Time", "L", "X", "r"])
        self.assertEqual(merged["L"].tolist(), [6.0, 6.1])
        self.assertEqual(merged["r"].tolist(), [8.0, 8.1])


if __name__ == "__main__":
    unittest.main()
