"""
Суточный AE-плот в компоновке Kyoto: нарезка суток и сохранение PNG.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile
import unittest

import matplotlib

matplotlib.use("Agg", force=True)

import numpy as np
import pandas as pd

from frontend.plot.ae_kyoto import plot_ae_daily, slice_ae_day
from frontend.plot.ae_symh_kyoto import plot_ae_symh_daily
from frontend.plot.kyoto_daily import daily_image_name
from frontend.plot.symh_kyoto import plot_symh_daily, slice_symh_day


class SliceAeDayTests(unittest.TestCase):
    def test_keeps_full_day_grid_and_gaps(self) -> None:
        times = [
            datetime(2017, 9, 8, 0, 0),
            datetime(2017, 9, 8, 0, 1),
            datetime(2017, 9, 8, 12, 0),
            datetime(2017, 9, 9, 0, 0),
        ]
        frame = pd.DataFrame({"Time": times, "AE": [80, 90, 400, 10]})
        day = slice_ae_day(frame, "2017-09-08")

        self.assertEqual(len(day), 1440)
        self.assertEqual(day.index[0], pd.Timestamp("2017-09-08 00:00:00"))
        self.assertEqual(day.index[-1], pd.Timestamp("2017-09-08 23:59:00"))
        self.assertEqual(day.loc[pd.Timestamp("2017-09-08 00:00:00"), "AE"], 80)
        self.assertEqual(day.loc[pd.Timestamp("2017-09-08 12:00:00"), "AE"], 400)
        self.assertTrue(pd.isna(day.loc[pd.Timestamp("2017-09-08 00:02:00"), "AE"]))

    def test_missing_day_raises(self) -> None:
        frame = pd.DataFrame({"Time": [datetime(2017, 9, 7, 10, 0)], "AE": [40]})
        with self.assertRaises(ValueError):
            slice_ae_day(frame, "2017-09-08")


class SliceSymhDayTests(unittest.TestCase):
    def test_keeps_full_day_grid(self) -> None:
        times = [
            datetime(2017, 3, 31, 0, 0),
            datetime(2017, 3, 31, 18, 0),
        ]
        frame = pd.DataFrame({"Time": times, "SYMH": [-20, -80]})
        day = slice_symh_day(frame, "2017-03-31")

        self.assertEqual(len(day), 1440)
        self.assertEqual(day.loc[pd.Timestamp("2017-03-31 18:00:00"), "SYMH"], -80)
        self.assertTrue(pd.isna(day.loc[pd.Timestamp("2017-03-31 00:01:00"), "SYMH"]))


class DailyImageNameTests(unittest.TestCase):
    def test_index_for_year_month_day(self) -> None:
        self.assertEqual(daily_image_name("ae", "2017-03-31"), "ae_for_2017_03_31.png")
        self.assertEqual(daily_image_name("symh", "2017-09-08"), "symh_for_2017_09_08.png")
        self.assertEqual(daily_image_name("ae_symh", "2017-03-31"), "ae_symh_for_2017_03_31.png")


class PlotAeDailyTests(unittest.TestCase):
    def test_writes_png_for_complete_hour(self) -> None:
        times = pd.date_range("2017-09-08 00:00:00", periods=60, freq="1min")
        frame = pd.DataFrame({"Time": times, "AE": range(60, 120)})
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "ae_for_2017_09_08.png"
            result = plot_ae_daily(frame, "2017-09-08", output_path=output, show=False)
            self.assertEqual(result, output)
            self.assertGreater(output.stat().st_size, 0)


class PlotSymhDailyTests(unittest.TestCase):
    def test_writes_png_without_fill(self) -> None:
        times = pd.date_range("2017-03-31 00:00:00", periods=60, freq="1min")
        frame = pd.DataFrame({"Time": times, "SYMH": np.linspace(-10, -40, 60)})
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "symh_for_2017_03_31.png"
            result = plot_symh_daily(frame, "2017-03-31", output_path=output, show=False)
            self.assertEqual(result, output)
            self.assertGreater(output.stat().st_size, 0)


class PlotAeSymhDailyTests(unittest.TestCase):
    def test_writes_stacked_png(self) -> None:
        times = pd.date_range("2017-03-31 00:00:00", periods=60, freq="1min")
        ae = pd.DataFrame({"Time": times, "AE": range(60, 120)})
        symh = pd.DataFrame({"Time": times, "SYMH": np.linspace(-10, -40, 60)})
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "ae_symh_for_2017_03_31.png"
            result = plot_ae_symh_daily(ae, symh, "2017-03-31", output_path=output, show=False)
            self.assertEqual(result, output)
            self.assertGreater(output.stat().st_size, 0)


if __name__ == "__main__":
    unittest.main()

