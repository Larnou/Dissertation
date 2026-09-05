"""
Разбор минутных индексов Kyoto WDC: SYM-H и AE.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
import tempfile
import unittest

from backend.src.io.kyoto_ae import parse_kyoto_ae_file
from backend.src.io.kyoto_symh import parse_kyoto_symh_file


def _wdc_values(*minutes: int, hourly: int) -> str:
    fields = [f"{value:6d}" for value in minutes]
    fields.append(f"{hourly:6d}")
    return "".join(fields)


class KyotoSymhParseTests(unittest.TestCase):
    def test_reads_symh_and_ignores_other_asy_sym_indices(self) -> None:
        asy_d = "ASYSYM N6E01170101D00ASYWDCC2KYOTO" + _wdc_values(*([18] * 60), hourly=12)
        asy_h = "ASYSYM N6E01170101H00ASYWDCC2KYOTO" + _wdc_values(*([25] * 60), hourly=25)
        sym_d = "ASYSYM N6E01170101D00SYMWDCC2KYOTO" + _wdc_values(*([-3] * 60), hourly=-3)
        sym_h = "ASYSYM N6E01170101H00SYMWDCC2KYOTO" + _wdc_values(-18, *([-17] * 59), hourly=-17)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "asy.txt"
            path.write_text("\n".join([asy_d, asy_h, sym_d, sym_h]), encoding="utf-8")
            frame = parse_kyoto_symh_file(path)

        self.assertEqual(len(frame), 60)
        self.assertEqual(list(frame.columns), ["Time", "SYMH"])
        self.assertEqual(frame["Time"].iloc[0], datetime(2017, 1, 1, 0, 0))
        self.assertEqual(frame["SYMH"].iloc[0], -18)
        self.assertEqual(frame["SYMH"].iloc[1], -17)
        self.assertNotIn(18, frame["SYMH"].tolist())
        self.assertNotIn(25, frame["SYMH"].tolist())

    def test_drops_official_missing_code(self) -> None:
        values = [-10] * 59 + [99999]
        line = "ASYSYM N6E01170101H00SYMWDCC2KYOTO" + _wdc_values(*values, hourly=-10)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "asy.txt"
            path.write_text(line + "\n", encoding="utf-8")
            frame = parse_kyoto_symh_file(path)

        self.assertEqual(len(frame), 59)
        self.assertNotIn(99999, frame["SYMH"].tolist())


class KyotoAeParseTests(unittest.TestCase):
    def test_reads_ae_and_ignores_al(self) -> None:
        ae = "AEALAOAU    170101E00AE PRVAE/E02 " + _wdc_values(82, 93, *([80] * 58), hourly=132)
        al = "AEALAOAU    170101L00AL PRVAE/E02 " + _wdc_values(*([-40] * 60), hourly=-40)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ae.request"
            path.write_text("\n".join([ae, al]), encoding="utf-8")
            frame = parse_kyoto_ae_file(path)

        self.assertEqual(len(frame), 60)
        self.assertEqual(frame["Time"].iloc[0], datetime(2017, 1, 1, 0, 0))
        self.assertEqual(frame["AE"].iloc[0], 82)
        self.assertEqual(frame["AE"].iloc[1], 93)
        self.assertNotIn(-40, frame["AE"].tolist())


if __name__ == "__main__":
    unittest.main()
