from dataclasses import dataclass

import pandas as pd

from backend.src.io.paths import DerivedDataset, Instrument

TimeInterval = tuple[pd.Timestamp, pd.Timestamp]


@dataclass(frozen=True, slots=True)
class AvailabilityRule:
    """
    Правило определения интервалов доступности для источника данных.
    """

    required_col: str
    min_hole_seconds: float
    min_interval_seconds: float


RULES: dict[Instrument | DerivedDataset, AvailabilityRule] = {
    Instrument.FGM: AvailabilityRule("GSM_Bx", 45, 3600),
    Instrument.ESA_ION: AvailabilityRule("GSM_Vix", 45, 3600),
    Instrument.ESA_ELECTRON: AvailabilityRule("GSM_Vex", 45, 3600),
    Instrument.EFI: AvailabilityRule("GSM_Ex", 45, 3600),
    Instrument.SSC: AvailabilityRule("GSM_X", 90, 3600),
    Instrument.STA: AvailabilityRule("GSM_Vsx", 90, 3600),
    Instrument.OMNI: AvailabilityRule("FP", 90, 3600),
    DerivedDataset.SHUE: AvailabilityRule("r", 90, 3600),
    DerivedDataset.BETA: AvailabilityRule("beta", 45, 3600),
}


@dataclass(frozen=True, slots=True)
class IntervalsView:
    """
    Неизменяемое представление списка временных интервалов.
    """

    intervals: tuple[TimeInterval, ...]

    def __iter__(self):
        return iter(self.intervals)

    def __len__(self) -> int:
        return len(self.intervals)

    def __str__(self) -> str:
        return "\n".join(self.format_lines())

    def __repr__(self) -> str:
        return str(self)

    def to_list(self) -> list[TimeInterval]:
        return list(self.intervals)

    @staticmethod
    def _format_timestamp(ts: pd.Timestamp) -> str:
        t = pd.Timestamp(ts)
        if t.tzinfo is not None:
            t = t.tz_convert("UTC").tz_localize(None)
        return t.strftime("%Y-%m-%d %H:%M:%S")

    def format_lines(self) -> list[str]:
        if not self.intervals:
            return ["(пусто)"]

        return [
            f"{i}. {self._format_timestamp(start)} - {self._format_timestamp(end)}"
            for i, (start, end) in enumerate(self.intervals, start=1)
        ]
