from dataclasses import dataclass
from typing import TypeAlias, Literal

import pandas as pd

DataSourceKind: TypeAlias = Literal[
    "fgm",
    "esa_ion",
    "esa_electron",
    "efi",
    "ssc",
    "sta",
    "omn",
    "shue",
]

TimeInterval: TypeAlias = tuple[pd.Timestamp, pd.Timestamp]


@dataclass(frozen=True, slots=True)
class AvailabilityRule:
    required_col: str
    min_hole_seconds: float
    min_interval_seconds: float


RULES: dict[DataSourceKind, AvailabilityRule] = {
    "fgm": AvailabilityRule("GSM_Bx", 45, 3600),
    "esa_ion": AvailabilityRule("GSM_Vix", 45, 3600),
    "esa_electron": AvailabilityRule("GSM_Vex", 45, 3600),
    "efi": AvailabilityRule("GSM_Ex", 45, 3600),
    "ssc": AvailabilityRule("GSM_X", 90, 3600),
    "sta": AvailabilityRule("GSM_Vsx", 90, 3600),
    "omn": AvailabilityRule("FP", 90, 3600),

    # Shue-датасет считается валидным, когда рассчитан r (и есть Time).
    # Дыры в r обычно идут от отсутствия OMNI/SSC для мэчинга.
    "shue": AvailabilityRule("r", 90, 3600),
}


@dataclass(frozen=True, slots=True)
class IntervalsView:
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
