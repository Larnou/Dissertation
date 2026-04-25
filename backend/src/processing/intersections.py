import csv
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import TypeAlias, TypedDict

from backend.src.config import config
from backend.src.io.paths import availability_periods_dir

Interval: TypeAlias = tuple[datetime, datetime]


class IntervalsSummary(TypedDict):
    count: int
    first: Interval | None
    last: Interval | None


def _ensure_interval(interval: Interval) -> Interval:
    start_dt, end_dt = interval

    if start_dt is None or end_dt is None:
        raise ValueError("Interval endpoints must not be None")

    if start_dt > end_dt:
        raise ValueError(f"Invalid interval: start > end: {interval!r}")

    return start_dt, end_dt


def normalize_intervals(intervals: Iterable[Interval]) -> list[Interval]:
    """
    Validate intervals and sort them by (start, end).

    Notes:
    - Adjacent intervals are not merged here; do it upstream if needed.
    """
    normalized = [_ensure_interval(interval=item) for item in intervals]
    normalized.sort(key=lambda item: (item[0], item[1]))
    return normalized


def _is_long_enough(interval: Interval, min_duration: timedelta) -> bool:
    start_dt, end_dt = interval
    return (end_dt - start_dt) >= min_duration


def intersect_two(first_intervals: Sequence[Interval], second_intervals: Sequence[Interval], min_duration: timedelta = timedelta(0)) -> list[Interval]:
    """
    Intersect two sorted interval sequences in O(n+m).

    Contract:
    - Inputs must be normalized (validated + sorted by start time).
    - Output is sorted by start time.
    """
    if not first_intervals or not second_intervals:
        return []

    if min_duration < timedelta(0):
        raise ValueError("min_duration must be >= 0")

    overlaps: list[Interval] = []
    i = 0
    j = 0

    while i < len(first_intervals) and j < len(second_intervals):
        start_a, end_a = first_intervals[i]
        start_b, end_b = second_intervals[j]

        start_dt = start_a if start_a >= start_b else start_b
        end_dt = end_a if end_a <= end_b else end_b

        if start_dt <= end_dt:
            interval = (start_dt, end_dt)
            if _is_long_enough(interval=interval, min_duration=min_duration):
                overlaps.append(interval)

        if end_a <= end_b:
            i += 1
        else:
            j += 1

    return overlaps


def intersect_many(interval_groups: Sequence[Iterable[Interval]], min_duration: timedelta = timedelta(0)) -> list[Interval]:
    """
    Intersect N groups of intervals.

    RORO:
    - Receive: {interval_groups, min_duration}
    - Return: list[Interval]
    """
    if not interval_groups:
        return []

    normalized_groups = [normalize_intervals(intervals=group) for group in interval_groups]
    normalized_groups.sort(key=len)

    current = normalized_groups[0]
    for group in normalized_groups[1:]:
        if not current:
            return []
        current = intersect_two(
            first_intervals=current,
            second_intervals=group,
            min_duration=min_duration,
        )

    save_intervals_csv(
        intervals=current,
        output_path=availability_periods_dir(config) / "intersections_availability_periods.csv",
    )

    return current


def summarize_intervals(intervals: Sequence[Interval]) -> IntervalsSummary:
    if not intervals:
        return {
            "count": 0,
            "first": None,
            "last": None,
        }

    return {
        "count": len(intervals),
        "first": intervals[0],
        "last": intervals[-1],
    }


def save_intervals_csv(intervals: Sequence[Interval], output_path: str | Path) -> Path:
    """
    Save intervals to CSV with unified columns:
    start, end, duration_seconds.
    """
    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    with destination.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(["start", "end", "duration_seconds"])
        for start_dt, end_dt in intervals:
            writer.writerow(
                [
                    start_dt.isoformat(),
                    end_dt.isoformat(),
                    (end_dt - start_dt).total_seconds(),
                ]
            )

    return destination