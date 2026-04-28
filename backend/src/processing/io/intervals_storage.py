import csv
from datetime import datetime
from pathlib import Path

from backend.src.config.schemas import AppConfig
from backend.src.io.paths import PathResolver

Interval = tuple[datetime, datetime]


def save_intervals_csv(intervals: list[Interval], output_path: str | Path) -> Path:
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


def save_source_periods(intervals: list[Interval], config: AppConfig, source_stem: str) -> Path:
    output_path = PathResolver(config).periods_file(source_stem)
    return save_intervals_csv(intervals=intervals, output_path=output_path)


def save_intersections_periods(intervals: list[Interval], config: AppConfig) -> Path:
    output_path = PathResolver(config).periods_file("intersections")
    return save_intervals_csv(intervals=intervals, output_path=output_path)
