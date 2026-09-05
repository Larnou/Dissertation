"""
Тесты схемы и загрузки конфигурации.
"""

from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

from pydantic import ValidationError

from backend.src.config import load_app_config, project_root
from backend.src.config.schemas import (
    AppConfig,
    ReadingConfig,
    WindowFilterConfig,
    parse_duration,
)
from backend.src.io.paths import PathResolver
from backend.src.io.time_borders import format_time_borders


def _payload(**overrides: object) -> dict:
    data: dict = {
        "reading": {
            "satellite": "A",
            "time_start": "2017-01-01 00:00:00",
            "time_end": "2018-03-01 00:00:00",
            "delta": "1M",
        },
        "window_filter": {"low_pass": 600, "high_pass": 45},
        "h_parameter": {"noise_e": 0.1, "noise_vb": 0.1},
        "paths": {"data_root": "backend/data"},
    }
    data.update(overrides)
    return data


class ParseDurationTests(unittest.TestCase):
    def test_month_is_thirty_days(self) -> None:
        self.assertEqual(parse_duration("1M"), timedelta(days=30))

    def test_hours_and_days(self) -> None:
        self.assertEqual(parse_duration("6H"), timedelta(hours=6))
        self.assertEqual(parse_duration("1D"), timedelta(days=1))

    def test_rejects_unknown_unit(self) -> None:
        with self.assertRaises(ValueError):
            parse_duration("1W")


class SchemaTests(unittest.TestCase):
    def test_real_config_json_loads(self) -> None:
        config = load_app_config()
        self.assertEqual(config.reading.satellite, "A")
        self.assertIsInstance(config.reading.time_start, datetime)
        self.assertEqual(config.reading.delta, timedelta(days=30))
        self.assertEqual(config.paths.data_root, "backend/data")

    def test_time_order(self) -> None:
        with self.assertRaises(ValidationError):
            ReadingConfig.model_validate(
                {
                    "satellite": "A",
                    "time_start": "2018-01-01 00:00:00",
                    "time_end": "2017-01-01 00:00:00",
                    "delta": "1D",
                }
            )

    def test_period_order(self) -> None:
        with self.assertRaises(ValidationError):
            WindowFilterConfig.model_validate({"low_pass": 10, "high_pass": 45})

    def test_unknown_section_forbidden(self) -> None:
        with self.assertRaises(ValidationError):
            AppConfig.model_validate(_payload(frequency_filter={"bandwidth": 0.001, "min_frequency": 0.1, "max_frequency": 1.0}))

    def test_events_path_is_derived(self) -> None:
        with self.assertRaises(ValidationError):
            AppConfig.model_validate(
                _payload(
                    paths={"data_root": "backend/data", "events": "backend/data/events"},
                )
            )

    def test_load_from_custom_path(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(_payload()), encoding="utf-8")
            config = load_app_config(path)
            self.assertEqual(config.reading.satellite, "A")
            self.assertEqual(config.window_filter.high_pass, 45)

    def test_missing_file(self) -> None:
        with self.assertRaises(FileNotFoundError):
            load_app_config(Path("missing-config.json"))

    def test_invalid_json(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text("{not json", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_app_config(path)


class PathResolverTests(unittest.TestCase):
    def test_event_id_and_events_dir(self) -> None:
        config = AppConfig.model_validate(_payload())
        resolver = PathResolver(config, root=project_root())
        self.assertEqual(resolver.event_id, "2017-01-01_2018-03-01")
        self.assertEqual(resolver.satellite_id, "THEMIS-A")
        self.assertEqual(resolver.events_root_dir, resolver.data_root_dir / "events")


class TimeBordersTests(unittest.TestCase):
    def test_splits_by_delta(self) -> None:
        reading = ReadingConfig.model_validate(
            {
                "satellite": "A",
                "time_start": "2017-01-01 00:00:00",
                "time_end": "2017-03-01 00:00:00",
                "delta": "1M",
            }
        )
        borders = format_time_borders(reading)
        self.assertEqual(len(borders), 2)
        self.assertEqual(borders[0]["start"], "2017-01-01T00:00:00Z")
        self.assertEqual(borders[-1]["end"], "2017-03-01T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
