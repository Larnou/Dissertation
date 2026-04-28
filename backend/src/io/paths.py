import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from backend.src.config.schemas import AppConfig, TIME_FORMAT

THEMIS_PREFIX = "THEMIS"
DATA_DIRNAME = "data"
PERIODS_DIRNAME = "periods"
MATRICES_DIRNAME = "matrices"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _safe_name(name: str, value: str, suffix: str | None = None) -> str:
    normalized = value.strip()
    if suffix and normalized.endswith(suffix):
        normalized = normalized[: -len(suffix)]
    if not normalized or re.search(r"[\\/]", normalized):
        raise ValueError(f"Invalid {name}: {value!r}")
    return normalized


@dataclass(frozen=True, slots=True)
class PathResolver:
    """
    Минимальный публичный API путей.

    Внешний код должен использовать только:
    - data_file(stem)
    - periods_file(stem)
    - matrix_file(filename)
    """

    config: AppConfig
    root: Path | None = None

    @property
    def project_root(self) -> Path:
        return (self.root or _project_root()).resolve()

    @property
    def event_id(self) -> str:
        start = datetime.strptime(self.config.reading.time_start, TIME_FORMAT)
        end = datetime.strptime(self.config.reading.time_end, TIME_FORMAT)
        return f"{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}"

    @property
    def satellite_id(self) -> str:
        return f"{THEMIS_PREFIX}-{self.config.reading.satellite.strip().upper()}"

    def _rooted(self, configured: str) -> Path:
        return (self.project_root / configured).resolve()

    def _event_root(self, configured: str) -> Path:
        return (self._rooted(configured) / self.event_id / self.satellite_id).resolve()

    def data_file(self, dataset_stem: str) -> Path:
        stem = _safe_name("dataset stem", dataset_stem, suffix=".parquet")
        return (self._event_root(self.config.paths.data) / DATA_DIRNAME / f"{stem}.parquet").resolve()

    def periods_file(self, source_stem: str) -> Path:
        stem = _safe_name("source stem", source_stem, suffix=".csv")
        return (self._event_root(self.config.paths.periods) / PERIODS_DIRNAME / f"{stem}_availability_periods.csv").resolve()

    def matrix_file(self, file_name: str) -> Path:
        safe_name = _safe_name("matrix file name", file_name)
        return (self._event_root(self.config.paths.matrices) / MATRICES_DIRNAME / safe_name).resolve()
