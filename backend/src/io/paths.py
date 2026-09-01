import re
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from functools import cached_property
from pathlib import Path

from backend.src.config.schemas import AppConfig, TIME_FORMAT

THEMIS_PREFIX = "THEMIS"
KYOTO_DIRNAME = "kyoto"
DYNAMICS_DIRNAME = "dynamics"
DATA_DIRNAME = "data"
PERIODS_DIRNAME = "periods"
MATRICES_DIRNAME = "matrices"
DISTRIBUTIONS_DIRNAME = "distributions"
IMAGES_DIRNAME = "images"
INTERSECTIONS_STEM = "intersections"


class Instrument(StrEnum):
    """
    Сырые parquet-инструменты THEMIS/OMNI в .../data/.
    """

    EFI = "efi"
    FGM = "fgm"
    ESA_ION = "esa_ion"
    ESA_ELECTRON = "esa_electron"
    SSC = "ssc"
    STA = "sta"
    OMNI = "omn"
    MOM = "mom"


class DerivedDataset(StrEnum):
    """
    Производные датасеты события, считаемые в processing.
    """

    SHUE = "shue"
    BETA = "beta"


class EventDataset(StrEnum):
    """
    Итоговые parquet события после интерполяции и подготовки H/G.
    """

    AVAILABLE = "available_data"
    PREPARED = "prepared_data"


class KyotoIndex(StrEnum):
    """
    Индексы Kyoto WDC; хранятся в backend/data/kyoto/, без привязки к событию.
    """

    AE = "ae"
    SYMH = "symh"


class DistributionParameter(StrEnum):
    """
    Параметры полярных распределений на сетке L–MLT.
    """

    H = "H"
    G = "G"
    J = "J"
    BETA = "Beta"


class Component(StrEnum):
    """
    Компоненты field-aligned базиса (f, a, r).
    """

    F = "f"
    A = "a"
    R = "r"


class Reducer(StrEnum):
    """
    Редукторы при агрегации значений в ячейках распределения.
    """

    MEAN = "mean"
    MEDIAN = "median"
    Q25 = "q25"
    Q75 = "q75"


AvailabilitySource = Instrument | DerivedDataset
EventDataSource = Instrument | DerivedDataset | EventDataset

_KYOTO_INDEX_DIRS: dict[KyotoIndex, str] = {
    KyotoIndex.AE: "ae_index",
    KyotoIndex.SYMH: "sym_index",
}


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _safe_name(name: str, value: str, suffix: str | None = None) -> str:
    normalized = value.strip()
    if suffix and normalized.endswith(suffix):
        normalized = normalized[: -len(suffix)]
    if not normalized or re.search(r"[\\/]", normalized):
        raise ValueError(f"Invalid {name}: {value!r}")
    return normalized


def paths(config: AppConfig | None = None, *, root: Path | None = None) -> "PathResolver":
    """
    Единая точка входа для построения путей backend и frontend.
    """

    if config is None:
        from backend.src.config import get_config

        config = get_config()
    return PathResolver(config, root=root)


@dataclass(frozen=True, slots=True)
class KyotoPaths:
    """
    Пути Kyoto WDC: индексы AE/SYMH и сырые файлы в backend/data/kyoto/.
    """

    _resolver: "PathResolver"

    def root_dir(self) -> Path:
        """
        Корень каталога Kyoto: backend/data/kyoto/.
        """

        return (self._resolver.data_root_dir / KYOTO_DIRNAME).resolve()

    def index_dir(self, index: KyotoIndex) -> Path:
        """
        Каталог индекса, например backend/data/kyoto/ae_index/.
        """

        return (self.root_dir() / _KYOTO_INDEX_DIRS[index]).resolve()

    def index_parquet(self, index: KyotoIndex) -> Path:
        """
        Готовый parquet индекса, например .../ae_index/ae.parquet.
        """

        return (self.index_dir(index) / f"{index.value}.parquet").resolve()

    def source_dir(self, index: KyotoIndex) -> Path:
        """
        Каталог сырых файлов Kyoto (.for.request, .txt) для индекса.
        """

        return self.index_dir(index)

    def dynamics_dir(self) -> Path:
        """
        Каталог PNG-графиков динамики индексов Kyoto.
        """

        return (self.root_dir() / DYNAMICS_DIRNAME).resolve()

    def dynamics_file(self, file_name: str) -> Path:
        """
        PNG-файл в backend/data/kyoto/dynamics/.
        """

        safe_name = _safe_name("kyoto dynamics file name", file_name)
        return (self.dynamics_dir() / safe_name).resolve()


@dataclass(frozen=True, slots=True)
class PathResolver:
    """
    Построение всех путей к датасетам, периодам, матрицам, распределениям и графикам.
    """

    config: AppConfig
    root: Path | None = None

    @cached_property
    def project_root(self) -> Path:
        """
        Абсолютный путь к корню репозитория.
        """

        return (self.root or _project_root()).resolve()

    @cached_property
    def data_root_dir(self) -> Path:
        """
        Корень backend/data/.
        """

        return (self.project_root / self.config.paths.data_root).resolve()

    @cached_property
    def events_root_dir(self) -> Path:
        """
        Корень каталога событий THEMIS.
        """

        return (self.project_root / self.config.paths.events).resolve()

    @cached_property
    def event_id(self) -> str:
        """
        Идентификатор события в формате YYYY-MM-DD_YYYY-MM-DD.
        """

        start = datetime.strptime(self.config.reading.time_start, TIME_FORMAT)
        end = datetime.strptime(self.config.reading.time_end, TIME_FORMAT)
        return f"{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}"

    @cached_property
    def satellite_id(self) -> str:
        """
        Идентификатор спутника в формате THEMIS-X.
        """

        return f"{THEMIS_PREFIX}-{self.config.reading.satellite.strip().upper()}"

    @cached_property
    def event_dir(self) -> Path:
        """
        Корень артефактов события: .../events/{event_id}/THEMIS-{sat}/.
        """

        return (self.events_root_dir / self.event_id / self.satellite_id).resolve()

    @cached_property
    def data_dir(self) -> Path:
        """
        Parquet датасетов события: .../data/.
        """

        return (self.event_dir / DATA_DIRNAME).resolve()

    @cached_property
    def periods_dir(self) -> Path:
        """
        CSV интервалов доступности: .../periods/.
        """

        return (self.event_dir / PERIODS_DIRNAME).resolve()

    @cached_property
    def matrices_dir(self) -> Path:
        """
        Long-матрицы распределений: .../matrices/.
        """

        return (self.event_dir / MATRICES_DIRNAME).resolve()

    @cached_property
    def distributions_dir(self) -> Path:
        """
        Сводные CSV распределений: .../distributions/.
        """

        return (self.event_dir / DISTRIBUTIONS_DIRNAME).resolve()

    @cached_property
    def images_dir(self) -> Path:
        """
        PNG-графики события: .../images/.
        """

        return (self.event_dir / IMAGES_DIRNAME).resolve()

    @cached_property
    def kyoto(self) -> KyotoPaths:
        """
        Доступ к путям Kyoto WDC (вне события).
        """

        return KyotoPaths(self)

    def instrument(self, name: Instrument | DerivedDataset) -> Path:
        """
        Parquet инструмента или производного датасета в .../data/{name}.parquet.
        """

        stem = _safe_name("instrument", str(name), suffix=".parquet")
        return (self.data_dir / f"{stem}.parquet").resolve()

    def dataset(self, name: EventDataset) -> Path:
        """
        Parquet итогового датасета события в .../data/{name}.parquet.
        """

        stem = _safe_name("dataset", str(name), suffix=".parquet")
        return (self.data_dir / f"{stem}.parquet").resolve()

    def availability_periods(self, source: AvailabilitySource) -> Path:
        """
        CSV интервалов доступности источника: .../periods/{source}_availability_periods.csv.
        """

        stem = _safe_name("availability source", str(source))
        return (self.periods_dir / f"{stem}_availability_periods.csv").resolve()

    def intersection_periods(self) -> Path:
        """
        CSV пересечения интервалов всех источников.
        """

        return (self.periods_dir / f"{INTERSECTIONS_STEM}_availability_periods.csv").resolve()

    def distribution_raw_long(
        self,
        parameter: DistributionParameter,
        component: Component,
    ) -> Path:
        """
        Long-parquet сырых значений: .../matrices/distribution_raw_long_{P}_{c}.parquet.
        """

        file_name = f"distribution_raw_long_{parameter.value}_{component.value}.parquet"
        return (self.matrices_dir / file_name).resolve()

    def distribution_map(
        self,
        parameter: DistributionParameter,
        reducer: Reducer,
        *,
        component: Component | None = None,
    ) -> Path:
        """
        CSV сводного распределения на сетке L–MLT.

        Для H/G/J: distribution_{P}_{c}_{reducer}.csv; для Beta: distribution_Beta_{reducer}.csv.
        """

        if parameter is DistributionParameter.BETA:
            file_name = f"distribution_{parameter.value}_{reducer.value}.csv"
        elif component is None:
            raise ValueError(f"component is required for parameter {parameter.value}")
        else:
            file_name = f"distribution_{parameter.value}_{component.value}_{reducer.value}.csv"
        return (self.distributions_dir / file_name).resolve()

    def distribution_map_by_key(self, parameter_key: str, reducer: Reducer) -> Path:
        """
        CSV распределения по ключу колонки (H_f, Beta, J_a и т.п.) из Distributions.build_maps.
        """

        safe_key = _safe_name("distribution parameter key", parameter_key)
        file_name = f"distribution_{safe_key}_{reducer.value}.csv"
        return (self.distributions_dir / file_name).resolve()

    def image(self, stem: str, *, suffix: str = ".png") -> Path:
        """
        PNG-график события в .../images/{stem}.png.
        """

        safe_stem = _safe_name("image stem", stem, suffix=suffix)
        return (self.images_dir / f"{safe_stem}{suffix}").resolve()
