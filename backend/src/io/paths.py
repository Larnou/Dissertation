"""
Правила имён файлов и каталогов данных.

Событие и спутник читаются из конфига. Методам достаточно того,
что отличает файл: датасет, индекс, параметр распределения.
"""

from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from backend.src.config.root import project_root
from backend.src.config.schemas import AppConfig

THEMIS_PREFIX = "THEMIS"
KYOTO_DIRNAME = "kyoto"
DYNAMICS_DIRNAME = "dynamics"
DATA_DIRNAME = "data"
EVENTS_DIRNAME = "events"
PERIODS_DIRNAME = "periods"
MATRICES_DIRNAME = "matrices"
DISTRIBUTIONS_DIRNAME = "distributions"
IMAGES_DIRNAME = "images"
INTERSECTIONS_STEM = "intersections"


class Instrument(StrEnum):
    """
    Стем parquet сырого инструмента THEMIS/OMNI в каталоге data/ события.
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
    Стем производного parquet события (модель Shue, β), который считается в processing.
    """

    SHUE = "shue"
    BETA = "beta"


class EventDataset(StrEnum):
    """
    Стем итогового parquet события после интерполяции и подготовки H/G.
    """

    AVAILABLE = "available_data"
    PREPARED = "prepared_data"


class KyotoIndex(StrEnum):
    """
    Индекс Kyoto WDC.

    Каталог — backend/data/kyoto/{имя}_index/, parquet — {имя}.parquet.
    """

    AE = "ae"
    SYMH = "symh"


class DistributionParameter(StrEnum):
    """
    Величина полярного распределения на сетке L-MLT; входит в имя файла.
    """

    H = "H"
    G = "G"
    J = "J"
    BETA = "Beta"


class Component(StrEnum):
    """
    Ось field-aligned базиса: f — вдоль поля, a — азимутальная, r — радиальная.
    """

    F = "f"
    A = "a"
    R = "r"


class Reducer(StrEnum):
    """
    Способ агрегации значений в ячейке распределения L-MLT.
    """

    MEAN = "mean"
    MEDIAN = "median"
    Q25 = "q25"
    Q75 = "q75"


def paths(config: AppConfig | None = None, *, root: Path | None = None) -> "PathResolver":
    """
    Собирает резолвер путей для текущего события и спутника.

    Args:
        config: параметры reading и каталогов данных. Если не передан, берётся из get_config().
        root: явный корень репозитория. Если не задан, определяется через project_root().

    Returns:
        Объект, в котором событие и спутник уже взяты из конфига.
    """

    if config is None:
        from backend.src.config import get_config

        config = get_config()
    return PathResolver(config, root=root)


@dataclass(frozen=True, slots=True)
class KyotoPaths:
    """
    Пути индексов Kyoto WDC вне дерева события: backend/data/kyoto/{индекс}_index/.
    """

    _resolver: "PathResolver"

    def root_dir(self) -> Path:
        """
        Корень каталогов Kyoto: backend/data/kyoto/.
        """

        return (self._resolver.data_root_dir / KYOTO_DIRNAME).resolve()

    def index_dir(self, index: KyotoIndex) -> Path:
        """
        Каталог индекса: backend/data/kyoto/{index}_index/.

        Args:
            index: AE или SYM-H; в имя каталога подставляется значение enum.
        """

        return (self.root_dir() / f"{index}_index").resolve()

    def index_parquet(self, index: KyotoIndex) -> Path:
        """
        Готовый parquet индекса: .../{index}_index/{index}.parquet.

        Args:
            index: AE или SYM-H.
        """

        return (self.index_dir(index) / f"{index}.parquet").resolve()

    def source_dir(self, index: KyotoIndex) -> Path:
        """
        Каталог исходников индекса. Это тот же каталог, где лежит parquet.

        Args:
            index: AE или SYM-H.
        """

        return self.index_dir(index)

    def dynamics_dir(self) -> Path:
        """
        Каталог PNG динамики индексов: backend/data/kyoto/dynamics/.
        """

        return (self.root_dir() / DYNAMICS_DIRNAME).resolve()

    def dynamics_file(self, file_name: str) -> Path:
        """
        PNG-файл в каталоге динамики индексов.

        Args:
            file_name: имя файла, включая расширение.
        """

        return (self.dynamics_dir() / file_name).resolve()


@dataclass(frozen=True, slots=True)
class PathResolver:
    """
    Собирает пути артефактов текущего события и спутника.

    Интервал и спутник читаются из конфига. Методам достаточно того,
    что отличает файл: датасет, индекс, параметр распределения.

    Attributes:
        config: конфигурация приложения (интервал, спутник, каталоги данных).
        root: явный корень репозитория; иначе используется project_root().
    """

    config: AppConfig
    root: Path | None = None

    @property
    def project_root(self) -> Path:
        """
        Корень репозитория: явный root или результат project_root().
        """

        return (self.root or project_root()).resolve()

    @property
    def data_root_dir(self) -> Path:
        """
        Корень данных из конфига, обычно backend/data/.
        """

        return (self.project_root / self.config.paths.data_root).resolve()

    @property
    def events_root_dir(self) -> Path:
        """
        Каталог событий THEMIS: {data_root}/events/.
        """

        return (self.data_root_dir / EVENTS_DIRNAME).resolve()

    @property
    def event_id(self) -> str:
        """
        Идентификатор события YYYY-MM-DD_YYYY-MM-DD по reading.time_start и time_end.
        """

        start = self.config.reading.time_start
        end = self.config.reading.time_end
        return f"{start.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}"

    @property
    def satellite_id(self) -> str:
        """
        Идентификатор спутника THEMIS-X по reading.satellite.
        """

        return f"{THEMIS_PREFIX}-{self.config.reading.satellite.strip().upper()}"

    @property
    def event_dir(self) -> Path:
        """
        Корень артефактов события: .../events/{event_id}/{satellite_id}/.
        """

        return (self.events_root_dir / self.event_id / self.satellite_id).resolve()

    @property
    def data_dir(self) -> Path:
        """
        Каталог parquet датасетов события: .../data/.
        """

        return (self.event_dir / DATA_DIRNAME).resolve()

    @property
    def periods_dir(self) -> Path:
        """
        Каталог CSV интервалов доступности: .../periods/.
        """

        return (self.event_dir / PERIODS_DIRNAME).resolve()

    @property
    def matrices_dir(self) -> Path:
        """
        Каталог long-матриц распределений: .../matrices/.
        """

        return (self.event_dir / MATRICES_DIRNAME).resolve()

    @property
    def distributions_dir(self) -> Path:
        """
        Каталог сводных CSV распределений: .../distributions/.
        """

        return (self.event_dir / DISTRIBUTIONS_DIRNAME).resolve()

    @property
    def images_dir(self) -> Path:
        """
        Каталог PNG-графиков события: .../images/.
        """

        return (self.event_dir / IMAGES_DIRNAME).resolve()

    @property
    def kyoto(self) -> KyotoPaths:
        """
        Пути Kyoto WDC; не привязаны к событию.
        """

        return KyotoPaths(self)

    def dataset(self, name: str) -> Path:
        """
        Parquet датасета события: .../data/{name}.parquet.

        Args:
            name: стем файла, например EventDataset.AVAILABLE или Instrument.FGM.
        """

        return (self.data_dir / f"{name}.parquet").resolve()

    def availability_periods(self, source: str) -> Path:
        """
        CSV интервалов доступности: .../periods/{source}_availability_periods.csv.

        Args:
            source: стем источника, например Instrument.FGM или DerivedDataset.SHUE.
        """

        return (self.periods_dir / f"{source}_availability_periods.csv").resolve()

    def intersection_periods(self) -> Path:
        """
        CSV пересечения интервалов всех источников: .../periods/intersections_availability_periods.csv.
        """

        return (self.periods_dir / f"{INTERSECTIONS_STEM}_availability_periods.csv").resolve()

    def distribution_raw_long(self, parameter: DistributionParameter, component: Component) -> Path:
        """
        Long-parquet сырых значений в ячейках сетки.

        Имя файла: distribution_raw_long_{parameter}_{component}.parquet в matrices/.

        Args:
            parameter: величина на сетке (H, G, J, Beta).
            component: ось field-aligned базиса.
        """

        file_name = f"distribution_raw_long_{parameter}_{component}.parquet"
        return (self.matrices_dir / file_name).resolve()

    def distribution_map(
        self,
        parameter: DistributionParameter,
        reducer: Reducer,
        *,
        component: Component | None = None,
    ) -> Path:
        """
        CSV сводного распределения на сетке L-MLT.

        Для H, G и J имя файла — distribution_{parameter}_{component}_{reducer}.csv.
        Для Beta компонента не нужна: distribution_Beta_{reducer}.csv.

        Args:
            parameter: величина на сетке.
            reducer: способ агрегации в ячейке.
            component: ось базиса; обязательна для H, G и J.

        Raises:
            ValueError: для H, G или J не передана component.
        """

        if parameter is DistributionParameter.BETA:
            file_name = f"distribution_{parameter}_{reducer}.csv"
        elif component is None:
            raise ValueError(f"component is required for parameter {parameter}")
        else:
            file_name = f"distribution_{parameter}_{component}_{reducer}.csv"
        return (self.distributions_dir / file_name).resolve()

    def distribution_map_by_key(self, parameter_key: str, reducer: Reducer) -> Path:
        """
        CSV распределения по ключу колонки из Distributions.build_maps.

        Args:
            parameter_key: имя колонки, например H_f, Beta, J_a.
            reducer: способ агрегации в ячейке.
        """

        return (self.distributions_dir / f"distribution_{parameter_key}_{reducer}.csv").resolve()

    def image(self, stem: str, *, suffix: str = ".png") -> Path:
        """
        PNG-график события: .../images/{stem}{suffix}.

        Args:
            stem: имя файла без расширения.
            suffix: расширение, по умолчанию .png.
        """

        return (self.images_dir / f"{stem}{suffix}").resolve()
