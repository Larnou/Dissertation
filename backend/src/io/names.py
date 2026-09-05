from enum import StrEnum


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
    Стем производного parquet события (модель Shue, β).
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
    STD = "std"
