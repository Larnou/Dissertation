"""
Артефакты на диске, CDAWeb и статичный Kyoto.
"""

from backend.src.io.cdaweb import CDAweb
from backend.src.io.loader import DataDownloading
from backend.src.io.names import (
    Component,
    DerivedDataset,
    DistributionParameter,
    EventDataset,
    Instrument,
    KyotoIndex,
    Reducer,
)
from backend.src.io.paths import PathResolver, paths
from backend.src.io.raw_data import RawData

__all__ = [
    "CDAweb",
    "Component",
    "DataDownloading",
    "DerivedDataset",
    "DistributionParameter",
    "EventDataset",
    "Instrument",
    "KyotoIndex",
    "PathResolver",
    "RawData",
    "Reducer",
    "paths",
]
