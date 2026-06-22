from backend.src.io.kyoto_ae import parse_kyoto_ae_file, read_kyoto_ae_directory, save_kyoto_ae_to_parquet
from backend.src.io.kyoto_symh import (
    parse_kyoto_symh_file,
    read_kyoto_symh_directory,
    save_kyoto_symh_to_parquet,
)

__all__ = [
    "CDAweb",
    "DataDownloading",
    "RawData",
    "parse_kyoto_ae_file",
    "parse_kyoto_symh_file",
    "read_kyoto_ae_directory",
    "read_kyoto_symh_directory",
    "save_kyoto_ae_to_parquet",
    "save_kyoto_symh_to_parquet",
]


def __getattr__(name: str):
    if name == "CDAweb":
        from backend.src.io.cdaweb import CDAweb

        return CDAweb
    if name == "DataDownloading":
        from backend.src.io.loader import DataDownloading

        return DataDownloading
    if name == "RawData":
        from backend.src.io.raw_data import RawData

        return RawData
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
