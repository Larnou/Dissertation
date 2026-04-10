from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.parquet import read_data_from_parquet, save_data_to_parquet
from backend.src.io.paths import event_interval_folder_name
from backend.src.io.raw_data import RawData

# Логические имена файлов (stem) внутри .../events/<даты>/THEMIS-X/
DATASET_ELECTRIC_FIELD = "efi"
# DATASET_PARTICLE_VELOCITY = "esa_*"
DATASET_MAGNETIC_FIELD = "fgm"
DATASET_OMNI = "omn"
DATASET_SSC = "ssc"
DATASET_STATE = "sta"


@dataclass(init=False)
class DataDownloading:
    """
    Загрузка датасетов THEMIS/OMNI.

    Два низкоуровневых загрузчика: ``read_from_disk`` (parquet) и ``fetch_from_cdaweb``
    (CDAWeb + сохранение в parquet). Методы по инструментам задают stem и вызывают один из них
    в зависимости от ``load_from_cdaweb``.

    Шаг интервала для CDAWeb — ``config.reading.delta``.
    """

    config: AppConfig
    load_from_cdaweb: bool

    def __init__(self, parameters: AppConfig, load_from_cdaweb: bool) -> None:
        self.config = AppConfig.model_validate(dict(parameters))
        self.load_from_cdaweb = load_from_cdaweb


    def format_file_name(self) -> str:
        """Сегмент папки по датам интервала (совместимость с прежним API)."""
        return event_interval_folder_name(self.config)


    def read_from_disk(self, stem: str) -> pd.DataFrame:
        """Читает ``<stem>.parquet`` из каталога события (локальный диск)."""
        return read_data_from_parquet(self.config, stem)


    def fetch_from_cdaweb(self, stem: str, fetch: Callable[[RawData], pd.DataFrame]) -> pd.DataFrame:
        """
        Скачивает данные через ``RawData``, сохраняет в ``<stem>.parquet`` и возвращает DataFrame.
        """

        raw_data = RawData(self.config)
        dataframe = fetch(raw_data)

        save_data_to_parquet(self.config, dataframe, stem)
        return dataframe


    def _load_by_source(self, stem: str, fetch: Callable[[RawData], pd.DataFrame]) -> pd.DataFrame:
        if self.load_from_cdaweb:
            return self.fetch_from_cdaweb(stem, fetch)
        return self.read_from_disk(stem)


    def get_ssc_data(self) -> pd.DataFrame:
        stem = DATASET_SSC
        return self._load_by_source(stem, lambda r: r.get_ssc_dataframe())

    def get_fgm_data(self) -> pd.DataFrame:
        stem = DATASET_MAGNETIC_FIELD
        return self._load_by_source(stem, lambda r: r.get_fgm_dataframe())

    def get_esa_data(self, particle: Literal["ion", "electron"]) -> pd.DataFrame:
        stem = f"esa_{particle}"
        return self._load_by_source(stem, lambda r: r.get_esa_dataframe(particle))

    def get_efi_data(self) -> pd.DataFrame:
        stem = DATASET_ELECTRIC_FIELD
        return self._load_by_source(stem, lambda r: r.get_efi_dataframe())

    def get_sta_data(self) -> pd.DataFrame:
        stem = DATASET_STATE
        return self._load_by_source(stem, lambda r: r.get_sta_dataframe())

    def get_omn_data(self) -> pd.DataFrame:
        stem = DATASET_OMNI
        return self._load_by_source(stem, lambda r: r.get_omn_dataframe())
