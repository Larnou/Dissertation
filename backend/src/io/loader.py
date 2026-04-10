from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.parquet import read_data_from_parquet, save_data_to_parquet
from backend.src.io.paths import event_interval_folder_name
from backend.src.io.raw_data import RawData

# Логические имена файлов (stem) внутри .../events/<даты>/THEMIS-X/
DATASET_SSC = "ssc"
DATASET_MAGNETIC_FIELD = "magnetic_field"
DATASET_ELECTRIC_FIELD = "electric_field"
DATASET_STATE = "state"
DATASET_OMNI = "omni"


@dataclass(init=False)
class DataDownloading:
    """
    Загрузка датасетов THEMIS/OMNI: с диска (parquet) или с CDAWeb.

    Файлы: ``backend/data/events/<YYYY-MM-DD_YYYY-MM-DD>/THEMIS-<X>/<stem>.parquet``.

    Шаг разбиения интервала для CDAWeb — только ``config.reading.delta`` (из JSON).

    Конфигурация — как в `AppConfig` (в т.ч. из `config.json`). Старый вид
    `parameters: dict` поддерживается: передайте словарь, он будет провалидирован.
    """

    config: AppConfig

    def __init__(self, parameters: AppConfig) -> None:
        self.config = AppConfig.model_validate(dict(parameters))

    def format_file_name(self) -> str:
        """Сегмент папки по датам интервала (совместимость с прежним API)."""
        return event_interval_folder_name(self.config)

    def get_ssc_data(self, load: bool) -> pd.DataFrame:
        stem = DATASET_SSC

        if load:
            return read_data_from_parquet(self.config, stem)

        raw = RawData(self.config)
        dataframe = raw.get_ssc_dataframe()
        save_data_to_parquet(self.config, dataframe, stem, title=True)
        return dataframe

    def get_fgm_data(self, load: bool) -> pd.DataFrame:
        stem = DATASET_MAGNETIC_FIELD

        if load:
            return read_data_from_parquet(self.config, stem)

        raw = RawData(self.config)
        dataframe = raw.get_fgm_dataframe()
        save_data_to_parquet(self.config, dataframe, stem, title=True)
        return dataframe

    def get_esa_data(self, load: bool, particle: Literal["ion", "electron"]) -> pd.DataFrame:
        stem = f"esa_{particle}"

        if load:
            return read_data_from_parquet(self.config, stem)

        raw = RawData(self.config)
        dataframe = raw.get_esa_dataframe(particle)
        save_data_to_parquet(self.config, dataframe, stem, title=True)
        return dataframe

    def get_efi_data(self, load: bool) -> pd.DataFrame:
        stem = DATASET_ELECTRIC_FIELD
        if load:
            return read_data_from_parquet(self.config, stem)

        raw = RawData(self.config)
        dataframe = raw.get_efi_dataframe()
        save_data_to_parquet(self.config, dataframe, stem, title=True)
        return dataframe

    def get_sta_data(self, load: bool) -> pd.DataFrame:
        stem = DATASET_STATE
        if load:
            return read_data_from_parquet(self.config, stem)

        raw = RawData(self.config)
        dataframe = raw.get_sta_dataframe()
        save_data_to_parquet(self.config, dataframe, stem, title=True)
        return dataframe

    def get_omn_data(self, load: bool) -> pd.DataFrame:
        stem = DATASET_OMNI
        if load:
            return read_data_from_parquet(self.config, stem)

        raw = RawData(self.config)
        dataframe = raw.get_omn_dataframe()
        save_data_to_parquet(self.config, dataframe, stem, title=True)
        return dataframe
