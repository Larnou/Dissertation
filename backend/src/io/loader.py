from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.names import Instrument
from backend.src.io.parquet import read_data_from_parquet, save_data_to_parquet
from backend.src.io.raw_data import RawData


@dataclass(frozen=True, slots=True)
class DataDownloading:
    """
    Чтение parquet или скачивание THEMIS/OMNI с CDAWeb.

    ``load_from_cdaweb=True`` — fetch + сохранение parquet.
    ``load_from_cdaweb=False`` — только диск.
    Шаг скачивания — ``config.reading.delta``.
    """

    config: AppConfig
    load_from_cdaweb: bool

    def read_from_disk(self, source: Instrument) -> pd.DataFrame:
        """
        Читает parquet инструмента из каталога события.
        """

        return read_data_from_parquet(self.config, source)

    def fetch_from_cdaweb(self, source: Instrument, fetch: Callable[[RawData], pd.DataFrame]) -> pd.DataFrame:
        """
        Скачивает данные через ``RawData``, сохраняет parquet и возвращает таблицу.
        """

        dataframe = fetch(RawData(self.config))
        save_data_to_parquet(self.config, dataframe, source)
        return dataframe

    def _load(self, source: Instrument, fetch: Callable[[RawData], pd.DataFrame]) -> pd.DataFrame:
        if self.load_from_cdaweb:
            return self.fetch_from_cdaweb(source, fetch)
        return self.read_from_disk(source)

    def get_ssc_data(self) -> pd.DataFrame:
        return self._load(Instrument.SSC, lambda raw: raw.get_ssc_dataframe())

    def get_fgm_data(self) -> pd.DataFrame:
        return self._load(Instrument.FGM, lambda raw: raw.get_fgm_dataframe())

    def get_esa_data(self, particle: Literal["ion", "electron"]) -> pd.DataFrame:
        source = Instrument.ESA_ION if particle == "ion" else Instrument.ESA_ELECTRON
        return self._load(source, lambda raw: raw.get_esa_dataframe(particle))

    def get_efi_data(self) -> pd.DataFrame:
        return self._load(Instrument.EFI, lambda raw: raw.get_efi_dataframe())

    def get_sta_data(self) -> pd.DataFrame:
        return self._load(Instrument.STA, lambda raw: raw.get_sta_dataframe())

    def get_omn_data(self) -> pd.DataFrame:
        return self._load(Instrument.OMNI, lambda raw: raw.get_omn_dataframe())

    def get_mom_data(self) -> pd.DataFrame:
        """
        ``mom.parquet``: Time, Ion_pressure (eV/см³), Ion_density.
        """

        return self._load(Instrument.MOM, lambda raw: raw.get_mom_dataframe())
