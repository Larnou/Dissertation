from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.kyoto_ae import read_kyoto_ae_directory
from backend.src.io.parquet import read_data_from_parquet, read_kyoto_index, save_data_to_parquet, save_kyoto_index
from backend.src.io.paths import DerivedDataset, Instrument, KyotoIndex, paths
from backend.src.io.raw_data import RawData
from backend.src.processing.interpolation.interpolate_omn_dataset import interpolate_omn_dataset
from backend.src.processing.services.build_beta_dataset import build_beta_dataset
from backend.src.processing.services.build_shue_dataset import build_shue_dataset


@dataclass(frozen=True, slots=True)
class KyotoLoading:
    """
    Загрузка минутных индексов AE из Kyoto WDC.

    ``load_from_request=True`` — парсинг ``*.for.request`` и сохранение в parquet.
    ``load_from_request=False`` — чтение готового ``ae.parquet`` из каталога Kyoto.
    """

    config: AppConfig
    load_from_request: bool

    def read_from_disk(self, index: KyotoIndex = KyotoIndex.AE) -> pd.DataFrame:
        return read_kyoto_index(self.config, index)

    def parse_from_request(self, index: KyotoIndex = KyotoIndex.AE) -> pd.DataFrame:
        source_dir = paths(self.config).kyoto.source_dir(index)
        dataframe = read_kyoto_ae_directory(source_dir)
        save_kyoto_index(self.config, dataframe, index)
        return dataframe

    def get_ae_data(self, index: KyotoIndex = KyotoIndex.AE) -> pd.DataFrame:
        if self.load_from_request:
            return self.parse_from_request(index)
        return self.read_from_disk(index)


@dataclass(frozen=True, slots=True)
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

    def read_from_disk(self, source: Instrument | DerivedDataset) -> pd.DataFrame:
        """
        Читает parquet инструмента из каталога события.
        """

        return read_data_from_parquet(self.config, source)

    def fetch_from_cdaweb(self, source: Instrument, fetch: Callable[[RawData], pd.DataFrame]) -> pd.DataFrame:
        """
        Скачивает данные через ``RawData``, сохраняет в parquet и возвращает DataFrame.
        """

        raw_data = RawData(self.config)
        dataframe = fetch(raw_data)
        save_data_to_parquet(self.config, dataframe, source)
        return dataframe

    def _load_by_source(self, source: Instrument, fetch: Callable[[RawData], pd.DataFrame]) -> pd.DataFrame:
        if self.load_from_cdaweb:
            return self.fetch_from_cdaweb(source, fetch)
        return self.read_from_disk(source)

    def get_ssc_data(self) -> pd.DataFrame:
        return self._load_by_source(Instrument.SSC, lambda raw: raw.get_ssc_dataframe())

    def get_fgm_data(self) -> pd.DataFrame:
        return self._load_by_source(Instrument.FGM, lambda raw: raw.get_fgm_dataframe())

    def get_esa_data(self, particle: Literal["ion", "electron"]) -> pd.DataFrame:
        source = Instrument.ESA_ION if particle == "ion" else Instrument.ESA_ELECTRON
        return self._load_by_source(source, lambda raw: raw.get_esa_dataframe(particle))

    def get_efi_data(self) -> pd.DataFrame:
        return self._load_by_source(Instrument.EFI, lambda raw: raw.get_efi_dataframe())

    def get_sta_data(self) -> pd.DataFrame:
        return self._load_by_source(Instrument.STA, lambda raw: raw.get_sta_dataframe())

    def get_omn_data(self) -> pd.DataFrame:
        raw_omn_dataset = self._load_by_source(Instrument.OMNI, lambda raw: raw.get_omn_dataframe())
        return interpolate_omn_dataset(omn_data=raw_omn_dataset)

    def get_mom_data(self) -> pd.DataFrame:
        """
        ``mom.parquet`` с CDAWeb: Time, Ion_pressure (eV/см³).
        """

        return self._load_by_source(Instrument.MOM, lambda raw: raw.get_mom_dataframe())

    def get_beta_data(self) -> pd.DataFrame:
        """
        FGM + MOM через :func:`build_beta_dataset` — колонки Time, beta, Ion_pressure, GSM_B*.
        """

        fgm_data = self.get_fgm_data()
        mom_data = self.get_mom_data()
        return build_beta_dataset(fgm_data=fgm_data, mom_data=mom_data)

    def get_shue_data(self) -> pd.DataFrame:
        """
        Датасет для модели Shue, построенный из SSC и OMNI.

        Возвращаемые колонки: Time, L, MLT, r.
        """

        ssc_data = self.get_ssc_data()
        omn_data = self.get_omn_data()
        return build_shue_dataset(ssc_data=ssc_data, omn_data=omn_data)
