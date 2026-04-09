import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

import numpy as np
import pandas as pd
from cdasws import CdasWs
from tqdm.notebook import tqdm

from backend.src.config.schemas import AppConfig
from backend.src.config.schemas import TIME_FORMAT, CDAWEB_TIME_FORMAT
from backend.src.io.cdaweb import CDAweb
from backend.src.io.utils.format_time_borders import format_time_borders

# _CONFIG_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
# _CDAWEB_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

DeltaUnit = Literal["M", "D", "H"]








@dataclass(frozen=True, slots=True)
class RawData:
    """
    Загрузка данных THEMIS/OMNI с CDAWeb и приведение к DataFrame.

    Это перенос логики из Colab-ноутбуков. `parameters: dict` заменён на `config: AppConfig`
    (доступ к параметрам через точку).
    """

    config: AppConfig

    def get_borders(self) -> list[dict[str, str]]:
        return format_time_borders(self.config)


    def get_satellite_letters(self):
        sat_lower = self.config.reading.satellite.lower()
        sat_upper = self.config.reading.satellite.upper()
        return sat_lower, sat_upper


    def get_efi_dataframe(self) -> pd.DataFrame:
        time_borders = self.get_borders()
        sat_lower, sat_upper = self.get_satellite_letters()

        time_column = f"th{sat_lower}_efs_dot0_epoch"
        efield_column = f"th{sat_lower}_efs_dot0_gsm"
        instrument = f"TH{sat_upper}_L2_EFI"

        api = CDAweb.default(dataset_name=instrument)

        dataframes: list[pd.DataFrame] = []
        tqdm_borders = tqdm(time_borders, desc="EFI: скачивание пакетов", file=sys.stdout)
        for border in tqdm_borders:
            data = api.get_dataset([efield_column], border["start"], border["end"])
            ef = np.asarray(data[efield_column].data).transpose()

            raw_data = {
                "Time": data[time_column].data,
                "GSM_Ex": ef[0],
                "GSM_Ey": ef[1],
                "GSM_Ez": ef[2],
            }

            df = pd.DataFrame(raw_data)
            df["Time"] = pd.to_datetime(df["Time"].dt.strftime(TIME_FORMAT))
            dataframes.append(df)

        result = pd.concat(dataframes).reset_index(drop=True)
        result = pd.DataFrame(result)
        result.drop_duplicates(subset=["Time"], inplace=True)
        return result


    def get_fgm_dataframe(self) -> pd.DataFrame:
        borders = self._borders()

        sat_lower = self.config.reading.satellite.lower()
        sat_upper = self.config.reading.satellite

        time_col = f"th{sat_lower}_fgs_epoch"
        fgs_col = f"th{sat_lower}_fgs_gsm"
        instrument = f"TH{sat_upper}_L2_FGM"

        client = CdasWs()
        api = CDAweb(dataset_name=instrument, client=client)
        dataframes: list[pd.DataFrame] = []
        tqd = tqdm(borders, desc="FGM: скачивание пакетов", file=sys.stdout)
        for border in tqd:
            data = api.get_dataset([fgs_col], border["start"], border["end"])
            bf = np.asarray(data[fgs_col].data).transpose()
            raw = {
                "Time": data[time_col].data,
                "GSM_Bx": bf[0],
                "GSM_By": bf[1],
                "GSM_Bz": bf[2],
            }
            df = pd.DataFrame(raw)
            df["Time"] = pd.to_datetime(df["Time"].dt.strftime(_CONFIG_TIME_FORMAT))
            dataframes.append(df)

        result = pd.concat(dataframes).reset_index(drop=True)
        result.drop_duplicates(subset=["Time"], inplace=True)
        return result

    def get_esa_dataframe(self, particle: Literal["ion", "electron"]) -> pd.DataFrame:
        borders = self._borders()

        sat_lower = self.config.reading.satellite.lower()
        sat_upper = self.config.reading.satellite
        instrument = f"TH{sat_upper}_L2_ESA"

        if particle == "ion":
            time_col = f"th{sat_lower}_peir_epoch"
            vel_col = f"th{sat_lower}_peir_velocity_gsm"
            out_cols = ("GSM_Vix", "GSM_Viy", "GSM_Viz")
        else:
            time_col = f"th{sat_lower}_peer_epoch"
            vel_col = f"th{sat_lower}_peer_velocity_gsm"
            out_cols = ("GSM_Vex", "GSM_Vey", "GSM_Vez")

        client = CdasWs()
        api = CDAweb(dataset_name=instrument, client=client)
        dataframes: list[pd.DataFrame] = []
        tqd = tqdm(borders, desc=f"ESA({particle}): скачивание пакетов", file=sys.stdout)
        for border in tqd:
            data = api.get_dataset([vel_col], border["start"], border["end"])
            v = np.asarray(data[vel_col].data).transpose()
            raw = {
                "Time": data[time_col].data,
                out_cols[0]: v[0],
                out_cols[1]: v[1],
                out_cols[2]: v[2],
            }
            df = pd.DataFrame(raw)
            df["Time"] = pd.to_datetime(df["Time"].dt.strftime(_CONFIG_TIME_FORMAT))
            dataframes.append(df)

        result = pd.concat(dataframes).reset_index(drop=True)
        result.drop_duplicates(subset=["Time"], inplace=True)
        return result

    def get_ssc_dataframe(self) -> pd.DataFrame:
        borders = self._borders()

        sat_lower = self.config.reading.satellite.lower()
        sat_upper = self.config.reading.satellite

        instrument = f"TH{sat_upper}_OR_SSC"
        time_col = f"th{sat_lower}_peif_epoch"
        xyz_col = "XYZ_GSM"
        columns = [xyz_col, "GSM_LAT", "GSM_LON", "L_VALUE"]

        client = CdasWs()
        api = CDAweb(dataset_name=instrument, client=client)
        dataframes: list[pd.DataFrame] = []
        tqd = tqdm(borders, desc="SSC: скачивание пакетов", file=sys.stdout)
        for border in tqd:
            data = api.get_dataset(columns, border["start"], border["end"])
            xyz = np.asarray(data[xyz_col].data).transpose()
            raw = {
                "Time": data["Epoch"].data,
                "Latitude": data["GSM_LAT"].data,
                "Longitude": data["GSM_LON"].data,
                "L": data["L_VALUE"].data,
                "GSM_X": xyz[0],
                "GSM_Y": xyz[1],
                "GSM_Z": xyz[2],
            }
            df = pd.DataFrame(raw)
            df["Time"] = pd.to_datetime(df["Time"].dt.strftime(_CONFIG_TIME_FORMAT))
            dataframes.append(df)

        result = pd.concat(dataframes).reset_index(drop=True)
        result.drop_duplicates(subset=["Time"], inplace=True)
        return result

    def get_sta_dataframe(self) -> pd.DataFrame:
        borders = self._borders()

        sat_lower = self.config.reading.satellite.lower()
        sat_upper = self.config.reading.satellite

        time_col = f"th{sat_lower}_state_epoch"
        vel_col = f"th{sat_lower}_vel_gsm"
        instrument = f"TH{sat_upper}_L1_STATE"

        client = CdasWs()
        api = CDAweb(dataset_name=instrument, client=client)
        dataframes: list[pd.DataFrame] = []
        tqd = tqdm(borders, desc="STA: скачивание пакетов", file=sys.stdout)
        for border in tqd:
            data = api.get_dataset([vel_col], border["start"], border["end"])
            vel = np.asarray(data[vel_col].data).transpose()
            raw = {
                "Time": data[time_col].data,
                "GSM_Vsx": vel[0],
                "GSM_Vsy": vel[1],
                "GSM_Vsz": vel[2],
            }
            df = pd.DataFrame(raw)
            df["Time"] = pd.to_datetime(df["Time"].dt.strftime(_CONFIG_TIME_FORMAT))
            dataframes.append(df)

        result = pd.concat(dataframes).reset_index(drop=True)
        result.drop_duplicates(subset=["Time"], inplace=True)
        return result

    def get_omn_dataframe(self) -> pd.DataFrame:
        borders = self._borders()

        instrument = "OMNI_HRO_1MIN"
        time_col = "Epoch"
        columns = ["Pressure", "BZ_GSM"]

        client = CdasWs()
        api = CDAweb(dataset_name=instrument, client=client)
        dataframes: list[pd.DataFrame] = []
        tqd = tqdm(borders, desc="OMNI: скачивание пакетов", file=sys.stdout)
        for border in tqd:
            data = api.get_dataset(columns, border["start"], border["end"])
            raw = {
                "Time": data[time_col].data,
                "FP": data["Pressure"].data,
                "Bz_GSM": data["BZ_GSM"].data,
            }
            df = pd.DataFrame(raw)
            df["Time"] = pd.to_datetime(df["Time"].dt.strftime(_CONFIG_TIME_FORMAT))
            dataframes.append(df)

        result = pd.concat(dataframes).reset_index(drop=True)
        result.drop_duplicates(subset=["Time"], inplace=True)
        return result

