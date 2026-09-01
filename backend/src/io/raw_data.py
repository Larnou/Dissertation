import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import numpy as np
import pandas as pd
from tqdm import tqdm

from backend.src.config.schemas import AppConfig
from backend.src.io.cdaweb import CDAweb
from backend.src.io.utils.format_time_borders import format_time_borders
from backend.src.log import progress_bar


@dataclass(frozen=True, slots=True)
class RawData:
    """
    Загрузка данных THEMIS/OMNI с CDAWeb и приведение к DataFrame.

    Это перенос логики из Colab-ноутбуков. `parameters: dict` заменён на `config: AppConfig`
    (доступ к параметрам через точку).
    """

    config: AppConfig

    def get_borders(self) -> list[dict[str, str]]:
        return format_time_borders(self.config.reading)


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

        columns = [efield_column]
        api = CDAweb.default(dataset_name=instrument)

        dataframes: list[pd.DataFrame] = []
        tqdm_borders = progress_bar(time_borders, desc="[raw] EFI: скачивание пакетов")
        for border in tqdm_borders:
            data = api.get_dataset(columns, border["start"], border["end"])
            ef = np.array(data[efield_column].data).transpose()

            raw_data = {
                'Time': data[time_column].data,
                'GSM_Ex': ef[0],
                'GSM_Ey': ef[1],
                'GSM_Ez': ef[2],
            }

            df = pd.DataFrame(raw_data)
            dataframes.append(df)

        result = pd.concat(dataframes, ignore_index=True)
        result = result.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])
        return result


    def get_fgm_dataframe(self) -> pd.DataFrame:
        time_borders = self.get_borders()
        sat_lower, sat_upper = self.get_satellite_letters()

        time_column = f"th{sat_lower}_fgs_epoch"
        fgs_column = f"th{sat_lower}_fgs_gsm"
        instrument = f"TH{sat_upper}_L2_FGM"

        columns = [fgs_column]
        api = CDAweb.default(dataset_name=instrument)

        dataframes: list[pd.DataFrame] = []
        tqdm_borders = progress_bar(time_borders, desc="[raw] FGM: скачивание пакетов")
        for border in tqdm_borders:
            data = api.get_dataset(columns, border["start"], border["end"])
            bf = np.asarray(data[fgs_column].data).transpose()

            raw_data = {
                "Time": data[time_column].data,
                "GSM_Bx": bf[0],
                "GSM_By": bf[1],
                "GSM_Bz": bf[2],
            }

            df = pd.DataFrame(raw_data)
            dataframes.append(df)

        result = pd.concat(dataframes, ignore_index=True)
        result = result.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])
        return result

    def get_esa_dataframe(self, particle: Literal["ion", "electron"]) -> pd.DataFrame:
        time_borders = self.get_borders()
        sat_lower, sat_upper = self.get_satellite_letters()

        instrument = f"TH{sat_upper}_L2_ESA"

        if particle == "ion":
            time_column = f"th{sat_lower}_peir_epoch"
            vel_column = f"th{sat_lower}_peir_velocity_gsm"
            out_cols = ("GSM_Vix", "GSM_Viy", "GSM_Viz")
        else:
            time_column = f"th{sat_lower}_peer_epoch"
            vel_column = f"th{sat_lower}_peer_velocity_gsm"
            out_cols = ("GSM_Vex", "GSM_Vey", "GSM_Vez")

        api = CDAweb.default(dataset_name=instrument)

        dataframes: list[pd.DataFrame] = []
        tqd = progress_bar(time_borders, desc=f"[raw] ESA({particle}): скачивание пакетов")
        for border in tqd:
            data = api.get_dataset([vel_column], border["start"], border["end"])
            velocity = np.asarray(data[vel_column].data).transpose()

            raw_data = {
                "Time": data[time_column].data,
                out_cols[0]: velocity[0],
                out_cols[1]: velocity[1],
                out_cols[2]: velocity[2],
            }

            df = pd.DataFrame(raw_data)
            dataframes.append(df)

        result = pd.concat(dataframes, ignore_index=True)
        result = result.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])
        return result

    def get_ssc_dataframe(self) -> pd.DataFrame:
        time_borders = self.get_borders()
        sat_lower, sat_upper = self.get_satellite_letters()

        instrument = f"TH{sat_upper}_OR_SSC"
        time_column = f"th{sat_lower}_peif_epoch"
        coordinates = "XYZ_GSM"

        columns = [coordinates, "GSM_LAT", "GSM_LON", "L_VALUE"]
        api = CDAweb.default(dataset_name=instrument)

        dataframes: list[pd.DataFrame] = []
        tqd = progress_bar(time_borders, desc="[raw] SSC: скачивание пакетов")
        for border in tqd:
            data = api.get_dataset(columns, border["start"], border["end"])
            xyz = np.asarray(data[coordinates].data).transpose()

            raw_data = {
                "Time": data["Epoch"].data,
                "Latitude": data["GSM_LAT"].data,
                "Longitude": data["GSM_LON"].data,
                "L": data["L_VALUE"].data,
                "GSM_X": xyz[0],
                "GSM_Y": xyz[1],
                "GSM_Z": xyz[2],
            }

            df = pd.DataFrame(raw_data)
            dataframes.append(df)

        result = pd.concat(dataframes, ignore_index=True)
        result = result.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])
        return result


    def get_sta_dataframe(self) -> pd.DataFrame:
        time_borders = self.get_borders()
        sat_lower, sat_upper = self.get_satellite_letters()

        time_column = f"th{sat_lower}_state_epoch"
        satellite_velocity = f"th{sat_lower}_vel_gsm"
        instrument = f"TH{sat_upper}_L1_STATE"

        columns = [satellite_velocity]
        api = CDAweb.default(dataset_name=instrument)

        dataframes: list[pd.DataFrame] = []
        tqd = progress_bar(time_borders, desc="[raw] STA: скачивание пакетов")
        for border in tqd:
            data = api.get_dataset(columns, border["start"], border["end"])
            vel = np.asarray(data[satellite_velocity].data).transpose()

            raw_data = {
                "Time": data[time_column].data,
                "GSM_Vsx": vel[0],
                "GSM_Vsy": vel[1],
                "GSM_Vsz": vel[2],
            }

            df = pd.DataFrame(raw_data)
            dataframes.append(df)

        result = pd.concat(dataframes, ignore_index=True)
        result = result.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])
        return result

    def get_omn_dataframe(self) -> pd.DataFrame:
        time_borders = self.get_borders()

        instrument = "OMNI_HRO_1MIN"
        time_col = "Epoch"

        columns = ["Pressure", "BZ_GSM"]
        api = CDAweb.default(dataset_name=instrument)

        dataframes: list[pd.DataFrame] = []
        tqd = progress_bar(time_borders, desc="[raw] OMNI: скачивание пакетов")
        for border in tqd:
            data = api.get_dataset(columns, border["start"], border["end"])

            raw_data = {
                "Time": data[time_col].data,
                "FP": data["Pressure"].data,
                "Bz_GSM": data["BZ_GSM"].data,
            }

            df = pd.DataFrame(raw_data)
            dataframes.append(df)

        result = pd.concat(dataframes, ignore_index=True)
        result = result.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])
        return result

    # TODO: переписать эту часть, пока что как заглушка
    def get_mom_dataframe(self) -> pd.DataFrame:
        # Получение границ скачиваемых промежутков
        time_borders = self.format_time_borders(self.parameters)

        # reformat columns
        time = 'tha_peim_epoch'.format(sat = self.parameters['reading']['satellite'].lower())
        pressure = 'th{sat}_peim_ptot'.format(sat = self.parameters['reading']['satellite'].lower())
        density = f'th{self.parameters['reading']['satellite'].lower()}_peim_density'

        instrument = 'TH{sat}_L2_MOM'.format(sat = self.parameters['reading']['satellite'])
        print('Обработка данных инструмента: ', instrument)

        # Колонки по которых будут собираться данные
        columns = [pressure, density]

        # Получение данных с сервиса CDAweb
        # При копировании в PyCharm - удалить [1]
        dataframes = []
        tqd = tqdm(time_borders, desc = 'Обработка пакетов датасетов', file = sys.stdout)

        for border in tqd:
            # Обработка данных для динамического отображения скачиваемого промежутка
            st_description = datetime.strptime(border['start'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
            et_description = datetime.strptime(border['end'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d')
            tqd.set_description("Скачивание: с {st} по {en}".format(st = st_description, en = et_description))

            # Скачивание данных через класс CDAweb
            data = CDAweb(instrument).get_dataset(columns, border['start'], border['end'])[1]

            # print(len(data[time].data), len(data[pressure].data))
            raw_data = {
                'Time': data[time].data,
                'Ion_pressure': data[pressure].data,
                'Ion_density': data[density].data,
            }

            # Создание датафрейма и перевод времени в нужный формат
            dataframe = pd.DataFrame(data = raw_data)
            dataframe['Time'] = pd.to_datetime(dataframe["Time"].dt.strftime('%Y-%m-%d %H:%M:%S'))
            dataframes.append(dataframe)

        print(' ')
        dataframe_concated = pd.concat(dataframes).reset_index(drop=True)
        dataframe_concated.drop_duplicates(subset=['Time'], inplace=True)
        return dataframe_concated