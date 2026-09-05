from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

import numpy as np
import pandas as pd

from backend.src.config.schemas import AppConfig
from backend.src.io.cdaweb import CDAweb
from backend.src.io.time_borders import format_time_borders
from backend.src.log import progress_bar


def _vector3(data: Any, column: str) -> np.ndarray:
    return np.asarray(data[column].data).transpose()


@dataclass(frozen=True, slots=True)
class RawData:
    """
    Скачивание THEMIS/OMNI с CDAWeb кусками по ``reading.delta``.
    """

    config: AppConfig

    def _satellite_letters(self) -> tuple[str, str]:
        satellite = self.config.reading.satellite
        return satellite.lower(), satellite.upper()

    def _download(
        self,
        instrument: str,
        columns: list[str],
        desc: str,
        to_frame: Callable[[Any], dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Качает датасет по нарезанным границам и склеивает куски.

        Args:
            instrument: имя датасета CDAWeb.
            columns: переменные запроса.
            desc: подпись progress-бара.
            to_frame: превращает ответ CDAWeb в словарь колонок одного куска.
        """

        api = CDAweb.default(dataset_name=instrument)
        frames: list[pd.DataFrame] = []
        for border in progress_bar(format_time_borders(self.config.reading), desc=desc):
            data = api.get_dataset(columns, border["start"], border["end"])
            frames.append(pd.DataFrame(to_frame(data)))

        if not frames:
            return pd.DataFrame()

        result = pd.concat(frames, ignore_index=True)
        return result.dropna(subset=["Time"]).drop_duplicates(subset=["Time"])

    def get_efi_dataframe(self) -> pd.DataFrame:
        sat_lower, sat_upper = self._satellite_letters()
        time_column = f"th{sat_lower}_efs_dot0_epoch"
        efield_column = f"th{sat_lower}_efs_dot0_gsm"

        def to_frame(data: Any) -> dict[str, Any]:
            field = _vector3(data, efield_column)
            return {
                "Time": data[time_column].data,
                "GSM_Ex": field[0],
                "GSM_Ey": field[1],
                "GSM_Ez": field[2],
            }

        return self._download(
            f"TH{sat_upper}_L2_EFI",
            [efield_column],
            "[raw] EFI: скачивание пакетов",
            to_frame,
        )

    def get_fgm_dataframe(self) -> pd.DataFrame:
        sat_lower, sat_upper = self._satellite_letters()
        time_column = f"th{sat_lower}_fgs_epoch"
        fgs_column = f"th{sat_lower}_fgs_gsm"

        def to_frame(data: Any) -> dict[str, Any]:
            field = _vector3(data, fgs_column)
            return {
                "Time": data[time_column].data,
                "GSM_Bx": field[0],
                "GSM_By": field[1],
                "GSM_Bz": field[2],
            }

        return self._download(
            f"TH{sat_upper}_L2_FGM",
            [fgs_column],
            "[raw] FGM: скачивание пакетов",
            to_frame,
        )

    def get_esa_dataframe(self, particle: Literal["ion", "electron"]) -> pd.DataFrame:
        sat_lower, sat_upper = self._satellite_letters()
        if particle == "ion":
            time_column = f"th{sat_lower}_peir_epoch"
            vel_column = f"th{sat_lower}_peir_velocity_gsm"
            out_cols = ("GSM_Vix", "GSM_Viy", "GSM_Viz")
        else:
            time_column = f"th{sat_lower}_peer_epoch"
            vel_column = f"th{sat_lower}_peer_velocity_gsm"
            out_cols = ("GSM_Vex", "GSM_Vey", "GSM_Vez")

        def to_frame(data: Any) -> dict[str, Any]:
            velocity = _vector3(data, vel_column)
            return {
                "Time": data[time_column].data,
                out_cols[0]: velocity[0],
                out_cols[1]: velocity[1],
                out_cols[2]: velocity[2],
            }

        return self._download(
            f"TH{sat_upper}_L2_ESA",
            [vel_column],
            f"[raw] ESA({particle}): скачивание пакетов",
            to_frame,
        )

    def get_ssc_dataframe(self) -> pd.DataFrame:
        _, sat_upper = self._satellite_letters()
        coordinates = "XYZ_GSM"

        def to_frame(data: Any) -> dict[str, Any]:
            xyz = _vector3(data, coordinates)
            return {
                "Time": data["Epoch"].data,
                "Latitude": data["GSM_LAT"].data,
                "Longitude": data["GSM_LON"].data,
                "L": data["L_VALUE"].data,
                "GSM_X": xyz[0],
                "GSM_Y": xyz[1],
                "GSM_Z": xyz[2],
            }

        return self._download(
            f"TH{sat_upper}_OR_SSC",
            [coordinates, "GSM_LAT", "GSM_LON", "L_VALUE"],
            "[raw] SSC: скачивание пакетов",
            to_frame,
        )

    def get_sta_dataframe(self) -> pd.DataFrame:
        sat_lower, sat_upper = self._satellite_letters()
        time_column = f"th{sat_lower}_state_epoch"
        velocity_column = f"th{sat_lower}_vel_gsm"

        def to_frame(data: Any) -> dict[str, Any]:
            velocity = _vector3(data, velocity_column)
            return {
                "Time": data[time_column].data,
                "GSM_Vsx": velocity[0],
                "GSM_Vsy": velocity[1],
                "GSM_Vsz": velocity[2],
            }

        return self._download(
            f"TH{sat_upper}_L1_STATE",
            [velocity_column],
            "[raw] STA: скачивание пакетов",
            to_frame,
        )

    def get_omn_dataframe(self) -> pd.DataFrame:
        def to_frame(data: Any) -> dict[str, Any]:
            return {
                "Time": data["Epoch"].data,
                "FP": data["Pressure"].data,
                "Bz_GSM": data["BZ_GSM"].data,
            }

        return self._download(
            "OMNI_HRO_1MIN",
            ["Pressure", "BZ_GSM"],
            "[raw] OMNI: скачивание пакетов",
            to_frame,
        )

    def get_mom_dataframe(self) -> pd.DataFrame:
        """
        MOM с CDAWeb: Time, Ion_pressure (eV/см³), Ion_density.
        """

        sat_lower, sat_upper = self._satellite_letters()
        time_column = f"th{sat_lower}_peim_epoch"
        pressure_column = f"th{sat_lower}_peim_ptot"
        density_column = f"th{sat_lower}_peim_density"

        def to_frame(data: Any) -> dict[str, Any]:
            return {
                "Time": data[time_column].data,
                "Ion_pressure": data[pressure_column].data,
                "Ion_density": data[density_column].data,
            }

        return self._download(
            f"TH{sat_upper}_L2_MOM",
            [pressure_column, density_column],
            "[raw] MOM: скачивание пакетов",
            to_frame,
        )
