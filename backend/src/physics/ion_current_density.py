import numpy as np
import pandas as pd

_ELEMENTARY_CHARGE_C = 1.602176634e-19
_CM3_TO_M3 = 1e6
_KM_S_TO_M_S = 1e3


class IonCurrentDensityModel:
    """
    Плотность ионного тока в field-aligned базисе: J_* = q · n_p · V_*.

    По умолчанию n_p в см⁻³, V_* в км/с, результат в А/м².
    """

    def __init__(
        self,
        data: pd.DataFrame,
        *,
        density_key: str = "density",
        velocity_keys: tuple[str, str, str] = ("V_a", "V_r", "V_f"),
        ion_charge_c: float = _ELEMENTARY_CHARGE_C,
        density_in_cm3: bool = True,
        velocity_in_km_s: bool = True,
    ):
        self.data = data
        self.density_key = density_key
        self.velocity_keys = velocity_keys
        self.ion_charge_c = ion_charge_c
        self.density_in_cm3 = density_in_cm3
        self.velocity_in_km_s = velocity_in_km_s

    def _density_si(self) -> np.ndarray:
        """
        Концентрация ионов в м⁻³.
        """

        density = np.asarray(self.data[self.density_key], dtype=np.float64)
        if self.density_in_cm3:
            density = density * _CM3_TO_M3
        return density

    def _velocity_si(self, key: str) -> np.ndarray:
        """
        Компонента скорости в м/с.
        """

        velocity = np.asarray(self.data[key], dtype=np.float64)
        if self.velocity_in_km_s:
            velocity = velocity * _KM_S_TO_M_S
        return velocity

    def model(self) -> pd.DataFrame:
        """
        DataFrame с колонками J_a, J_r, J_f [А/м²], индекс совпадает с self.data.
        """

        density = self._density_si()
        va_key, vr_key, vf_key = self.velocity_keys

        j_a = self.ion_charge_c * density * self._velocity_si(va_key)
        j_r = self.ion_charge_c * density * self._velocity_si(vr_key)
        j_f = self.ion_charge_c * density * self._velocity_si(vf_key)

        return pd.DataFrame(
            {
                "J_a": j_a,
                "J_r": j_r,
                "J_f": j_f,
            },
            index=self.data.index,
        )
