import numpy as np

# 1 eV/cm³ = 1.602×10⁻¹⁹ J / 10⁻⁶ m³ = 1.602×10⁻¹³ Pa
_EV_CM3_TO_PA = 1.602e-13
_MU0 = 4.0 * np.pi * 1e-7
_NANO_TESLA_TO_TESLA = 1e-9


class BetaModel:
    """
    Плазменный параметр β = P_kin / P_mag для строк входного DataFrame.

    P_kin берётся из MOM (столбец давления в eV/cm³), P_mag = |B|² / (2μ₀), |B| из FGM.
    """

    def __init__(
        self,
        data,
        *,
        pressure_column: str = "Ion_pressure",
        bx_key: str = "GSM_Bx",
        by_key: str = "GSM_By",
        bz_key: str = "GSM_Bz",
        field_nanotesla: bool = True,
        min_b_tesla: float = 1e-15,
    ):
        """
        :param data: таблица с колонками давления и компонент B.
        :param pressure_column: столбец полного ионного давления, eV/cm³.
        :param bx_key, by_key, bz_key: компоненты магнитного поля; по умолчанию нТл.
        :param field_nanotesla: если True, |B| переводится из нТл в Тл перед расчётом P_mag.
        :param min_b_tesla: порог |B|; ниже него β = nan.
        """

        self.data = data
        self.pressure_column = pressure_column
        self.bx_key = bx_key
        self.by_key = by_key
        self.bz_key = bz_key
        self.field_nanotesla = field_nanotesla
        self.min_b_tesla = min_b_tesla

    @staticmethod
    def kinetic_pressure_pa(ptot_ev_cm3: np.ndarray) -> np.ndarray:
        """
        Кинетическое давление [Па] из столбца в eV/cm³.
        """

        return np.asarray(ptot_ev_cm3, dtype=np.float64) * _EV_CM3_TO_PA

    @staticmethod
    def magnetic_pressure_pa(b_tesla: np.ndarray) -> np.ndarray:
        """
        Магнитное давление P_mag = |B|² / (2μ₀) [Па]; |B| задаётся в тesla.
        """

        b = np.asarray(b_tesla, dtype=np.float64)
        return (b * b) / (2.0 * _MU0)

    def model(self) -> np.ndarray:
        """
        Массив β по строкам data; при |B| < min_b_tesla возвращается nan.
        """

        ptot = np.asarray(self.data[self.pressure_column], dtype=np.float64)
        bx = np.asarray(self.data[self.bx_key], dtype=np.float64)
        by = np.asarray(self.data[self.by_key], dtype=np.float64)
        bz = np.asarray(self.data[self.bz_key], dtype=np.float64)

        b_mag = np.sqrt(bx * bx + by * by + bz * bz)
        if self.field_nanotesla:
            b_mag = b_mag * _NANO_TESLA_TO_TESLA

        p_kin = self.kinetic_pressure_pa(ptot)
        p_mag = self.magnetic_pressure_pa(b_mag)

        out = np.full_like(p_kin, np.nan, dtype=np.float64)
        mask = b_mag > self.min_b_tesla
        out[mask] = p_kin[mask] / p_mag[mask]

        return out
