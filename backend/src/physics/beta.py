import numpy as np

# 1 eV/cm³ = 1.602×10⁻¹⁹ J / 10⁻⁶ m³ = 1.602×10⁻¹³ J/m³ = 1.602×10⁻¹³ Па
_EV_CM3_TO_PA = 1.602e-13
_MU0 = 4.0 * np.pi * 1e-7


class BetaModel:
    """
    Параметр плазмы β = P_kin / P_mag.

    P_kin — ионное полное давление из MOM (например ``tha_peim_ptot``), единицы eV/cm³.
    P_mag = |B|² / (2 μ₀), поле B из FGM; в проекте компоненты обычно в нТл.

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
        self.data = data
        self.pressure_column = pressure_column
        self.bx_key = bx_key
        self.by_key = by_key
        self.bz_key = bz_key
        self.field_nanotesla = field_nanotesla
        self.min_b_tesla = min_b_tesla

    @staticmethod
    def kinetic_pressure_pa(ptot_ev_cm3: np.ndarray) -> np.ndarray:
        """Давление ионов [Па] из столбца в eV/cm³."""
        return np.asarray(ptot_ev_cm3, dtype=np.float64) * _EV_CM3_TO_PA

    @staticmethod
    def magnetic_pressure_pa(b_tesla: np.ndarray) -> np.ndarray:
        """Магнитное давление [Па] по модулю B [Тл] (массив |B|)."""
        b = np.asarray(b_tesla, dtype=np.float64)
        return (b * b) / (2.0 * _MU0)

    def model(self) -> np.ndarray:
        """
        Возвращает массив β по строкам ``data`` (те же длины, что и входные колонки).
        Где |B| слишком мало, подставляется ``nan``, чтобы избежать деления на ноль.
        """

        ptot = np.asarray(self.data[self.pressure_column], dtype=np.float64)
        bx = np.asarray(self.data[self.bx_key], dtype=np.float64)
        by = np.asarray(self.data[self.by_key], dtype=np.float64)
        bz = np.asarray(self.data[self.bz_key], dtype=np.float64)

        b_mag = np.sqrt(bx * bx + by * by + bz * bz) * 1e-9
        p_kin = self.kinetic_pressure_pa(ptot)

        p_mag = self.magnetic_pressure_pa(b_mag)
        out = np.full_like(p_kin, np.nan, dtype=np.float64)
        mask = b_mag > self.min_b_tesla
        out[mask] = p_kin[mask] / p_mag[mask]

        return out
