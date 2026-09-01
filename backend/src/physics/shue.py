import numpy as np


class ShueModel:
    """
    Магнетопауза Shue et al. (1998): радиус r(θ) в Earth radii (R_E).

    Входной DataFrame должен содержать GSM_X/Y/Z [km], Bz [nT], FP (динамическое давление) [nPa].
    Угол θ отсчитывается от оси Sun–Earth (+X GSM); cos θ = x / |r|.
    """

    def __init__(self, data):
        """
        :param data: таблица с колонками GSM_X, GSM_Y, GSM_Z, Bz, FP.
        """

        self.data = data

    @staticmethod
    def get_r0(bz, dp):
        """
        Standoff distance r0 [R_E] как функция Bz [nT] и динамического давления DP [nPa].
        """

        condition_list = [
            {"condition": bz < 0, "value": 11.4 + 0.14 * bz},
            {"condition": bz >= 0, "value": 11.4 + 0.013 * bz},
        ]

        conditions = [item["condition"] for item in condition_list]
        values = [item["value"] for item in condition_list]
        output = np.select(conditions, values, default=0)

        return output * (dp ** (-1 / 6.6))

    @staticmethod
    def get_alpha(bz, dp):
        """
        Показатель степени α модели Shue [безразмерный].
        """

        return (0.58 - 0.01 * bz) * (1 + 0.01 * dp)

    @staticmethod
    def get_r(r0, cos_theta, alpha):
        """
        Радиус магнетопаузы r(θ) = r0 · (2 / (1 + cos θ))^α [R_E].
        """

        return r0 * ((2 / (1 + cos_theta)) ** alpha)

    @staticmethod
    def get_cos_theta(x_coord, y_coord, z_coord):
        """
        cos θ = x / |r|; θ — угол от оси +X GSM.
        """

        radius = np.sqrt(x_coord ** 2 + y_coord ** 2 + z_coord ** 2)
        with np.errstate(divide="ignore", invalid="ignore"):
            cos_theta = x_coord / radius
        return cos_theta

    def model(self):
        """
        Массив радиуса магнетопаузы r [R_E] по строкам self.data.
        """

        x = self.data["GSM_X"]
        y = self.data["GSM_Y"]
        z = self.data["GSM_Z"]
        bz = self.data["Bz"]
        dp = self.data["FP"]

        cos_theta = self.get_cos_theta(x, y, z)
        alpha = self.get_alpha(bz, dp)
        r0 = self.get_r0(bz, dp)

        return self.get_r(r0, cos_theta, alpha)
