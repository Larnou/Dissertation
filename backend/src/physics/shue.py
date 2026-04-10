import numpy as np


class ShueModel:

    def __init__(self, data):
        self.data = data


    @staticmethod
    def get_r0(bz, dp):
        condition_list = [
            {'condition': bz < 0, 'value': 11.4 + 0.14 * bz},
            {'condition': bz >= 0, 'value': 11.4 + 0.013 * bz}
        ]

        conditions = [item['condition'] for item in condition_list]
        values = [item['value'] for item in condition_list]
        output = np.select(conditions, values, default=0)

        return output * (dp ** (-1 / 6.6))


    @staticmethod
    def get_alpha(bz, dp):
        return (0.58 - 0.01 * bz) * (1 + 0.01 * dp)


    @staticmethod
    def get_r(r0, cos_theta, alpha):
        return r0 * ((2 / (1 + cos_theta)) ** alpha)


    @staticmethod
    def get_cos_theta(x_coord, y_coord, z_coord):
        return x_coord / np.sqrt(x_coord ** 2 + y_coord ** 2 + z_coord ** 2)


    def model(self):
        x = self.data['GSM_X']
        y = self.data['GSM_Y']
        z = self.data['GSM_Z']
        bz = self.data['Bz']
        dp = self.data['FP']

        cos_theta = self.get_cos_theta(x, y, z)
        alpha = self.get_alpha(bz, dp)
        r0 = self.get_r0(bz, dp)

        r = self.get_r(r0, cos_theta, alpha)

        # Сохранение полученного результата
        # data = self.data.copy(deep=True)
        # data['r'] = r

        return r