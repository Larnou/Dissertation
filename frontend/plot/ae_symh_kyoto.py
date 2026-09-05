from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from frontend.plot.ae_kyoto import draw_ae_daily
from frontend.plot.kyoto_daily import daily_image_name, save_daily_figure
from frontend.plot.symh_kyoto import draw_symh_daily


def plot_ae_symh_daily(
    ae_data: pd.DataFrame,
    symh_data: pd.DataFrame,
    day: str | datetime | pd.Timestamp,
    *,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    """
    Сутки AE и SYM-H на одном рисунке: AE сверху, SYM-H снизу, общая ось UT.

    Args:
        ae_data: таблица с Time и AE.
        symh_data: таблица с Time и SYMH.
        day: сутки UTC.
        output_path: куда сохранить PNG; ``None`` — не писать файл.
        show: показать окно matplotlib.
    """

    fig, (axis_ae, axis_symh) = plt.subplots(
        2,
        1,
        figsize=(12.4, 8.4),
        sharex=True,
        layout="constrained",
    )
    day_start = draw_ae_daily(axis_ae, ae_data, day, show_xlabel=False)
    draw_symh_daily(axis_symh, symh_data, day, show_xlabel=True)
    return save_daily_figure(fig, output_path, show, daily_image_name("ae_symh", day_start))
