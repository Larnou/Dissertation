import matplotlib.pyplot as plt
import pandas as pd

from backend.src.config import config
from backend.src.config import get_logger
from backend.src.io import DataDownloading

logger = get_logger()

load_from_cdaweb = False
loader = DataDownloading(config, load_from_cdaweb=load_from_cdaweb)

# Загрузка данных
logger.info(f"Загрузка данных с {'CDAweb' if load_from_cdaweb else 'диска'}:")
shue_data = loader.get_shue_data()

plot_df = shue_data.copy()

if plot_df.empty:
    raise ValueError("shue_data пуст — нечего рисовать (проверьте OMNI/SSC, мэчинг по времени и фильтр L>=4 & L<=r)")

plot_df["Time"] = pd.to_datetime(plot_df["Time"], utc=True, errors="coerce").dt.tz_localize(None)
plot_df = plot_df.dropna(subset=["Time", "L", "r"]).sort_values("Time")

fig, ax = plt.subplots(1, 1, figsize=(18, 6), layout="constrained", sharex=True)
ax.plot(plot_df["Time"], plot_df["L"], label="L", linewidth=1.2)
ax.plot(plot_df["Time"], plot_df["r"], label="r (Shue)", linewidth=1.2)

ax.set_xlabel("Time")
ax.set_ylabel("RE")
ax.grid(True, alpha=0.25)
ax.legend()
plt.show()