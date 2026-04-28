import matplotlib.pyplot as plt
import pandas as pd


def read_shue_dataset(path: str) -> pd.DataFrame:
    dataframe = pd.read_parquet(path)
    dataframe["Time"] = pd.to_datetime(dataframe["Time"], utc=True, errors="coerce").dt.tz_localize(None)
    return dataframe.dropna(subset=["Time", "L", "r"]).sort_values("Time").reset_index(drop=True)


def plot_shue_model(dataframe: pd.DataFrame) -> None:
    if dataframe.empty:
        raise ValueError("Dataset is empty, nothing to plot.")

    fig, ax = plt.subplots(1, 1, figsize=(18, 6), layout="constrained", sharex=True)
    ax.plot(dataframe["Time"], dataframe["L"], label="L", linewidth=1.2)
    ax.plot(dataframe["Time"], dataframe["r"], label="r (Shue)", linewidth=1.2)
    ax.set_xlabel("Time")
    ax.set_ylabel("RE")
    ax.grid(True, alpha=0.25)
    ax.legend()
    plt.show()