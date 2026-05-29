from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd


def _to_utc_naive(series: pd.Series) -> pd.Series:
    return (
        pd.to_datetime(series, utc=True, errors="coerce")
        .dt.tz_convert("UTC")
        .dt.tz_localize(None)
    )


def plot_component_dynamics(
    data: pd.DataFrame,
    component: str,
    start_time: str,
    end_time: str,
    output_path: str | Path | None = None,
    show: bool = True,
) -> Path:
    normalized_component = component.lower()
    if normalized_component not in {"f", "a", "r"}:
        raise ValueError("Component must be one of: 'f', 'a', 'r'.")

    e_column = f"E_{normalized_component}_meas"
    v_column = f"V_{normalized_component}_meas"
    h_column = f"H_{normalized_component}"
    x_column = f"X_{normalized_component}_conv"
    required_columns = ("Time", e_column, v_column, x_column, h_column)
    missing = [column for column in required_columns if column not in data.columns]
    if missing:
        raise KeyError(f"Required columns are missing: {missing}")

    start_ts = pd.to_datetime(start_time, utc=True, errors="raise").tz_convert("UTC").tz_localize(None)
    end_ts = pd.to_datetime(end_time, utc=True, errors="raise").tz_convert("UTC").tz_localize(None)
    if end_ts <= start_ts:
        raise ValueError("end_time must be later than start_time.")

    work = data.loc[:, required_columns].copy()
    work["Time"] = _to_utc_naive(work["Time"])
    work = work.dropna(subset=["Time"]).sort_values("Time")
    work = work[(work["Time"] >= start_ts) & (work["Time"] <= end_ts)]
    work = work.dropna(subset=[e_column, v_column, x_column, h_column])
    if work.empty:
        raise ValueError("No data found in selected time range for the required columns.")

    fig, axes = plt.subplots(3, 1, figsize=(14, 9), sharex=True, layout="constrained")

    axes[0].plot(work["Time"], work[v_column], color="#ff7f0e", linewidth=1.2)
    axes[0].set_ylabel(v_column)
    axes[0].grid(alpha=0.35)

    axes[1].plot(work["Time"], work[x_column], color="#2ca02c", linewidth=1.2, label=x_column)
    axes[1].plot(work["Time"], work[e_column], color="#1f77b4", linewidth=1.1, alpha=0.9, label=e_column)
    axes[1].set_ylabel(f"{x_column} / {e_column}")
    axes[1].grid(alpha=0.35)
    axes[1].legend(loc="upper right", fontsize=9)

    h_values = work[h_column].to_numpy(dtype=float)
    h_mean = float(h_values.mean())
    h_sigma = float(h_values.std())
    h_minus_sigma = h_mean - h_sigma
    h_plus_sigma = h_mean + h_sigma

    axes[2].plot(work["Time"], work[h_column], color="#d62728", linewidth=1.2, label=h_column)
    axes[2].axhline(h_mean, color="#111111", linestyle="--", linewidth=1.3, label=f"mean={h_mean:.2f}")
    axes[2].axhline(h_minus_sigma, color="#7f7f7f", linestyle=":", linewidth=1.2, label=f"mean-1σ={h_minus_sigma:.2f}")
    axes[2].axhline(h_plus_sigma, color="#7f7f7f", linestyle=":", linewidth=1.2, label=f"mean+1σ={h_plus_sigma:.2f}")
    axes[2].fill_between(
        work["Time"],
        h_minus_sigma,
        h_plus_sigma,
        color="#7f7f7f",
        alpha=0.3,
        label="mean ± 1σ",
    )
    axes[2].set_ylabel(h_column)
    axes[2].set_xlabel("Time (UTC)")
    axes[2].grid(alpha=0.35)
    axes[2].legend(loc="upper right", fontsize=9)

    axes[0].set_title(
        f"Dynamic parameters for component '{normalized_component}'"
        f"\n{start_ts} — {end_ts}",
        fontsize=13,
    )
    axes[2].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d %H:%M"))
    fig.autofmt_xdate(rotation=25)

    resolved_output = Path(output_path) if output_path else Path(
        f"dynamic_{normalized_component}_{start_ts:%Y%m%d_%H%M%S}_{end_ts:%Y%m%d_%H%M%S}.png"
    )
    if output_path is not None:
        resolved_output.parent.mkdir(parents=True, exist_ok=True)
        fig.savefig(resolved_output, dpi=300)

    if show:
        plt.show()
    else:
        plt.close(fig)

    return resolved_output
