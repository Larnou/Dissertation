import matplotlib.pyplot as plt
import numpy as np
from matplotlib.patches import Circle
from matplotlib.ticker import AutoMinorLocator, MultipleLocator


def plot_h_parameter_range() -> None:
    t_values = np.linspace(-10, 10, 500)
    h_values = 1 - (2 * t_values / (t_values**2 + 1))

    fig, ax = plt.subplots(figsize=(20, 8))
    ax.plot(t_values, h_values, label="H(t)", color="darkred")

    ax.set_xlabel("t, t = E / ε", size=20)
    ax.set_ylabel("H(t)", size=20)
    ax.set_xlim(-5, 5)
    ax.set_ylim(-0.2, 2.2)

    ax.xaxis.set_major_locator(MultipleLocator(1))
    ax.xaxis.set_minor_locator(AutoMinorLocator(5))
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))

    ax.grid(alpha=0.9, which="major")
    ax.grid(alpha=0.3, which="minor", linestyle=":")
    ax.axhline(0, color="black", linewidth=0.5)
    ax.axhline(1, color="black", linestyle="--", linewidth=1.5)
    ax.axvline(0, color="black", linewidth=0.5)
    ax.add_patch(Circle((0, 1), 0.06, fill=False, edgecolor="black", linewidth=1))

    ax.tick_params(axis="y", which="major", color="black", length=8, width=2, labelsize=16)
    ax.tick_params(axis="y", which="minor", color="black", length=4, width=1, labelsize=16)
    ax.tick_params(axis="x", which="major", color="black", length=8, width=2, labelsize=16)
    ax.tick_params(axis="x", which="minor", color="black", length=4, width=1, labelsize=16)
    ax.set_aspect("equal", adjustable="box")
    ax.legend(fontsize=16)
    plt.show()