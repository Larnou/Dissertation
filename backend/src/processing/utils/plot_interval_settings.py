from matplotlib.dates import AutoDateLocator, DateFormatter
from matplotlib.ticker import AutoMinorLocator


def set_plot_interval_settings(ax, x_label: str, y_label: str):
    locator = AutoDateLocator(minticks=3, maxticks=10)

    ax.grid(alpha=0.7)
    ax.legend(fontsize=16)

    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(DateFormatter("%d %b %H:%M"))
    ax.xaxis.set_minor_locator(AutoMinorLocator(6))

    ax.tick_params(axis='y', which='major', color='black', length=8, width=2, labelsize=16)
    ax.tick_params(axis='y', which='minor', color='black', length=4, width=1, labelsize=16)
    ax.tick_params(axis='x', which='major', color='black', length=8, width=2, labelsize=16)
    ax.tick_params(axis='x', which='minor', color='black', length=4, width=1, labelsize=16)

    ax.set_xlabel(x_label, size=20)
    ax.set_ylabel(y_label, size=20)