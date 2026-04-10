import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator, MultipleLocator
from matplotlib.patches import Circle

def show_h_parameter_range():
    def h(t):
        return 1 - (2 * t / (t ** 2 + 1))

    t_values = np.linspace(-10, 10, 500)
    h_values = h(t_values)

    fig, ax = plt.subplots(figsize=(20, 8))
    ax.plot(t_values, h_values, label='H(t)', color='darkred')

    ax.set_xlabel('t, t = E / Ɛ', size=20)
    ax.set_ylabel('H(t)', size=20)

    ax.set_xlim(-5, 5)
    ax.set_ylim(-0.2, 2.2)

    ax.xaxis.set_major_locator(MultipleLocator(1))   # <-- шаг 1 по X
    ax.xaxis.set_minor_locator(AutoMinorLocator(5))  # 0.2 внутри каждого шага 1
    ax.yaxis.set_minor_locator(AutoMinorLocator(5))

    ax.grid(alpha=0.9, which='major')
    ax.grid(alpha=0.3, which='minor', linestyle=':')

    ax.axhline(0, color='black', linewidth=0.5)
    ax.axhline(1, color='black', linestyle='--', linewidth=1.5)
    ax.axvline(0, color='black', linewidth=0.5)

    circle = Circle((0, 1), 0.06, fill=False, edgecolor='black', linewidth=1)
    ax.add_patch(circle)

    ax.tick_params(axis='y', which='major', color='black', length=8, width=2, labelsize=16)
    ax.tick_params(axis='y', which='minor', color='black', length=4, width=1, labelsize=16)
    ax.tick_params(axis='x', which='major', color='black', length=8, width=2, labelsize=16)
    ax.tick_params(axis='x', which='minor', color='black', length=4, width=1, labelsize=16)

    ax.set_aspect('equal', adjustable='box')
    ax.legend(fontsize=16)
    plt.show()