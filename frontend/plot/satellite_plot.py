import math
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import Normalize


@dataclass(slots=True)
class SatellitePlot:
    lshell_range: int = 16
    mlt_bins: int = 24
    min_lshell: int = 4

    @staticmethod
    def _round_up(value: float, decimals: int = 0) -> float:
        multiplier = 10**decimals
        return math.ceil(value * multiplier) / multiplier

    @staticmethod
    def _realign_polar_xticks(ax) -> None:
        for theta, label in zip(ax.get_xticks(), ax.get_xticklabels(), strict=False):
            theta = theta * ax.get_theta_direction() + ax.get_theta_offset()
            theta = np.pi / 2 - theta
            y_pos, x_pos = np.cos(theta), np.sin(theta)
            if x_pos >= 0.1:
                label.set_horizontalalignment("left")
            if x_pos <= -0.1:
                label.set_horizontalalignment("right")
            if y_pos >= 0.5:
                label.set_verticalalignment("bottom")
            if y_pos <= -0.5:
                label.set_verticalalignment("top")

    def _colorbar_ticks(self, matrix: np.ndarray, bins_multiplier: int) -> tuple[np.ndarray, int]:
        min_val = 0.0
        max_val = float(np.nanmax(matrix))
        tick_count = 5
        if max_val > 10:
            ticks = np.linspace(min_val, self._round_up(max_val, -1), tick_count + 1)
            ticks = np.round(ticks, 0)
        else:
            ticks = np.linspace(min_val, round(max_val, 1), tick_count + 1)
            ticks = np.round(ticks, 2)
        bins = (len(ticks) - 1) * bins_multiplier
        return ticks, bins

    def _build_mesh(
        self,
        fig,
        ax,
        data_matrix: np.ndarray,
        theta: np.ndarray,
        radius: np.ndarray,
        ticks: np.ndarray,
        bins: int,
        title: str,
        max_lshell: int,
    ) -> None:
        cmap = plt.get_cmap("turbo", bins).copy()
        matrix = np.array(data_matrix, copy=True, dtype=float)
        matrix[matrix == -1] = np.nan

        norm = Normalize(np.nanmin(ticks), np.nanmax(ticks))
        cmap.set_bad(color="grey", alpha=0.5)
        color_mesh = ax.pcolormesh(theta, radius, matrix, cmap=cmap, vmax=np.nanmax(ticks), vmin=np.nanmin(ticks))

        colorbar = fig.colorbar(color_mesh, orientation="vertical", pad=0.1, extend="neither")
        colorbar.ax.tick_params(labelsize=12)
        colorbar.set_ticks(ticks)
        colorbar.set_label(title, rotation=270, labelpad=20, size=12)

        samples_per_sector = max(int(360 / self.mlt_bins), 2)
        for r_index in range(len(radius) - 1):
            for theta_index in range(len(theta) - 1):
                t1, t2 = theta[theta_index], theta[theta_index + 1]
                r1, r2 = radius[r_index], radius[r_index + 1]
                t_values = np.linspace(t1, t2, samples_per_sector)
                value = matrix[r_index, theta_index]
                ax.fill_between(t_values, r1, r2, color=cmap(norm(value)), alpha=1.0)

        ax.set_xticks(np.arange(0, 2.0 * np.pi, np.pi / 12.0))
        ax.set_yticks(np.arange(0, max_lshell, 1))
        ax.set_theta_zero_location("E")
        ax.set_xticklabels(
            ["0", "", "", "3", "", "", "6", "", "", "9", "", "", "12", "", "", "15", "", "", "18", "", "", "21", "", ""],
            size=14,
        )
        ax.set_yticklabels([])
        ax.grid(color="#383838", linestyle="--", linewidth=1)
        self._realign_polar_xticks(ax)

        center_circle = plt.Circle((0.0, 0.0), self.min_lshell, transform=ax.transData._b, color="white", alpha=1, zorder=2)
        ax.add_artist(center_circle)

    def _plot_single(
        self,
        matrix: np.ndarray,
        *,
        title: str = "",
        max_lshell: int = 16,
        bins_multiplier: int = 3,
        figure_size: tuple[float, float] = (9, 8),
    ):
        max_lshell = min(max_lshell, self.lshell_range)
        radial_bins = max_lshell - self.min_lshell
        radius = np.linspace(self.min_lshell, max_lshell, radial_bins + 1)
        theta = np.linspace(0, 2 * np.pi, self.mlt_bins + 1)
        clipped = matrix[self.min_lshell:max_lshell]
        ticks, bins = self._colorbar_ticks(clipped, bins_multiplier=bins_multiplier)

        fig = plt.figure(figsize=figure_size, layout="constrained")
        ax = fig.add_subplot(111, polar=True)
        self._build_mesh(fig, ax, clipped, theta, radius, ticks, bins, title, max_lshell)
        return fig

    @staticmethod
    def _can_show_interactive() -> bool:
        backend = plt.get_backend().lower()
        non_interactive_backends = {"agg", "pdf", "pgf", "ps", "svg", "template", "cairo"}
        # TkAgg/QtAgg/WebAgg are interactive backends despite containing "agg".
        return backend not in non_interactive_backends

    def draw_polar_plot(
        self,
        matrix: np.ndarray,
        *,
        max_lshell: int = 16,
        title: str = "",
        output_path: str | Path | None = None,
        show: bool = True,
        figure_size: tuple[float, float] = (9, 8),
    ) -> None:
        # добавить 1e9 если параметр J берём
        fig = self._plot_single(
            np.asarray(matrix),
            title=title,
            max_lshell=max_lshell,
            bins_multiplier=3,
            figure_size=figure_size,
        )
        if output_path is not None:
            fig.savefig(output_path, dpi=300)
        if show and self._can_show_interactive():
            plt.show(block=True)
        else:
            plt.close(fig)

    def draw_quantity_plot(
        self,
        matrix: list[list[list]] | np.ndarray,
        *,
        max_lshell: int = 16,
        title: str = "",
        output_path: str | Path | None = None,
        show: bool = True,
    ) -> None:
        quantity = np.zeros((self.lshell_range, self.mlt_bins), dtype=int)
        for lshell_index in range(len(matrix)):
            for mlt_index in range(len(matrix[lshell_index])):
                quantity[lshell_index, mlt_index] = len(matrix[lshell_index][mlt_index])

        fig = self._plot_single(quantity, title=title, max_lshell=max_lshell, bins_multiplier=3)
        if output_path is not None:
            fig.savefig(output_path, dpi=300)
        if show and self._can_show_interactive():
            plt.show(block=True)
        else:
            plt.close(fig)

    def draw_hparam_plots(
        self,
        mean_matrix: np.ndarray,
        median_matrix: np.ndarray,
        *,
        max_lshell: int = 16,
        save_image: bool = False,
        component: str = "",
        first_title: str = "Mean",
        second_title: str = "Median",
        output_path: str | Path | None = None,
        show: bool = True,
    ) -> None:
        max_lshell = min(max_lshell, self.lshell_range)
        radial_bins = max_lshell - self.min_lshell
        radius = np.linspace(self.min_lshell, max_lshell, radial_bins + 1)
        theta = np.linspace(0, 2 * np.pi, self.mlt_bins + 1)

        mean_clipped = np.asarray(mean_matrix)[self.min_lshell:max_lshell]
        median_clipped = np.asarray(median_matrix)[self.min_lshell:max_lshell]
        mean_ticks, mean_bins = self._colorbar_ticks(mean_clipped, bins_multiplier=4)
        median_ticks, median_bins = self._colorbar_ticks(median_clipped, bins_multiplier=4)

        fig, (ax_mean, ax_median) = plt.subplots(1, 2, figsize=(24, 8), subplot_kw={"polar": True})
        self._build_mesh(
            fig,
            ax_mean,
            mean_clipped,
            theta,
            radius,
            mean_ticks,
            mean_bins,
            f"{first_title} H{component} param".strip(),
            max_lshell,
        )
        self._build_mesh(
            fig,
            ax_median,
            median_clipped,
            theta,
            radius,
            median_ticks,
            median_bins,
            f"{second_title} H{component} param".strip(),
            max_lshell,
        )

        if save_image:
            resolved_output = Path(output_path) if output_path is not None else Path(f"h_{component}_mean_median.png")
            fig.savefig(resolved_output, dpi=300)

        if show and self._can_show_interactive():
            plt.show(block=True)
        else:
            plt.close(fig)

    def draw_hparam_four_plots(
        self,
        mean_matrix: np.ndarray,
        median_matrix: np.ndarray,
        q1_matrix: np.ndarray,
        q3_matrix: np.ndarray,
        *,
        max_lshell: int = 16,
        save_image: bool = False,
        component: str = "",
        mean_title: str = "Mean",
        median_title: str = "Median",
        q1_title: str = "Q1",
        q3_title: str = "Q3",
        output_path: str | Path | None = None,
        show: bool = True,
    ) -> None:
        max_lshell = min(max_lshell, self.lshell_range)
        radial_bins = max_lshell - self.min_lshell
        radius = np.linspace(self.min_lshell, max_lshell, radial_bins + 1)
        theta = np.linspace(0, 2 * np.pi, self.mlt_bins + 1)

        matrices = [
            (np.asarray(mean_matrix)[self.min_lshell:max_lshell], mean_title),
            (np.asarray(median_matrix)[self.min_lshell:max_lshell], median_title),
            (np.asarray(q1_matrix)[self.min_lshell:max_lshell], q1_title),
            (np.asarray(q3_matrix)[self.min_lshell:max_lshell], q3_title),
        ]

        fig, axes = plt.subplots(2, 2, figsize=(24, 14), subplot_kw={"polar": True})
        axes_flat = axes.flatten()

        for axis, (matrix, title_text) in zip(axes_flat, matrices, strict=False):
            ticks, bins = self._colorbar_ticks(matrix, bins_multiplier=4)
            plot_title = f"{title_text} H{component} param".strip()
            self._build_mesh(fig, axis, matrix, theta, radius, ticks, bins, plot_title, max_lshell)

        if save_image:
            resolved_output = Path(output_path) if output_path is not None else Path(f"h_{component}_mean_median_q1_q3.png")
            fig.savefig(resolved_output, dpi=300)

        if show and self._can_show_interactive():
            plt.show(block=True)
        else:
            plt.close(fig)


def plot_satellite_matrix(
    matrix: np.ndarray,
    *,
    title: str = "",
    max_lshell: int = 16,
    output_path: str | Path | None = None,
    show: bool = True,
) -> None:
    SatellitePlot().draw_polar_plot(matrix, max_lshell=max_lshell, title=title, output_path=output_path, show=show)
