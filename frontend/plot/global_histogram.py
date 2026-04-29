def set_bins_parameters(values):
    bincs = 0.05
    hist_min = round(min(values), 1)
    hist_max = round(max(values), 1)
    bins_count = round( (hist_max - hist_min ) / bincs  )
    bins = np.linspace(hist_min, hist_max, bins_count + 1)

    return bins, bincs

def show_hist_by_lmlt_range(ax1, hdata, l_range, r_range, component):

    if r_range[0] > r_range[1]:
        data_sector = [row[r_range[0]:24][0] for row in hdata[l_range[0]:l_range[1]]]
        data_flat1 = [item for row in data_sector for item in row]

        data_sector = [row[0:r_range[1]][0] for row in hdata[l_range[0]:l_range[1]]]
        data_flat2 = [item for row in data_sector for item in row]

        data_flat = data_flat1 + data_flat2
        data = np.array(data_flat)
    else:
        data_sector = [row[r_range[0]:r_range[1]][0] for row in hdata[l_range[0]:l_range[1]]]
        data_flat = [item for row in data_sector for item in row]
        data = np.array(data_flat)

    data = data[~np.isnan(data)]   # удаляем NaN

    data_mean = np.mean(data)
    data_std = np.std(data)
    data_q25 = np.quantile(data, 0.25)
    data_q50 = np.quantile(data, 0.50)
    data_q75 = np.quantile(data, 0.75)

    # fig, (ax1) = plt.subplots(1, 1, figsize=(20, 6), layout="constrained")

    # hparam in lmlt
    bins, bincs = set_bins_parameters(data)
    histogram = ax1.hist(data, bins=bins, edgecolor="black", color = '#F48849', rwidth=0.8)
    # ax1.vlines(data_mean, 0, max(histogram[0]), linewidth=5, color='#1F53A0', label='$H_(avg)$')
    ax1.axvspan(data_mean - data_std, data_mean + data_std, 0, max(histogram[0]), color='red', alpha=0.2, label='$Average ±σ$')
    line_width = 0.004

    ax1.axvspan(data_mean - line_width, data_mean + line_width, 0, max(histogram[0]), color='#5b5b5b', label='Average')

    ax1.axvspan(data_q25 - line_width, data_q25 + line_width, 0, max(histogram[0]), color='#1369b7', label='Percentile 25')
    ax1.axvspan(data_q50 - line_width, data_q50 + line_width, 0, max(histogram[0]), color='#3fb60c', label='Median')
    ax1.axvspan(data_q75 - line_width, data_q75 + line_width, 0, max(histogram[0]), color='#b11b1b', label='Percentile 75')

    ax1.grid(alpha = 0.6)

    ax1.yaxis.set_minor_locator(AutoMinorLocator(5))
    ax1.xaxis.set_minor_locator(AutoMinorLocator(0.5 / bincs))
    ax1.tick_params(axis='y',  which='major', color='black', length=8, width=2, labelsize=14)
    ax1.tick_params(axis='y',  which='minor', color='black', length=4, width=1, labelsize=14)
    ax1.tick_params(axis='x',  which='major', color='black', length=8, width=2, labelsize=14)
    ax1.tick_params(axis='x',  which='minor', color='black', length=4, width=1, labelsize=14)
    # ax1.set_xlabel('H, value', size = 20)
    ax1.set_title(f'H parameter for MLT={r_range[0]}-{r_range[1]}', size = 16)
    # ax1.set_title(
        # f'Распределение Hparam между Е и vxB в компоненте {component}, \n L={l_range[0]}-{l_range[1]}, MLT={r_range[0]}-{r_range[1]}, Havg = {data_mean:.3f}, STD = {data_std:.3f} \n p25 = {data_q25:.2f}, p50 = {data_q50:.2f}, p75 = {data_q75:.2f}', size = 20)
    ax1.legend(fontsize=12)



def show_hist_sectors(data, component, lfrom, lto):


    fig, ((ax1, ax2, ax3, ax4)) = plt.subplots(4, 1, figsize=(10, 10), layout="constrained")

    show_hist_by_lmlt_range(ax1, data, [lfrom, lto], [3, 9], component)
    show_hist_by_lmlt_range(ax2, data, [lfrom, lto], [9, 15], component)
    show_hist_by_lmlt_range(ax3, data, [lfrom, lto], [15, 21], component)
    show_hist_by_lmlt_range(ax4, data, [lfrom, lto], [21, 3], component)

    image_path = f'{PARAMETERS['images_path']}h{component}_per_mlt_l{lfrom}-l{lto}.svg'
    # plt.close(fig)
    fig.savefig(image_path, dpi = 300)