load_from_cdaweb = False
loader = DataDownloading(config, load_from_cdaweb=load_from_cdaweb)

# Загрузка данных
ssc_data = loader.get_ssc_data()
fgm_data = loader.get_fgm_data()
esa_ion_data = loader.get_esa_data(particle="ion")
# esa_electron_data = loader.get_esa_data(particle="electron")
efi_data = loader.get_efi_data()
sta_data = loader.get_sta_data()
omn_data = loader.get_omn_data()
shue_data = loader.get_shue_data()


# Доступность данных
availability = AvailabilityIntervals(show_progress=True)

ssc_intervals = availability.from_dataframe(ssc_data, "ssc")
fgm_intervals = availability.from_dataframe(fgm_data, "fgm")
esa_ion_intervals = availability.from_dataframe(esa_ion_data, "esa_ion")
# esa_electron_intervals = availability.from_dataframe(esa_electron_data, "esa_electron")
efi_intervals = availability.from_dataframe(efi_data, "efi")
sta_intervals = availability.from_dataframe(sta_data, "sta")
shue_intervals = availability.from_dataframe(shue_data, "shue")