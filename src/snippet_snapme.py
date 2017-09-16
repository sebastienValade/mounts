import snapme as gpt

# --- set logging behaviour
# NB: logging behaviour overwritten in file: /usr/local/lib/python2.7/dist-packages/snappy/jpyutil.py
# logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
# logging.info('>> script started')

# --- print engine configuration
# NB: engine configuration set in file 'snap.properties'. Read in order of priority:
# 1) user setting: $HOME/.snap/etc/snap.properties
# 2) default setting: $SNAP/etc/snap.properties
# gpt.print_engineConfig()


# =============================================
# DINSAR processing chain
# =============================================

# # --- read master product
# master_abspath = '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'
# m = gpt.read_product(path_and_file=master_abspath)

# # --- read slave product
# slave_abspath = '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.SAFE.zip'
# s = gpt.read_product(path_and_file=slave_abspath)

# # --- split product
# subswath = 'IW2'
# polarization = 'VV'
# m = gpt.topsar_split(m, subswath=subswath, polarisation=polarization)
# s = gpt.topsar_split(s, subswath=subswath, polarisation=polarization)

# # --- apply orbit file
# m = gpt.apply_orbit_file(m)
# s = gpt.apply_orbit_file(s)

# # --- back-geocoding
# p = gpt.back_geocoding(m, s)

# # --- interferogram
# p = gpt.interferogram(p)

# # --- deburst
# p = gpt.deburst(p)

# # --- topographic phase removal
# p = gpt.topo_phase_removal(p)

# # --- phase filtering
# p = gpt.goldstein_phase_filtering(p)

# # --- terrain correction (geocode)
# bdnames = gpt.get_bandnames(p, print_bands=1)
# idx_phase = [idx for idx, dbname in enumerate(bdnames) if 'Phase_' in dbname][0]
# idx_coh = [idx for idx, dbname in enumerate(bdnames) if 'coh_' in dbname][0]
# sourceBands = [bdnames[idx_phase], bdnames[idx_coh]]
# p = gpt.terrain_correction(p, sourceBands)

# # --- subset
# subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale
# p = gpt.subset(p, **subset_bounds)

# # --- plot
# gpt.plotBand(p, sourceBands, cmap=['gist_rainbow', 'binary_r'])

# # --- dispose (releases all of the resources used by this object instance and all of its owned children)
# p.dispose()


# =============================================
# PLOT
# =============================================

# # --- plot RGB from S2 data
# file_abspath = '/home/khola/DATA/data_satellite/ertaale_S2/S2A_MSIL1C_20170330T073611_N0204_R092_T37PFR_20170330T075431.SAFE'
# p = gpt.read_product(path_and_file=file_abspath)
# gpt.plot_RGB(p)
# gpt.plot_RGB(p, bname_red='B12', bname_green='B11', bname_blue='B8A', f_out='waou.png') #-> cf S2 image erta ale 2017-03-30


# --- collocate products (S1 geocoded = master, S2 not-geocoded = slave)
master_abspath = '/home/khola/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE_Orb_Stack_deb_ifg_dinsar_flt_TC.dim'
m = gpt.read_product(path_and_file=master_abspath)
slave_abspath = '/home/khola/DATA/data_satellite/ertaale_S2/S2A_MSIL1C_20170330T073611_N0204_R092_T37PFR_20170330T075431.SAFE'
s = gpt.read_product(path_and_file=slave_abspath)

s = gpt.resample(s, referenceBand='B2')
p = gpt.collocate(m, s)
gpt.get_bandnames(p, print_bands=1)

subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale
p = gpt.subset(p, **subset_bounds)

gpt.plotBand(p, ['coh_IW2_VV_11Jan2017_04Feb2017_M', 'B8A_S'], cmap=['binary_r', 'binary_r'])
gpt.plotBand(p, ['B12_S'], cmap=['binary_r'])
gpt.plot_RGB(p, bname_red='B12_S', bname_green='B11_S', bname_blue='B8A_S', f_out='waou.png')  # -> cf S2 image erta ale 2017-03-30

# =============================================
# MISC
# =============================================

# --- run processing graph (operators defined in configuration file)
# config_file = './conf/config_processing.yml'
# gpt.graph_processing(config_file)

# --- write product
# gpt.write_product(p, pathout='home/sebastien/DATA/')

# --- resample
# p = gpt.resample(p, referenceBand='Intensity_IW2_VV')

# --- print band names
# gpt.get_bandnames(p, print_bands=1)

# --- print raster dimensions
# gpt.print_rasterDim(p, 'Phase_ifg_IW2_VV_11Jan2017_11Jan2017')

# TODO: collocate S1 and S2
# Raster -> collocate => collocate S1 (terrain corrected) and S2
