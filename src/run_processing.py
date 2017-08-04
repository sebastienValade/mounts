import os
import fetchme
import snapme as gpt
import logging

# --- set logging behaviour
# NB: logging behaviour overwritten in file: /usr/local/lib/python2.7/dist-packages/snappy/jpyutil.py
# logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.info('>> script started')

# --- print engine configuration
# NB: engine configuration set in file 'snap.properties'. Read in order of priority:
# 1) user setting: $HOME/.snap/etc/snap.properties
# 2) default setting: $SNAP/etc/snap.properties
gpt.print_engineConfig()


# --- simulate downloaded product
path = os.environ['HOME'] + '/DATA/data_satellite/'
master = fetchme.Product()
master.path_and_file = path + 'S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'
slave = fetchme.Product()
slave.path_and_file = path + 'S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.SAFE.zip'


# --- read product
m = gpt.read_product(master)
s = gpt.read_product(slave)


# --- split product
m = gpt.topsar_split(m, subswath='IW2')
s = gpt.topsar_split(s, subswath='IW2')


# --- apply orbit file
m = gpt.apply_orbit_file(m)
s = gpt.apply_orbit_file(s)


# --- back-geocoding
p = gpt.back_geocoding(m, s)


# --- interferogram
p = gpt.interferogram(p)

# # ===> write interferogram
# gpt.write_product(p, pathout='/home/khola/DATA/data_satellite/ERTAALE_gpt/')


# --- deburst
p = gpt.deburst(p)


# --- topographic phase removal
p = gpt.topo_phase_removal(p)


# --- phase filtering
p = gpt.goldstein_phase_filtering(p)


# --- terrain correction (geocode)
gpt.get_bandnames(p, print_bands=1)
sourceBands = ['Intensity_VV_11Jan2017_04Feb2017', 'Phase_VV_11Jan2017_04Feb2017', 'coh_IW2_VV_11Jan2017_04Feb2017']
p = gpt.terrain_correction(p, sourceBands)


# --- plot
# p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
# gpt.plotBand(p_subset, sourceBands[0], f_out='int_TC', cmap='binary')

p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
gpt.plotBand(p_subset, sourceBands[1], cmap='gist_rainbow')

p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
gpt.plotBand(p_subset, sourceBands[2], cmap='binary')


##########

# Raster -> collocate => collocate S1 (terrain corrected) and S2

# # --- print band names
# gpt.get_bandnames(p, print_bands=1)

# # --- print raster dimensions
# gpt.print_rasterDim(p, 'Phase_ifg_IW2_VV_11Jan2017_11Jan2017')
