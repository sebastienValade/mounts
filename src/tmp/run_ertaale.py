import fetchme
import snapme as gpt
import logging

# --- set logging behaviour
# NB: 	logging behaviour overwritten in file: /usr/local/lib/python2.7/dist-packages/snappy/jpyutil.py
# 		setting it here is useless.
# logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.info('>> script started')

# --- print engine configuration
gpt.print_engineConfig()


# --- simulate product
path = '/home/sebastien/DATA/data_satellite/'
master = fetchme.Product()
master.path_and_file = path + 'S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'
slave = fetchme.Product()
slave.path_and_file = path + 'S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.SAFE.zip'

# --- read product
m = gpt.read_product(master)
s = gpt.read_product(slave)

# --- split product
subswath_name = 'IW2'
polarisation = 'VV'
m = gpt.topsar_split(m, subswath_name, polarisation)
s = gpt.topsar_split(s, subswath_name, polarisation)

# --- apply orbit file
m = gpt.apply_orbit_file(m)
s = gpt.apply_orbit_file(s)

# --- back-geocoding
p = gpt.back_geocoding(m, s)

# # ===> write
# gpt.write_product(stack, pathout='/home/khola/DATA/data_satellite/ERTAALE_gpt/')

# quit()

# # --- read saved coregistered stack
# stck = fetchme.Product()
# stck.path_and_file = '/home/sebastien/DATA/data_satellite/ERTAALE_gpt/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE_Orb_Stack.dim'
# p = gpt.read_product(stck)

# print('====> plot stack')
# bds = ['Intensity_IW2_VV_mst_11Jan2017', 'bbb', 'q_IW2_VV_slv1_04Feb2017']
# p_subset = gpt.subset(p, north_bound=13.54, west_bound=40.66, south_bound=13.6, east_bound=40.715)
# gpt.plotBand(p_subset, bds)


# --- interferogram
p = gpt.interferogram(p)


# print('====> IFG plot')
# gpt.get_bandnames(p, print_bands=1)
# bds = ['Phase_ifg_IW2_VV_11Jan2017_04Feb2017', 'bbb', 'coh_IW2_VV_11Jan2017_04Feb2017']
# p_subset = gpt.subset(p, north_bound=13.54, west_bound=40.66, south_bound=13.6, east_bound=40.715)
# gpt.plotBand(p_subset, bds)

# # ===> write interferogram
# gpt.write_product(p, pathout='/home/khola/DATA/data_satellite/ERTAALE_gpt/')


# --- deburst
p = gpt.deburst(p)

# print('====> DEBURST plot')
# gpt.get_bandnames(p, print_bands=1)
# bds = ['Phase_ifg_IW2_VV_11Jan2017_04Feb2017', 'bbb', 'coh_IW2_VV_11Jan2017_04Feb2017']
# p_subset = gpt.subset(p, north_bound=13.54, west_bound=40.66, south_bound=13.6, east_bound=40.715)
# gpt.plotBand(p_subset, bds)


# --- topographic phase removal
p = gpt.topo_phase_removal(p)

# print('====> TOPO plot')
# gpt.get_bandnames(p, print_bands=1)
# bds = ['Phase_ifg_srd_VV_11Jan2017_04Feb2017', 'topo_phase_VV_11Jan2017_04Feb2017']
# p_subset = gpt.subset(p, north_bound=13.54, west_bound=40.66, south_bound=13.6, east_bound=40.715)
# gpt.plotBand(p_subset, bds)


# --- phase filtering
p = gpt.goldstein_phase_filtering(p)

# print('====> goldstein plot')
# gpt.get_bandnames(p, print_bands=1)
# bds = ['Phase_VV_11Jan2017_04Feb2017']
# p_subset = gpt.subset(p, north_bound=13.54, west_bound=40.66, south_bound=13.6, east_bound=40.715)
# gpt.plotBand(p_subset, bds)


# --- terrain correction (geocode)
gpt.get_bandnames(p, print_bands=1)
sourceBands = ['Intensity_VV_11Jan2017_04Feb2017', 'Phase_VV_11Jan2017_04Feb2017', 'coh_IW2_VV_11Jan2017_04Feb2017', 'aa']
p = gpt.terrain_correction(p, sourceBands)


# --- plot
p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
gpt.plotBand(p_subset, sourceBands[0], f_out='int_TC', cmap='binary')

p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
gpt.plotBand(p_subset, sourceBands[1], f_out='phase_TC', cmap='gist_rainbow')

p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
gpt.plotBand(p_subset, sourceBands[2], f_out='coh_TC', cmap='binary')

quit()

##########

# --- get band names
gpt.get_bandnames(p)

# --- print raster dimensions
gpt.print_rasterDim(p, 'Phase_ifg_IW2_VV_11Jan2017_11Jan2017')

# --- subset
p = gpt.subset(p, north_bound=13.58, west_bound=40.69, south_bound=13.6, east_bound=40.71)

# --- plot
gpt.plotBand(p, 'Phase_ifg_IW2_VV_11Jan2017_11Jan2017')
