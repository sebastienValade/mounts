import fetchESA
import snapme as gpt
import logging

# --- set logging behaviour
# NB: 	logging behaviour overwritten in file: /usr/local/lib/python2.7/dist-packages/snappy/jpyutil.py
# 		setting it here is useless.
# logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.info('>> script started')

# --- print engine configuration
gpt.print_engineConfig()

# # --- simulate product
# path = '/home/khola/DATA/data_satellite/ERTAALE/'
# master = fetchESA.Product()
# master.path_and_file = path + 'S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'
# slave = fetchESA.Product()
# slave.path_and_file = path + 'S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'

# # --- read product
# m = gpt.read_product(master)
# s = gpt.read_product(slave)

# # --- split product
# subswath_name = 'IW2'
# polarisation = 'VV'
# m = gpt.topsar_split(m, subswath_name, polarisation)
# s = gpt.topsar_split(s, subswath_name, polarisation)

# # --- apply orbit file
# m = gpt.apply_orbit_file(m)
# s = gpt.apply_orbit_file(s)

# # --- back-geocoding
# stack = gpt.back_geocoding(m, s)

# # ===> write
# gpt.write_product(stack, pathout='/home/khola/DATA/data_satellite/ERTAALE_gpt/')

# quit()

stck = fetchESA.Product()
stck.path_and_file = '/home/khola/DATA/data_satellite/ERTAALE_gpt/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE_Orb_Stack.dim'
p = gpt.read_product(stck)

# --- interferogram
p = gpt.interferogram(p)

# ===> write
gpt.write_product(p, pathout='/home/khola/DATA/data_satellite/ERTAALE_gpt/')

# --- deburst
# p = gpt.deburst(stack)

# --- topographic phase removal
# p = gpt.topo_phase_removal(p)

# --- phase filtering
# p = gpt.goldstein_phase_filtering(p)

# --- terrain correction (geocode)
# p = gpt.terrain_correction(p)

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
