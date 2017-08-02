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
path = '/home/khola/DATA/data_satellite/ERTAALE/'
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
m_spl = gpt.topsar_split(m, subswath_name, polarisation)
s_spl = gpt.topsar_split(s, subswath_name, polarisation)

# --- apply orbit file
m_obt = gpt.apply_orbit_file(m_spl)
s_obt = gpt.apply_orbit_file(s_spl)

# --- back-geocoding
p_stk = gpt.back_geocoding(m_obt, s_obt)

# ===> write
gpt.write_product(p_stk, pathout='/home/khola/DATA/data_satellite/ERTAALE_gpt/')

# quit()

# stck = fetchme.Product()
# stck.path_and_file = '/home/khola/DATA/data_satellite/ERTAALE_gpt/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE_Orb_Stack.dim'
# p_stk = gpt.read_product(stck)

# --- interferogram
p_ifg = gpt.interferogram(p_stk)

# ===> write
gpt.write_product(p_ifg, pathout='/home/khola/DATA/data_satellite/ERTAALE_gpt/')

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
