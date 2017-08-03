import fetchme
import snapme as gpt
import logging

logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.info('>> script started')


# --- simulate product
prod = fetchme.Product()
#prod.path_and_file = '/home/khola/.snap/snap-python/snappy/testdata/MER_FRS_L1B_SUBSET.dim'
prod.path_and_file = '/home/khola/DATA/data_satellite/tmp/S2A_MSIL1C_20170202T090201_N0204_R007_T35SNA_20170202T090155.SAFE'
# prod.path_and_file = '/home/khola/DATA/data_satellite/tmp/S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.SAFE'

#. ertaale
prod.path_and_file = '/home/khola/DATA/data_satellite/ERTAALE/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'


# --- read product
p = gpt.read_product(prod)

b = gpt.get_bandnames(p)


# --- resample
p = gpt.resample(p)

# --- subset
polygon_wkt = "POLYGON((27.088 36.527, 27.241 36.527, 27.241 36.657, 27.088 36.657, 27.088 36.527))"
p = gpt.subset(p, polygon_wkt=polygon_wkt)
# p = gpt.subset(p, north_bound=36.657, west_bound=27.088, south_bound=36.527, east_bound=27.241)


# --- plot
gpt.plotBand(p, 'B4')


###########################################

# # --- get band dimensions
# w, h = gpt.get_rasterDim(p, 'B3')
# print(str(w) + ' x ' + str(h))

# --- read xml nodes
# processSAR.read_xmlnodes(prod)

# --- write
# print('---> saving')
# gpt.write_product(p)
