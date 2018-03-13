import os
import subprocess
from shapely.wkt import loads
import elevation

path_bin = '/home/sebastien/SOFTWARES/FORCE/bin'

# # --- unzip s2
path_raw = '/home/sebastien/DATA/data_sar2opt/raw/poland/S1-20171226T161842_S2-20171226T094359/'
# path_zip = '/home/sebastien/DATA/data_sar2opt/raw/poland/S1-20171226T161842_S2-20171226T094359/S2B_MSIL2A_20171226T094359_N0206_R036_T34UFB_20171226T113723.zip'
# cmd = 'unzip {}'.format(path_zip)


# --- get s2 image bounds and download DEM
# with open(path_raw + '/S2_wkt.txt') as f:
#     image_wkt = f.readline()
# geometry = loads(image_wkt)
# image_bounds = geometry.bounds
# print('=== image bounds:' + str(image_bounds))
# print('=== downloading SRTM DEM')
# elevation.clip(bounds=image_bounds, output=path_masks + '/image_DEM.tif')


# # --- path to granule
# path_granule = strrep(path_zip, '.zip', '.SAFE')
# path_granule = '/home/khola/DATA/data_satellite/ertaale/S2A_MSIL1C_20170310T073721_N0204_R092_T37PFR_20170310T074549.SAFE/GRANULE/L1C_T37PFR_A008955_20170310T074549'


# # --- create dir for masks
path_pcssed = '/home/sebastien/DATA/data_sar2opt/processed/poland/S1-20171226T161842_S2-20171226T094359/'
path_masks = path_pcssed + '/masks'
# cmd = 'mkdir {}'.format(path_masks)


# # --- create config file
# cmd = './{}/force-parameter-level2 {}'.format(path_bin, path_masks)


# # --- replace strings
# path_cfgfile = path_masks + '/level2-skeleton.prm'
# # DO_REPROJ = FALSE
# # DO_TILE = FALSE
# # RES_MERGE = FALSE
# # DO_TOPO = FALSE
# # DO_ATMO = FALSE
# # FILE_DEM = NULL


# --- process
path_granule = '/home/sebastien/DATA/data_sar2opt/raw/poland/S1-20171226T161842_S2-20171226T094359/S2B_MSIL2A_20171226T094359_N0206_R036_T34UFB_20171226T113723.SAFE/GRANULE/L2A_T34UFB_A004209_20171226T094401'
path_cfgfile = '/home/sebastien/DATA/data_sar2opt/processed/poland/S1-20171226T161842_S2-20171226T094359/masks/level2-skeleton.prm'
cmd = './{}/force-l2ps {} {}'.format(path_bin, path_granule, path_cfgfile)
print cmd

# # os.system(cmd)
subprocess.call([path_bin + '/force-l2ps', path_granule, path_cfgfile])
