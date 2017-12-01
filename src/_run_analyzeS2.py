import snapme as gpt
import numpy as np
import matplotlib.pyplot as plt

f = '/home/sebastien/DATA/data_satellite/ertaale/S2A_MSIL1C_20170228T075211_N0204_R092_T37PFR_20170228T075238.zip'
subset_wkt = 'POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))'

p = gpt.read_product(path_and_file=f)
p = gpt.resample(p, referenceBand='B2')
p = gpt.subset(p, geoRegion=subset_wkt)

bname = 'B12'
band = p.getBand(bname)
w, h = gpt.get_rasterDim(p, bname)
band_data = np.zeros(w * h, np.float32)
band.readPixels(0, 0, w, h, band_data)
band_data.shape = h, w

mask = np.where(band_data > 0.5, 1, 0)
lava_nbpix = np.count_nonzero(mask)
print lava_nbpix

m = np.ma.masked_where(a == 2, b)
masked_array(data=[a b - - d],
             mask=[False False  True False],
             fill_value=N / A)

# plt.imshow(mask)
# plt.colorbar()
# plt.savefig('B12.png')
