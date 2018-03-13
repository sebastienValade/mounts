import snapme as gpt
import fnmatch
import numpy as np
import matplotlib.pyplot as plt

f = '/home/sebastien/DATA/data_snap/20170128T030737_20170209T030748_IW2_VV.dim'
p = gpt.read_product(path_and_file=f)

bdnames = gpt.get_bandnames(p, print_bands=None)
bname = fnmatch.filter(bdnames, 'coh_*')[0]

# --- get band data
band = p.getBand(bname)
w, h = gpt.get_rasterDim(p, bname)
band_data = np.zeros(w * h, np.float32)
band.readPixels(0, 0, w, h, band_data)
band_data.shape = h, w
B = band_data

# --- analyze
thresh = 0.5
mask = np.where(band_data < thresh, 0, 1)
nbpix = np.count_nonzero(mask == 0)

colormap = None
imgplot = plt.imshow(band_data, cmap=colormap)
plt.show()

# fig = plt.imshow(cube)
# fig.write_png('oulala')
