import os
import mapme
from osgeo import ogr

# --- image 1 (wroclaw) => training image
wkt1 = 'POLYGON ((16.43887679440474 51.44235231685392, 18.017025008525813 51.41234265506691, 17.953869713140705 50.42629366061962, 16.408715109637644 50.45527216066188, 16.43887679440474 51.44235231685392))'

# --- image 2 (poznan) => testing image
wkt2 = 'POLYGON ((16.498460428340927 53.24020834947074,18.141769904481162 53.20820137275637,18.07167848704459 52.22256603361336,16.464980367164795 52.25345680664421,16.498460428340927 53.24020834947074))'

# --- plot test areas
mapme.plot_wkt(wkt2plot=[wkt1, wkt2], plot_country='Poland', f_out='map_of_testdata', p_out='/home/sebastien/DATA/data_sar2opt/raw/poland')

# --- plot all downloaded data
wkts = []
path = '/home/sebastien/DATA/data_sar2opt/raw/poland/'
pairs = filter(lambda x: os.path.isdir(path + x), os.listdir(path))
for p in pairs:
    with open(path + p + '/S1_wkt.txt') as f:
        wkts.append(f.readline())
    with open(path + p + '/S2_wkt.txt') as f:
        wkts.append(f.readline())
mapme.plot_wkt(wkt2plot=wkts, plot_country='Poland', f_out='map_of_newdata2', p_out='/home/sebastien/DATA/data_sar2opt/raw/poland')

# --- print info about test areas
wkt1_poly = ogr.CreateGeometryFromWkt(wkt1)
wkt2_poly = ogr.CreateGeometryFromWkt(wkt2)
wkt1_area = wkt1_poly.GetArea()
wkt2_area = wkt2_poly.GetArea()
print 'wroclaw area (s2): ' + str(wkt1_area)
print 'poznan area (s2): ' + str(wkt2_area)


# --- plot test areas alone
# NB: comment out import mapme, because matplotlib imported with backend 'Agg'
# import geopandas as gpd
# from geopandas import GeoSeries, GeoDataFrame
# from shapely.wkt import loads
# import matplotlib.pyplot as plt

# # --- get poland boundaries
# world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
# poland = world[world.name == 'Poland']
# poland_bounds = poland.geometry.bounds

# f, ax = plt.subplots(1)
# world.plot(ax=ax, facecolor='lightgray', edgecolor='gray')
# poland.plot(ax=ax, facecolor='lightgray', edgecolor='red')
# gs = GeoSeries([loads(wkt1), loads(wkt2)])
# gs.plot(ax=ax, cmap='Set2', alpha=0.7, edgecolor='black')

# ax.set_axis_off()
# plt.axis('equal')
# plt.xlim([poland_bounds.minx.min() - 2, poland_bounds.maxx.max() + 2])
# plt.ylim([poland_bounds.miny.min() - 2, poland_bounds.maxy.max() + 2])
# plt.show()
