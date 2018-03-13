import fetchme
from osgeo import ogr
import pandas as pd
import geopandas as gpd
from geopandas import GeoSeries, GeoDataFrame
from shapely.wkt import loads
import dateutil.parser
import pickle
import os
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

obj = fetchme.Scihub()

# --- test area (poznan)
poznan_wkt = 'POLYGON ((16.498460428340927 53.24020834947074,18.141769904481162 53.20820137275637,18.07167848704459 52.22256603361336,16.464980367164795 52.25345680664421,16.498460428340927 53.24020834947074))'
poznan_poly = ogr.CreateGeometryFromWkt(poznan_wkt)

# --- poland boundaries
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
poland = world[world.name == 'Poland']
poland_wkt = poland.geometry.iloc[0].wkt
poland_bounds = poland.geometry.bounds
poland_poly = ogr.CreateGeometryFromWkt(poland_wkt)

# --- set search options
# beginposition = "[2017-01-01T00:00:00.000Z TO 2017-06-01T00:00:00.000Z]"
# beginposition = "[2017-05-01T00:00:00.000Z TO 2017-06-01T00:00:00.000Z]"
s1_optn = {'filename': 'S1*IW*SLC*',
           'footprint': poland_wkt,
           # 'beginposition': beginposition,
           'maxrecords': 100
           }

s2_optn = {'filename': 'S2*MSIL2A*',
           'cloudcoverpercentage': '[0 TO 2]',
           'footprint': poland_wkt,
           # 'beginposition': beginposition,
           'maxrecords': 100
           }

# --- pair selection options
download_pairs = 0
# download_dir = '/home/sebastien/DATA/data_sar2opt/raw/poland/'
download_dir = '/home/sebastien/DATA/data_sar2opt/raw/poland/quicklooks/'
plot_downloadedPairs = 0
plot_identifiedPairs = 0
print_pairsmetadata = 0
save_pairsList = 0

thresh_overlap_s1s2 = 90       # >> threshold overlap between s1 and s2 products (pair considered only if overlap >= threshold)
thresh_dt_s1s2 = 24 * 15         # >> thresholf time difference [hours] between between s1 and s2 products
thresh_overlap_testImg = 0      # >> threshold overlap between test image (s2 poznan) and s2 scihub product (discards product if overlap > threshold)
thresh_overlap_poland = 75     # >> threshold overlap between poland and s2 scihub product (discards product if overlap < threshold)
thresh_area_s2 = 1  # 1.5            # >> threshold area s2 product [degrees?] (NB: wroclaw s2 = 1.54, poznan s2 = 1.60)


pairs = []
pairs_wkt = []
p2download = []
print_space = '          '


def plot_wkt(wkt2plot=None, f_out=None, p_out=None):

    if wkt2plot is None:
        return

    # --- construct GeoDataFrame
    geometry = [loads(wkt) for wkt in wkt2plot]
    df = pd.DataFrame()
    gdf = GeoDataFrame(df, geometry=geometry)

    # --- construct GeoSeries
    #gs = GeoSeries(geometry)

    # --- plot
    f, ax = plt.subplots(1)
    world.plot(ax=ax, facecolor='lightgray', edgecolor='gray')
    poland.plot(ax=ax, facecolor='lightgray', edgecolor='red')
    gdf.plot(ax=ax, cmap='Set2', alpha=0.1, edgecolor='black')
    # gs.plot(ax=ax, cmap='Set2', alpha=0.7, edgecolor='black')

    ax.set_axis_off()
    plt.axis('equal')
    plt.xlim([poland_bounds.minx.min() - 2, poland_bounds.maxx.max() + 2])
    plt.ylim([poland_bounds.miny.min() - 2, poland_bounds.maxy.max() + 2])
    plt.show()

    # --- save png
    if f_out is None:
        f_out = 'pair_s1s2.png'

    if p_out is None:
        p_out = '../data/'
    else:
        p_out = os.path.join(p_out, '')  # add trailing slash if missing (os independent)

    print p_out
    plt.savefig(p_out + f_out)


ti = '2017-01-01'
tf = '2018-01-01'
#--- every 01 of each month
monthlist = pd.date_range(start=ti, end=tf, freq='MS').strftime('%Y-%m-%d')  # MS=month start

#--- every 15 of each month
# monthlist = pd.date_range(start='2017-01-01', end='2017-12-31', freq='SMS').tolist()
# monthlist = monthlist[1::2]

for i in range(len(monthlist) - 1):

    beginposition = "[{}T00:00:00.000Z TO {}T00:00:00.000Z]".format(monthlist[i], monthlist[i + 1])
    s1_optn['beginposition'] = beginposition
    s2_optn['beginposition'] = beginposition
    print('======= ' + beginposition)

    # --- query scihub
    s1 = obj.scihub_search(**s1_optn)
    s2 = obj.scihub_search(**s2_optn)

    # --- loop through s1 products
    for p1 in s1:

        # print('  | ' + p1.metadata.title)
        s1_wkt = p1.metadata.footprint
        s1_poly = ogr.CreateGeometryFromWkt(s1_wkt)
        s1_area = s1_poly.GetArea()

        # --- loop through s2 products
        for p2 in s2:

            # print('  |   - ' + p2.metadata.title)
            s2_wkt = p2.metadata.footprint
            s2_poly = ogr.CreateGeometryFromWkt(s2_wkt)
            s2_area = s2_poly.GetArea()

            if s2_area < thresh_area_s2:
                # print(print_space + 'product too small')
                continue

            # --- calculate intersection between s2 product and poland borders
            itsc = s2_poly.Intersection(poland_poly)
            itsc_wkt = itsc.ExportToWkt()
            itsc_area = itsc.GetArea()
            overlap_pct_poland = (itsc_area / s2_area) * 100
            if overlap_pct_poland < thresh_overlap_poland:
                # print 's2 product not inside polish borders => discarded'
                continue

            # --- calculate intersection between s2 product and s2 test area (poznan)
            # do not download if s2 overlaps test s2 (poznan), or if s1 identical is the very same product as during testing
            itsc = s2_poly.Intersection(poznan_poly)
            itsc_wkt = itsc.ExportToWkt()
            itsc_area = itsc.GetArea()
            overlap_pct_poznan = (itsc_area / s2_area) * 100
            # print 'overlap poznan = ' + str(overlap_pct_poznan)

            if overlap_pct_poznan > 0:
                # print 's2 product overlaps test area (poznan) => discarded'
                # plot_wkt([poznan_wkt, s2_wkt]) # control plot
                continue

            # --- calculate intersection between s1 product and s2
            # (https://pcjericks.github.io/py-gdalogr-cookbook/geometry.html)
            intersection = s1_poly.Intersection(s2_poly)
            intersection_wkt = intersection.ExportToWkt()
            intersection_area = intersection.GetArea()
            overlap_percentage = (intersection_area / s2_area) * 100
            # print overlap_percentage

            if overlap_percentage >= thresh_overlap_s1s2:

                t1 = dateutil.parser.parse(p1.metadata.beginposition)
                t2 = dateutil.parser.parse(p2.metadata.beginposition)
                dt = abs(t1 - t2)
                t1_iso = t1.strftime('%Y%m%dT%H%M%S')
                t2_iso = t2.strftime('%Y%m%dT%H%M%S')
                # print(print_space + 'overlap = ' + str(overlap_percentage))
                # print(print_space + 'dt = ' + str(dt))

                if dt.days * 86400 + dt.seconds < thresh_dt_s1s2 * 3600:
                    print(print_space + 'overlap = ' + str(overlap_percentage))
                    print(print_space + 't1 = ' + p1.metadata.beginposition + ' (' + p1.metadata.title + ')')
                    print(print_space + 't2 = ' + p2.metadata.beginposition + ' (' + p2.metadata.title + ')')
                    print(print_space + 'dt = ' + str(dt))

                    # --- save pair
                    pairs.append([p1.metadata.title + '.zip', p2.metadata.title + '.zip'])
                    pairs_wkt.append(s1_wkt)
                    pairs_wkt.append(s2_wkt)
                    p2download.append(p2)

                    # --- download product if area of intersection above threshold
                    if download_pairs:

                        # --- create result folder
                        pair_name = 'S1-' + t1_iso + '_S2-' + t2_iso
                        dir_pair = download_dir + pair_name
                        if not os.path.exists(dir_pair):
                            os.makedirs(dir_pair)
                            print 'Directory "' + dir_pair + '" created'

                        # --- plot pair
                        if plot_downloadedPairs:
                            plot_wkt(wkt2plot=[s1_wkt, s2_wkt], f_out=pair_name, p_out=dir_pair)

                        # --- save wkt file
                        text_file = open(dir_pair + "/S1_wkt.txt", "w")
                        text_file.write(p1.metadata.footprint)
                        text_file.close()

                        text_file = open(dir_pair + "/S2_wkt.txt", "w")
                        text_file.write(p2.metadata.footprint)
                        text_file.close()

                        # --- download pair
                        print(print_space + 'downloading: ' + p1.metadata.title)
                        # p1.getFullproduct(download_dir=dir_pair)
                        p1.getQuicklook(download_dir=dir_pair)

                        print(print_space + 'downloading: ' + p2.metadata.title)
                        # p2.getFullproduct(download_dir=dir_pair)
                        p2.getQuicklook(download_dir=dir_pair)


if save_pairsList:
    pickle.dump(pairs, open("sar2opt_pairs.p", "wb"))
    pickle.dump(pairs_wkt, open("sar2opt_pairswkt.p", "wb"))


# --- plot all selected pairs on single plot
if plot_identifiedPairs:

    if not pairs:
        print('0 pair found responding to search options')
        import sys
        sys.exit()
    else:
        print(pairs)

    plot_wkt(wkt2plot=pairs_wkt, f_out='all_selected_pairs', p_out=download_dir)


if print_pairsmetadata:
    for p in p2download:
        print(p.metadata)
