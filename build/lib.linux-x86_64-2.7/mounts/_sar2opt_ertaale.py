import os
import sys
import glob
import snapme as gpt
import utilme
import numpy as np
import pandas as pd
from sqlalchemy import create_engine  # database connection


path_out = '/home/sebastien/DATA/data_sar2opt/ertaale/'
dt_thresh_h = 2  # >> discard S1/S2 pairs if dt>threshold
start_idx = 2

export_s1 = 0
export_s2 = 0
export_ifgcoh = 0
export_s2p = 0
save_metadata = 0
plot_png = 1


# # --- open .dim files with ifg+coh
# path = '/home/sebastien/DATA/data_snap/*.dim'
# fpath = glob.glob(path)
# fnames = [os.path.basename(f) for f in fpath]

# for f in fpath:
#     print(f)
#     p = gpt.read_product(path_and_file=f)
#     fmt_out = 'GeoTIFF-BigTIFF'
#     gpt.write_product(p, f_out='s1_ifgcoh', fmt_out=fmt_out, p_out=path_out)


# --- covariance matrix
db_url = 'mysql://root:br12Fol!@127.0.0.1/DB_MOUNTS'
disk_engine = create_engine(db_url)
subset_wkt = 'POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))'

stmt = '''
    SELECT *
    FROM DB_MOUNTS.archive
    WHERE target_id = '221080'
        and acqstarttime >= '2016-01-01'
        and acqstarttime < '<2018-01-01'
        and orbitdirection = 'DESCENDING'
        and polarization = 'VH VV'
        # and ( polarization = 'VV' OR polarization = 'VH VV')
    ORDER BY acqstarttime ASC
'''
df_S1 = pd.read_sql(stmt, disk_engine)

stmt = '''
    SELECT *
    FROM DB_MOUNTS.archive
    WHERE target_id = '221080'
        and acqstarttime >= '2016-01-01' #leave timspan broader because must look for images
        and acqstarttime < '2018-01-01'
        and producttype = 'S2MSI1C'
    ORDER BY acqstarttime ASC
'''
df_S2 = pd.read_sql(stmt, disk_engine)


# --- get pairs of S1 with closest S2
pairs = []
for index, row in df_S1.iterrows():

    # - search for closest S2 image to s1 date
    date_s1 = row['acqstarttime']
    idx = np.argmin(abs(date_s1 - df_S2.acqstarttime))
    s2_closest = df_S2.iloc[idx]
    dt = s2_closest['acqstarttime'] - date_s1
    dt_hours = dt.total_seconds() / (3600 * 24)
    dt_hours_rnd = round(dt_hours, 1)

    if abs(dt_hours) > dt_thresh_h:
        continue

    if idx >= 0:
        s2_closest_prec = df_S2.iloc[idx - 1]
    else:
        print('cannot find S2 image preceding the closest S1/S2 match')

    print('----')
    print('S1: ' + str(date_s1))
    print('S2: ' + str(s2_closest['acqstarttime']))
    print('dt: ' + str(dt_hours_rnd))
    print('S2 prec: ' + str(s2_closest_prec['acqstarttime']))

    pairs.append([row['abspath'], s2_closest['abspath'], s2_closest_prec['abspath'], dt_hours_rnd])

# print df_S1['acqstarttime']
print df_S1['acqstarttime']
print len(pairs)
sys.exit()

# --- process
for k, r in enumerate(pairs[start_idx::], start=start_idx):

    s1_abspath = r[0]
    s2_abspath = r[1]
    s2p_abspath = r[2]
    dt_hours = r[3]
    s1_fname = os.path.basename(s1_abspath)
    s2_fname = os.path.basename(s2_abspath)

    print '---'
    print 'idx ' + str(k)
    print 'S1 = ' + s1_fname
    print 'S2 = ' + s2_fname
    print 'dt = ' + str(dt_hours)
    # import sys
    # sys.exit()

    # s2 product
    # =====================================================================
    s2 = gpt.read_product(path_and_file=s2_abspath)
    s2 = gpt.resample(s2, referenceBand='B2')
    s2_metadata = gpt.get_metadata_S2(s2)
    # subset_bounds = s2_metadata['footprint']

    # s1 product
    # =====================================================================
    s1 = gpt.read_product(path_and_file=s1_abspath)
    s1_metadata = gpt.get_metadata_S1(s1)
    # s1 = gpt.topsar_split(s1, subswath='IW2', polarisation='VV')
    s1 = gpt.apply_orbit_file(s1)
    s1 = gpt.deburst(s1)

    # --- get intensity bands
    # sourceBands = ['Intensity_IW2_VV']
    bdnames = gpt.get_bandnames(s1, print_bands=0)
    s1_intbands = [bdnames[idx] for idx, dbname in enumerate(bdnames) if 'Intensity_' in dbname]
    s1 = gpt.terrain_correction(s1, s1_intbands)

    # --- get polarimetric covariance matrix
    s1_bis = gpt.read_product(path_and_file=s1_abspath)
    s1_bis = gpt.deburst(s1_bis)
    polmat = gpt.polarimetric_matrix(s1_bis)
    polmat = gpt.apply_orbit_file(polmat)
    gpt.get_bandnames(polmat, print_bands=0)
    polmat_bands = ['C11', 'C12_real', 'C12_imag', 'C22']
    polmat = gpt.terrain_correction(polmat, polmat_bands)

    # collocate s1/s2
    # =====================================================================
    # NB1: 1st arg = master (pixel conserved), 2nd arg = slave (pixel resampled on master grid)
    # NB2: Default renaming of bands in collocated product: master components = ${ORIGINAL_NAME}_M, slave components = ${ORIGINAL_NAME}_S
    # NB3: export 'collocation_flags' to avoid error when opening in SNAP Desktop
    p = gpt.collocate(polmat, s2)  # >> master=S1, slave=S2
    gpt.get_bandnames(p, print_bands=0)
    p = gpt.subset(p, geoRegion=subset_wkt)

    # --- compute C12_ampl + add to polmat product (band math + merge)
    # bandmath_expression = 'ampl(C12_real, C12_imag)'
    # targetband_name = 'C12_ampl'
    # p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name=targetband_name)
    # polmat = gpt.merge(polmat, p_new)
    # gpt.get_bandnames(polmat, print_bands=1)

    # export to tiff
    # =====================================================================

    fmt_out = 'GeoTIFF-BigTIFF'

    # --- create result folder
    pair_name = 'S1-' + s1_metadata['acqstarttime_str'] + '_S2-' + s2_metadata['acqstarttime_str'] + '_dtH' + str(int(dt_hours * 24))
    dir_pair = path_out + pair_name
    if not os.path.exists(dir_pair):
        os.makedirs(dir_pair)
        print 'Directory "' + dir_pair + '" created'

    # --- export s1 covariance matrix elements
    prod2export_s1 = gpt.band_select(p, sourceBands=['C11_M', 'C12_real_M', 'C12_imag_M', 'C22_M', 'collocation_flags'])
    if export_s1:
        print('  | exporting s1.tif')
        gpt.get_bandnames(p, print_bands=None)
        gpt.write_product(prod2export_s1, f_out='s1', fmt_out=fmt_out, p_out=dir_pair)

    # --- export s2 bands
    prod2export_s2 = gpt.band_select(p, sourceBands=['B2_S', 'B3_S', 'B4_S', 'B8_S', 'collocation_flags'])
    if export_s2:
        print('  | exporting s2.tif')
        gpt.get_bandnames(prod2export_s2, print_bands=None)
        gpt.write_product(prod2export_s2, f_out='s2', fmt_out=fmt_out, p_out=dir_pair)

    # --- export mask to tiff (available only if S2 is a L2 product)
    # =====================================================================
    # mask_name = 'cirrus_clouds_10m'
    # p_new = gpt.band_maths(s2, expression=mask_name, targetband_name=mask_name)
    # fmt_out = 'GeoTIFF'  # NB: error when opening 'GeoTIFF-BigTIFF' format?
    # gpt.write_product(p_new, f_out=mask_name, fmt_out=fmt_out, p_out=dir_pair)

    # export ifg+coh
    # =====================================================================
    if export_ifgcoh:
        print('  | exporting s1_ifgcoh.tif')
        day_ifgslave = s1_metadata['acqstarttime_str'][0:8]
        f = '/home/sebastien/DATA/data_snap/*_{}*.dim'.format(day_ifgslave)
        fpath = glob.glob(f)
        if fpath:
            p_pcssed = gpt.read_product(path_and_file=fpath[0])
            gpt.write_product(p_pcssed, f_out='s1_ifgcoh', fmt_out=fmt_out, p_out=dir_pair)
        else:
            print('No DIM file found for this product: cannot export ifg+coh')

    # export S2 prec (collocated with S1)
    # =====================================================================
    s2p = gpt.read_product(path_and_file=s2p_abspath)
    s2p = gpt.resample(s2p, referenceBand='B2')
    s2p_metadata = gpt.get_metadata_S2(s2p)
    pp = gpt.collocate(polmat, s2p)  # >> master=S1, slave=S2
    gpt.get_bandnames(pp, print_bands=0)
    pp = gpt.subset(pp, geoRegion=subset_wkt)
    prod2export_s2p = gpt.band_select(pp, sourceBands=['B2_S', 'B3_S', 'B4_S', 'B8_S', 'collocation_flags'])
    if export_s2p:
        print('  | exporting s2_prec.tif')
        gpt.get_bandnames(prod2export_s2p, print_bands=None)
        gpt.write_product(prod2export_s2p, f_out='s2_prec', fmt_out=fmt_out, p_out=dir_pair)

    # save metadata as yml file
    # =====================================================================
    if save_metadata:
        utilme.save_dict2xml(s1_metadata, f_out='s1_metadata.xml', p_out=dir_pair)
        utilme.save_dict2xml(s2_metadata, f_out='s2_metadata.xml', p_out=dir_pair)
        utilme.save_dict2xml(s2p_metadata, f_out='s2p_metadata.xml', p_out=dir_pair)

    # plot png
    # =====================================================================
    if plot_png:
        gpt.plotBands_rgb(prod2export_s2, bname_red='B4_S', bname_green='B3_S', bname_blue='B2_S', f_out='s2', p_out=dir_pair)
        gpt.plotBands_rgb(prod2export_s2p, bname_red='B4_S', bname_green='B3_S', bname_blue='B2_S', f_out='s2_prec', p_out=dir_pair)

        # --- compute C12_ampl (band math + merge)
        # TODO: subset first
        # bandmath_expression = 'ampl(C12_real, C12_imag)'
        # targetband_name = 'C12_ampl'
        # p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name=targetband_name)
        # polmat = gpt.merge(polmat, p_new)
        # gpt.get_bandnames(polmat, print_bands=None)
        # gpt.plotBands_rgb(polmat, bname_red='C11', bname_green='C22', bname_blue='C12_ampl', f_out='s1', p_out=dir_pair)

    # free memory (attempts...)
    # =====================================================================
    # --- dispose => Releases all of the resources used by this object instance and all of its owned children.
    print('Product dispose (release all resources used by object)')
    s1.dispose()
    s2.dispose()
    s1_bis.dispose()
    polmat.dispose()
    p.dispose()
    prod2export_s1.dispose()
    prod2export_s2.dispose()
    pp.dispose()
    prod2export_s2p.dispose()
    # --- call garbage collector (http://forum.step.esa.int/t/closing-destroying-files-after-reading-writing/2730/5)
    # from snappy import jpy
    # System = jpy.get_type('java.lang.System')
    # System.gc()
    # import time
    # time.sleep(60)

    # TODO: try delay, maybe garbage collector needs time?
    # TODO: Try closing entire java VM ?
    # TODO: monitor memory when script runned from command line, maybe SublimeText build system is messing things up?

    # import time
    # time.sleep(60 * 15)

    # if k == start_idx + 1:
    #     print('idx stopped at ' + str(k))
    #     import sys
    #     sys.exit()

    # print 'FINISHED'
    # import sys
    # sys.exit()
