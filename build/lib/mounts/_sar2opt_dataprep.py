import utilityme as utils
import snapme as gpt
import os
import pickle
import glob


# --- define s1/s2 pairs from available directories in path
path = '/home/sebastien/DATA/data_sar2opt/raw/poland/'
pairs = filter(lambda x: os.path.isdir(path + x), os.listdir(path))
path_out = '/home/sebastien/DATA/data_sar2opt/processed/poland/'

# --- define folders manually
# pairs = []
# pairs.append('S1-20170522T163459_S2-20170528T095031')
# pairs.append('S1-20170523T162638_S2-20170518T095031')
# pairs.append('S1-20170523T162638_S2-20170528T095031')
# pairs.append('S1-20170525T050002_S2-20170528T095031')

# new pairs
# pairs.append('S1-20170822T161848_S2-20170816T095031')
# pairs.append('S1-20170921T161843_S2-20170909T093031')
# pairs.append('S1-20171226T161842_S2-20171226T094359')
# pairs.append('S1-20170730T050048_S2-20170730T100031')  # s2 from paper, new s1

print pairs
# import sys
# sys.exit()

start_idx = 0
for k, r in enumerate(pairs[start_idx::], start=start_idx):

    s1_abspath = glob.glob(path + r + '/S1*.zip')[0]
    s2_abspath = glob.glob(path + r + '/S2*.zip')[0]
    s1_fname = os.path.basename(s1_abspath)
    s2_fname = os.path.basename(s2_abspath)

    print '---'
    print 'idx ' + str(k)
    print 'S1 = ' + s1_fname
    print 'S2 = ' + s2_fname
    # import sys
    # sys.exit()

    # s2 product
    # =====================================================================
    s2 = gpt.read_product(path_and_file=s2_abspath)
    s2 = gpt.resample(s2, referenceBand='B2')
    s2_metadata = gpt.get_metadata_S2(s2)
    subset_bounds = s2_metadata['footprint']

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

    # --- compute C12_ampl + add to polmat product (band math + merge)
    # bandmath_expression = 'ampl(C12_real, C12_imag)'
    # targetband_name = 'C12_ampl'
    # p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name=targetband_name)
    # polmat = gpt.merge(polmat, p_new)
    ## gpt.get_bandnames(polmat, print_bands=1)

    # collocate s1/s2
    # =====================================================================
    # NB1: 1st arg = master (pixel conserved), 2nd arg = slave (pixel resampled on master grid)
    # NB2: Default renaming of bands in collocated product: master components = ${ORIGINAL_NAME}_M, slave components = ${ORIGINAL_NAME}_S
    # NB3: export 'collocation_flags' to avoid error when opening in SNAP Desktop
    p = gpt.collocate(s2, polmat)
    gpt.get_bandnames(p, print_bands=0)
    p = gpt.subset(p, geoRegion=subset_bounds)

    # export to tiff
    # =====================================================================

    # fmt_out = 'GeoTIFF-BigTIFF'

    # # --- create result folder
    pair_name = 'S1-' + s1_metadata['acqstarttime_str'] + '_S2-' + s2_metadata['acqstarttime_str']
    dir_pair = path_out + pair_name
    # if not os.path.exists(dir_pair):
    #     os.makedirs(dir_pair)
    #     print 'Directory "' + dir_pair + '" created'

    # # --- export s1 covariance matrix elements
    # print('  | exporting s1.tif')
    # prod2export_s1 = gpt.band_select(p, sourceBands=['C11_S', 'C12_real_S', 'C12_imag_S', 'C22_S', 'collocation_flags'])
    # gpt.get_bandnames(prod2export_s1, print_bands=1)
    # gpt.write_product(prod2export_s1, f_out='s1', fmt_out=fmt_out, p_out=dir_pair)

    # # --- export s2 bands
    # print('  | exporting s2.tif')
    # prod2export_s2 = gpt.band_select(p, sourceBands=['B2_M', 'B3_M', 'B4_M', 'B8_M', 'collocation_flags'])
    # gpt.get_bandnames(prod2export_s2, print_bands=1)
    # gpt.write_product(prod2export_s2, f_out='s2', fmt_out=fmt_out, p_out=dir_pair)

    # # --- dispose => Releases all of the resources used by this object instance and all of its owned children.
    # print('Product dispose (release all resources used by object)')
    # s1.dispose()
    # s2.dispose()
    # s1_bis.dispose()
    # polmat.dispose()
    # p.dispose()
    # prod2export_s1.dispose()
    # prod2export_s2.dispose()

    # save metadata as yml file
    # =====================================================================
    # utils.save_dict2yaml(s1_metadata, f_out='s1_metadata.yml', p_out=dir_pair)
    # utils.save_dict2yaml(s2_metadata, f_out='s2_metadata.yml', p_out=dir_pair)

    utils.save_dict2xml(s1_metadata, f_out='s1_metadata.xml', p_out=dir_pair)
    utils.save_dict2xml(s2_metadata, f_out='s2_metadata.xml', p_out=dir_pair)

    # export mask to tiff (available only if L2 product)
    # =====================================================================
    # mask_name = 'cirrus_clouds_10m'
    # p_new = gpt.band_maths(s2, expression=mask_name, targetband_name=mask_name)
    # fmt_out = 'GeoTIFF'  # NB: error when opening 'GeoTIFF-BigTIFF' format?
    # gpt.write_product(p_new, f_out=mask_name, fmt_out=fmt_out, p_out=dir_pair)

    # TODO: try delay, maybe garbage collector needs time?
    # Try closing entire java VM ?

    # print 'FINISHED'
    # import sys
    # sys.exit()

    #import time
    #time.sleep(60 * 15)
