import utilityme as utils
import snapme as gpt
import os

dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')

# --- get dataset
# dat = dbo.get_dataset(dbname='DB_ARCHIVE', tbname='ertaale')
# print dat[0][1]
# rows = dbo.get_dataset(dbname='DB_ARCHIVE', tbname='ertaale')
# for r in rows:
#     print(r.title, r.abspath)

# --- open product
# p = gpt.read_product(path_and_file=dat[0][1])
# print p.getName()

# --- select products
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE SUBSTRING(title,13,4) = '1SDV'"
# stmt = "SELECT * FROM DB_ARCHIVE.etna WHERE orbitdirection = 'DESCENDING' AND polarization = 'VH VV';"

# --- processing options
volcanoname = 'ertaale'
subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale
pathout_root = '/home/sebastien/DATA/data_sar2opt_tmp/'

# --- select s1/s2 pairs from mysql
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and orbitdirection = 'DESCENDING' ORDER BY acqstarttime ASC"
# rows = dbo.execute_query(stmt)
# dat = rows.all()
# # --- check selection
# for r in dat:
#     print(r.title, r.orbitdirection)

# --- select s1/s2 pairs
s1_path = '/home/sebastien/DATA/data_satellite/ertaale/'
s2_path = '/home/sebastien/DATA/data_satellite/ertaale_S2/'
pairs = []
pairs.append(['S1A_IW_SLC__1SDV_20170209T030748_20170209T030815_015201_018E37_C90D.zip', 'S2A_MSIL1C_20170208T074051_N0204_R092_T37PFR_20170208T075343.zip'])
pairs.append(['S1A_IW_SLC__1SDV_20170221T030748_20170221T030815_015376_0193AB_1E98.zip', 'S2A_MSIL1C_20170218T073941_N0204_R092_T37PFR_20170218T074453.zip'])
pairs.append(['S1A_IW_SLC__1SDV_20170329T030748_20170329T030815_015901_01A365_CE26.zip', 'S2A_MSIL1C_20170330T073611_N0204_R092_T37PFR_20170330T075431.zip'])
pairs.append(['S1A_IW_SLC__1SDV_20170410T030748_20170410T030815_016076_01A8A7_CF75.zip', 'S2A_MSIL1C_20170409T075211_N0204_R092_T37PFR_20170409T075210.zip'])
pairs.append(['S1A_IW_SLC__1SDV_20170528T030751_20170528T030818_016776_01BE07_93C5.zip', 'S2A_MSIL1C_20170529T073611_N0205_R092_T37PFR_20170529T075550.zip'])
pairs.append(['S1A_IW_SLC__1SDV_20170609T030752_20170609T030819_016951_01C371_4390.zip', 'S2A_MSIL1C_20170608T075211_N0205_R092_T37PFR_20170608T075214.zip'])
pairs.append(['S1A_IW_SLC__1SDV_20170808T030755_20170808T030822_017826_01DE1E_BCCE.zip', 'S2A_MSIL1C_20170807T075211_N0205_R092_T37PFR_20170807T075214.zip'])

start_idx = 0
print 'starting from idx ' + str(start_idx)

for k, r in enumerate(pairs, start=start_idx):

    s1_fname = pairs[k][0]
    s2_fname = pairs[k][1]

    print '---'
    print 'idx ' + str(k)
    print 'S1 = ' + s1_fname
    print 'S2 = ' + s2_fname
    # import sys
    # sys.exit()

    # s1 product
    # =====================================================================

    s1_abspath = s1_path + s1_fname
    s1 = gpt.read_product(path_and_file=s1_abspath)
    # s1 = gpt.topsar_split(s1, subswath='IW2', polarisation='VV')
    s1 = gpt.apply_orbit_file(s1)
    s1 = gpt.deburst(s1)

    # --- get intensity bands
    # sourceBands = ['Intensity_IW2_VV']
    bdnames = gpt.get_bandnames(s1, print_bands=1)
    s1_intbands = [bdnames[idx] for idx, dbname in enumerate(bdnames) if 'Intensity_' in dbname]
    s1 = gpt.terrain_correction(s1, s1_intbands)

    # --- get polarimetric covariance matrix
    s1_bis = gpt.read_product(path_and_file=s1_abspath)
    s1_bis = gpt.deburst(s1_bis)
    polmat = gpt.polarimetric_matrix(s1_bis)
    polmat = gpt.apply_orbit_file(polmat)
    gpt.get_bandnames(polmat, print_bands=1)
    polmat_bands = ['C11', 'C12_real', 'C12_imag', 'C22']
    polmat = gpt.terrain_correction(polmat, polmat_bands)
    polmat = gpt.subset(polmat, **subset_bounds)

    # --- compute C12_ampl (band math + merge)
    bandmath_expression = 'ampl(C12_real, C12_imag)'
    targetband_name = 'C12_ampl'
    p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name=targetband_name)
    polmat = gpt.merge(polmat, p_new)
    # gpt.get_bandnames(polmat, print_bands=1)

    # --- normalize bands (cf Andy)
    # WARNING: not working because all the methods in the Band Maths are pixel-based. They don't compute values for a whole band.
    # bandmath_expression = '(log(C11)-avg(log(C11)))/stddev(log(C11))'
    # p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name='C11_norm')
    # polmat = gpt.merge(polmat, p_new)
    # bandmath_expression = '(log(C22)-avg(log(C22)))/stddev(log(C22))'
    # p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name='C22_norm')
    # polmat = gpt.merge(polmat, p_new)
    # bandmath_expression = '(log(C12_ampl)-avg(log(C12_ampl)))/stddev(log(C12_ampl))'
    # p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name='C12_ampl_norm')
    # polmat = gpt.merge(polmat, p_new)
    # gpt.get_bandnames(polmat, print_bands=1)

    # --- merge products (not working, likely because of different product sizes)
    # http://forum.step.esa.int/t/product-sourceproduct-is-not-compatible-to-master-product/1761/6
    # s1_full = gpt.merge(s1, polmat)

    # s2 product
    # =====================================================================

    s2_abspath = s2_path + s2_fname
    s2 = gpt.read_product(path_and_file=s2_abspath)
    s2 = gpt.resample(s2, referenceBand='B2')

    # collocate s1/s2 + subset
    # =====================================================================

    p = gpt.collocate(s1, s2)
    gpt.get_bandnames(p, print_bands=0)
    p = gpt.subset(p, **subset_bounds)

    # plot + export
    # =====================================================================

    # --- get metadata
    s1_metadata = gpt.get_metadata_abstracted(s1)
    s2_metadata_time = s2.getMetadataRoot().getElement('Level-1C_DataStrip_ID').getElement('General_Info').getElement('Datastrip_Time_Info').getAttributeString('DATASTRIP_SENSING_START')
    from dateutil.parser import parse
    s2_metadata_time = parse(s2_metadata_time).strftime('%Y%m%dT%H%M%S')
    pair_name = 'S1_' + s1_metadata['acqstarttime_str'] + '__S2_' + s2_metadata_time

    # --- create result folder
    dir_pair = pathout_root + pair_name
    if not os.path.exists(dir_pair):
        os.makedirs(dir_pair)
        print 'Directory "' + dir_pair + '" created'

    # --- plot covariance matrices
    s1cov_fout = [s1_metadata['acqstarttime_str'] + '_Covmat_' + s for s in polmat_bands]
    gpt.plotBands(polmat, band_name=polmat_bands, f_out=s1cov_fout, p_out=dir_pair)

    # --- plot single bands (in collocated product)
    # s1_bnames = ['Intensity_IW2_VV_M']
    bdnames = gpt.get_bandnames(p, print_bands=0)
    p_intbands = [bdnames[idx] for idx, dbname in enumerate(bdnames) if 'Intensity_' in dbname]
    s1_bnames = p_intbands
    s1_fout = [s1_metadata['acqstarttime_str'] + '_' + '_'.join(s.split('_')[:2]) for s in s1_bnames]
    s2_bnames = ['B2_S', 'B3_S', 'B4_S', 'B5_S', 'B6_S', 'B7_S', 'B8_S', 'B8A_S', 'B11_S', 'B12_S']
    # s2_bnames = ['B2_S', 'B3_S', 'B4_S', 'B8A_S', 'B11_S', 'B12_S']
    # s2_bnames = ['B1_S', 'B2_S', 'B3_S', 'B4_S', 'B5_S', 'B6_S', 'B7_S', 'B8_S', 'B8A_S', 'B9_S', 'B10_S', 'B11_S', 'B12_S']
    s2_fout = [s2_metadata_time + '_' + s.split('_')[0] for s in s2_bnames]
    bnames = s1_bnames + s2_bnames
    bnames_fout = s1_fout + s2_fout
    gpt.plotBands(p, band_name=bnames, f_out=bnames_fout, p_out=dir_pair)

    # --- plot rgb bands (in collocated product)
    bname_red = 'B12_S'
    bname_green = 'B11_S'
    bname_blue = 'B8A_S'
    f_out = s2_metadata_time + '_' + bname_red.split('_')[0] + bname_green.split('_')[0] + bname_blue.split('_')[0]
    gpt.plotBands_rgb(p, bname_red=bname_red, bname_green=bname_green, bname_blue=bname_blue, f_out=f_out, p_out=dir_pair)

    bname_red = 'B4_S'
    bname_green = 'B3_S'
    bname_blue = 'B2_S'
    f_out = s2_metadata_time + '_' + bname_red.split('_')[0] + bname_green.split('_')[0] + bname_blue.split('_')[0]
    gpt.plotBands_rgb(p, bname_red=bname_red, bname_green=bname_green, bname_blue=bname_blue, f_out=f_out, p_out=dir_pair)

    # --- export
    fmt_out = 'GeoTIFF-BigTIFF'
    for k, bd_name in enumerate(bnames):
        bd = gpt.band_select(p, sourceBands=[bd_name])
        gpt.write_product(bd, f_out=bnames_fout[k], fmt_out=fmt_out, p_out=dir_pair)
        bd.dispose()

    for k, bd_name in enumerate(polmat_bands):
        bd = gpt.band_select(polmat, sourceBands=[bd_name])
        gpt.write_product(bd, f_out=s1cov_fout[k], fmt_out=fmt_out, p_out=dir_pair)
        bd.dispose()

    # --- plot rgb of radar as R=C11, G=C22, B=ampl(C12_real, C12_imag)
    f_out = s1_metadata['acqstarttime_str'] + '_C11C22C12'
    gpt.plotBands_rgb(polmat, bname_red='C11', bname_green='C22', bname_blue='C12_ampl', f_out=f_out, p_out=dir_pair)
    # gpt.plotBands_rgb(polmat, bname_red='C11', bname_green='C22', bname_blue='C12_real', f_out=f_out, p_out=dir_pair)

    # --- dispose => Releases all of the resources used by this object instance and all of its owned children.
    print('Product dispose (release all resources used by object)')
    s1.dispose()
    s2.dispose()
    s1_bis.dispose()
    polmat.dispose()
    p.dispose()

    # import sys
    # sys.exit()
