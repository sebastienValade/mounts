import snapme as gpt

# --- set logging behaviour
# NB: logging behaviour overwritten in file: /usr/local/lib/python2.7/dist-packages/snappy/jpyutil.py
# logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
# logging.info('>> script started')

# --- print engine configuration
# NB: engine configuration set in file 'snap.properties'. Read in order of priority:
# 1) user setting: $HOME/.snap/etc/snap.properties
# 2) default setting: $SNAP/etc/snap.properties
# gpt.print_engineConfig()


# =============================================
# DINSAR processing chain
# =============================================

# # --- read main product
# main_abspath = '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.zip'
# m = gpt.read_product(path_and_file=main_abspath)

# # --- read subordinate product
# # subordinate_abspath = '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.SAFE.zip'
# subordinate_abspath = '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.zip'
# s = gpt.read_product(path_and_file=subordinate_abspath)

# # --- split product
# subswath = 'IW2'
# polarization = 'VV'
# m = gpt.topsar_split(m, subswath=subswath, polarisation=polarization)
# s = gpt.topsar_split(s, subswath=subswath, polarisation=polarization)

# # --- apply orbit file
# m = gpt.apply_orbit_file(m)
# s = gpt.apply_orbit_file(s)

# # --- back-geocoding
# p = gpt.back_geocoding(m, s)

# # --- interferogram
# p = gpt.interferogram(p)

# # --- deburst
# p = gpt.deburst(p)

# # --- topographic phase removal
# p = gpt.topo_phase_removal(p)

# # --- phase filtering
# p = gpt.goldstein_phase_filtering(p)

# # --- terrain correction (geocode)
# bdnames = gpt.get_bandnames(p, print_bands=1)
# idx_phase = [idx for idx, dbname in enumerate(bdnames) if 'Phase_' in dbname][0]
# idx_coh = [idx for idx, dbname in enumerate(bdnames) if 'coh_' in dbname][0]
# sourceBands = [bdnames[idx_phase], bdnames[idx_coh]]
# p = gpt.terrain_correction(p, sourceBands)

# # --- subset
# subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale
# p = gpt.subset(p, **subset_bounds)

# # --- plot
# # gpt.plotBands_np(p, sourceBands, cmap=['gist_rainbow', 'binary_r'])
# gpt.plotBands(p, sourceBands)

# # --- dispose (releases all of the resources used by this object instance and all of its owned children)
# p.dispose()

# =============================================
# INTERFEROMETY various
# =============================================

# --- compute polarimetric covariance matrix (need polarimetric data => SDV)
# abspath = '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SDV_20170209T030748_20170209T030815_015201_018E37_C90D.zip'
# p = gpt.read_product(path_and_file=abspath)
# pdb = gpt.deburst(p)
# polmat = gpt.polarimetric_matrix(pdb, matrix='C2')
# polmat = gpt.apply_orbit_file(polmat)
# sourceBands = ['C11', 'C12_real', 'C12_imag', 'C22']
# # NB: C11 and C22 = diagonal elts of the cov matrix, corresponding to the intensities of the two polarimetric images (intensity_VV, intensity_HV)
# # C12_real and C12_imag denote respectively real and imaginary parts of the complex element of the covariance matrix (non diagonal element)
# polmat = gpt.terrain_correction(polmat, sourceBands)
# polmat = gpt.subset(polmat, **subset_bounds)
# gpt.plotBands(polmat, band_name=sourceBands)

# --- speckle filter
# sourceBands = ['Intensity_IW2_VV']
# p = gpt.speckle_filter(p, sourceBands)

# =============================================
# PLOT
# =============================================

# # --- plot RGB from S2 data
# file_abspath = '/home/sebastien/DATA/data_satellite/ertaale_S2/S2A_MSIL1C_20170129T075211_N0204_R092_T37PFR_20170129T075205.zip'
# p = gpt.read_product(path_and_file=file_abspath)
# p = gpt.resample(p, referenceBand='B2')
# p = gpt.subset(p, geoRegion='POLYGON((40.611005 13.654661, 40.712886 13.654661, 40.712886 13.555889, 40.611005 13.555889, 40.611005 13.654661))')
# gpt.plotBands_rgb(p)
# gpt.plotBands_rgb(p, bname_red='B12', bname_green='B11', bname_blue='B8A')  # -> cf S2 image erta ale 2017-03-30

# # --- collocate products (S1 geocoded = main, S2 not-geocoded = subordinate)
# s1_abspath = '/home/sebastien/DATA/data_satellite/ertaale/S1A_IW_SLC__1SDV_20170410T030748_20170410T030815_016076_01A8A7_CF75.zip'
# s1 = gpt.read_product(path_and_file=s1_abspath)
# s1 = gpt.topsar_split(s1, subswath=subswath, polarisation=polarization)
# s1 = gpt.apply_orbit_file(s1)
# s1 = gpt.deburst(s1)
# s1 = gpt.terrain_correction(s1, ['Intensity_IW2_VV'])
# s1 = gpt.subset(s1, **subset_bounds)
#
# s2_abspath = '/home/sebastien/DATA/data_satellite/ertaale_S2/S2A_MSIL1C_20170409T075211_N0204_R092_T37PFR_20170409T075210.zip'
# s2 = gpt.read_product(path_and_file=s2_abspath)
# s2 = gpt.resample(s2, referenceBand='B2')
#
# p = gpt.collocate(s1, s2)
#
# gpt.get_bandnames(p, print_bands=1)
# subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale
# p = gpt.subset(p, **subset_bounds)
# gpt.plotBands(p, band_name='Intensity_IW2_VV_M')
# gpt.plotBands_rgb(p, bname_red='B12_S', bname_green='B11_S', bname_blue='B8A_S')  # -> cf S2 image erta ale 2017-03-30

# =============================================
# MISC
# =============================================

# --- band math to compute amplitude from real/imag
# bandmath_expression = 'ampl(C12_real, C12_imag)'
# targetband_name = 'C12_ampl'
# p_new = gpt.band_maths(polmat, expression=bandmath_expression, targetband_name=targetband_name)
# polmat = gpt.merge(polmat, p_new)

# --- run processing graph (operators defined in configuration file)
# config_file = './conf/config_processing.yml'
# gpt.graph_processing(config_file)

# --- write product
# gpt.write_product(p, p_out='home/sebastien/DATA/')

# --- extract band(s) from product
# bd = gpt.band_select(p, sourceBands=['Intensity_IW2_VV'])

# --- export
# fmt_out = 'GeoTIFF'
# f_out = 'mygeotiff'
# gpt.write_product(p, f_out=f_out, fmt_out=fmt_out)

# --- export all bands as distinct files
# fmt_out = 'GeoTIFF'
# bdnames = gpt.get_bandnames(p, print_bands=1)
# bnames_intensity = [bdnames[idx] for idx, dbname in enumerate(bdnames) if 'Intensity_' in dbname]
# for k, bd_name in enumerate(bnames_intensity):
#     bd = gpt.band_select(p, sourceBands=[bd_name])
#     gpt.write_product(bd, fmt_out=fmt_out)
#     bd.dispose()

# --- resample
# p = gpt.resample(p, referenceBand='Intensity_IW2_VV')

# --- print band names
# gpt.get_bandnames(p, print_bands=1)

# --- print raster dimensions
# gpt.print_rasterDim(p, 'Phase_ifg_IW2_VV_11Jan2017_11Jan2017')


# MASKS
# ---------------------------------------------

# # --- get list of masks in product
# gpt.get_masknames(p, print_masks=1)

# # --- compute masks from idepix
# p = gpt.read_product(path_and_file=p + f)
# p = gpt.resample(p, referenceBand='B2')
# p = gpt.idepix_sentinel2(p)

# # --- export mask
# # NB: must use band math to convert mask to band, then export/plot
# mask_2export = ['IDEPIX_CLOUD', 'IDEPIX_INVALID', 'IDEPIX_CLOUD_SHADOW']
# gpt.export_mask(p, mask_2export=mask_2export, fmt_out='png', f_out=f_out, p_out=p_out)

# # --- convert masks to bands
# s2 = gpt.idepix_sentinel2(s2)
# masknames = ['IDEPIX_CLOUD', 'IDEPIX_INVALID', 'IDEPIX_CLOUD_SHADOW']
# pm = gpt.convert_mask2band(s2, maskname=masknames)  # >> convert masks into bands

# # --- export mask collocated
# # NB: order must be 1) compute masks, 2) convert masks into bands, 3) collocate the resulting product with polmat
# s2 = gpt.idepix_sentinel2(s2)  # >> compute masks
# masknames = ['IDEPIX_CLOUD', 'IDEPIX_INVALID', 'IDEPIX_CLOUD_SHADOW']
# pm = gpt.convert_mask2band(s2, maskname=masknames)  # >> convert masks into bands
# pm = gpt.collocate(polmat, pm)
# pm = gpt.subset(pm, geoRegion=subset_wkt)
# masknames_c = [m + '_S' for m in masknames]
# fname_out = ['s2mask_' + m.split('_', 1)[1] for m in masknames]
# gpt.plotBands(pm, band_name=masknames_c, f_out=fname_out, p_out=dir_pair, fmt_out='png')
