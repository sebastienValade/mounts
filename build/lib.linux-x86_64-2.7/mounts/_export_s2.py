import snapme as gpt

subset_wkt = 'POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))'

p = '/home/sebastien/DATA/data_satellite/ertaale/'
# f = 'S2A_MSIL1C_20170129T075211_N0204_R092_T37PFR_20170129T075205.zip'
f = 'S2A_MSIL1C_20170728T073611_N0205_R092_T37PFR_20170728T075430.zip'
f = 'S2A_OPER_PRD_MSIL1C_PDMC_20161001T130040_R092_V20161001T075212_20161001T075208.zip'

# --- open product
# ------------------------------------------
s2 = gpt.read_product(path_and_file=p + f)
s2 = gpt.resample(s2, referenceBand='B2')
s2 = gpt.subset(s2, geoRegion=subset_wkt)
s2_metadata = gpt.get_metadata_S2(s2)
# prod2export_s2 = gpt.band_select(s2, sourceBands=['B2', 'B3', 'B4', 'B8', 'collocation_flags'])
prod2export_s2 = s2

# --- export product
# ------------------------------------------
fmt_out = 'BEAM-DIMAP'  # 'GeoTIFF-BigTIFF'
f_out = '_'.join(['S2', s2_metadata['acqstarttime_str']])
p_out = '/home/sebastien/DATA/data_sar2opt/ertaale/'
gpt.get_bandnames(prod2export_s2, print_bands=None)
# gpt.write_product(prod2export_s2, f_out=f_out, fmt_out=fmt_out, p_out=p_out)


# --- get info on an operator options
# operator_name = 'Idepix.Sentinel2'
# from snappy import GPF
# GPF.getDefaultInstance().getOperatorSpiRegistry().loadOperatorSpis()
# op_spi = GPF.getDefaultInstance().getOperatorSpiRegistry().getOperatorSpi(operator_name)
# print('Op name:', op_spi.getOperatorDescriptor().getName())
# print('Op alias:', op_spi.getOperatorDescriptor().getAlias())
# param_Desc = op_spi.getOperatorDescriptor().getParameterDescriptors()
# for param in param_Desc:
#     print(param.getName(), "or", param.getAlias())

# import sys
# sys.exit()

# --- get masks
# ------------------------------------------
p = gpt.idepix_sentinel2(s2)

# --- get list of masks in product
mgrp = p.getMaskGroup()
print(list(mgrp.getNodeNames()))

# --- export masks as distinct tif files
mask_name = ['IDEPIX_CLOUD'] #, 'IDEPIX_INVALID', 'IDEPIX_CLOUD_SHADOW']
for m in mask_name:
    f_out = 's2mask_' + m.split('_')[1]
    print('  | Exporting mask ' + f_out)
    p_new = gpt.band_maths(p, expression=m, targetband_name='mask')
    gpt.write_product(p_new, f_out=f_out, fmt_out='GeoTIFF', p_out=p_out)
