import utilityme as utils
import snapme as gpt


dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')

# >> ertaale optns
subswath = 'IW2'
polarization = 'VV'
volcanoname = 'ertaale'
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE SUBSTRING(title,13,4) = '1SDV'"
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01' and orbitdirection = 'DESCENDING'"
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01' and orbitdirection = 'DESCENDING' ORDER BY acqstarttime ASC"
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01' and orbitdirection = 'ASCENDING' ORDER BY acqstarttime ASC"
stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and orbitdirection = 'DESCENDING' ORDER BY acqstarttime ASC"
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and orbitdirection = 'ASCENDING' ORDER BY acqstarttime ASC"
subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale


rows = dbo.execute_query(stmt)
dat = rows.all()

# --- check selection
for r in dat:
    print(r.title, r.orbitdirection)
# import sys
# sys.exit()

start_idx = 0
# ertaale start_idx = 4, 7, 11, 14, 17
print 'starting from idx ' + str(start_idx)

for k, r in enumerate(dat, start=start_idx):

    title = dat[k].title
    print '---'
    print 'idx ' + str(k)
    print 'FILE = ' + title + ' (' + dat[k].orbitdirection + ')'
    # import sys
    # sys.exit()

    # --- read product
    abspath = dat[k].abspath
    p = gpt.read_product(path_and_file=abspath)

    # --- split product
    p = gpt.topsar_split(p, subswath=subswath, polarisation=polarization)

    # --- apply orbit file
    p = gpt.apply_orbit_file(p)

    # --- deburst
    p = gpt.deburst(p)

    # # --- terrain correction (geocoding)
    # bdnames = gpt.get_bandnames(p, print_bands=1)
    sourceBands = ['Intensity_' + subswath + '_' + polarization]
    # p = gpt.terrain_correction(p, sourceBands)

    # # --- speckle filter
    # sourceBands = ['Intensity_' + subswath + '_' + polarization]
    # p = gpt.speckle_filter(p, sourceBands)

    # --- subset
    p = gpt.subset(p, **subset_bounds)
    # p = gpt.subset(p, geoRegion=polygon_wkt)

    # --- set output file name based on metadata
    metadata = gpt.get_metadata_abstracted(p)
    # fnameout_band = '_'.join([metadata['acqstarttime_str'], subswath, polarization, 'int', 'filt']) + '.png'
    fnameout_band = '_'.join([metadata['acqstarttime_str'], subswath, polarization, 'int']) + '.png'

    # # --- plot
    gpt.plotBands(p, band_name=sourceBands, f_out=fnameout_band)

    # # --- export
    fmt_out = 'GeoTIFF'
    f_out = '_'.join([metadata['acqstarttime_str'], subswath, polarization, 'int'])
    # bd = gpt.band_select(p, sourceBands=sourceBands)
    # gpt.get_bandnames(bd, print_bands=1)
    # gpt.write_product(bd, f_out=f_out, fmt_out=fmt_out)
    gpt.write_product(p, f_out=f_out, fmt_out=fmt_out)

    # --- dispose => Releases all of the resources used by this object instance and all of its owned children.
    print('Product dispose (release all resources used by object)')
    p.dispose()

    import sys
    sys.exit()
