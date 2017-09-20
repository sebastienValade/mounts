import utilityme as utils
import snapme as gpt


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

# >> etna optns
# subswath = 'IW2'
# polarization = 'VV'
# volcanoname = 'etna'
# stmt = "SELECT * FROM DB_ARCHIVE.etna WHERE orbitdirection = 'DESCENDING' AND polarization = 'VH VV';"
## polygon_wkt = 'POLYGON((14.916129 37.344437, 14.979386 37.344437, 14.979386 37.306283, 14.916129 37.306283, 14.916129 37.344437))' #>> small region over lake for testing
# subset_bounds = {'north_bound': 37.9, 'west_bound': 14.8, 'south_bound': 37.59, 'east_bound': 15.2} 	#>> etna large view

# >> ertaale optns
subswath = 'IW2'
polarization = 'VV'
volcanoname = 'ertaale'
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE SUBSTRING(title,13,4) = '1SDV'"
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01'"
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01'"
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01' and orbitdirection = 'DESCENDING'"
stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01' and orbitdirection = 'DESCENDING' ORDER BY acqstarttime ASC"
stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01' and orbitdirection = 'ASCENDING' ORDER BY acqstarttime ASC"
subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale


rows = dbo.execute_query(stmt)
dat = rows.all()

# --- check selection
for r in dat:
    print(r.title, r.orbitdirection)
#import sys
#sys.exit()

start_idx = 0
# ertaale start_idx = 4, 7, 11, 14, 17
print 'starting from idx ' + str(start_idx)

for k, r in enumerate(dat, start=start_idx):

    if k >= len(dat) - 1:
        break

    print '---'
    print 'idx ' + str(k)
    print 'MASTER = ' + dat[k].title + ' (' + dat[k].orbitdirection + ')'
    print 'SLAVE = ' + dat[k + 1].title + ' (' + dat[k].orbitdirection + ')'
    # import sys
    # sys.exit()

    # --- read master product
    master_abspath = dat[k].abspath
    m = gpt.read_product(path_and_file=master_abspath)

    # --- read slave product
    slave_abspath = dat[k + 1].abspath
    s = gpt.read_product(path_and_file=slave_abspath)

    # --- split product
    m = gpt.topsar_split(m, subswath=subswath, polarisation=polarization)
    s = gpt.topsar_split(s, subswath=subswath, polarisation=polarization)

    # --- apply orbit file
    m = gpt.apply_orbit_file(m)
    s = gpt.apply_orbit_file(s)

    # -- subset giving pixel-coordinates to avoid DEM-assisted back-geocoding on full swath???
    # TODO: try!  
    
    # --- back-geocoding (TOPS coregistration)
    p = gpt.back_geocoding(m, s)

    # --- interferogram
    p = gpt.interferogram(p)

    # --- deburst
    p = gpt.deburst(p)

    # --- topographic phase removal
    p = gpt.topo_phase_removal(p)

    # --- phase filtering
    p = gpt.goldstein_phase_filtering(p)

    # --- terrain correction (geocoding)
    bdnames = gpt.get_bandnames(p, print_bands=1)
    idx_phase = [idx for idx, dbname in enumerate(bdnames) if 'Phase_' in dbname][0]
    idx_coh = [idx for idx, dbname in enumerate(bdnames) if 'coh_' in dbname][0]
    sourceBands = [bdnames[idx_phase], bdnames[idx_coh]]
    p = gpt.terrain_correction(p, sourceBands)

    # --- subset
    p = gpt.subset(p, **subset_bounds)
    # p = gpt.subset(p, geoRegion=polygon_wkt)

    # --- set output file name based on metadata
    metadata_master = gpt.get_metadata_abstracted(m)
    metadata_slave = gpt.get_metadata_abstracted(s)
    fnameout_band1 = metadata_master['acqstarttime_str'] + '_' + metadata_slave['acqstarttime_str'] + '_' + '_'.join(sourceBands[0].split('_')[0:3]) + '.png'
    fnameout_band2 = metadata_master['acqstarttime_str'] + '_' + metadata_slave['acqstarttime_str'] + '_' + '_'.join(sourceBands[1].split('_')[0:3]) + '.png'

    # --- plot
    gpt.plotBand(p, sourceBands, cmap=['gist_rainbow', 'binary_r'], f_out=[fnameout_band1, fnameout_band2])
    # gpt.write_image(p, band_name=bdnames[idx_coh], f_out=fnameout_band2)

    # --- dispose => Releases all of the resources used by this object instance and all of its owned children.
    print('Product dispose (release all resources used by object)')
    p.dispose()

    # --- store image file to DB_RESULTS
    # dict_val = {'title': fnameout_band1,
    #             'abspath': '/home/sebastien/Documents/MOUNTS/mounts/data' + fnameout_band1,
    #             'type': 'ifg',
    #             'mission': metadata_master['mission'],
    #             'orbitdirection': metadata_master['orbitdirection'],
    #             'relativeorbitnumber': metadata_master['relativeorbitnumber'],
    #             'acquisitionmode': metadata_master['acquisitionmode'],
    #             'acqstarttime': metadata_master['acqstarttime'],
    #             'polarization': metadata_master['polarization']}
    # dbo.dbres_loadfile(dict_val, tbname=volcanoname)

    # import sys
    # sys.exit()
