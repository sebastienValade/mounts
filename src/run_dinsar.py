import utilityme as utils
import snapme as gpt


dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')

printselection_without_processing = 1


# === INPUTS
volcanoname = 'ertaale'
subswath = 'IW2'
polarization = 'VV'
subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale

username = 'khola'
pathout_root = '/home/' + username + '/DATA/data_mounts/'

# === GET ARCHIVE
query_optns = {'target_name': volcanoname}
stmt = dbo.dbmounts_archive_querystmt(**query_optns)
# stmt = "SELECT * FROM DB_MOUNTS.archive WHERE target_name = '{}' ORDER BY orbitdirection, acqstarttime ASC".format(volcanoname)
rows = dbo.execute_query(stmt)
dat = rows.all()


# --- create master slave pairs (msp)
msp_ASC = [(x, y) for x, y in zip(dat[0::], dat[1::]) if x.orbitdirection == 'ASCENDING' and y.orbitdirection == 'ASCENDING']
msp_DSC = [(x, y) for x, y in zip(dat[0::], dat[1::]) if x.orbitdirection == 'DESCENDING' and y.orbitdirection == 'DESCENDING']
msp = msp_ASC + msp_DSC


if printselection_without_processing:
    print('=== Selected products (ordered by orbitdirection/acqstarttime):')
    for r in dat:
        print(r.title, r.orbitdirection)

    print('=== Created master/slave pairs:')
    for k, val in enumerate(msp):
        print('- pair ' + str(k + 1) + '/' + str(len(msp)))
        print '  master = ' + msp[k][0].title + ' ' + msp[k][0].orbitdirection
        print '  slave = ' + msp[k][1].title + ' ' + msp[k][1].orbitdirection

    import sys
    sys.exit()


# === PROCESS
start_idx = 0
print 'starting from idx ' + str(start_idx)

for k, r in enumerate(msp, start=start_idx):

    master_title = msp[k][0].title
    slave_title = msp[k][1].title
    master_id = str(msp[k][0].id)
    slave_id = str(msp[k][1].id)
    print '---'
    print 'idx ' + str(k)
    print 'MASTER = ' + master_title + ' (' + msp[k][0].orbitdirection + ')'
    print 'SLAVE = ' + slave_title + ' (' + msp[k][1].orbitdirection + ')'
    # import sys
    # sys.exit()

    # --- read master product
    master_abspath = msp[k][0].abspath
    m = gpt.read_product(path_and_file=master_abspath)

    # --- read slave product
    slave_abspath = msp[k][1].abspath
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
    metadata_master = gpt.get_metadata_S1(m)
    metadata_slave = gpt.get_metadata_S1(s)
    fnameout_band1 = '_'.join([metadata_master['acqstarttime_str'], metadata_slave['acqstarttime_str'], subswath, polarization, 'ifg']) + '.png'
    fnameout_band2 = '_'.join([metadata_master['acqstarttime_str'], metadata_slave['acqstarttime_str'], subswath, polarization, 'coh']) + '.png'

    # --- plot
    p_out = pathout_root + volcanoname + '/'
    # gpt.plotBands_np(p, sourceBands, cmap=['gist_rainbow', 'binary_r'], f_out=[fnameout_band1, fnameout_band2])
    gpt.plotBands(p, sourceBands, f_out=[fnameout_band1, fnameout_band2], p_out=p_out)

    # --- dispose => Releases all of the resources used by this object instance and all of its owned children.
    print('Product dispose (release all resources used by object)')
    p.dispose()

    # --- store image file to database
    print('Store to DB_MOUNTS.results_img')
    dict_val = {'title': [fnameout_band1, fnameout_band2],
                'abspath': [p_out + fnameout_band1, p_out + fnameout_band2],
                'type': ['ifg', 'coh'],
                'id_master': [master_id, master_id],
                'id_slave': [slave_id, slave_id]}
    dbo.insert('DB_MOUNTS', 'results_img', dict_val)
