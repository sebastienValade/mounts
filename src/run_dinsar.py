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
subswath = 'IW2'
polarization = 'VV'

# >> etna
stmt = "SELECT * FROM DB_ARCHIVE.etna WHERE orbitdirection = 'DESCENDING' AND polarization = 'VH VV';"
# subset_bounds = {'north_bound': 37.7, 'west_bound': 14.95, 'south_bound': 37.6, 'east_bound': 15.05}  # >> etna
# subset_bounds = {'north_bound': 37.8, 'west_bound': 15.09, 'south_bound': 37.63, 'east_bound': 15.29}  # >> etna coast

# >> ertaale
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE SUBSTRING(title,13,4) = '1SDV'"
# subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735}  # >> ertaale


rows = dbo.execute_query(stmt)
dat = rows.all()

# --- check selection
# for r in dat:
#     print(r.title, r.orbitdirection)
# import sys
# sys.exit()

start_idx = 0
# ertaale start_idx = 4, 7, 11, 14, 17
print 'starting from idx ' + str(start_idx)

for k, r in enumerate(dat, start=start_idx):

    if k >= len(dat) - 1:
        break

    print '---'
    print 'MASTER = ' + dat[k].title + ' (' + dat[k].orbitdirection + ')'
    print 'SLAVE = ' + dat[k + 1].title + ' (' + dat[k].orbitdirection + ')'
    # import sys
    # sys.exit()

    # --- read master product
    master_abspath = dat[k].abspath
    m = gpt.read_product(path_and_file=master_abspath)

    # --- debug
    # p = gpt.read_product(path_and_file=master_abspath)
    # p = gpt.topsar_split(p, subswath=subswath, polarisation=polarization)
    # bdnames = gpt.get_bandnames(p, print_bands=1)
    # p = gpt.resample(p, referenceBand='Intensity_IW2_VV')
    # p = gpt.subset(p, **subset_bounds)
    # band = gpt.plotBand(p, ['Intensity_IW2_VV'], cmap=['binary_r'])
    # p.dispose()
    # import sys
    # sys.exit()

    # --- read slave product
    slave_abspath = dat[k + 1].abspath
    s = gpt.read_product(path_and_file=slave_abspath)

    # --- split product
    m = gpt.topsar_split(m, subswath=subswath, polarisation=polarization)
    s = gpt.topsar_split(s, subswath=subswath, polarisation=polarization)

    # --- apply orbit file
    m = gpt.apply_orbit_file(m)
    s = gpt.apply_orbit_file(s)

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
    # => zona lago lentini
    # polygon_wkt = 'POLYGON((14.890766 37.417891, 15.117702 37.417891, 15.117702 37.289897, 14.890766 37.289897, 14.890766 37.417891))'
    # p = gpt.subset(p, geoRegion=polygon_wkt)37.289897
    # => zona lago lentini - etna
    # subset_bounds = {'north_bound': 37.8, 'west_bound': 14.890766, 'south_bound': 37.289897, 'east_bound': 15.117702}
    # => zona etna extra large
    # subset_bounds = {'north_bound': 38, 'west_bound': 14.7, 'south_bound': 37.5, 'east_bound': 15.2}
    subset_bounds = {'north_bound': 37.9, 'west_bound': 14.78, 'south_bound': 37.5, 'east_bound': 15.2}
    p = gpt.subset(p, **subset_bounds)

    # --- plot
    band = gpt.plotBand(p, sourceBands, cmap=['gist_rainbow', 'binary_r'])

    # Dispose => Releases all of the resources used by this object instance and all of its owned children.
    print('Product dispose (release all resources used by object)')
    p.dispose()

    # import sys
    # sys.exit()
