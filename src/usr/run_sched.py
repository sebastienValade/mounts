import fetchme
import logging
import utilityme as utils
import snapme as gpt


# --- set logging behaviour
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.info('>> script started')


# --- create database
# dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# dbo.delete_db('volcanoes')
# dbo.create_db('volcanoes')
# dicts = {'prod_title': 'TEXT', 'prod_abspath': 'TEXT'}
# dbo.create_tb(dbname='volcanoes', tbname='ertaale', dicts=dicts)

# --- empty table and populate manually with 2 files
# dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# dbo.empty_tb(dbname='volcanoes', tbname='ertaale')
# dicts = {'prod_title': 'S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703', 'prod_abspath': '/home/sebastien/DATA/data_satellite/ertaale_cron/S1A_IW_SLC__1SSV_20170111T152712_20170111T152739_014786_018145_5703.SAFE.zip'}
# dbo.insert(dbname='volcanoes', tbname='ertaale', dicts=dicts)
# dicts = {'prod_title': 'S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD', 'prod_abspath': '/home/sebastien/DATA/data_satellite/ertaale_cron/S1A_IW_SLC__1SSV_20170204T152711_20170204T152738_015136_018C0E_12BD.SAFE.zip'}
# dbo.insert(dbname='volcanoes', tbname='ertaale', dicts=dicts)
# dbo.print_dataset(dbname='volcanoes', tbname='ertaale')  # , colname='prod_title')

# # --- test writing in db
# from datetime import datetime
# dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# dicts = {'prod_title': datetime.now(), 'prod_abspath': datetime.now()}
# dbo.insert(dbname='volcanoes', tbname='ertaale', dicts=dicts)


# === (1) SCIHUB query
# -------------------------------------------------------------------------------
obj = fetchme.Scihub()
obj.query_auth('sebastien.valade', 'wave*worm')
conffile = '_config_ertaale.yml'
obj.read_configfile('./conf/' + conffile)
productlist = obj.scihub_search(export_result=None, print_url=1)
newprod = productlist[0]
logging.info('LASTEST PRODUCT AVAILABLE: ')
logging.info('  => ' + str(newprod.metadata.title) + ' (' + str(newprod.metadata.size) + ')')


# === (2) CHECK if new product has been downloaded yet
# -------------------------------------------------------------------------------
dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
dat = dbo.get_dataset(dbname='volcanoes', tbname='ertaale')
oldprod = dat[-1]

# - check if database is empty
if not dat:
    logging.info('0 product downloaded yet')
    oldprod_title = 'none'
else:
    oldprod_title = oldprod.prod_title

logging.info('LAST DOWNLOADED PRODUCT TITLE:')
logging.info('  => ' + oldprod_title)

# - download product
if oldprod_title != newprod.metadata.title:

    # --- download data
    logging.info('NEW PRODUCT TO DOWNLOAD !')

    # newprod.getQuicklook()
    newprod.getFullproduct()

    # --- update database
    logging.info('Updating Database with new downloaded product')
    dicts = {'prod_title': newprod.metadata.title, 'prod_abspath': newprod.path_and_file}
    dbo.insert(dbname='volcanoes', tbname='ertaale', dicts=dicts)
    dbo.print_dataset(dbname='volcanoes', tbname='ertaale')

else:
    logging.info('NOTHING NEW: waiting till next cron run.')
    quit()


# === (3) INTERFEROMETRIC processing chain
# -------------------------------------------------------------------------------
if len(dat) < 2:
    logging.info('2 products need to have been downloaded to perform interferometric processing.')
    quit()

dbo.print_dataset(dbname='volcanoes', tbname='ertaale')


# --- read master product
master_abspath = oldprod.prod_abspath
# master_abspath = dat[-2].prod_abspath
print master_abspath
m = gpt.read_product(path_and_file=master_abspath)

# --- read slave product
# s = gpt.read_product(newprod)
slave_abspath = dat[-1].prod_abspath
print slave_abspath
s = gpt.read_product(path_and_file=slave_abspath)

# --- split product
m = gpt.topsar_split(m, subswath='IW2')
s = gpt.topsar_split(s, subswath='IW2')

# --- apply orbit file
m = gpt.apply_orbit_file(m)
s = gpt.apply_orbit_file(s)

# --- back-geocoding
p = gpt.back_geocoding(m, s)

# --- interferogram
p = gpt.interferogram(p)

# --- deburst
p = gpt.deburst(p)

# --- topographic phase removal
p = gpt.topo_phase_removal(p)

# --- phase filtering
p = gpt.goldstein_phase_filtering(p)

# --- terrain correction (geocode)
gpt.get_bandnames(p, print_bands=1)
sourceBands = ['Intensity_VV_11Jan2017_04Feb2017', 'Phase_VV_11Jan2017_04Feb2017', 'coh_IW2_VV_11Jan2017_04Feb2017']
p = gpt.terrain_correction(p, sourceBands)

# --- plot
# p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
# gpt.plotBand_np(p_subset, sourceBands[0], f_out='int_TC', cmap='binary')

print '--> plotting IFG'
p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
gpt.plotBand_np(p_subset, sourceBands[1], cmap='gist_rainbow')

print '--> plotting COH'
p_subset = gpt.subset(p, north_bound=13.55, west_bound=40.64, south_bound=13.62, east_bound=40.715)
gpt.plotBand_np(p_subset, sourceBands[2], cmap='binary')

print '--> finished!'

logging.info('FINISHED!')
