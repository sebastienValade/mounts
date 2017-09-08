import utilityme as utils
import snapme as gpt

dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')

# --- get dataset
# dat = dbo.get_dataset(dbname='DB_ARCHIVE', tbname='ertaale')
# print dat[0][1]
# rows = dbo.get_dataset(dbname='DB_ARCHIVE', tbname='ertaale')
# for r in rows:
#     print(r.prod_title, r.prod_abspath)

# --- open product
# p = gpt.read_product(path_and_file=dat[0][1])
# print p.getName()

# --- select only 1SDV files (excluding 1SSV files)
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE SUBSTRING(prod_title,13,4) = '1SDV'"
stmt = "SELECT * FROM DB_ARCHIVE.etna WHERE SUBSTRING(prod_title,13,4) = '1SDV'"

rows = dbo.execute_query(stmt)
dat = rows.all()

master_idx = 1
slave_idx = 3

print 'MASTER = ' + dat[master_idx].prod_title
print 'SLAVE = ' + dat[slave_idx].prod_title
# import sys
# sys.exit()

# --- read master product
master_abspath = dat[master_idx].prod_abspath
m = gpt.read_product(path_and_file=master_abspath)

# --- read slave product
slave_abspath = dat[slave_idx].prod_abspath
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
bdnames = gpt.get_bandnames(p, print_bands=1)

idx_phase = [idx for idx, dbname in enumerate(bdnames) if 'Phase_' in dbname][0]
idx_coh = [idx for idx, dbname in enumerate(bdnames) if 'coh_' in dbname][0]

sourceBands = [bdnames[idx_phase], bdnames[idx_coh]]
p = gpt.terrain_correction(p, sourceBands)

# --- plot
import pdb
pdb.set_trace()
# p_subset = gpt.subset(p, north_bound=37.7, west_bound=14.95, south_bound=37.6, east_bound=15.05)
subset_bounds = {'north_bound': 37.7, 'west_bound': 14.95, 'south_bound': 37.6, 'east_bound': 15.05}  # >> etna
# subset_bounds = {'north_bound': 13.53, 'west_bound': 40.63, 'south_bound': 13.64, 'east_bound': 40.735} #>> ertaale
p_subset = gpt.subset(p, **subset_bounds)
band = gpt.plotBand(p_subset, sourceBands, cmap=['gist_rainbow', 'binary_r'])
