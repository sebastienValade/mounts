import utilityme as utils

username = 'sebastien'

f = file('./conf/credentials_mysql.txt')
(db_usr, db_pwd) = f.readline().split(' ')
dbo = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

# === LOAD TARGETS
# dbo.dbmounts_addtarget(id=221080, fullname='Erta Ale', name='ertaale', country='Ethiopia', lat=13.6, lon=40.67, alt=613,
#                        processing="{'dinsar': {'subswath':'IW2', 'polarization':'VV', 'bands2plot':['ifg', 'coh']}, 'sar': {'subswath': 'IW2', 'bands2plot': ['int_HV']}, 'nir': {'bname_red':'B12', 'bname_green':'B11', 'bname_blue':'B8A'} }",
#                        subset_wkt='POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))')
# dbo.dbmounts_addtarget(id=211060, fullname='Etna', name='etna', country='Italy', lat=37.748, lon=14.999, alt=3295,
#                        processing="{'dinsar': {'subswath':'IW2', 'polarization':'VV', 'bands2plot':['ifg', 'coh']}, 'sar': {'subswath': 'IW2', 'bands2plot': ['int_HV']}, 'nir': {'bname_red':'B12', 'bname_green':'B11', 'bname_blue':'B8A'} }",
#                        subset_wkt='POLYGON((14.916129 37.344437, 14.979386 37.344437, 14.979386 37.306283, 14.916129 37.306283, 14.916129 37.344437))')

# # === LOAD S2 files to DB_MOUNTS.archive
# stmt = "SELECT name FROM DB_MOUNTS.targets"
# rows = dbo.execute_query(stmt)
# for r in rows:
#     volcanoname = r.name
#     archive_dir = '/home/' + username + '/DATA/data_satellite/' + volcanoname
#     dbo.dbmounts_loadarchive(path_dir=archive_dir, target_name=volcanoname, print_metadata=1)

# === LOAD S2 file manually
# dict_newarchive = {'acqstarttime_str': '20170310T074549',
#                    'title': 'S2A_MSIL1C_20170310T073721_N0204_R092_T37PFR_20170310T074549',
#                    'target_name': 'ertaale',
#                    'abspath': '/home/khola/DATA/data_satellite/ertaale/S2A_MSIL1C_20170310T073721_N0204_R092_T37PFR_20170310T074549.zip',
#                    'acquisitionmode': '-',
#                    'target_id': '221080',
#                    'mission': 'SENTINEL-2A',
#                    'orbitdirection': 'DESCENDING',
#                    'polarization': '-',
#                    'relativeorbitnumber': '1',
#                    'acqstarttime': '2017-03-10 07:45:49.154000',
#                    'producttype': 'S2MSI1C'}
# dbo.insert('DB_MOUNTS', 'archive2', dict_newarchive)

# # --- create tb "results_dat"
# tbname = 'results_dat'
# dicts = {'id': 'INT NOT NULL AUTO_INCREMENT',
#          'time': 'DATETIME',
#          'data': 'FLOAT',
#          'type': 'CHAR(25)',
#          'id_image': 'INT',
#          'target_id': 'INT'}
# dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts,
#               primarykey='id',
#               foreignkey=['id_image', 'target_id'],
#               foreignkey_ref=['DB_MOUNTS.results_img(id)', 'DB_MOUNTS.targets(id)'],
#               unique_contraint=['time', 'type'])  # => combination of time/type values cannot be duplicate

# === create table with unique constraint
# tbname = 'archive2'
# dicts = {'id': 'INT NOT NULL AUTO_INCREMENT',
#          'title': 'VARCHAR(150)',
#          'abspath': 'TEXT',
#          'producttype': 'CHAR(25)',
#          'mission': 'CHAR(25)',
#          'orbitdirection': 'CHAR(25)',
#          'relativeorbitnumber': 'CHAR(25)',
#          'acquisitionmode': 'CHAR(25)',
#          'acqstarttime': 'DATETIME',
#          'acqstarttime_str': 'CHAR(25)',
#          'polarization': 'CHAR(25)',
#          'target_id': 'INT',
#          'target_name': 'CHAR(100)'}
# dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts,
#               primarykey='id',
#               foreignkey='target_id',
#               foreignkey_ref='DB_MOUNTS.targets(id)',
#               unique_contraint='title')
