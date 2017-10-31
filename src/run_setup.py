import utilityme as utils
import ast

dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')

username = 'khola'

# =============================================
# setup DATABASE
# =============================================

dbo.delete_db('DB_MOUNTS')

# --- create db
dbo.create_db('DB_MOUNTS')


# --- create tb "targets"
tbname = 'targets'
dicts = {'id': 'INT',               # = volcano number defined by GVP
         'fullname': 'CHAR(100)',   # = Erta Ale
         'name': 'CHAR(100)',       # = ertaale
         'country': 'CHAR(100)',
         'lat': 'FLOAT',
         'lon': 'FLOAT',
         'download': 'INT',
         'processing': 'CHAR(100)',
         'subset_wkt': 'TEXT'}
dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts, primarykey='id')


# --- create tb "archive"
# NB: fieldnames taken from names recovered by "get_metadata_abstracted" (snapme)
tbname = 'archive'
dicts = {'id': 'INT NOT NULL AUTO_INCREMENT',
         'title': 'VARCHAR(150)',
         'abspath': 'TEXT',
         'producttype': 'CHAR(25)',
         'mission': 'CHAR(25)',
         'orbitdirection': 'CHAR(25)',
         'relativeorbitnumber': 'CHAR(25)',
         'acquisitionmode': 'CHAR(25)',
         'acqstarttime': 'DATETIME',
         'acqstarttime_str': 'CHAR(25)',
         'polarization': 'CHAR(25)',
         'target_id': 'INT',
         'target_name': 'CHAR(100)'}
dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts, primarykey='id', foreignkey='target_id', foreignkey_ref='DB_MOUNTS.targets(id)')


# --- create tb "results_img"
tbname = 'results_img'
dicts = {'id': 'INT NOT NULL AUTO_INCREMENT',
         'title': 'VARCHAR(100)',
         'abspath': 'TEXT',
         'type': 'TEXT',
         'id_master': 'INT',
         'id_slave': 'INT'}
dbo.create_tb(dbname='DB_MOUNTS',
              tbname=tbname,
              dicts=dicts,
              primarykey='id',
              foreignkey=['id_master', 'id_slave'],
              foreignkey_ref=['DB_MOUNTS.archive(id)', 'DB_MOUNTS.archive(id)']
              )

# # --- create tb "processing"
# tbname = 'processing'
# dicts = {'id': 'INT',               # = volcano number defined by GVP
#          'name': 'CHAR(100)',       # = ertaale
#          'dinsar': 'CHAR(100)',
#          'subset': 'CHAR(250)'}
# dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts, primarykey='id')
# # --- add processing options
# # => copy columns 'name', 'id' from table 'targets'
# stmt = "INSERT INTO DB_MOUNTS.processing (id, name) SELECT id, name FROM DB_MOUNTS.targets"
# rows = dbo.execute_query(stmt)

# =============================================
# load DATABASE
# =============================================

# --- add volcanoes to target list
dbo.dbmounts_addtarget(id=221080, fullname='Erta Ale', name='ertaale', country='Ethiopia', lat=13.6, lon=40.67, processing="{'dinsar': {'subswath':'IW2', 'polarization':'VV'} }", subset_wkt='POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))')
dbo.dbmounts_addtarget(id=211060, fullname='Etna', name='etna', country='Italy', lat=37.748, lon=14.999, processing="{'dinsar': {'subswath':'IW2', 'polarization':'VV'} }", subset_wkt='POLYGON((14.916129 37.344437, 14.979386 37.344437, 14.979386 37.306283, 14.916129 37.306283, 14.916129 37.344437))')

# --- store archive zip files of each listed target to database
stmt = "SELECT name FROM DB_MOUNTS.targets"
rows = dbo.execute_query(stmt)
for r in rows:
    volcanoname = r.name
    archive_dir = '/home/' + username + '/DATA/data_satellite/' + volcanoname
    dbo.dbmounts_loadarchive(path_dir=archive_dir, target_name=volcanoname, print_metadata=0)


# =============================================
# process ARCHIVE
# =============================================

# => loop through 'targets' and run field 'processing'

stmt = "SELECT * FROM DB_MOUNTS.targets"
rows = dbo.execute_query(stmt)
for r in rows:

    volcanoname = r.name
    pcss_str = r.processing
    print volcanoname

    # --- get processing options if defined
    if pcss_str is None or not pcss_str:
        continue
    else:
        pcss = ast.literal_eval(pcss_str)

    # --- run dinsar
    if 'dinsar' in pcss:

        import snapme as gpt
        cfg_productselection = {'target_name': volcanoname} 	# = sql search options
        cfg_dinsar = pcss['dinsar']				# = dinsar options
        cfg_plot = {'subset_wkt': r.subset_wkt, 'pathout_root': '/home/' + username + '/DATA/data_mounts/'}
        gpt.dinsar(cfg_productselection, cfg_dinsar, cfg_plot)
