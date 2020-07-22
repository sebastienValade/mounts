import utilme

dbo = utilme.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')

# =============================================
# MANAGE db
# =============================================

# === CREATE

# --- create db
# dbo.create_db('DB_MOUNTS')

# --- create tb
# tbname = 'targets'
# dicts = {'id': 'INT',
#          'name': 'CHAR',
#          'shortname': 'CHAR',
#          'country': 'CHAR',
#          'lat': 'FLOAT',
#          'lon': 'FLOAT'}
# dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts, primarykey='id')


# === DELETE

# --- delete db
# dbo.delete_db('DB_MOUNTS')

# --- delete tb
# NB: works only if table does not have foreign key constraints
# dbo.delete_tb(dbname='DB_MOUNTS', tbname='targets')


# === EMPTY

# --- empty tb
# NB: works only if table does not have foreign key constraints
# dbo.empty_tb(dbname='DB_MOUNTS', tbname='targets')


# =============================================
# QUERY db
# =============================================

# --- convert Records output to Tablib format, and use all of the library's functionalities:
# dbo = utilme.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# stmt = "SELECT * FROM DB_MOUNTS.results_img WHERE target_id = '221080'"
# rows = dbo.execute_query(stmt)
# tbl = rows.dataset
# # tble.append_col([22, 20, 12, 11], header='Age') #= append new column

# --- records lib simplest usage
# db_url = 'mysql://root:br12Fol!@127.0.0.1/DB_MOUNTS'
# dbo = records.Database(db_url)
# stmt = "SELECT * FROM DB_MOUNTS.archive WHERE target_id = 221080 ORDER BY acqstarttime DESC LIMIT 10".format(target_id)
# rows = dbo.query(stmt)
# for r in rows:
#     print r.title
#
# - if only 1 row expected:
# print rows.first().title

# --- print content
# dbo.print_dataset(dbname='DB_MOUNTS', tbname='targets')  # , colname='prod_title')

# --- get entire table dataset
# dbo = utilme.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# rows = dbo.get_dataset(dbname='DB_MOUNTS', tbname='targets')
# for r in rows:
#     print(r.name, r.id)
#
# with tablib functionalities:
# A = rows.dataset
# A['name']
#
# or: rows.dataset['name']

# --- get specific dataset: select elts containing specific string (ex: string '1SSV' at char nb 13)
# stmt = "SELECT * FROM DB_MOUNTS.archive WHERE SUBSTRING(title,13,4) = '1SSV'"
# rows = dbo.execute_query(stmt)
# dat = rows.all()

# --- get specific dataset (basic):
# => AND condition: SELECT * FROM table WHERE column1 = 'var1' AND column2 = 'var2';
# => OR condition: SELECT * FROM table WHERE column1 = 'var1' OR column2 = 'var2';
# stmt = "SELECT * FROM DB_MOUNTS.archive WHERE target_id = '221080' and acqstarttime >= '2016-01-01' and acqstarttime < '2018-01-01' and orbitdirection = 'ASCENDING' and orbitdirection = 'ASCENDING' and polarization = 'VV' ORDER BY acqstarttime ASC"
# rows = dbo.execute_query(stmt)
# for r in rows:
#     print(r.title)


# --- get specific dataset into PANDAS data frame:
# db_url = 'mysql://user:pwd@127.0.0.1/DB_MOUNTS'
# disk_engine = create_engine(db_url)
# stmt = '''
#     SELECT *
#     FROM DB_MOUNTS.archive
#     WHERE target_id = '221080'
#         and acqstarttime >= '2016-10-01' and acqstarttime < '2017-04-01'
#         and orbitdirection = 'DESCENDING'
#         and polarization = 'VH VV'
#     ORDER BY acqstarttime ASC
# '''
# df = pd.read_sql(stmt, disk_engine)
# for index, row in df.iterrows():
#     print(row['title'])

# --- sort based on foreign key
# EX: get ifg image titles (in 'result_img' table) sorted by acquisition time (in 'archive' table)
db_name = 'DB_MOUNTS'
dbo = utilme.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql', db_name=db_name)

id = '22180'
stmt = '''
    SELECT R.title, A.acqstarttime
    FROM results_img AS R
    INNER JOIN archive AS A
    ON R.id_main = A.id
    WHERE R.target_id = {} AND R.type = 'ifg' OR R.type = 'coh'
    ORDER BY A.acqstarttime desc
    '''
stmt = stmt.format(88)

res = dbo.execute_query(stmt)
print(res.dataset)


# =============================================
# DB_MOUNTS
# =============================================

# === store archive S1 zip files to database
# volcanoname = 'ertaale'
# archive_dir = '/home/sebastien/DATA/data_satellite/' + volcanoname
# dbo.dbmounts_loadarchive(path_dir=archive_dir, print_metadata=1)

# === query targets table
# --- ex: get target id
# volcanoname = 'ertaale'
# stmt = "SELECT id FROM DB_MOUNTS.targets WHERE shortname = '{}'".format(volcanoname)
# rows = dbo.execute_query(stmt)
# print rows[0][0]

# --- ex: get list of targets
stmt = "SELECT shortname FROM DB_MOUNTS.targets"
rows = dbo.execute_query(stmt)
for r in rows:
    print(r.shortname)
