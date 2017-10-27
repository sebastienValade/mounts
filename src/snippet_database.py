import utilityme as utils

dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')

# =============================================
# MANAGE db
# =============================================

# === CREATE

# --- create db
# dbo.create_db('DB_ARCHIVE')

# --- create tb
# dicts = {'title': 'VARCHAR(100)', 'abspath': 'TEXT'}
# dbo.create_tb(dbname='DB_ARCHIVE', tbname='etna', dicts=dicts, primarykey='title')

# === DELETE

# --- delete db
# dbo.delete_db('DB_ARCHIVE')

# --- delete tb
# dbo.delete_tb(dbname='DB_RESULTS', tbname='etna')

# === EMPTY

# --- empty db
# dbo.empty_tb(dbname='DB_ARCHIVE', tbname='ertaale')

# --- empty tb
# dbo.empty_tb(dbname='DB_RESULTS', tbname='etna')

# === MISC

# --- store archive zip files to database
# dbo.store_dir2db('/home/sebastien/DATA/data_satellite/ertaale/', dbname='DB_ARCHIVE', tbname='ertaale')

# --- print content
# dbo.print_dataset(dbname='volcanoes', tbname='ertaale')  # , colname='prod_title')

# =============================================
# QUERY db
# =============================================

# --- get entire table dataset
# dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# rows = dbo.get_dataset(dbname='DB_ARCHIVE', tbname='ertaale')
# for r in rows:
#     print(r.title, r.abspath)

# # --- get specific dataset: select elts containing specific string (ex: string '1SDV' at char nb 13)
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE SUBSTRING(title,13,4) = '1SDV'"
# rows = dbo.execute_query(stmt)
# dat = rows.all()

# --- get specific dataset:
# => AND condition: SELECT * FROM table WHERE column1 = 'var1' AND column2 = 'var2';
# => OR condition: SELECT * FROM table WHERE column1 = 'var1' OR column2 = 'var2';
# stmt = "SELECT * FROM DB_ARCHIVE.etna WHERE orbitdirection = 'ASCENDING' AND polarization = 'VH VV';"
# rows = dbo.execute_query(stmt)
# dat = rows.all()


# =============================================
# DB_ARCHIVE
# =============================================

# --- store new volcano data to DB_ARCHIVE
# volcanoname = 'etna'
# # dbo.dbarch_newtable(tbname=volcanoname)
# dbo.dbarch_loaddir('/home/sebastien/DATA/data_satellite/' + volcanoname, tbname=volcanoname)


# =============================================
# DB_RESULTS
# =============================================

# --- create table with pre-defined fields
# volcanoname = 'ertaale'
# dbo.dbres_newtable(tbname=volcanoname)

# --- store image file to DB_RESULTS
# IMPORTANT: 'master_title' and 'slave_title' are foreign keys linked to DB_ARCHIVE.volcano.title
# 	Specifying their values is optional, BUT if they are specified, they MUST be found in the reference table (DB_ARCHIVE.volcano.title)
# dict_val = {'title': 'interferogram',
#             'abspath': '/home/sebastien/',
#             'type': 'ifg',
#             'master_title': 'S1A_IW_SLC__1SDV_20141229T152648_20141229T152715_003936_004BAC_752F',
#             }
# dict_val = {'title': ['interferogram', 'coherence'],
#             'abspath': ['/home/sebastien/', '/home/sebastien/'],
#             'type': ['ifg', 'coh'],
#             'master_title': ['S1A_IW_SLC__1SDV_20141229T152648_20141229T152715_003936_004BAC_752F', 'S1A_IW_SLC__1SDV_20141229T152648_20141229T152715_003936_004BAC_752F'],
#             }
# dbo.insert('DB_RESULTS', 'ertaale', dict_val)


# --- retrieve data from foreign table
# NB: INNER JOIN statement => selects records (=column values) that have matching values in both tables.
# ----- select just one field:
# stmt = "SELECT acqstarttime_str FROM DB_ARCHIVE.ertaale INNER JOIN DB_RESULTS.ertaale ON DB_ARCHIVE.ertaale.title=DB_RESULTS.ertaale.ref_master;"
# rows = dbo.execute_query(stmt)
# ----- select all fields
# stmt = "SELECT * FROM DB_ARCHIVE.ertaale INNER JOIN DB_RESULTS.ertaale ON DB_ARCHIVE.ertaale.title=DB_RESULTS.ertaale.ref_master;"
# rows = dbo.execute_query(stmt)
# print rows.all()
# print rows[0]['acqstarttime_str']

# --- store result png files to database
# dbo.create_db('DB_RESULTS')
# dicts = {'file_name': 'VARCHAR(100)', 'file_abspath': 'TEXT', 'master_date':}
# dbo.create_tb(dbname='DB_RESULTS', tbname='ertaale', dicts=dicts, primarykey='file_name')
# dbo.store_dir2db('/home/sebastien/DATA/data_mounts/ertaale', dbname='DB_RESULTS', tbname='ertaale')

# =============================================
# MISC
# =============================================


# --- plot timing of dinsar master-slave pairs
# t_str = []
# ti_str = []
# tf_str = []
# ind = []
# for k, r in enumerate(dat):
#     # if k >= len(dat) - 1:
#     #     break
#     t_str.append(r.title[17:32])
#     ind.append(k + 1)

# ti_str = t_str[0:-1]
# tf_str = ti_str[1:]
# tf_str.append(dat[-1].title[17:32])

# import pandas as pd
# import matplotlib.pyplot as plt
# import matplotlib.dates as mdates

# t = pd.to_datetime(t_str)
# ti = pd.to_datetime(ti_str)
# tf = pd.to_datetime(tf_str)
# xs = zip(ti, tf)
# ys = zip(ind, ind)

# f = plt.figure()
# # plt.ylim(y_min, y_max)
# for x, y in zip(xs, ys):
#     plt.plot(x, y, 'b', solid_capstyle='butt')
# plt.grid(True)
# plt.yticks(ind, fontsize=8)
# plt.xticks(t, fontsize=8)
# xfmt = mdates.DateFormatter('%Y-%m-%d')
# f.gca().xaxis.set_major_formatter(xfmt)
# f.autofmt_xdate()
# # plt.show()

# f.savefig('timings.png')

# k = 0
# for x, y in zip(xs, ys):
#     k += 1
#     a = plt.plot(x, y, 'r', solid_capstyle='butt', linewidth=8)
#     f.savefig('timings_' + str(k).zfill(2) + '.png')
#     a.pop(0).remove()
