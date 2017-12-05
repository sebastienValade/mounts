import utilityme as utils

# dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
# stmt = "SELECT * FROM DB_MOUNTS.results_img WHERE target_id = '221080'"


db_name = 'DB_MOUNTS'
dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='br12Fol!', db_type='mysql', db_name=db_name)
# stmt = "SELECT r.title FROM results_img r INNER JOIN archive a ON r.id_master = a.id WHERE r.target_id = '221080' ORDER BY a.acqstarttime desc"

# stmt = '''
#     SELECT R.title, A.acqstarttime
#     FROM results_img AS R
#     INNER JOIN archive AS A
#     ON R.id_master = A.id
#     WHERE R.target_id = '221080' AND R.type = 'ifg' OR R.type = 'coh'
#     ORDER BY A.acqstarttime desc
#     '''


# -- USE PANDAS
id = '221080'

# --- get target s1 results (ifg+coh)
q = '''
    SELECT R.title, R.abspath, R.type, S.acqstarttime AS acqstarttime_slave, M.acqstarttime AS acqstarttime_master
    FROM results_img AS R
    INNER JOIN archive AS S ON R.id_slave = S.id
    INNER JOIN archive AS M ON R.id_master = M.id
    WHERE R.target_id = {} AND S.acqstarttime > '2017-01-01' AND (R.type = 'ifg' OR R.type = 'coh' OR R.type = 'int_VV')
    ORDER BY S.acqstarttime desc, R.type desc
'''
stmtS1 = q.format(id)

# --- get target s2 results (nir)
q = '''
    SELECT R.title, R.abspath, R.type, S.acqstarttime AS acqstarttime_slave, M.acqstarttime AS acqstarttime_master
    FROM results_img AS R
    INNER JOIN archive AS S ON R.id_slave = S.id
    INNER JOIN archive AS M ON R.id_master = M.id
    WHERE R.target_id = {} AND R.type = 'nir'
    ORDER BY M.acqstarttime desc
'''
stmtS2 = q.format(id)


# -> records lib
# imgS1 = dbo.execute_query(stmtS1)
# imgS2 = dbo.execute_query(stmtS2)
# A = imgS2.dataset
# A['acqstarttime']

# -> pandas lib
import pandas as pd
from sqlalchemy import create_engine  # database connection
db_host = '127.0.0.1'
db_usr = 'root'
db_pwd = 'br12Fol!'
db_type = 'mysql'
db_name = 'DB_MOUNTS'
db_url = db_type + '://' + db_usr + ':' + db_pwd + '@' + db_host + '/' + db_name
disk_engine = create_engine(db_url)
df_S1 = pd.read_sql(stmtS1, disk_engine)
df_S2 = pd.read_sql(stmtS2, disk_engine)

gb = df_S1.groupby('acqstarttime_master', sort=False, as_index=False)

import numpy as np
idx = [np.argmin(abs(i - df_S2.acqstarttime_master)) for i in sorted(gb.groups.iterkeys(), reverse=True)]
imgS2 = df_S2.iloc[idx]

df = pd.DataFrame(np.random.randn(2, 5), columns=['title', 'abspath', 'type', 'acqstarttime_slave', 'acqstarttime_master'])
s = df.iloc[0]

for name, group in gb:
    print '=========='
    print name
    idx = np.argmin(abs(name - df_S2.acqstarttime_master))
    group = group.append(df_S2.iloc[idx], ignore_index=True)
    # print group
    # gb.update(group)

sg = []
sgd = {}
for name, group in gb:
    print '=========='
    print name
    idx = np.argmin(abs(name - df_S2.acqstarttime_master))
    group = group.append(df_S2.iloc[idx], ignore_index=True)
    sg.append(group.to_dict('list'))
    sgd[name] = group.to_dict('list')

for k, v in sgd:
    print v


# b = pd.concat([group, df_S2.iloc[idx]], ignore_index=True)
# print b


# # for name, (acqstarttime_master, acqstarttime_slave, abspath, title, type) in gb:
# for name, (v1, v2, v3, v4, v5) in gb:
#     print '----'
#     print name
#     print v1
#     print v2
#     print v3
#     print v4
#     print v5

# for name, group in gb:
#     print '----'
#     print name
#     for i, j in zip(group['type'].tolist(), group['title'].tolist()):
#         print i
#         print j


# for (k1, k2), group in gb:
#     print k1

# --- get closest combination
# NB: in order to work query results must be sorted in ascending order
# dt_set = pd.merge_asof(df_S1, df_S2, 'acqstarttime')
# print dt_set.title_x, dt_set.title_y

# --- get closest value (does not return index...)
# dt_min = [min(abs(df_S2.acqstarttime - pivot)) for pivot in df_S1.acqstarttime]
# # dt_min = [min(df_S2.acqstarttime, key=lambda x: abs(x - pivot)) for pivot in df_S1.acqstarttime]  #also works
# print dt_min

# --- get closest index
# import numpy as np
# dt_idx = [np.argmin(abs(pivot - df_S2.acqstarttime)) for pivot in df_S1.acqstarttime]
# print dt_idx
# for k, i in enumerate(df_S1.acqstarttime):
#     print k
#     print df_S1.acqstarttime[k]
#     print df_S2.acqstarttime[dt_idx[k]]

# # --- groupby
# # gb = df_S1.groupby(by=['acqstarttime'])
# gb = df_S1.groupby('acqstarttime')    # => groups will be datetimes!
# # print gb.describe()

# for name, group in gb:
#     print name
#     print(group.abspath)

#     # for i in group:
#     #     print('--- ' + i)
#     #     print group

#     for i in group.abspath.tolist():
#         print('--- ' + i)
#     # print(group.acqstarttime.tolist())
#     # print(group.type.tolist())
#     # print(group.abspath.tolist())


# print gb.groups   # => groups = datetime
# print gb.get_group('2017-05-04 03:07:50')

# --- using GROUP BY statement
# stmt = '''
#     SELECT type, COUNT(*) AS "groups_type"
#     FROM results_img
#     WHERE target_id = '221080'
#     GROUP BY type
#     '''

# stmt = '''
#     SELECT type, COUNT(*) AS "groups_type"
#     FROM results_img AS R
#     INNER JOIN archive AS A
#     ON R.id_master = A.id
#     WHERE R.target_id = '221080'
#     GROUP BY R.type
#     '''

# res = dbo.execute_query(stmt)

# print(res.dataset)
# print ""


# print(res.as_dict())
# print ""


# from collections import defaultdict
# res_sorted = defaultdict(list)
# for i in res:
#     print i
#     res_sorted[i['acqstarttime']].append(i)

# print res_sorted
# print ""
# res_sorted = dict(res_sorted)

# from collections import OrderedDict
# res_sorted = OrderedDict(sorted(res_sorted.items()))
# print res_sorted
# print type(res_sorted)


# # --- sort results by master_id
# from collections import defaultdict
# res_sorted = defaultdict(list)
# for i in res:
#     res_sorted[i['id_master']].append(i)
# res_sorted = dict(res_sorted)

# print(res_sorted)


# for r in res:
#     abspath = r.abspath
#     print abspath
