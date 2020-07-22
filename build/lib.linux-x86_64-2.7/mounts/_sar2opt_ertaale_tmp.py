import numpy as np
import pandas as pd
from sqlalchemy import create_engine  # database connection


db_url = 'mysql://root:br12Fol!@127.0.0.1/DB_MOUNTS'
disk_engine = create_engine(db_url)


id = 221080


# --- get target s1 results (ifg+coh+int)
q = '''
    SELECT R.title, R.abspath, R.type, S.acqstarttime AS acqstarttime_subordinate, M.acqstarttime AS acqstarttime_main
    FROM results_img AS R
    INNER JOIN archive AS S ON R.id_subordinate = S.id
    INNER JOIN archive AS M ON R.id_main = M.id
    WHERE R.target_id = {} AND S.acqstarttime > '2017-01-01' AND (R.type = 'ifg' OR R.type = 'coh' OR R.type = 'int_VV')
    ORDER BY S.acqstarttime desc, R.type desc
'''
stmt = q.format(id)
df_S1 = pd.read_sql(stmt, disk_engine)

# --- get target s2 results (nir)
q = '''
    SELECT R.title, R.abspath, R.type, S.acqstarttime AS acqstarttime_subordinate, M.acqstarttime AS acqstarttime_main
    FROM results_img AS R
    INNER JOIN archive AS S ON R.id_subordinate = S.id
    INNER JOIN archive AS M ON R.id_main = M.id
    WHERE R.target_id = {} AND R.type = 'nir'
    ORDER BY M.acqstarttime desc
'''
stmt = q.format(id)
df_S2 = pd.read_sql(stmt, disk_engine)

# --- group S1 dataframe by subordinate image acquisition date
gb = df_S1.groupby('acqstarttime_subordinate', sort=False)

img_groups = []
for group_name, group in gb:

    # - search for closest S2 image to group date, and append
    idx = np.argmin(abs(group_name - df_S2.acqstarttime_main))
    group = group.append(df_S2.iloc[idx], ignore_index=True)

    # - store group as dictionary
    group_dict = group.to_dict('list')
    group_dict['groupdate'] = group_name    # => field stores group date (timestamp)
    group_dict['dt'] = df_S2.iloc[idx]['acqstarttime_main'] - group_name  # => field stores delta t between group date and selected S2 img
    img_groups.append(group_dict)

print img_groups
