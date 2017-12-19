import utilityme as utils
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
from dateutil.parser import parse

path_dir = '/home/sebastien/DATA/data_snap/'
# f = glob.glob(os.path.join(path_dir, '') + '20170104*.dim')
f = glob.glob(os.path.join(path_dir, '') + '*.dim')


f_cred = file('./conf/credentials_mysql.txt')
(db_usr, db_pwd) = f_cred.readline().split(' ')
dbo = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

# --- get data in database
stmt = "SELECT time, data FROM DB_MOUNTS.results_dat WHERE type='coh' ORDER BY time ASC"
rows = dbo.execute_query(stmt)
dat = rows.dataset
t_str = dat.get_col(0)
y = dat.get_col(1)

# --- plot
# import datetime
# t_datetime = [datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S") for t in t_str]
import pandas as pd
t_datetime = pd.to_datetime(t_str)

plt.plot(t_datetime, y)
plt.xticks(rotation=10)
plt.draw()
plt.savefig('coh3.png')
