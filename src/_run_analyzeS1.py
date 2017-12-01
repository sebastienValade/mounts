import snapme as gpt
import utilityme as utils
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
from dateutil.parser import parse

path_dir = '/home/khola/DATA/data_snap/'
# f = glob.glob(os.path.join(path_dir, '') + '20170104*.dim')
f = glob.glob(os.path.join(path_dir, '') + '*.dim')


f_cred = file('./conf/credentials_mysql.txt')
(db_usr, db_pwd) = f_cred.readline().split(' ')
dbo = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')


# print('Emptying table')
# dbo.empty_tb(dbname='DB_MOUNTS', tbname='results_dat')


for k, fpath in enumerate(f):

    fname = os.path.basename(fpath)
    title = os.path.splitext(fname)[0]
    print('  | ' + title)

    # get info from database
    time_str = title[0:15]  # 20161218T152714
    time_datetime = parse(time_str).strftime('%Y-%m-%d %H:%M:%S.%f')
    target_id = 221080
    id_image = 4        # = id of saved image (coh, ...) or saved process (ref)

    p = gpt.read_product(path_and_file=fpath)
    bdnames = gpt.get_bandnames(p, print_bands=None)
    idx_coh = [idx for idx, dbname in enumerate(bdnames) if 'coh_' in dbname][0]
    bname = bdnames[idx_coh]

    # --- get band data
    band = p.getBand(bname)
    w, h = gpt.get_rasterDim(p, bname)
    band_data = np.zeros(w * h, np.float32)
    band.readPixels(0, 0, w, h, band_data)
    band_data.shape = h, w

    # --- analyze
    img = band_data.copy()

    idx_change = img[img < 0.5].sum()
    print idx_change
    # print band_data.min()
    # print band_data.max()

    img[img > 0.5] = 1
    img[img < 0.5] = 0
    plt.imshow(img)
    # plt.show()
    # plt.imsave(title + '.png', img, cmap='gray')

    print('Store to DB_MOUNTS.results_dat')
    dict_val = {'time': time_datetime,
                'type': 'coh',
                'data': str(idx_change),
                'id_image': str(id_image),
                'target_id': str(target_id)}
    dbo.insert('DB_MOUNTS', 'results_dat', dict_val)


# --------------------------------------------------------------------------------------

# # --- get data in database
# stmt = "SELECT time, data FROM DB_MOUNTS.results_dat WHERE type='coh' ORDER BY time ASC"
# rows = dbo.execute_query(stmt)
# dat = rows.dataset
# t_str = dat.get_col(0)
# y = dat.get_col(1)

# # --- plot
# import datetime
# t_datetime = [datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%S") for t in t_str]
# plt.plot(t_datetime, y)
# plt.xticks(rotation=10)
# plt.draw()
# plt.savefig('coh.png')
