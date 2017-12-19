from celery import Celery, chain
from celery.schedules import crontab
import utilityme as utils
import fetchme
import records
import json
from dateutil.parser import parse
from datetime import datetime

app = Celery('tasks', broker='pyamqp://guest@localhost//')


# --- connect to MOUNTS database
db_host = '127.0.0.1'
db_usr = 'root'
db_pwd = 'br12Fol!'
db_type = 'mysql'
db_name = 'DB_MOUNTS'
db_url = db_type + '://' + db_usr + ':' + db_pwd + '@' + db_host + '/' + db_name
dbo = records.Database(db_url)

dbo_bis = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

# --- instace for scihub queries
obj = fetchme.Scihub()
f = file('./conf/credentials_scihub.txt')
(usr, pwd) = f.readline().split(' ')
obj.query_auth(usr, pwd)

# --- scihub product instance
pdc = fetchme.Product()
pdc.query_auth(usr, pwd)


# def setup_periodic_tasks(sender, **kwargs):

# --- get list of targets
stmt = "SELECT name, id, download, subset_wkt FROM DB_MOUNTS.targets ORDER BY fullname"
rows = dbo.query(stmt)

# --- loop through targets
for r in rows:

    if r.name != 'ertaale':
        continue

    # --- get target options
    target_name = r.name
    target_id = r.id
    subset_wkt = r.subset_wkt
    download_optn = json.loads(r.download)
    crontab_hr = download_optn['crontab']['hour']
    crontab_mn = download_optn['crontab']['minute']
    scihub_optn = download_optn['scihub_optn']
    download_rootdir = download_optn['download_rootdir']
    if 'footprint' not in scihub_optn:
        scihub_optn['footprint'] = subset_wkt
    download_dir = download_rootdir + target_name

    print('****  setting crontab for: ' + r.name)
    print('    . crontab min = ' + crontab_mn)
    print('    . crontab hr = ' + crontab_hr)
    print('    . download_dir = ' + download_dir)


# def query(scihub_optn, target_id):

# TODO: download all products available after last entry in DB_MOUNTS

# === get latest products in SCIHUB
# TODO: set query option date > last entry in DB to download all?
max_nb2download = 6
scihub_optn['maxrecords'] = max_nb2download
pdct_inScihub = obj.scihub_search(**scihub_optn)
pdct_inScihub_title = [p.metadata['title'] for p in pdct_inScihub]
pdct_inScihub_datestr = [parse(p.metadata['beginposition']).strftime('%Y-%m-%d %H:%M:%S.%f') for p in pdct_inScihub]
pdct_inScihub_datetime = [datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f') for t in pdct_inScihub_datestr]

print('===> lastest {} products in SCIHUB:'.format(max_nb2download))
for p in pdct_inScihub_title:
    print(p)

# --- get mission parameter
if 'platformname' in scihub_optn:
    # NB: to use wildcards in sql querys, use operator 'LIKE' instead of '=' allows usage of wildcards in query:
    # The wildcard '%' represents zero, one, or multiple characters.
    mission = scihub_optn['platformname'] + '%'
else:
    # TODO: consider case if 'filename' option is passed:
    # filename='S1*' => mission='SENTINEL-1%'
    print('TODO!')


# === get latest products in DB_MOUNTS
stmt = '''
    SELECT title, acqstarttime 
    FROM DB_MOUNTS.archive 
    WHERE target_id = {} AND mission LIKE '{}' 
    ORDER BY acqstarttime DESC 
    LIMIT {}
    '''.format(target_id, mission, 1)

pdct_inMounts = dbo.query(stmt)
pdct_inMounts_title = [p.title for p in pdct_inMounts]
pdct_inMounts_datetime = [p.acqstarttime for p in pdct_inMounts]
print('===> lastest product in DB_MOUNTS:')
for p in pdct_inMounts_title:
    print(p)

# === get products to download
# => download all products in scihub more recent then latest product in db_mounts. Max number of products to download fixed by 'max_nb2download'.
# pdct2download = [p for i, p in enumerate(pdct_inScihub) if p.metadata['title'] not in pdct_inMounts_title]
pdct2download = [p for dt, p in zip(pdct_inScihub_datetime, pdct_inScihub) if dt > pdct_inMounts_datetime[0]]
pdct2download_metadata = [p.metadata for p in pdct2download]
print('===> products to download:')
if not pdct2download_metadata:
    print('   => nothing new to download')

    # TODO: check if this prevents from proceeding to next task in task chain
else:
    for p in pdct2download_metadata:
        print(p['title'])


print pdct2download_metadata
