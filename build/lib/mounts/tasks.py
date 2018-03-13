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


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):

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

        # --- set periodic task for target
        task = mounts_chain.s(scihub_optn, target_id, target_name, download_dir)  # .set(serializer='pickle')

        # - define by frequency
        #fq_sec = 60.0
        #sender.add_periodic_task(fq_sec, task, name=target_name)

        # - define by crontab
        sender.add_periodic_task(crontab(minute=crontab_mn, hour=crontab_hr), task, name=target_name)


@app.task
def mounts_chain(scihub_optn, target_id, target_name, download_dir):
    res = chain(
        query.s(scihub_optn, target_id),        # 1. query scihub
        download.s(download_dir=download_dir),  # 2. download uri
        archive.s(target_id=target_id, target_name=target_name)                             # 3. archive
    )()
    return res


@app.task
def query(scihub_optn, target_id):

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
        return

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
        return
        # TODO: check if this prevents from proceeding to next task in task chain
    else:
        for p in pdct2download_metadata:
            print(p['title'])

    # # === download products
    # if pdct2download:
    #     for p in pdct2download:
    #         p.getQuicklook()
    #         # p.getFullproduct()

    return pdct2download_metadata


@app.task
def download(pdct2download_metadata, download_dir=None):

    fpath = []

    # === download products
    print('===> downloading products:')
    if pdct2download_metadata:
        for p in pdct2download_metadata:
            fname = p['title'] + '.zip'
            uri = p['link']
            uri_alternative = p['link_alternative']

            print(' -- downloading ' + fname)
            f = pdc.getUri(uri, fname, download_dir=download_dir, check_md5sum=None)
            fpath.append(f)

    else:
        print(' -- no product to download')

    # fpath.append('/home/sebastien/DATA/data_satellite/ertaale/S1B_IW_SLC__1SDV_20171207T152618_20171207T152647_008615_00F4C0_7F71.zip')

    return (fpath, pdct2download_metadata)


@app.task
def archive(pdct, target_id=None, target_name=None):

    fpath = pdct[0]
    fmetadata = pdct[1]
    id_newentry = []

    for f, m in zip(fpath, fmetadata):
        # === check md5sum
        # cf. getUri & getMd5sum
        file_complete = True

        # === store to DB_MOUNTS.archive
        if file_complete is True:

            ftitle = m['title']
            ftype = ftitle[0:2]  # >> get product type = 'S1' | 'S2'
            fplatform = ftitle[2]  # >> get platform type = 'A' | 'B'

            # --- get metadata (both S1, S2)
            print('  | uploading ' + ftitle)

            acqstart_str = m['beginposition']
            acqstart_datetime = parse(acqstart_str).strftime('%Y-%m-%d %H:%M:%S.%f')
            acqstart_iso = parse(acqstart_str).strftime('%Y%m%dT%H%M%S')

            metadata = {'title': m['title'],
                        'producttype': m['producttype'],
                        'mission': (m['platformname'] + fplatform).upper(),
                        'acquisitionmode': m['sensoroperationalmode'] if ftype == 'S1' else '-',
                        'acqstarttime': acqstart_datetime,
                        'acqstarttime_str': acqstart_iso,
                        'relativeorbitnumber': m['relativeorbitnumber'],
                        'orbitdirection': m['orbitdirection'],
                        'polarization': m['polarisationmode'] if ftype == 'S1' else '-',
                        'abspath': f,
                        'target_id': str(target_id),
                        'target_name': target_name
                        }

            # --- load to data base
            dbo_bis.insert('DB_MOUNTS', 'archive', metadata)

            # TODO: return id of new archive
            id = 0
            id_newentry.append(id)

            print('      sql upload succeeded')

    return {'id': id_newentry}


@app.task
def print_targetname(arg):
    print(arg)
    return 'oulala'
