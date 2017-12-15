from celery import Celery, chain
import fetchme
import records
import json

app = Celery('tasks', broker='pyamqp://guest@localhost//')


# --- connect to MOUNTS database
db_host = '127.0.0.1'
db_usr = 'root'
db_pwd = 'br12Fol!'
db_type = 'mysql'
db_name = 'DB_MOUNTS'
db_url = db_type + '://' + db_usr + ':' + db_pwd + '@' + db_host + '/' + db_name
dbo = records.Database(db_url)


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

        # --- get target options
        target_name = r.name
        target_id = r.id
        subset_wkt = r.subset_wkt
        download_optn = json.loads(r.download)
        fq = download_optn['fq']
        scihub_optn = download_optn['scihub_optn']
        download_rootdir = download_optn['download_rootdir']
        if 'footprint' not in scihub_optn:
            scihub_optn['footprint'] = subset_wkt

        download_dir = download_rootdir + target_name
        print('---> download_dir = ' + download_dir)

        # --- set periodic task for target
        task = mounts_chain.s(scihub_optn, target_id, target_name, download_dir)  # .set(serializer='pickle')

        # - define by frequency
        sender.add_periodic_task(fq, task, name=target_name)

        # - define by crontab
        # sender.add_periodic_task(
        #    crontab(minute='*/15'),
        #    task,
        #    name=target_name)


@app.task
def mounts_chain(scihub_optn, target_id, target_name, download_dir):
    res = chain(
        query.s(scihub_optn, target_id),        # 1. query scihub
        download.s(download_dir=download_dir),  # 2. download uri
        archive.s()                             # 3. archive
    )()
    return res


@app.task
def query(scihub_optn, target_id):

    nb_products2compare = 2

    # === get latest products in SCIHUB
    scihub_optn['maxrecords'] = nb_products2compare
    pdct_inScihub = obj.scihub_search(**scihub_optn)
    pdct_inScihub_title = [p.metadata['title'] for p in pdct_inScihub]
    print('===> last 2 in scihub:')
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
        SELECT title 
        FROM DB_MOUNTS.archive 
        WHERE target_id = {} AND mission LIKE '{}' 
        ORDER BY acqstarttime DESC 
        LIMIT {}
        '''.format(target_id, mission, nb_products2compare)

    pdct_inMounts = dbo.query(stmt)
    pdct_inMounts_title = [p.title for p in pdct_inMounts]
    print('===> last 2 in db mounts:')
    for p in pdct_inMounts_title:
        print(p)

    # === get products to download
    pdct2download = [p for i, p in enumerate(pdct_inScihub) if p.metadata['title'] not in pdct_inMounts_title]
    pdct2download_uri = [p.metadata['link'] for p in pdct2download]
    pdct2download_title = [p.metadata['title'] for p in pdct2download]
    print('===> products to download:')
    for p in pdct2download_title:
        print(p)

    # # === download products
    # if pdct2download:
    #     for p in pdct2download:
    #         p.getQuicklook()
    #         # p.getFullproduct()

    return zip(pdct2download_title, pdct2download_uri)


@app.task
def download(pdct2download, download_dir=None):

    # === download products
    print('===> downloading products:')
    if pdct2download:
        for p in pdct2download:

            fname = p[0] + '.zip'
            uri = p[1]

            print(' -- downloading ' + fname)
            fpath = pdc.getUri(uri, fname, download_dir=download_dir, check_md5sum=None)

    else:
        print(' -- no product to download')

    return fpath


@app.task
def archive(fpath):

    # === check md5sum
    # cf. getUri & getMd5sum
    file_complete = True

    # === store to DB_MOUNTS.archive
    if file_complete is True:

        print('####### ' + fpath)
        # fname = os.path.basename(fpath)
        # ftype = fname[0:2]  # >> get product type = 'S1', 'S2'

        # # --- get metadata (both S1, S2)
        # print('  | retrieving metadata from ' + fname)
        # metadata = self.get_product_metadata(path_and_file=fpath, product_type=ftype)

        # if metadata is None:
        #     continue

        # # --- dict with column/value to upload
        # d = {}
        # d = {'abspath': fpath, 'target_id': str(target_id), 'target_name': target_name}
        # d.update(metadata)

        # if print_metadata:
        #     print d

        # self.insert('DB_MOUNTS', 'archive', d)

        # # --- store archive zip files (S1+S2) of each listed target to database
        # stmt = "SELECT name FROM DB_MOUNTS.targets"
        # rows = dbo.execute_query(stmt)
        # for r in rows:
        #     volcanoname = r.name
        #     archive_dir = '/home/' + username + '/DATA/data_satellite/' + volcanoname
        #     dbo.dbmounts_loadarchive(path_dir=archive_dir, target_name=volcanoname, print_metadata=0)


@app.task
def print_targetname(arg):
    print(arg)
    return 'oulala'
