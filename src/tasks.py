import fetchme
import utilme
import pandas as pd
from sqlalchemy import create_engine
import os
import json

# === initialize
# - get database credentials
f = file('./conf/credentials_mysql.txt')
(db_usr, db_pwd) = f.readline().split(' ')
db_url = 'mysql://{}:{}@127.0.0.1/DB_MOUNTS'.format(db_usr, db_pwd)

# - connect to database
dbo = utilme.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

# - create disk engine (sql alchemy)
db_url = 'mysql://{}:{}@127.0.0.1/DB_MOUNTS'.format(db_usr, db_pwd)
dbe = create_engine(db_url)

# - fetchme instance
obj = fetchme.Scihub()


def query(optn, optn_s1=None, optn_s2=None):

    print('=== QUERY task')

    p = []

    # - get target footprint
    stmt = "SELECT * FROM DB_MOUNTS.targets WHERE name = '{}' ".format(optn['target_name'])
    df = pd.read_sql(stmt, dbe)  # =>  name = df.name[0]
    footprint = df.subset_wkt[0]

    # === loop through monthly bins (starting on 01 of each month)
    monthlist = pd.date_range(start=optn['acq_tspan'][0], end=optn['acq_tspan'][1], freq='MS').strftime('%Y-%m-%d')  # MS=month start
    for i in range(len(monthlist) - 1):

        beginposition = "[{}T00:00:00.000Z TO {}T00:00:00.000Z]".format(monthlist[i], monthlist[i + 1])
        print('- ' + beginposition)

        # --- query scihub
        if optn_s1:
            s1 = obj.scihub_search(
                beginposition=beginposition,
                footprint=footprint,
                plot_footprints=0,
                print_result=optn['print_queryresult'],
                **optn_s1)
            p.extend(s1)

        if optn_s2:
            s2 = obj.scihub_search(
                beginposition=beginposition,
                footprint=footprint,
                plot_footprints=0,
                print_result=optn['print_queryresult'],
                **optn_s2)
            p.extend(s2)

    # return p, footprint, obj #=> will return list
    return p


def download(productlist,
             download_pnode=None,
             download_rootdir=None,
             load_2dbarchive=None,
             target_name=None,
             target_id=None):

    print('=== DOWNLOAD task')

    if target_id is None:
        target_id = dbo.dbmounts_target_nameid(target_name=target_name)

    if target_name is None:
        target_name = dbo.dbmounts_target_nameid(target_id=target_id)

    if download_pnode not in ('quicklook', 'fullproduct'):
        print('WARNING: "download_pnode" invalid, set to quicklook | fullproduct')
        return

    # - set download dir
    if download_rootdir:
        download_dir = os.path.join(download_rootdir, target_name)
    else:
        download_dir = None

    for p in productlist:

        # --- download
        if download_pnode == 'quicklook':
            fpath = p.getQuicklook(download_dir=download_dir)
        elif download_pnode == 'fullproduct':
            fpath = p.getFullproduct(download_dir=download_dir)

        # --- store to db
        if load_2dbarchive:
            print('      => uploading DB_mounts.archive (update if entry exists)')
            dbo.dbmounts_loadproduct(p, fpath, target_name, target_id,
                                     loadarchive=1,
                                     printme=0)
            # TODO: get id of new/updated row
            print('      => upload successful')


def process(pcss_sar, pcss_dinsar, pcss_nir,
            cfg_productselection=None,
            ignore_processedProducts=None,
            store_result2db=None,
            print_sqlResult=None,
            print_sqlQuery=None,
            quit_after_querydb=None,
            pathout_root=None):
    # TODO: enable possibility to avoid reprocessing of data if existing

    # FAILED ATTEMPS TO RELEASE MEMORY
    # NB: exiting the function is not enough, to release the program must exit ...
    #
    # http://forum.step.esa.int/t/how-to-free-java-memory-snappy/5738
    # import jpy
    # System = jpy.get_type('java.lang.System')
    # System.gc()
    #
    # https://github.com/kedziorm/mySNAPscripts/blob/master/myScripts.py
    # import os
    # os.system('ulimit -c unlimited')

    import snapme as gpt

    # --- get processing options
    stmt = "SELECT * FROM DB_MOUNTS.targets WHERE name = '{}' ".format(cfg_productselection['target_name'])
    df = pd.read_sql(stmt, dbe)  # =>  name = df.name[0]
    pcss_str = df.processing[0]

    if pcss_str is None or not pcss_str:
        print('Processing options not defined in database for target "{}"'.format(target_name))
        return
    else:
        pcss = json.loads(pcss_str)

    # --- run dinsar
    if 'dinsar' in pcss and pcss_dinsar:
        # cfg_productselection = {'target_name': target_name, 'acqstarttime': acqstarttime}     # = sql search options
        cfg_dinsar = pcss['dinsar']             # = dinsar options
        cfg_plot = {'subset_wkt': df.subset_wkt[0], 'pathout_root': pathout_root, 'thumbnail': True}
        gpt.dinsar(cfg_productselection, cfg_dinsar, cfg_plot,
                   ignore_processedProducts=ignore_processedProducts,
                   store_result2db=store_result2db,
                   print_sqlQuery=print_sqlQuery,
                   print_sqlResult=print_sqlResult,
                   quit_after_querydb=quit_after_querydb)

    # --- run sar
    if 'sar' in pcss and pcss_sar:
        # cfg_productselection = {'target_name': target_name, 'acqstarttime': acqstarttime}     # = sql search options
        cfg_sar = pcss['sar']             # = dinsar options
        cfg_plot = {'subset_wkt': df.subset_wkt[0], 'pathout_root': pathout_root, 'thumbnail': True}
        gpt.sar(cfg_productselection, cfg_sar, cfg_plot,
                ignore_processedProducts=ignore_processedProducts,
                store_result2db=store_result2db,
                print_sqlQuery=print_sqlQuery,
                print_sqlResult=print_sqlResult,
                quit_after_querydb=quit_after_querydb)

    # --- run nir
    if 'nir' in pcss and pcss_nir:
        # cfg_productselection = {'target_name': target_name, 'acqstarttime': acqstarttime}     # = sql search options
        cfg_nir = pcss['nir']             # = nir options
        cfg_plot = {'subset_wkt': df.subset_wkt[0], 'pathout_root': pathout_root, 'thumbnail': True}
        gpt.nir(cfg_productselection, cfg_nir, cfg_plot,
                ignore_processedProducts=ignore_processedProducts,
                store_result2db=store_result2db,
                print_sqlQuery=print_sqlQuery,
                print_sqlResult=print_sqlResult,
                quit_after_querydb=quit_after_querydb)
