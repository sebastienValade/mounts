import utilityme as utils
import json
# import ast

username = 'sebastien'
setup_database = 0
process_archive = 1
pcss_dinsar = 0
pcss_sar = 0
pcss_nir = 1

acqstarttime = '>2016-12-29 <2017-01-01'
quit_after_querydb = None
print_sqlResult = 1

# --- get database credentials
f = file('./conf/credentials_mysql.txt')
(db_usr, db_pwd) = f.readline().split(' ')

# --- connect to database
dbo = utils.Database(db_host='127.0.0.1', db_usr=db_usr, db_pwd=db_pwd, db_type='mysql')

# =============================================
# setup DATABASE
# =============================================

if setup_database:
    dbo.delete_db('DB_MOUNTS')

    # --- create db
    dbo.create_db('DB_MOUNTS')

    # --- create tb "targets"
    tbname = 'targets'
    dicts = {'id': 'INT',               # = volcano number defined by GVP
             'fullname': 'CHAR(100)',   # = Erta Ale
             'name': 'CHAR(100)',       # = ertaale
             'country': 'CHAR(100)',    # = Ethiopia
             'lat': 'FLOAT',
             'lon': 'FLOAT',
             'alt': 'INT',
             'download': 'TEXT',
             'processing': 'TEXT',
             'subset_wkt': 'TEXT'}
    dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts, primarykey='id')

    # --- create tb "archive"
    # NB: fieldnames taken from names recovered by "get_metadata_S1" (snapme)
    tbname = 'archive'
    dicts = {'id': 'INT NOT NULL AUTO_INCREMENT',
             'title': 'VARCHAR(150)',
             'abspath': 'TEXT',
             'producttype': 'CHAR(25)',
             'mission': 'CHAR(25)',
             'orbitdirection': 'CHAR(25)',
             'relativeorbitnumber': 'CHAR(25)',
             'acquisitionmode': 'CHAR(25)',
             'acqstarttime': 'DATETIME',
             'acqstarttime_str': 'CHAR(25)',
             'polarization': 'CHAR(25)',
             'target_id': 'INT',
             'target_name': 'CHAR(100)'}
    dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts,
                  primarykey='id',
                  foreignkey='target_id',
                  foreignkey_ref='DB_MOUNTS.targets(id)',
                  unique_contraint='title')  # => title values cannot be duplicate

    # --- create tb "results_img"
    tbname = 'results_img'
    dicts = {'id': 'INT NOT NULL AUTO_INCREMENT',
             'title': 'VARCHAR(100)',
             'abspath': 'TEXT',
             'type': 'CHAR(25)',    # ifg, coh, nir, ref (= reference process used to analyze rather then plot)
             'id_master': 'INT',
             'id_slave': 'INT',
             'target_id': 'INT'}
    dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts,
                  primarykey='id',
                  foreignkey=['id_master', 'id_slave', 'target_id'],
                  foreignkey_ref=['DB_MOUNTS.archive(id)', 'DB_MOUNTS.archive(id)', 'DB_MOUNTS.targets(id)'],
                  unique_contraint='title')  # => title values cannot be duplicate

    # --- create tb "results_dat"
    tbname = 'results_dat'
    dicts = {'id': 'INT NOT NULL AUTO_INCREMENT',
             'time': 'DATETIME',
             'data': 'FLOAT',
             'type': 'CHAR(25)',
             'id_image': 'INT',     # -> id result_img
             'target_id': 'INT'}
    dbo.create_tb(dbname='DB_MOUNTS', tbname=tbname, dicts=dicts,
                  primarykey='id',
                  foreignkey=['id_image', 'target_id'],
                  foreignkey_ref=['DB_MOUNTS.results_img(id)', 'DB_MOUNTS.targets(id)'],
                  unique_contraint=['time', 'type'])  # => combination of time/type values should be unique, no duplicates

    # =============================================
    # load DATABASE
    # =============================================

    # --- add volcanoes to target list (no json)
    # dbo.dbmounts_addtarget(id=221080, fullname='Erta Ale', name='ertaale', country='Ethiopia', lat=13.6, lon=40.67, alt=613,
    #                        processing="{'dinsar': {'subswath':'IW2', 'polarization':'VV', 'bands2plot':['ifg', 'coh']}, 'sar': {'subswath': 'IW2', 'bands2plot': ['int_HV']}, 'nir': {'bname_red':'B12', 'bname_green':'B11', 'bname_blue':'B8A'} }",
    #                        subset_wkt='POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))')
    # dbo.dbmounts_addtarget(id=211060, fullname='Etna', name='etna', country='Italy', lat=37.748, lon=14.999, alt=3295,
    #                        processing="{'dinsar': {'subswath':'IW2', 'polarization':'VV', 'bands2plot':['ifg', 'coh']}, 'sar': {'subswath': 'IW2', 'bands2plot': ['int_HV']}, 'nir': {'bname_red':'B12', 'bname_green':'B11', 'bname_blue':'B8A'} }",
    #                        subset_wkt='POLYGON((14.916129 37.344437, 14.979386 37.344437, 14.979386 37.306283, 14.916129 37.306283, 14.916129 37.344437))')
    # NB: alternative way to populate target list
    # dict_targets = {'id': ['1', '2'],
    #             'fullname': ['COCO', 'NUT'],
    #             'name': ['BANANA', 'SHAKE'],
    #             'country': ['Italy', 'Ethiopia'],
    #             'lat': ['1', '1'],
    #             'lon': ['2', '2'],
    #             'alt': ['3', '3'],
    #             'processing': ['a', 'b'],
    #             'subset_wkt': ['a', 'b']}
    # dbo.insert('DB_MOUNTS', 'targets', dict_targets)

    # --- add volcanoes to target list (json supported)
    processing = dict(
        dinsar=dict(
            subswath='IW2',
            polarization='VV',
            bands2plot=['ifg', 'coh']),
        sar=dict(
            subswath='IW2',
            bands2plot=['int_HV']),
        nir=dict(
            bname_red='B12',
            bname_green='B11',
            bname_blue='B8A')
    )

    dbo.dbmounts_addtarget_sqlalchemy(id=221080, fullname='Erta Ale', name='ertaale', country='Ethiopia', lat=13.6, lon=40.67, alt=613,
                                      processing=processing,
                                      subset_wkt='POLYGON((40.63 13.64, 40.735 13.64, 40.735 13.53, 40.63 13.53, 40.63 13.64))')
    dbo.dbmounts_addtarget_sqlalchemy(id=211060, fullname='Etna', name='etna', country='Italy', lat=37.748, lon=14.999, alt=3295,
                                      processing=processing,
                                      subset_wkt='POLYGON((14.916129 37.344437, 14.979386 37.344437, 14.979386 37.306283, 14.916129 37.306283, 14.916129 37.344437))')

    # --- store archive zip files (S1+S2) of each listed target to database
    stmt = "SELECT name FROM DB_MOUNTS.targets"
    rows = dbo.execute_query(stmt)
    for r in rows:
        volcanoname = r.name
        archive_dir = '/home/' + username + '/DATA/data_satellite/' + volcanoname
        dbo.dbmounts_loadarchive(path_dir=archive_dir, target_name=volcanoname, print_metadata=0)


# =============================================
# process ARCHIVE
# =============================================

if process_archive:
    # => loop through 'targets' and run field 'processing'

    import snapme as gpt
    stmt = "SELECT * FROM DB_MOUNTS.targets"
    rows = dbo.execute_query(stmt)
    for r in rows:

        volcanoname = r.name
        pcss_str = r.processing
        print volcanoname

        if volcanoname == 'etna':
            continue

        # --- get processing options if defined
        if pcss_str is None or not pcss_str:
            continue
        else:
            # pcss = ast.literal_eval(pcss_str)
            pcss = json.loads(pcss_str)

        # --- run dinsar
        if 'dinsar' in pcss and pcss_dinsar:
            cfg_productselection = {'target_name': volcanoname, 'acqstarttime': acqstarttime}     # = sql search options
            cfg_dinsar = pcss['dinsar']             # = dinsar options
            cfg_plot = {'subset_wkt': r.subset_wkt, 'pathout_root': '/home/' + username + '/DATA/data_mounts/', 'thumbnail': True}
            gpt.dinsar(cfg_productselection, cfg_dinsar, cfg_plot, store_result2db=True, print_sqlResult=print_sqlResult, quit_after_querydb=quit_after_querydb)

        # --- run dinsar
        if 'sar' in pcss and pcss_sar:
            cfg_productselection = {'target_name': volcanoname, 'acqstarttime': acqstarttime}     # = sql search options
            cfg_sar = pcss['sar']             # = dinsar options
            cfg_plot = {'subset_wkt': r.subset_wkt, 'pathout_root': '/home/' + username + '/DATA/data_mounts/', 'thumbnail': True}
            gpt.sar(cfg_productselection, cfg_sar, cfg_plot, store_result2db=True, print_sqlResult=print_sqlResult, quit_after_querydb=quit_after_querydb)

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

        # --- run nir
        if 'nir' in pcss and pcss_nir:
            cfg_nir = pcss['nir']             # = nir options
            cfg_productselection = {'target_name': volcanoname, 'acqstarttime': acqstarttime}     # = sql search options
            cfg_plot = {'subset_wkt': r.subset_wkt, 'pathout_root': '/home/' + username + '/DATA/data_mounts/', 'thumbnail': True}
            gpt.nir(cfg_productselection, cfg_nir, cfg_plot, store_result2db=True, print_sqlResult=print_sqlResult, quit_after_querydb=quit_after_querydb)
