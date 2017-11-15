import yaml
import sched
import time
import records
import logging
import os


def read_configfile(configfile):
    with open(configfile, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)

    return cfg


class PeriodicScheduler(object):
    def __init__(self):
        self.scheduler = sched.scheduler(time.time, time.sleep)

    def setup(self, interval, action, actionargs=()):
        action(*actionargs)
        self.scheduler.enter(interval, 1, self.setup,
                             (interval, action, actionargs))

    def run(self):
        self.scheduler.run()


class Database:
    """Work sql database.

    Examples:
        # --- access database
        dbo = utils.Database(db_host='127.0.0.1', db_usr='root', db_pwd='wave', db_type='mysql')
        print(dbo.db_url)

        # --- create database
        dbo.create_db('glaciers')

        # --- delete database
        dbo.delete_db('glaciers')

        # --- create table (without primary key)
        dicts = {'prod_title': 'TEXT', 'prod_abspath': 'TEXT'}
        dbo.create_tb(dbname='volcanoes', tbname='ertaale', dicts=dicts)

        # --- create table (specifying primary key)
        # dicts = {'prod_title': 'VARCHAR(100)', 'prod_abspath': 'TEXT'}
        # dbo.create_tb(dbname='DB_ARCHIVE', tbname='ertaale', dicts=dicts, primarykey='prod_title')

        # --- empty table
        dbo.empty_tb(dbname='volcanoes', tbname='ertaale')

        # --- delete table
        dbo.delete_tb(dbname='volcanoes', tbname='nyiragongo')

        # --- insert value
        dicts = {'prod_type': 'S3', 'prod_title': 'S3A_xxx_xxx', 'prod_abspath': 'oulala'}
        dbo.insert(dbname='volcanoes', tbname='ertaale', dicts=dicts)

        # --- get value
        rows = dbo.get_dataset(dbname='DB_ARCHIVE', tbname='ertaale')
        print rows[0][1]
        for r in rows:
            print(r.prod_title, r.prod_abspath)

        # --- execute query
        q = "INSERT INTO volcanoes.ertaale (prod_type, prod_title, prod_abspath) VALUES ('S1', 'S1_2017', 'data/ertaale/');"
        dbo.execute_query(q)

        # --- print content
        dbo.print_dataset(dbname='volcanoes', tbname='ertaale')  # , colname='prod_title')

        # -- store archive zip files to database
        dbo.store_dir2db('/home/sebastien/DATA/data_satellite/ERTAALE', dbname='DB_ARCHIVE', tbname='ertaale')

    To do:
        # --- search for elements which contain a given string (string from column 'prod_title', starting at character 13, length 4 character)
        SELECT prod_title FROM DB_ARCHIVE.ertaale WHERE SUBSTRING(prod_title,13,4) = '1SSV'

    """

    def __init__(self, db_host='127.0.0.1', db_usr='root', db_pwd='root', db_type='mysql', db_name=None):
        self.db_host = db_host
        self.db_usr = db_usr
        self.db_pwd = db_pwd
        self.db_url = db_type + '://' + db_usr + ':' + db_pwd + '@' + db_host + '/'
        if db_name is not None:
            self.db_url += db_name

        self.db_conn = self.connect()

    def connect(self):
        """Connect to database from url. Return database connector."""
        # logging.info('Connecting to database ' + self.db_url)
        dbc = records.Database(self.db_url)
        return dbc

    def execute_query(self, query_statement):
        """Execute query statement"""
        return self.db_conn.query(query_statement)

    def create_db(self, dbname):
        q = "CREATE DATABASE " + dbname
        self.execute_query(q)

    def delete_db(self, dbname):
        q = "DROP DATABASE " + dbname
        self.execute_query(q)

    def create_tb(self, dbname=None, tbname=None, dicts=None, primarykey=None, foreignkey=None, foreignkey_ref=None):
        """Create table.

        Query statement: "CREATE TABLE table (field1 type1, field2 type2, ...)"

        """

        # --- set field/type
        a = []
        for k, v in dicts.items():
            a.append(str(k) + ' ' + str(v))

            # --- if field 'id' -> add autoincrement behavior
            # if k == 'id':
            #     a[-1] = str(k) + ' ' + str(v) + ' NOT NULL AUTO_INCREMENT'

        # --- set PRIMARY KEY
        if primarykey is not None:
            # WARNING: primary key cannot be of type TEXT.
            # In order to use a column containing a string as a primary key, a length should be specified (=> how many characters to guarantee maintain unique).
            # This is only possible with VARCHAR type:
            # EX:   >> dicts = {'title': 'VARCHAR(100)', 'abspath': 'TEXT'}
            #       >> dbo.create_tb(dbname='DB_ARCHIVE', tbname='etna', dicts=dicts, primarykey='title')
            a.append('PRIMARY KEY (' + primarykey + ')')

        # --- set FOREIGN KEY constraint
        if foreignkey is not None and foreignkey_ref is not None:
            # Syntax: FOREIGN KEY (foreignkey) REFERENCES foreignkey_ref
            #   where: foreignkey = colname, foreignkey_ref = dbref_name.tbref_name(colref_name)
            # Example: FOREIGN KEY (ref_master) REFERENCES DB_ARCHIVE.etna(title)
            # Source: https://www.w3schools.com/sql/sql_foreignkey.asp
            # Usage: => The INNER JOIN keyword selects records that have matching values in both tables.
            #   stmt = "SELECT acqstarttime_str FROM DB_ARCHIVE.ertaale INNER JOIN DB_RESULTS.ertaale ON DB_ARCHIVE.ertaale.title=DB_RESULTS.ertaale.ref_master;"
            #   rows = dbo.execute_query(stmt)

            if isinstance(foreignkey, str):
                foreignkey = [foreignkey]
                foreignkey_ref = [foreignkey_ref]

            for k, fkey in enumerate(foreignkey):
                a.append('FOREIGN KEY (' + foreignkey[k] + ') REFERENCES ' + foreignkey_ref[k])

        table = '.'.join([dbname, tbname])
        q = 'create table {}'.format(table)
        q += ' (' + ', '.join(a) + ') '

        self.execute_query(q)

    def delete_tb(self, dbname=None, tbname=None):
        q = "DROP TABLE IF EXISTS " + dbname + '.' + tbname
        self.execute_query(q)

    def empty_tb(self, dbname=None, tbname=None):
        q = "TRUNCATE TABLE " + '.'.join([dbname, tbname])
        self.execute_query(q)

    def gen_insert(self, dbname=None, tbname=None, dicts=None):
        """Generate insert statement.
            One row:
                dicts = {'col1':'val1', 'col2':'val2', ...}
                "INSERT INTO table1 (field1, field2, ...) VALUES ('value1', 'value2', ...)"
            Multiple rows:
                dicts = {'col1':['row1', 'row2'], 'col2':['row1', 'row2'], ...}
                "INSERT INTO table1 (field1, field2, ...) VALUES ('row1_col1', 'row1_col2', ...), ('row2_col1', 'row2_col2', ...)"

            NB: 'ignore' statement will generate a warning when entery already exists
        """

        # === one row insertion only:
        # table = '.'.join([dbname, tbname])
        # q = 'insert ignore into {}'.format(table)

        # ksql = []
        # vsql = []
        # for k, v in dicts.items():
        #     ksql.append(str(k))
        #     vsql.append("'" + str(v) + "'")
        # q += ' (' + ','.join(ksql) + ') '
        # q += ' values (' + ','.join(vsql) + ')'

        # TODO: check example on Records lib website using variables !? (https://github.com/kennethreitz/records/blob/master/examples/randomuser-sqlite.py)
        # db.query('INSERT INTO persons (key, fname, lname, email) VALUES(:key, :fname, :lname, :email)', key=key, fname=fname, lname=lname, email=email)

        # --- get dbname.tbname
        tb_str = '.'.join([dbname, tbname])

        # --- get keys (columns)
        k_list = dicts.keys()
        k_str = '(' + ', '.join(k_list) + ') '

        # --- get values
        v_list = dicts.values()

        if isinstance(v_list[0], list):
            # => nested list: multiple rows to write
            row_dict = []
            for r in zip(*v_list):
                row_str = '(' + ', '.join('"' + item + '"' for item in r) + ')'
                row_dict.append(row_str)
            v_str = ', '.join(row_dict)

        else:
            v_str = '(' + ', '.join('"' + item + '"' for item in v_list) + ')'

        # --- assemble query string
        q = 'insert ignore into {} {} values {}'.format(tb_str, k_str, v_str)

        return q

    def gen_select(self, dbname=None, tbname=None, colname='*'):
        # Select all columns from database/table
        #   SELECT * FROM sentinel1.ertaale
        # Select only one column:
        #   SELECT product_title FROM sentinel1.ertaale

        a = '.'.join([dbname, tbname])
        q = 'select {} from {}'.format(colname, a)
        return q

    def insert(self, dbname=None, tbname=None, dicts=None):
        """Insert values into database."""

        q = self.gen_insert(dbname=dbname, tbname=tbname, dicts=dicts)

        self.db_conn.query(q)

    def print_dataset(self, dbname=None, tbname=None, colname='*'):

        q = self.gen_select(dbname=dbname, tbname=tbname, colname=colname)
        rows = self.db_conn.query(q)

        if not rows.all():
            logging.info('Table is empty!')
            quit()

        print(rows.dataset)

    def get_dataset(self, dbname=None, tbname=None, colname='*'):
        q = self.gen_select(dbname=dbname, tbname=tbname, colname=colname)

        rows = self.db_conn.query(q)

        # return rows
        return rows.all()

    def get_product_metadata(self, path_and_file=None):
        from snapme import read_product
        from snapme import get_metadata_abstracted

        p = read_product(path_and_file=path_and_file)
        if p is None:
            logging.info('ERROR opening zip file = skipping')
            metadata_abs = None
            return

        metadata_abs = get_metadata_abstracted(p)

        return metadata_abs

    # -----------------------------------------------------------------------------------------

    # TODO: delete, obsolete as of v2 ?
    # def store_dir2db(self, path_dir, dbname=None, tbname=None):
    #     """Store file name and abspath contained in a directory into a datablase with fields 'prod_title' and 'prod_abspath'"""
    #
    #     if dbname == 'DB_ARCHIVE':
    #         file_ext = 'zip'
    #         colname_title = 'prod_title'
    #         colname_abspath = 'prod_abspath'
    #     elif dbname == 'DB_RESULTS':
    #         file_ext = 'png'
    #         colname_title = 'file_name'
    #         colname_abspath = 'file_abspath'
    #
    #     import glob
    #     f = glob.glob(os.path.join(path_dir, '') + '*.' + file_ext)
    #
    #     for k, fpath in enumerate(f):
    #         fname = os.path.basename(fpath)
    #         ftitle = os.path.splitext(fname)[0]
    #
    #         self.insert(dbname, tbname, {colname_title: ftitle, colname_abspath: fpath})

    # TODO: delete, obsolte as of v2
    # def dbarch_newtable(self, tbname=None):
    #     """Use names recovered from "get_metadata_abstracted" (snapme)."""
    #     dicts = {'title': 'VARCHAR(100)',
    #              'abspath': 'TEXT',
    #              'producttype': 'TEXT',
    #              'mission': 'TEXT',
    #              'orbitdirection': 'TEXT',
    #              'relativeorbitnumber': 'TEXT',
    #              'acquisitionmode': 'TEXT',
    #              'acqstarttime': 'DATETIME',
    #              'acqstarttime_str': 'TEXT',
    #              'polarization': 'TEXT'}
    #
    #     self.create_tb(dbname='DB_ARCHIVE', tbname=tbname, dicts=dicts, primarykey='title')

    # TODO: remove, replaced by 'dbmounts_loadarchive' as of v2
    # def dbarch_loaddir(self, path_dir, tbname=None):
    #
    #     import glob
    #     f = glob.glob(os.path.join(path_dir, '') + '*.zip')
    #
    #     for k, fpath in enumerate(f):
    #         fname = os.path.basename(fpath)
    #
    #         # --- get metadata
    #         metadata_abs = self.get_product_metadata(path_and_file=fpath)
    #
    #         if metadata_abs is None:
    #             continue
    #
    #         # --- dict with column/value to upload
    #         d = {}
    #         d = {'abspath': fpath}
    #         d.update(metadata_abs)
    #
    #         self.insert('DB_ARCHIVE', tbname, d)

    # TODO: delete, obsolte as of v2
    # def dbres_newtable(self, tbname=None):
    #
    #     dicts = {'title': 'VARCHAR(100)',
    #              'abspath': 'TEXT',
    #              'type': 'TEXT',
    #              'master_title': 'VARCHAR(100)',
    #              'slave_title': 'VARCHAR(100)'}
    #
    #     self.create_tb(dbname='DB_RESULTS',
    #                    tbname=tbname,
    #                    dicts=dicts,
    #                    primarykey='title',
    #                    foreignkey=['master_title', 'slave_title'],
    #                    foreignkey_ref=['DB_ARCHIVE.' + tbname + '(title)', 'DB_ARCHIVE.' + tbname + '(title)']
    #                    )

    def dbmounts_loadarchive(self, path_dir=None, target_name=None, print_metadata=None):

        # --- get 'target_id' => find in table 'DB_MOUNTS.target' element matching selected name
        stmt = "SELECT id FROM DB_MOUNTS.targets WHERE name = '{}'".format(target_name)
        rows = self.execute_query(stmt)
        target_id = rows[0][0]

        import glob
        f = glob.glob(os.path.join(path_dir, '') + '*.zip')

        for k, fpath in enumerate(f):
            fname = os.path.basename(fpath)

            # --- get metadata
            metadata_abs = self.get_product_metadata(path_and_file=fpath)

            if metadata_abs is None:
                continue

            # --- dict with column/value to upload
            d = {}
            d = {'abspath': fpath, 'target_id': str(target_id), 'target_name': target_name}
            d.update(metadata_abs)

            if print_metadata:
                print d

            self.insert('DB_MOUNTS', 'archive', d)

    def dbmounts_archive_querystmt(self,
                                   id=None,
                                   title=None,
                                   abspath=None,
                                   producttype=None,
                                   mission=None,
                                   orbitdirection=None,
                                   relativeorbitnumber=None,
                                   acquisitionmode=None,
                                   acqstarttime=None,
                                   acqstarttime_str=None,
                                   polarization=None,
                                   target_id=None,
                                   target_name=None):

        # --- format sql options as "option1='value' and option2='value2 and ..." => parse the locals() dictionnary (excluding 'self' and None values)
        options = [k[0] + "='" + str(k[1]) + "'" for k in locals().iteritems() if k[1] is not None and k[0] != 'self']
        options = ' and '.join(options)

        # --- TODO: format acqstarttime to allow syntax like:
        # stmt = "SELECT * FROM DB_ARCHIVE.ertaale WHERE acqstarttime >= '2016-01-01' and acqstarttime < '2017-01-01'"

        # --- contruct SQL statement
        # NB: results ordered by orbitdirection/acqstarttime, in ascending order
        stmt = "SELECT * FROM DB_MOUNTS.archive WHERE {} ORDER BY orbitdirection, acqstarttime ASC".format(options)

        return stmt

    # def dbmounts_addtarget(self,
    #                        id=None,
    #                        fullname=None,
    #                        name=None,
    #                        country=None,
    #                        lat=None,
    #                        lon=None,
    #                        processing=None,
    #                        download=None):

    #     dicts = {'id': str(id), 'fullname': fullname, 'name': name, 'country': country, 'lat': str(lat), 'lon': str(lon), 'processing': processing, 'download': download}
    #     dicts_no_none = dict((k, v) for k, v in dicts.iteritems() if v is not None)

    #     # TODO: Python3 syntax:
    #     # dicts_no_none = {k:v for k,v in dicts.items() if v is not None}

    #     self.insert('DB_MOUNTS', 'targets', dicts_no_none)

    def dbmounts_addtarget(self, **kwargs):

        # --- make sure every values is passed in as string in mysql statement
        dicts = dict((k, str(v)) for k, v in kwargs.iteritems())

        self.insert('DB_MOUNTS', 'targets', dicts)

    def dbmounts_target_nameid(self, target_name=None, target_id=None):
        """ Get target_id from name, or target_name from id. """

        if target_id is None and target_name is not None:
            stmt = "SELECT id FROM DB_MOUNTS.targets WHERE name = '{}'".format(target_name)
        elif target_id is not None and target_name is None:
            stmt = "SELECT name FROM DB_MOUNTS.targets WHERE id = '{}'".format(target_id)

        rows = self.execute_query(stmt)

        return rows[0][0]
