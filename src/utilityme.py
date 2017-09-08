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

    def create_tb(self, dbname=None, tbname=None, dicts=None, primarykey=None):
        """Create table.

        Query statement: "CREATE TABLE table (field1 type1, field2 type2, ...)"

        """

        table = '.'.join([dbname, tbname])

        q = 'create table {}'.format(table)
        a = []
        for k, v in dicts.items():
            a.append(str(k) + ' ' + str(v))

        if primarykey is not None:
            # WARNING: primary key cannot be of type TEXT.
            # In order to use a column containing a string as a primary key, a length should be specified (=> how many characters to guarantee maintain unique).
            # This is only possible with VARCHAR type, ex: VARCHAR(100)
            q += ' (' + ','.join(a) + ', PRIMARY KEY (' + primarykey + ')) '
        else:
            q += ' (' + ','.join(a) + ') '

        # --- add id column as primary key with auto_incrementation
        # q += ' (id int NOT NULL AUTO_INCREMENT, ' + ','.join(a) + ', PRIMARY KEY (id))'

        self.execute_query(q)

    def delete_tb(self, dbname=None, tbname=None):
        q = "DROP TABLE IF EXISTS " + dbname + '.' + tbname
        self.execute_query(q)

    def empty_tb(self, dbname=None, tbname=None):
        q = "TRUNCATE TABLE " + '.'.join([dbname, tbname])
        self.execute_query(q)

    def gen_insert(self, dbname=None, tbname=None, dicts=None):
        """Generate insert statement.
            "INSERT INTO table1 (field1, field2, ...) VALUES (value1, value2, ...)"

            dicts = {'col1':'val1', 'col2':'val2', ...}

            NB: 'ignore' statement will generate a warning when entery already exists
        """
        table = '.'.join([dbname, tbname])

        q = 'insert ignore into {}'.format(table)
        ksql = []
        vsql = []
        for k, v in dicts.items():
            ksql.append(str(k))
            vsql.append("'" + str(v) + "'")
        q += ' (' + ','.join(ksql) + ') '
        q += ' values (' + ','.join(vsql) + ')'

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
        print q
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

    def store_dir2db(self, path_dir, dbname=None, tbname=None):
        """Store file name and abspath contained in a directory into a datablase with fields 'prod_title' and 'prod_abspath'"""

        if dbname == 'DB_ARCHIVE':
            file_ext = 'zip'
            colname_title = 'prod_title'
            colname_abspath = 'prod_abspath'
        elif dbname == 'DB_RESULTS':
            file_ext = 'png'
            colname_title = 'file_name'
            colname_abspath = 'file_abspath'

        import glob
        f = glob.glob(os.path.join(path_dir, '') + '*.' + file_ext)

        for k, fpath in enumerate(f):
            fname = os.path.basename(fpath)
            ftitle = os.path.splitext(fname)[0]

            self.insert(dbname, tbname, {colname_title: ftitle, colname_abspath: fpath})

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

    def dbarch_newtable(self, tbname=None):
        """Use names recovered from "get_metadata_abstracted" (snapme)."""
        dicts = {'title': 'VARCHAR(100)',
                 'abspath': 'TEXT',
                 'producttype': 'TEXT',
                 'mission': 'TEXT',
                 'orbitdirection': 'TEXT',
                 'relativeorbitnumber': 'TEXT',
                 'acquisitionmode': 'TEXT',
                 'acqstarttime': 'DATETIME',
                 'polarization': 'TEXT'}

        self.create_tb(dbname='DB_ARCHIVE', tbname=tbname, dicts=dicts, primarykey='title')

    def dbarch_loaddir(self, path_dir, tbname=None):

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
            d = {'abspath': fpath}
            d.update(metadata_abs)

            self.insert('DB_ARCHIVE', tbname, d)
