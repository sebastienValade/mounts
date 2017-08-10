import yaml
import sched
import time
import records
import logging


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

        # --- create table
        dicts = {'prod_title': 'TEXT', 'prod_abspath': 'TEXT'}
        dbo.create_tb(dbname='volcanoes', tbname='ertaale', dicts=dicts)

        # --- empty table
        dbo.empty_tb(dbname='volcanoes', tbname='ertaale')

        # --- delete table
        dbo.delete_tb(dbname='volcanoes', tbname='nyiragongo')

        # --- insert value
        dicts = {'prod_type': 'S3', 'prod_title': 'S3A_xxx_xxx', 'prod_abspath': 'oulala'}
        dbo.insert(dbname='volcanoes', tbname='ertaale', dicts=dicts)

        # --- execute query
        q = "INSERT INTO volcanoes.ertaale (prod_type, prod_title, prod_abspath) VALUES ('S1', 'S1_2017', 'data/ertaale/');"
        dbo.execute_query(q)

        # --- print content
        dbo.print_dataset(dbname='volcanoes', tblname='ertaale')  # , colname='prod_title')
    """

    def __init__(self, db_host='127.0.0.1', db_usr='root', db_pwd='root', db_type='mysql'):
        self.db_host = db_host
        self.db_usr = db_usr
        self.db_pwd = db_pwd
        self.db_url = db_type + '://' + db_usr + ':' + db_pwd + '@' + db_host + '/'
        self.db_conn = self.connect()

    def connect(self):
        """Connect to database from url. Return database connector."""
        # logging.info('Connecting to database ' + self.db_url)
        dbc = records.Database(self.db_url)
        return dbc

    def execute_query(self, query_statement):
        """Execute query statement"""
        self.db_conn.query(query_statement)

    def create_db(self, dbname):
        q = "CREATE DATABASE " + dbname
        self.execute_query(q)

    def delete_db(self, dbname):
        q = "DROP DATABASE " + dbname
        self.execute_query(q)

    def create_tb(self, dbname=None, tbname=None, dicts=None):
        """Create table.

        Query statement: "CREATE TABLE table (field1 type1, field2 type2, ...)"

        """

        table = '.'.join([dbname, tbname])

        q = 'create table {}'.format(table)
        a = []
        for k, v in dicts.items():
            a.append(str(k) + ' ' + str(v))
        q += ' (' + ','.join(a) + ') '

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
        """
        table = '.'.join([dbname, tbname])

        q = 'insert into {}'.format(table)
        ksql = []
        vsql = []
        for k, v in dicts.items():
            ksql.append(str(k))
            vsql.append("'" + str(v) + "'")
        q += ' (' + ','.join(ksql) + ') '
        q += ' values (' + ','.join(vsql) + ')'

        return q

    def gen_select(self, dbname=None, tblname=None, colname='*'):
        # Select all columns from database/table
        #   SELECT * FROM sentinel1.ertaale
        # Select only one column:
        #   SELECT product_title FROM sentinel1.ertaale

        a = '.'.join([dbname, tblname])
        q = 'select {} from {}'.format(colname, a)
        return q

    def insert(self, dbname=None, tbname=None, dicts=None):
        """Insert values into database."""

        q = self.gen_insert(dbname=dbname, tbname=tbname, dicts=dicts)
        self.db_conn.query(q)

    def print_dataset(self, dbname=None, tblname=None, colname='*'):

        q = self.gen_select(dbname=dbname, tblname=tblname, colname=colname)

        rows = self.db_conn.query(q)

        if not rows.all():
            logging.info('Table is empty!')
            quit()

        print(rows.dataset)

    def get_dataset(self, dbname=None, tblname=None, colname='*'):
        q = self.gen_select(dbname=dbname, tblname=tblname, colname=colname)

        rows = self.db_conn.query(q)
        return rows.all()
