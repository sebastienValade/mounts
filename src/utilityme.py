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
    def __init__(self, db_host='127.0.0.1', db_usr='root', db_pwd='root', db_type='mysql'):
        self.db_host = db_host
        self.db_usr = db_usr
        self.db_pwd = db_pwd
        self.db_url = db_type + '://' + db_usr + ':' + db_pwd + '@' + db_host + '/'
        self.db_conn = self.db_connect()

    def db_connect(self):
        """Connect to database from url. Return database connector."""
        logging.info('Creating DB connector (url = ' + self.db_url + ')')
        dbc = records.Database(self.db_url)
        return dbc

    def db_upload(self, query=None):
        q = "INSERT INTO sentinel1.ertaale (product_title, product_abspath) VALUES ('oh', 'my');"
        self.db_conn.query(q)

    def db_print(self, db_name=None, tbl_name=None, col_name=None):
        # EX1: select all columns from database/table
        #   SELECT * FROM sentinel1.ertaale
        # EX1: select only one column:
        #   SELECT product_title FROM sentinel1.ertaale


        # format input args into string "dbname.tablename.columnname"
        a = filter(None, [db_name, tbl_name, col_name])
        if a is None:
            print('No database name is specified.')
            return
        else:
            a = '.'.join(a)

        q = 'select * from ' + a

        rows = self.db_conn.query(q)
        print(rows.dataset)
