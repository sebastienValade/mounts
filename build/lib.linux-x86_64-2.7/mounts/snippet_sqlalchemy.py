from sqlalchemy import create_engine

# https://www.pythonsheets.com/notes/python-sqlalchemy.html


# === Connect
db_uri = 'mysql://root:br12Fol!@127.0.0.1/DB_MOUNTS'
engine = create_engine(db_uri)


# === Get Database Information [inspect]
# from sqlalchemy import inspect
# inspector = inspect(engine)

# # --- get table information
# print inspector.get_table_names()

# # --- get table information
# print inspector.get_columns('targets')


# # === Reflection - Loading Table from Existing Database
# => reflect=True option when creating metadata instance is fundamental!
# from sqlalchemy import MetaData

# metadata = MetaData()
# print metadata.tables
# print '---'
# # reflect db schema to MetaData
# metadata.reflect(bind=engine)
# print metadata.tables


# === Get Table from MetaData
from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table

# Create MetaData instance
metadata = MetaData(engine, reflect=True)
print metadata.tables

# Get Table
ex_table = metadata.tables['targets']
print ex_table


# === DEFINE STATEMENTS

engine = create_engine(db_uri)
conn = engine.connect()
metadata = MetaData(engine, reflect=True)
targets = metadata.tables['targets']

# method 1
upd = targets.update().where(targets.c.id == kwargs['id']).values(
    id=kwargs['id'],
    fullname=kwargs['fullname'],
    name=kwargs['name'],
    country=kwargs['country'],
    lat=kwargs['lat'],
    lon=kwargs['lon'],
    alt=kwargs['alt'],
    processing=json.dumps(kwargs['processing']),
    subset_wkt=kwargs['subset_wkt']
)

# method 2
u = update(targets).where(targets.c.id == kwargs['id'])
upd = u.values(
    id=kwargs['id'],
    fullname=kwargs['fullname'],
    name=kwargs['name'],
    country=kwargs['country'],
    lat=kwargs['lat'],
    lon=kwargs['lon'],
    alt=kwargs['alt'],
    processing=json.dumps(kwargs['processing']),
    subset_wkt=kwargs['subset_wkt']
)

conn.execute(ins)
