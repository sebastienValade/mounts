import fetchme

# --- create object for scihub queries
obj = fetchme.Scihub()

# - scihub authentification
obj.query_auth('sebastien.valade', 'wave*worm')

# --- read config file
# conffile = '_config_etna_archive.yml'
# obj.read_configfile('./conf/' + conffile)
# print obj.cfg['optn_download']

# --- set logging behaviour
import logging
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.info('>> script started')


# =============================================
# QUERY
# =============================================

# --- scihub query
# productlist = obj.scihub_search(print_url=1)

# --- parse options from kwargs
# obj.scihub_search(filename='S1*')
# obj.scihub_search(footprint='POLYGON((-2.211 49.9654, -0.835 49.9654, -0.835 49.1206, -2.211 49.1206, -2.211 49.9654))')

# --- parse options from dict
# optns = {'filename': 'S1A*', 'maxrecords': 5}
# obj.scihub_search(**optns)

# --- parse options from yaml configuration file
# obj.scihub_search(configfile='./conf/_config_ertaale.yml')

# -- parse options from loaded yaml configration file
# obj.read_configfile('./conf/_config_ertaale.yml')
# obj.scihub_search()

# ex: search specific file
# productlist = obj.scihub_search(filename='S1A_IW_SLC__1SDV_20170507T050422_20170507T050449_016471_01B4A7_9DF7*')

# ex: print url, export results as xml
# obj.scihub_search(configfile='./conf/_config_pitonfournaise_archive.yml', print_url=1, export_result=1)


# =============================================
# ANALYZE QUERY output
# =============================================

# --- get product metadata
# print(productlist[0].metadata)

# --- get product md5sum
# productlist[0].getMd5sum()


# =============================================
# DOWNLOAD
# =============================================

# --- get product quicklook
# productlist[0].getQuicklook()

# --- get full product
# productlist[0].getFullproduct()

# --- get full product specify download dir
# productlist[0].getFullproduct(download_dir='/home/sebastien/DATA/data_satellite/etna')

# --- get full product + force not to check file integrity if file is found
# productlist[0].getFullproduct(download_dir='/home/sebastien/DATA/data_satellite/etna', check_md5sum=False)

# --- scihub download
# obj.scihub_download(productlist)
