import fetchme
import logging

# --- set logging behaviour
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.info('>> script started')


# --- create object for scihub queries
obj = fetchme.Scihub()


# - scihub authentification
obj.query_auth('sebastien.valade', 'wave*worm')


# - read config file
obj.read_configfile('./conf/config.yml')


# --- scihub query
productlist = obj.scihub_search(export_result=None)

# print(productlist[0].metadata.title)
# productlist[0].getQuicklook()

# --- scihub download
obj.scihub_download(productlist)
