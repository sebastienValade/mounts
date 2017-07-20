import fetchESA
import logging

# AHAHAH

# --- set logging behaviour
logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
logging.getLogger('requests').setLevel(logging.ERROR)
logging.info('>> script started')

obj = fetchESA.sentinel()


# --- set query authentification
obj.query_auth('sebastien.valade', 'wave*worm')


# --- set query options
#    'filename'
#    'platformname': Sentinel-1 | Sentinel-2
#    'format': 'json'
#    'polarisationmode'
#    'productType'
#    'aoi': (lat, lon) | (lon1, lat1, lon2, lat2)
optns = {
    'filename': 'S1A*',
    'productType': 'SLC',
    'maxrecords': 1}

# --- query product
productlist = obj.product_search(optns, export_result=None)

# print(productlist[0]['title'])
# print(productlist[0]['uuid'])

# --- download product
obj.product_fetch(productlist)

logging.info('Finished')
