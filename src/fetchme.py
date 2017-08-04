import sys
import os
import logging
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
import utilityme as utils


class Esa:
    """
    Parent class
    """

    def __init__(self):
        # Set default parameters of instance
        self.user = None
        self.pwd = None

    def query_auth(self, user, pwd):
        """Set ESA apihub authentification username and password."""
        self.user = user
        self.pwd = pwd

    def read_configfile(self, configfile):
        self.cfg = utils.read_configfile(configfile)


class Product(Esa):
    """
    Manage product located in Copernicus Scientific Hub
    """

    def __init__(self, user=None, pwd=None, cfg=None):
        Esa.__init__(self)
        self.user = user
        self.pwd = pwd
        self.cfg = cfg

    class metadata:
        """Class used to store product's metadata into attribute metadata
        >> product.metadata.title
        returns S1A_IW_SLC__1SDV_20170107T050420_20170107T050447_014721_017F5D_B588
        """
        pass

    def store_metadata(self, metadata_dict):
        """Store product metadata as subattributes in Product class attribute "metadata"

        Arguments:
            metadata_dict {dict} -- [dictionnary of product metadata, as returned by parse_xml]
        """

        for param in metadata_dict:
            setattr(self.metadata, param, metadata_dict[param])

    def getQuicklook(self):
        """Download product's quicklook (png)"""

        product_title = self.metadata.title
        product_uuid = self.metadata.uuid

        uri_root = Scihub.uri_opendata
        uri = "{0}('{1}')/Nodes('{2}.SAFE')/Nodes('preview')/Nodes('quick-look.png')/$value".format(uri_root, product_uuid, product_title)
        fname = product_title + '_quicklook.png'

        # --- download product
        logging.info('downloading quicklook')
        print('   | ' + product_title)
        self.getUri(uri, fname)

    def getFullproduct(self):
        """Download full product (zip)"""

        uri_root = Scihub.uri_opendata
        product_title = self.metadata.title
        product_uuid = self.metadata.uuid

        uri = "{0}('{1}')/$value".format(uri_root, product_uuid)
        fname = product_title + '.zip'

        # --- download product
        logging.info('downloading full product')
        print('   | ' + product_title)
        self.getUri(uri, fname)

    def getUri(self, uri, fname):
        """Download product from uri address.

        Arguments:
            uri {string} -- uri link
            fname {string} -- downloaded file name
        """

        # --- check download directory
        if 'download_dir' in self.cfg['optn_download']:
            p = self.cfg['optn_download']['download_dir']
        else:
            # p = os.getcwd()
            p = '../data/'
        p = chk_dir(p)

        # --- send request
        try:
            r = requests.get(uri, auth=(self.user, self.pwd), stream=True)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:  # base-class exception handling all cases
            logging.info('--> request error')
            logging.info('--> uri = ' + uri)

        # --- download product
        path_and_file = p + fname
        total_size = int(r.headers.get('content-length'))
        with open(path_and_file, 'wb') as f:
            for data in tqdm(r.iter_content(64 * 1024), total=total_size, unit='B', unit_scale=True):
                f.write(data)

        # --- save where product stored
        self.path_and_file = path_and_file


class Scihub(Esa):
    """
    Interact with Copernicus Scientific Hub
    """

    # NB: if apihub not working, try dhus
    # NB: cf option --dhus from Sentinel_download.py: -> Try dhus interface when apihub is not working
    uri_opensearch = 'https://scihub.copernicus.eu/apihub/search'
    uri_opendata = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
    # uri_opensearch = 'https://scihub.copernicus.eu/dhus/search'
    # uri_opendata = 'https://scihub.copernicus.eu/dhus/odata/v1/Products'

    def __init__(self):
        Esa.__init__(self)

    def scihub_search(self, export_result=None, print_url=None):
        # """Query Open Search API to discover products in the Data Hub archive"""

        # --- format query options
        optns = self.cfg['optn_search']
        optns_fmtd, export_fmt = self.format_query_optns(optns)

        # --- create url query string, parsing formated query options
        logging.info('generating request (requests lib)')
        try:
            req = requests.get(self.uri_opensearch, params=optns_fmtd)
        except requests.exceptions.ConnectionError:
            logging.error('ConnectionError')
            sys.exit(1)

        if print_url is not None:
            print('query url = ' + req.url)

        # --- send request
        logging.info('querying DataHub archive')
        try:
            resp = requests.get(req.url, auth=(self.user, self.pwd))
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:  # base-class exception handling all cases
            logging.info('--> request error')
            logging.info('--> query url = ' + req.url)
            logging.info('--> exiting script')
            sys.exit(1)

        # --- write request output to file
        if export_result is not None:
            with open('OSquery_results.' + export_fmt, 'w') as f:
                f.write(resp.text)
            logging.info('. query search results saved to %s file' % (export_fmt))

        # --- parse request output
        if export_fmt == 'xml':
            productlist = self.parse_xml(resp)
        elif export_fmt == 'json':
            productlist = self.parse_json(resp)

        # print('======')
        # print('debug: ' + productlist[-2].metadata.title)
        # print('debug: ' + productlist[-1].metadata.title)
        # print('======')

        # --- print product summary/title
        print('WARNING: list results in the same product!!!')
        # self.print_product_summary(productlist)
        self.print_product_title(productlist)

        return productlist

    def scihub_download(self, productlist):
        """Download products from Scihub.

        Download all elements in productlist.
        Product component to download (e.g., quicklook, fullproduct) is specified in yaml configuration file.

        Arguments:
            productlist {list} -- list of objects returned by parse_xml
        """

        logging.info('downloading products:')

        # --- loop though products
        part_to_download = self.cfg['optn_download']['product_node']
        for i, prod in enumerate(productlist):
            try:
                if part_to_download == 'quicklook':
                    productlist[i].getQuicklook()
                elif part_to_download == 'fullproduct':
                    productlist[i].getFullproduct()
            except:
                logging.info('--> DOWNLOAD FAILED! continuing to next product')
                continue

    def format_query_optns(self, optns):
        """Format options parsed from yaml configuration file, into format valid for scihub product query.

        Arguments:
            optns {dictionnary} -- dictionnary of options as defined in configuration file.

        Returns:
            optns_fmtd {dictionary} -- dictionnary of options sorted in such a way that it can be parsed to requests
        """

        optns = optns.items()
        optns_fmtd = dict()
        export_fmt = 'xml'

        key_q = []
        for k, v in optns:

            # --- option "footprint" => prepare query statement for area of interest
            if k == 'footprint':
                if isinstance(v, list):
                    if len(v) == 2:
                        # --- point query: "Intersects(lat, lon)"
                        v = '"Intersects({}, {})"'.format(v[0], v[1])

                    elif len(v) > 2:
                        # --- polygon query: "Intersects(POLYGON((P1Lon P1Lat, P2Lon P2Lat, ..., PnLon PnLat, ..., P1Lon P1Lat)))
                        polygon_str = '"Intersects(POLYGON(('
                        for idx in xrange(0, len(v), 2):
                            polygon_str += str(v[idx]) + ' ' + str(v[idx + 1]) + ', '

                        # --- close string + check if last polygon point coincides with the first point
                        if v[0] == v[-2]:
                            polygon_str = polygon_str[:-2] + ')))"'
                        else:
                            polygon_str += str(v[0]) + ' ' + str(v[1]) + ')))"'

                        v = polygon_str

            # --- option "maxrecords"
            if k == 'maxrecords':
                optns_fmtd['rows'] = v
                continue

            # --- option "orderby"
            if k == 'orderby':
                optns_fmtd['orderby'] = v
                continue

            # --- option "format"
            if k == 'format':
                optns_fmtd['format'] = v
                export_fmt = 'json'
                continue

            # --- apend formated option if dictionary key is valid
            optns_valid = ('filename', 'footprint', 'platformname', 'polarisationmode',
                           'productType', 'sensoroperationalmode', 'orbitdirection',
                           'swathidentifier', 'cloudcoverpercentage'
                           'beginposition', 'endposition', 'ingestiondate'
                           )

            if k in optns_valid:
                key_q.append(k + ':' + v)
            else:
                print('warning: "%s" is not a valid option, ignored' % (k))
                continue

        optns_fmtd['q'] = ' AND '.join(key_q)
        return optns_fmtd, export_fmt

    def parse_xml(self, resp):
        """Parse query response formatted in xml, and return list of dictionnaries, one per ESA product found.
        EX: print title of first product: print(P[0]['title'])
        """

        root = ET.fromstring(resp.text)

        # --- define namespace used in xml (xmlns)
        ns = {'orgpensearch': 'http://a9.com/-/spec/opensearch/1.1/',
              'default_xmlns': 'http://www.w3.org/2005/Atom'}

        productlist = []
        # k = -1

        # --- loop through products
        for product in root.findall('default_xmlns:entry', ns):

            D = {}

            # --- loop through children with specific tag
            tags = ['title', 'id', 'summary']
            for i, param in enumerate(tags):
                value = product.find('default_xmlns:' + param, ns).text
                D[param] = value

            # --- loop through children 'str'
            for child in product.findall('default_xmlns:str', ns):
                param = child.get('name')
                value = child.text
                D[param] = value

            # --- loop through children 'int'
            for child in product.findall('default_xmlns:int', ns):
                param = child.get('name')
                value = child.text
                D[param] = value

            # --- loop through children 'date'
            for child in product.findall('default_xmlns:date', ns):
                param = child.get('name')
                value = child.text
                D[param] = value

            # === create product object, and save metadata as attributes
            # NB: pass user/pwd (&cfg) otherwise inherits default None values from Esa class
            objprod = Product(self.user, self.pwd, self.cfg)
            objprod.store_metadata(D)
            productlist.append(objprod)

            print('-------')
            print('debug: appending "' + D['title'] + '"')
            # print('debug: ' + productlist[0].metadata.title)
            # print('debug: ' + productlist[-1].metadata.title)

        # print('==================>')
        # print(productlist[0].metadata.title)
        # print(productlist[1].metadata.title)
        # print('==================>')

        return productlist

    def parse_json(self, resp):
        """Parse query response formatted in json."""

        print('JSON parsing not implemented yet.')
        productlist = []
        return productlist
        # import json
        # parsed_json = json.loads(resp.text)
        # print(parsed_json)

    def print_product_summary(self, productlist):
        """Print summary of queried products.
        Input argument = product list returned by 'parse_xml' or 'parse_json' method.
        """

        logging.info('queried product summary:')

        for i, prod in enumerate(productlist):
            print('   | ' + productlist[i].metadata.summary)

    def print_product_title(self, productlist):
        """Print summary of queried products.
        Input argument = product list returned by 'parse_xml' or 'parse_json' method.
        """

        logging.info('queried product title:')

        for i, prod in enumerate(productlist):
            print('   | ' + productlist[i].metadata.title)


def chk_dir(d):
    """Check if directory validity."""

    # - check if valid directory
    if d == '' or d == 'None':
        d = os.getcwd()

    # - check if directory exists, else create
    if not os.path.exists(d):
        logging.info('unexistent directory --> creating: ' + d)
        os.makedirs(d)

    # - add trailing slash if missing (os independent)
    d = os.path.join(d, '')

    return d
