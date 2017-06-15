import requests
import xml.etree.ElementTree as ET
import sys
import logging


class esa:
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


class sentinel(esa):
    """
    Child class inheriting from esa()
    """

    uri_opensearch = 'https://scihub.copernicus.eu/apihub/search'
    uri_opendata = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'

    def __init__(self):
        esa.__init__(self)

    def format_query_optns(self, optns):

        optns = optns.items()
        optns_fmtd = dict()
        export_fmt = 'xml'

        key_q = []
        for k, v in optns:

            # --- option "aoi" => prepare query statement for area of interest
            if k == 'aoi':
                k = 'footprint'
                if len(v) == 2:
                    v = '"Intersects({}, {})"'.format(v[0], v[1])
                elif len(v) == 4:
                    v = '"Intersects(POLYGON(({} {},{} {})))"'.format(v[0], v[1], v[2], v[3])

            # --- option "maxrecords"
            if k == 'maxrecords':
                optns_fmtd['rows'] = v
                continue

            # --- option "format"
            if k == 'format':
                optns_fmtd['format'] = v
                export_fmt = 'json'
                continue

            # --- apend formated option if dictionary key is valid
            optns_valid = ('filename', 'footprint', 'platformname', 'polarisationmode', 'productType')
            if k in optns_valid:
                key_q.append(k + ':' + v)
            else:
                print('warning: "%s" is not a valid option, ignored' % (k))
                continue

        optns_fmtd['q'] = ' AND '.join(key_q)
        return optns_fmtd, export_fmt

    def product_search(self, optns, export_result=None):
        # """Query Open Search API to discover products in the Data Hub archive"""

        # --- format query options
        optns_fmtd, export_fmt = self.format_query_optns(optns)

        # --- create url query string, parsing formated query options
        try:
            req = requests.get(self.uri_opensearch, params=optns_fmtd)
        except requests.exceptions.ConnectionError:
            logging.error('ConnectionError')
            sys.exit(1)

        logging.info('query url: ' + req.url)

        # --- send request
        logging.info('querying DataHub archive')
        try:
            resp = requests.get(req.url, auth=(self.user, self.pwd))
            resp.raise_for_status()
        except requests.exceptions.Timeout:
            logging.error('Timeout')
            sys.exit(1)
        except requests.exceptions.TooManyRedirects:
            logging.error('TooManyRedirects')
            sys.exit(1)
        except requests.exceptions.HTTPError:
            logging.error('HTTPError')
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

        # --- print product summary
        self.print_product_summary(productlist)

        return productlist

    def parse_xml(self, resp):
        """Parse query response formatted in xml, and return list of dictionnaries, one per ESA product found.
        EX: print title of first product: print(P[0]['title'])
        """

        root = ET.fromstring(resp.text)

        # --- define namespace used in xml (xmlns)
        ns = {'orgpensearch': 'http://a9.com/-/spec/opensearch/1.1/',
              'default_xmlns': 'http://www.w3.org/2005/Atom'}

        productlist = []

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

            productlist.append(D)

        return productlist

    def parse_json(self, resp):
        """Parse query response formatted in json."""

        print('JSON parsing not implemented yet.')
        productlist = []
        return productlist
        # import json
        # parsed_json = json.loads(resp.text)
        # print(parsed_json)

    def product_fetch(self, productlist):

        uri_root = self.uri_opendata

        logging.info('downloading products:')

        # --- loop though products
        for i, prod in enumerate(productlist):
            product_title = productlist[i]['title']
            product_uuid = productlist[i]['uuid']

            print('   | ' + product_title)

            # --- build query string depending on product part to download
            part_to_download = 'quicklook'
            if part_to_download == 'fullproduct':
                uri = '{0}({1})/$value'.format(uri_root, product_uuid)
                fname = product_title + '.zip'

            elif part_to_download == 'quicklook':
                uri = "{0}('{1}')/Nodes('{2}.SAFE')/Nodes('preview')/Nodes('quick-look.png')/$value".format(uri_root, product_uuid, product_title)
                fname = product_title + '_quicklook.png'

            # --- query product
            try:
                r = requests.get(uri, auth=(self.user, self.pwd))
                r.raise_for_status()
            except requests.exceptions.Timeout:
                logging.error('Timeout')
                continue
            except requests.exceptions.TooManyRedirects:
                logging.error('TooManyRedirects')
                continue
            except requests.exceptions.HTTPError:
                logging.error('HTTPError')
                logging.error('uri = ' + uri)
                continue

            # --- download product
            with open(fname, 'wb') as fd:
                fd.write(r.content)
                # for chunk in r.iter_content(chunk_size=1024):
                #     fd.write(chunk)

    def print_product_summary(self, productlist):
        """Print summary of queried products.
        Input argument = product list returned by 'parse_xml' or 'parse_json' method.
        """

        logging.info('queried product summary:')

        for i, prod in enumerate(productlist):
            prodsummary = productlist[i]['summary']
            print('   | ' + prodsummary)
