import sys
import os
import logging
import requests
import xml.etree.ElementTree as ET
from tqdm import tqdm
import utilme

# TODO: look into "sentinelsat" python API:
# http://sentinelsat.readthedocs.io/en/stable/api.html


class Esa:
    """
    Parent class
    """

    def __init__(self):
        # self.user = None
        # self.pwd = None
        self.user = 'sebastien.valade'
        self.pwd = 'wave*worm'
        self.cfg = None

    def query_auth(self, user, pwd):
        """Set ESA apihub authentification username and password."""
        self.user = user
        self.pwd = pwd

    def read_configfile(self, configfile):
        """Summary

        Args:
            configfile (TYPE): Description
        """
        self.cfg = utilme.read_configfile(configfile)

    def reset_cfg(self):
        """Set instance attribute "cfg" storing search/download/processing options back to None"""
        self.cfg = None


class Product(Esa):
    """
    Manage product located in Copernicus Scientific Hub
    """

    class Metadata(dict):
        """Class used to store product's metadata into attribute metadata
        >> product.metadata.title
        returns S1A_IW_SLC__1SDV_20170107T050420_20170107T050447_014721_017F5D_B588
        """

        def __getattr__(self, key):
            return self.get(key, None)

        def __setattr__(self, key, value):
            self[key] = value

    def __init__(self, user=None, pwd=None, cfg=None):
        Esa.__init__(self)
        self.user = user
        self.pwd = pwd
        self.cfg = cfg
        self.metadata = self.Metadata()

    def store_metadata(self, metadata_dict):
        """Store product metadata as subattributes in Product class attribute "metadata"

        Arguments:
            metadata_dict {dict} - - [dictionnary of product metadata, as returned by parse_xml]
        """

        for param in metadata_dict:
            setattr(self.metadata, param, metadata_dict[param])

    def getQuicklook(self, download_dir=None):
        """Download product's quicklook (png)"""

        product_title = self.metadata.title
        product_uuid = self.metadata.uuid
        if product_uuid is None:
            # NB: some xml are found without uuid field => get field id
            product_uuid = self.metadata.id

        # --- quicklook image (available for S1 only) GOOD RESOLUTION
        if product_title[0:2] == 'S1':
            uri_root = Scihub.uri_opendata
            uri = "{0}('{1}')/Nodes('{2}.SAFE')/Nodes('preview')/Nodes('quick-look.png')/$value".format(uri_root, product_uuid, product_title)
            fname = product_title + '_quicklook.png'

        # --- thumbnail image (displayed in scihub browser, available for S1 & S2) LOW RESOLUTION
        if product_title[0:2] == 'S2':
            uri_root = Scihub.uri_opendata
            uri = "{0}('{1}')/Products('Quicklook')/$value".format(uri_root, product_uuid)
            fname = product_title + '_quicklook.jpg'

        # --- download product
        logging.info('downloading quicklook')
        print('   | ' + product_title)
        self.getUri(uri, fname, download_dir)

    def getFullproduct(self, download_dir=None, check_md5sum=True):
        """Download full product (zip)"""

        uri_root = Scihub.uri_opendata
        product_title = self.metadata.title
        product_uuid = self.metadata.uuid
        if product_uuid is None:
            # NB: some xml are found without uuid field => get field id
            product_uuid = self.metadata.id

        # TODO: use download link recovered from xml: self.metadata.link
        uri = "{0}('{1}')/$value".format(uri_root, product_uuid)
        fname = product_title + '.zip'

        # --- download product
        logging.info('downloading full product')
        print('   | ' + product_title)
        self.getUri(uri, fname, download_dir, check_md5sum=check_md5sum)

    def getUri(self, uri, fname, download_dir=None, check_md5sum=True):
        """Download product from uri address.

        Arguments:
            uri {string} - - uri link
            fname {string} - - downloaded file name
        """

        # --- get download_dir
        if download_dir is None:
            try:
                download_dir = self.cfg['optn_download']['download_dir']
            except:
                download_dir = '../data/'
        p = chk_dir(download_dir)

        # --- check file existence
        path_and_file = p + fname
        if os.path.isfile(path_and_file):
            file_exists = True
        else:
            file_exists = False

        # --- check file integrity
        if file_exists and check_md5sum:
            # - md5sum online product
            md5sum_web = self.getMd5sum()

            # - md5sum local product
            print('      file exists, calculating md5 checksum ("' + fname + '"") ... ')
            import hashlib
            md5sum_local = hashlib.md5(open(path_and_file, 'rb').read()).hexdigest()

            if md5sum_web.lower() == md5sum_local.lower():
                file_complete = True
            else:
                print('      => md5 checksum failed: downloaded file incomplete')
                print('         online product = ' + md5sum_web)
                print('         local product = ' + md5sum_local)
                file_complete = False
        else:
            file_complete = False

        # --- if file exists and integrity is verified, do not download
        if file_exists & file_complete:
            print('      => file already downloaded and integrety checked.')
            return

        # --- send request
        try:
            r = requests.get(uri, auth=(self.user, self.pwd), stream=True)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:  # base-class exception handling all cases
            logging.info('--> request error')
            logging.info('--> uri = ' + uri)

        # --- download product
        total_size = int(r.headers.get('content-length'))
        with open(path_and_file, 'wb') as f:
            for data in tqdm(r.iter_content(64 * 1024), total=total_size, unit='B', unit_scale=True):
                f.write(data)

        # --- store information to database
        # dbo = utilme.Database(...)
        # dbo.store_metadata(...)

        # --- save where product stored
        self.path_and_file = path_and_file

        return path_and_file

    def getMd5sum(self, print_md5sum=None, print_url=None):
        """Get product MD5 checksum. (verifying download integrity)
        Each product published on the Data Hub provides an MD5 checksum of the downloadable ZIP file.
        The Message Digit of the file can be discovered using the following OData query:
        # https://scihub.copernicus.eu/dhus/odata/v1/Products('UUID')/Checksum/Value/$value
        """

        uri_root = Scihub.uri_opendata
        product_uuid = self.metadata.uuid

        uri = "{0}('{1}')/Checksum/Value/$value".format(uri_root, product_uuid)

        if print_url is not None:
            print('query url = ' + uri)

        # --- send request
        try:
            r = requests.get(uri, auth=(self.user, self.pwd), stream=True)
            r.raise_for_status()
        except requests.exceptions.RequestException as e:  # base-class exception handling all cases
            logging.info('--> request error')
            logging.info('--> uri = ' + uri)

        md5sum = r.text

        if print_md5sum is not None:
            print('product md5sum = ' + md5sum)

        return md5sum

    # def read(self):
    #     """Open with snapme once downloaded"""
    #     import snapme as gpt
    #     p = gpt.read_product(self)
    #     return


class Scihub(Esa):
    """
    Interact with Copernicus Scientific Hub
    """

    # TODO: data access point should be changed by option 'hub' in method 'scihub_search'
    # = 'api' => API Hub : access point for API users with no graphical interface. All API users regularly downloading the latest data are encouraged to use this access point for a better performance.
    # = 'openaccess' => Open Access Hub : access point for all Sentinel missions with access to the interactive graphical user interface.
    # hub = 'openaccess'
    hub = 'api'
    if hub == 'api':
        uri_opensearch = 'https://scihub.copernicus.eu/apihub/search'
        uri_opendata = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
    elif hub == 'openaccess':
        uri_opensearch = 'https://scihub.copernicus.eu/dhus/search'
        uri_opendata = 'https://scihub.copernicus.eu/dhus/odata/v1/Products'

    def __init__(self):
        Esa.__init__(self)

    def scihub_search(self,
                      filename=None,
                      platformname=None,
                      producttype=None,
                      polarisationmode=None,
                      sensoroperationalmode=None,
                      orbitdirection=None,
                      swathidentifier=None,
                      footprint=None,
                      ingestiondate=None,
                      beginposition=None,
                      endposition=None,
                      maxrecords=None,
                      orderby=None,
                      cloudcoverpercentage=None,
                      hub='api',
                      fullmeta=None,  # TODO: full metadata: https://github.com/sentinelsat/sentinelsat/blob/127619f6baede1b5cc852b208d4e57e9f4d518ee/sentinelsat/sentinel.py
                      configfile=None,
                      export_result=None,
                      print_result=None,
                      print_url=None,
                      download_pnode=None,
                      download_dir=None):
        """Query Open Search API to discover products in the Data Hub archive.
        TO DO: finish implementing option 'hub'
        TO DO: read product nodes:
            from snappy import ProductIO
            s1meta = "manifest.safe"
            s1prd = "/workspace/data/%s/%s.SAFE/%s" % (s1path, s1path, s1meta)
            reader = ProductIO.getProductReader("SENTINEL-1")
            product = reader.readProductNodes(s1prd, None)

        Args:
            filename (None, optional): Search based on the product filename, expressed according to the product naming convention
                S1 convention: 'MMM_BB_TTR_LFPP_YYYMMDDTHHMMSS_YYYYMMDDTHHMMSS_OOOOO_DDDDD_CCCC'
                Wildcard * = any sequence of zero or more characters
                Wildcard ? = any one character
                = 'S1A*'
            platformname (None, optional): Satellite Platform name(regardless of the serial identifier, e.g. A, B, C ...)
                = 'Sentinel-1', 'Sentinel-2'
            productType (None, optional): output product type
                = 'SLC', 'GRD', 'OCN', 'S2MSI1C'
            polarisationmode (None, optional): valid polarisations for the S1 SAR instrument
                = 'HH', 'VV', 'HV', 'VH', 'HH HV', 'VV VH'
            sensoroperationalmode (None, optional): SAR instrument imaging modes
                = 'SM', 'IW', 'EW'
            orbitdirection (None, optional): direction of the orbit(ascending, descending)
                = 'Ascending', 'Descending'
            swathidentifier (None, optional): swath identifiers for the Sentinel - 1 SAR instrument.
                = S1 - S6: swaths apply to SM products
                = IW, IW1-3: swaths apply to IW products(IW is used for detected IW products where the 3 swaths are merged into one image)
                = EW, EW1-5: swaths apply to EW products(EW is used for detected EW products where the 5 swaths are merged into one image)
            footprint (None, optional): Geographical search of the products whose footprint intersects or is included in a specific geographic type.
                = [lat, lon], [lon1, lat1, lon2, lat2, ...]
            ingestiondate (None, optional): time interval search based on the time of publication of the product on the Data Hub.
                = '[< timestamp > TO < timestamp > ]' where < timestamp > can be expressed in one of the the following formats: yyyy-MM-ddThh:mm:ss.SSSZ, NOW, NOW-<n>MINUTE(S), NOW-<n>HOUR(S), NOW-<n>DAY(S), NOW-<n>MONTH(S)
                = '[2017-01-25T00:00:00.000Z TO 2017-02-15T00:00:00.000Z]'
            beginposition (None, optional): time interval search based on the Sensing Start Time of the products.
                = '[< timestamp > TO < timestamp > ]' where < timestamp > can be expressed in one of the the following formats: yyyy-MM-ddThh:mm:ss.SSSZ, NOW, NOW-<n>MINUTE(S), NOW-<n>HOUR(S), NOW-<n>DAY(S), NOW-<n>MONTH(S)
                = '[2017-01-25T00:00:00.000Z TO 2017-02-15T00:00:00.000Z]'
            endposition (None, optional):
                = '[< timestamp > TO < timestamp > ]' where < timestamp > can be expressed in one of the the following formats: yyyy-MM-ddThh:mm:ss.SSSZ, NOW, NOW-<n>MINUTE(S), NOW-<n>HOUR(S), NOW-<n>DAY(S), NOW-<n>MONTH(S)
                = '[2017-01-25T00:00:00.000Z TO 2017-02-15T00:00:00.000Z]'
            maxrecords (None, optional): number of results listed per page (max=100)
                = integer
            orderby (None, optional): order result by
                = 'beginposition asc' => sorts results by sensing date arranged in ascending order
                = 'beginposition desc' => sorts results by sensing date arranged in descending order
                = 'ingestiondate asc' => sorts results by ingestion date arranged in ascending order
                = 'ingestiondate desc' => sorts results by ingestion date arranged in descending order
            cloudcoverpercentage: Percentage of cloud coverage of product for each area covered by a reference band.
                = 95
                = '[0 TO 50]'
            hub ('api', optional): data access point
                WARNING: not yet operational!
                = 'api' => API Hub : access point for API users with no graphical interface. All API users regularly downloading the latest data are encouraged to use this access point for a better performance.
                = 'openaccess' => Open Access Hub : access point for all Sentinel missions with access to the interactive graphical user interface.
            configfile (None, optional): path to yaml configuration file
            export_result (None, optional): export search results as xml
            print_result: print products found (title or summary)
            print_url (None, optional): display url string generated
            download_pnode (None, optional): product part to download
                = S1 options: quicklook, fullproduct
            download_dir (None, optional): download directory

        Returns:
            TYPE: Description
        """

        # --- get query options
        if configfile is not None:
            # ... from config file specified in kwarg 'configfile'
            self.read_configfile(configfile)
            optns = self.cfg['optn_search']
        elif self.cfg is not None:
            # ... from instance attribute 'cfg' if configfile already loaded
            optns = self.cfg['optn_search']
        else:
            # ... from kwargs
            optns = locals()

        # --- get data access point
        # if hub == 'api':
        #     uri_opensearch = 'https://scihub.copernicus.eu/apihub/search'
        #     uri_opendata = 'https://scihub.copernicus.eu/apihub/odata/v1/Products'
        # elif hub == 'openaccess':
        #     uri_opensearch = 'https://scihub.copernicus.eu/dhus/search'
        #     uri_opendata = 'https://scihub.copernicus.eu/dhus/odata/v1/Products'
        # TO DO: to finish implementation, these variables should be accessible to getQuicklook, getFullproduct, ... (methods of class Product)

        # --- format query options for request
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

        #full = True
        # if full:
        #    url += '&$expand=Attributes'

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

        # --- print product summary/title
        if print_result:
            # self.print_product_summary(productlist)
            self.print_product_title(productlist)

        # --- download
        if download_pnode is not None:
            self.scihub_download(productlist, download_pnode=download_pnode, download_dir=download_dir)
        elif self.cfg is not None:
            try:
                optn_download = self.cfg['optn_download']
                self.scihub_download(productlist, **optn_download)
            except Exception as e:
                logging.info('WARNING: download cfg parameter "[optn_download]" not properly specified in configuration file.')

        return productlist

    def scihub_download(self,
                        productlist,
                        download_pnode='fullproduct',
                        download_dir=None,
                        configfile=None):
        """Download products from Scihub.

        Download all elements in productlist.
        Product component to download(e.g., quicklook, fullproduct) is specified in yaml configuration file.

        Arguments:
            productlist {list} - - list of objects returned by parse_xml
        """

        # --- get download options
        if configfile is not None:
            # ... from config file specified in kwarg 'configfile'
            self.read_configfile(configfile)
            optns = self.cfg['optn_download']
        elif self.cfg is not None:
            # ... from instance attribute 'cfg' if configfile already loaded
            optns = self.cfg['optn_download']
        else:
            # ... from kwargs
            optns = locals()

        logging.info('downloading products:')

        # --- loop though products
        for i, prod in enumerate(productlist):
            try:
                if optns['download_pnode'] == 'quicklook':
                    productlist[i].getQuicklook(download_dir=download_dir)
                elif optns['download_pnode'] == 'fullproduct':
                    productlist[i].getFullproduct(download_dir=download_dir)
            except Exception as e:
                logging.info('--> DOWNLOAD FAILED! continuing to next product')
                continue

    def format_query_optns(self, optns):
        """Format options into format valid for scihub product query.

        Arguments:
            optns {dictionnary} - - dictionnary of options as defined in configuration file.

        Returns:
            optns_fmtd {dictionary} - - dictionnary of options sorted in such a way that it can be parsed to requests
        """

        optns = optns.items()
        optns_fmtd = dict()
        export_fmt = 'xml'

        key_q = []
        for k, v in optns:

            if v is None:
                continue

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

                else:
                    if v[0:7] == 'POLYGON':
                        polygon_str = '"Intersects(' + v + ')"'
                        v = polygon_str

            # --- option "maxrecords"
            if k == 'maxrecords':
                optns_fmtd['rows'] = v
                continue

            # --- option "fullmeta"
            # TODO: still to implement/parse output
            if k == 'fullmeta':
                optns_fmtd['$expand'] = 'Attributes'
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
                           'producttype', 'sensoroperationalmode', 'orbitdirection',
                           'swathidentifier', 'cloudcoverpercentage',
                           'beginposition', 'endposition', 'ingestiondate',
                           )

            if k in optns_valid:
                key_q.append(k + ':' + v)
            else:
                logging.debug('warning: "%s" is not a valid option, ignored' % (k))
                continue

        if not key_q:
            # if key_q is empty => set query search parameter q=* (no filter)
            optns_fmtd['q'] = '*'
        else:
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

        # --- loop through products
        for product in root.findall('default_xmlns:entry', ns):

            D = dict()

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

            # --- loop through children 'link'
            for child in product.findall('default_xmlns:link', ns):
                param = child.get('rel')
                value = child.get('href')

                if param is None:
                    # NB: main download link has no field 'rel' => name as 'link'
                    param = 'link'
                elif param == 'alternative':
                    param = 'link_alternative'
                else:
                    continue
                D[param] = value

            # === create product object, and save metadata as attributes
            # NB: pass user/pwd (&cfg) otherwise inherits default None values from Esa class
            objprod = Product(user=self.user, pwd=self.pwd, cfg=self.cfg)
            objprod.store_metadata(D)
            productlist.append(objprod)

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

        if not productlist:
            print('   | 0 product found')
            return

        for i, prod in enumerate(productlist):
            print('   | ' + productlist[i].metadata.summary)

    def print_product_title(self, productlist):
        """Print summary of queried products.
        Input argument = product list returned by 'parse_xml' or 'parse_json' method.
        """

        logging.info('queried product title:')

        if not productlist:
            print('   | 0 product found')
            return

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
