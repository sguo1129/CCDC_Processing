import requests
import ConfigParser

import psycopg2
from osgeo import ogr


class DBConnect(object):
    """
    Class for connecting to a localhost postgresql database
    """

    def __init__(self, host, port, database, user, password, autocommit=False):
        self.conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
        try:
            self.cursor = self.conn.cursor()
        except psycopg2.Error:
            raise

        self.autocommit = autocommit
        self.fetcharr = []

    def execute(self, sql_str):
        try:
            self.cursor.execute(sql_str)
        except psycopg2.Error:
            raise

        if self.autocommit:
            self.commit()

    def select(self, sql_str, vals):
        if not isinstance(vals, tuple):
            raise TypeError

        self.cursor.execute(sql_str, vals)

        try:
            self.fetcharr = self.cursor.fetchall()
        except psycopg2.Error:
            raise

    def commit(self):
        try:
            self.conn.commit()
        except psycopg2.Error:
            raise

    def rollback(self):
        self.conn.rollback()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def __len__(self):
        return len(self.fetcharr)

    def __iter__(self):
        return iter(self.fetcharr)

    def __getitem__(self, item):
        if item >= len(self.fetcharr):
            raise IndexError
        return self.fetcharr[item]

    def __del__(self):
        self.cursor.close()
        self.conn.close()

        del self.cursor
        del self.conn


class APIException(Exception):
    pass


class APIConnect(object):
    def __init__(self, username, password, host):
        self.host = host
        self.username = username

        self.auth_tpl = (username, password)

        # Check the connection
        self.test_connection()

    def _request(self, method, resource=None, status=None, **kwargs):
        """
        Make a call into the API
        Args:
            method: HTTP method to use
            resource: API resource to touch
        Returns: response and status code
        """
        valid_methods = ('get', 'put', 'delete', 'head', 'options', 'post')

        if method not in valid_methods:
            raise APIException('Invalid method {}'.format(method))

        if resource and resource[0] == '/':
            url = '{}{}'.format(self.host, resource)
        elif resource:
            url = '{}/{}'.format(self.host, resource)
        else:
            url = self.host

        try:
            resp = requests.request(method, url, auth=self.auth_tpl, **kwargs)
        except requests.RequestException as e:
            raise APIException(e)

        if status and resp.status_code != status:
            self._unexpected_status(resp.status_code, url, resp)

        return resp.json(), resp.status_code

    @staticmethod
    def _unexpected_status(code, url, resp):
        """
        Throw exception for an unhandled http status
        Args:
            code: http status that was received
            url: URL that was used
        """
        raise Exception('Received unexpected status code: {}\n'
                        'for URL: {}\n'
                        'Reason given: {}'.format(code, url, resp))

    def test_connection(self):
        """
        Tests the base URL for the class
        Returns: True if 200 status received, else False
        """
        self._request('get', '/user', status=200)

    def post_available_prods(self, scene_list):
        data_dict = {'inputs': scene_list}
        url = '/available-products'

        resp, status = self._request('post', url, json=data_dict, status=200)

        return resp

    def post_order(self, espa_order):
        url = '/order'

        resp, status = self._request('post', url, json=espa_order, status=200)

        return resp

    def list_orders(self, email=''):
        url = '/list-orders/{}'.format(email)

        resp, status = self._request('get', url, status=200)

        return resp

    def order_status(self, order_id):
        url = '/order/{}'.format(order_id)

        resp, status = self._request('get', url, status=200)

        return resp

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Nothing persistent to close out
        pass

    def __repr__(self):
        return 'APIConnect({0}:{1})'.format(self.username, self.host)


def calc_ext(shapefile):
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer()

    ext1 = layer.GetExtent()
    xmin = fifteen_offset(ext1[0])
    xmax = fifteen_offset(ext1[1])
    ymin = fifteen_offset(ext1[2])
    ymax = fifteen_offset(ext1[3])

    return xmin, ymax, xmax, ymin


def epsg_from_file(shapefile):
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer()
    spatialref = layer.GetSpatialRef()

    return spatialref.ExportToEPSG()


def fifteen_offset(coord):
    return (coord // 30) * 30 + 15


def get_cfg(cfgfile='ccdc.cfg'):
    cfg_info = {}

    config = ConfigParser.ConfigParser()
    config.read(cfgfile)

    for sect in config.sections():
        cfg_info[sect] = {}
        for opt in config.options(sect):
            cfg_info[sect][opt] = config.get(sect, opt)

    return cfg_info


def db_instance(config_file):
    return DBConnect(**get_cfg(config_file)['db'])
