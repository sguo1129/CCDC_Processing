import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from CCDC_Processing import utils


requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


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
            resp = requests.request(method, url, auth=self.auth_tpl, verify=False, **kwargs)
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
        raise APIException('Received unexpected status code: {0}\n'
                           'for URL: {1}\n'
                           'Reason given: {2}'.format(code, url, resp.json()))

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

    def item_status(self, order_id):
        url = '/item-status/{}'.format(order_id)

        resp, status = self._request('get', url, status=200)

        return resp

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Nothing persistent to close out
        pass

    def __repr__(self):
        return 'APIConnect({0}:{1})'.format(self.username, self.host)


def api_instance(config_path=None):
    if not config_path:
        cfg = utils.get_cfg()
    else:
        cfg = utils.get_cfg(config_path)

    return APIConnect(**cfg['API'])
