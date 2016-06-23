import requests


class APIException(Exception):
    pass


class APIConnect(object):
    def __init__(self, username, password, host):
        self.host = host

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
            self._unexpected_status(resp.status_code, url)

        return resp.json(), resp.status_code

    @staticmethod
    def _unexpected_status(code, url):
        """
        Throw exception for an unhandled http status
        Args:
            code: http status that was received
            url: URL that was used
        """
        raise Exception('Received unexpected status code: {}\n'
                        'for URL: {}'.format(code, url))

    def test_connection(self):
        """
        Tests the base URL for the class
        Returns: True if 200 status received, else False
        """
        resp, status = self._request('get')

        if status == 200:
            return True

        return False

    def available_prods(self, scene_list):
        data_dict = {'inputs': scene_list}
        url = '/available-products'

        resp, status = self._request('post', url, json=data_dict, status=200)

        return resp

    def place_order(self, espa_order):
        url = '/order'

        resp, status = self._request('post', url, json=espa_order, status=200)

        return resp

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Nothing persistent to close out
        pass

    def __repr__(self):
        return 'APIConnect({0}:{1})'.format(self.username, self.host)
