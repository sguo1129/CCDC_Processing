from CCDC_Processing.api_connect import APIConnect
import CCDC_Processing.utils as utils
from CCDC_Processing.espa_ordering.landsat_meta import LandsatMeta


class ESPAOrderException(Exception):
    pass


class ESPAOrder(APIConnect):
    lcmap_prods = ['sr', 'toa', 'cloud', 'bt', 'source_metadata']

    def __init__(self, username, password, host):
        super(ESPAOrder, self).__init__(username, password, host)

        self.espa_order = {'resampling_method': 'cc',
                           'format': 'gtiff'}

    def add_sensor(self, sensor):
        if not isinstance(sensor, dict):
            raise TypeError

        self.espa_order.update(sensor)

    def add_acquisitions_from_list(self, acq_list):
        avail = self.post_available_prods(acq_list)

        # if 'not_implemented' or 'date_restricted' in avail:
        #     raise ESPAOrderException('Acquisition list contains errors: {}'.format(avail))

        for sensor in avail:
            avail[sensor]['products'] = [_ for _ in self.lcmap_prods]

        self.espa_order.update(avail)

    def add_extent(self, xmin, xmax, ymin, ymax):
        upd = {'image_extents': {'north': ymax,
                                 'south': ymin,
                                 'east': xmax,
                                 'west': xmin,
                                 'units': 'meters'}}

        self.espa_order.update(upd)

    def add_projection(self, proj):
        if not isinstance(proj, dict):
            raise TypeError

        self.espa_order.update({'projection': proj})

    def add_note(self, note):
        self.espa_order.update({'note': note})

    def place_order(self):
        return self.post_order(self.espa_order)


class AlbersProjections(object):
    def __setattr__(self, key, value):
        pass

    CONUS = {'aea': {'standard_parallel_1': 29.5,
                     'standard_parallel_2': 45.5,
                     'central_meridian': -96,
                     'latitude_of_origin': 23,
                     'false_easting': 0,
                     'false_northing': 0,
                     'datum': 'nad83'}}

    AK = {'aea': {'standard_parallel_1': 55,
                  'standard_parallel_2': 65,
                  'central_meridian': -154,
                  'latitude_of_origin': 50,
                  'false_easting': 0,
                  'false_northing': 0,
                  'datum': 'nad83'}}

    HI = {'aea': {'standard_parallel_1': 8,
                  'standard_parallel_2': 18,
                  'central_meridian': -157,
                  'latitude_of_origin': 3,
                  'false_easting': 0,
                  'false_northing': 0,
                  'datum': 'nad83'}}


def order_instance(config_path=None):
    if not config_path:
        cfg = utils.get_cfg()
    else:
        cfg = utils.get_cfg(config_path)

    return ESPAOrder(**cfg['API'])


def order_weld_tile(h, v, location='CONUS', config=None):
    """
    Step through ordering all the scenes that intersect a specified WELD tile frame

    :param h: horizontal tile location
    :param v: vertical tile location
    :param location: CONUS/AK/HI
    :param config: config file path
    :return: order id or error
    """
    # Initialize the base order and Landsat Metadata DB connection
    order = order_instance(config)
    meta = LandsatMeta()

    # Add the projection information based on the AOI
    proj = AlbersProjections.__getattribute__(location)
    order.add_projection(proj)

    # Retrieve the intersecting scenes for the order and extents
    scene_ls = meta.query_tile(h, v, location)
    xmin, ymin, xmax, ymax = meta.fetch_tile_extents(h, v, location)

    # Add acquisitions and extent information
    order.add_acquisitions_from_list(scene_ls)
    order.add_extent(xmin, xmax, ymin, ymax)

    # Add a note for easier order tracking
    order.add_note('h{}v{} loc: {}'.format(h, v, location))

    # Place the order and return the subsequent order_id or error
    return order.place_order()
