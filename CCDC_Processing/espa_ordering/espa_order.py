import CCDC_Processing.utils as utils


class ESPAOrderException(Exception):
    pass


class ESPAOrder(utils.APIConnect):
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

    conus = {'aea': {'standard_parallel_1': 29.5,
                     'standard_parallel_2': 45.5,
                     'central_meridian': -96,
                     'latitude_of_origin': 23,
                     'false_easting': 0,
                     'false_northing': 0,
                     'datum': 'nad83'}}

    alaska = {'aea': {'standard_parallel_1': 55,
                      'standard_parallel_2': 65,
                      'central_meridian': -154,
                      'latitude_of_origin': 50,
                      'false_easting': 0,
                      'false_northing': 0,
                      'datum': 'nad83'}}

    hawaii = {'aea': {'standard_parallel_1': 8,
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
