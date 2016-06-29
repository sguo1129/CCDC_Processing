from CCDC_Processing.ordering.ESPA_API import APIConnect


class ESPAOrder(APIConnect):
    def __init__(self, username, password, host):
        super(ESPAOrder, self).__init__(username, password, host)

        self.espa_order = {'resampling_method': 'cc',
                           'format': 'gtiff'}

    def add_sensor(self, sensor):
        if not isinstance(sensor, dict):
            raise TypeError

        self.espa_order.update(sensor)

    def add_extent(self, xmin, xmax, ymin, ymax):
        upd = {'image_extents': {'north': ymax,
                                 'south': ymin,
                                 'east': xmax,
                                 'west': xmin}}

        self.espa_order.update(upd)

    def add_projection(self, proj):
        if not isinstance(proj, dict):
            raise TypeError

        self.espa_order.update({'projection': proj})

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
                     'datum': 'wgs84'}}

    alaska = {'aea': {'standard_parallel_1': 55,
                      'standard_parallel_2': 65,
                      'central_meridian': -154,
                      'latitude_of_origin': 50,
                      'false_easting': 0,
                      'false_northing': 0,
                      'datum': 'wgs84'}}

    hawaii = {'aea': {'standard_parallel_1': 8,
                      'standard_parallel_2': 18,
                      'central_meridian': -157,
                      'latitude_of_origin': 3,
                      'false_easting': 0,
                      'false_northing': 0,
                      'datum': 'wgs84'}}
