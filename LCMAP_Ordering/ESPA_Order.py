class ESPAOrder(object):
    def add_component(self, comp):
        self.__setattr__(type(comp).__name__, comp)

    def __repr__(self):
        return self.__dict__


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
