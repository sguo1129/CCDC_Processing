import ConfigParser

from osgeo import ogr

from CCDC_Processing.db_connect import DBConnect


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
