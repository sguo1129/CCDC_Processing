"""
Geospatial Utilities
"""
from collections import namedtuple

from osgeo import gdal, ogr


GeoExtent = namedtuple('GeoExtent', ['x_min', 'y_max', 'x_max', 'y_min'])
GeoAffine = namedtuple('GeoAffine', ['ul_x', 'x_pixel_size', 'x_offset', 'ul_y', 'y_offset', 'y_pixel_size'])
GeoCoordinate = namedtuple('GeoCoordinate', ['x', 'y'])
RowColumn = namedtuple('RowColumn', ['row', 'column'])


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


def geo_to_rowcol(affine, coord):
    return RowColumn(row=(affine.ul_y - coord.y) / -affine.y_pixel_size,
                     column=(coord.x - affine.ul_x) / affine.x_pixel_size)


def array_from_extent(raster_file, geo_extent):
    ds = gdal.Open(raster_file, gdal.GA_ReadOnly)
    affine = GeoAffine(ds.GetGeoTransform())
