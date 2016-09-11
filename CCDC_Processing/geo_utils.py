"""
Geospatial Utilities
"""
import math
from collections import namedtuple

from osgeo import gdal, ogr


GeoExtent = namedtuple('GeoExtent', ['x_min', 'y_max', 'x_max', 'y_min'])
GeoAffine = namedtuple('GeoAffine', ['ul_x', 'x_resolution', 'x_offset', 'ul_y', 'y_offset', 'y_resolution'])
GeoCoordinate = namedtuple('GeoCoordinate', ['x', 'y'])
RowColumn = namedtuple('RowColumn', ['row', 'column'])
RowColumnExtent = namedtuple('RowColumnExtent', ['ul_row', 'ul_col', 'lr_row', 'lr_col'])


def shapefile_extent(shapefile):
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer()
    ext1 = layer.GetExtent()

    return GeoExtent(x_min=ext1[0],
                     x_max=ext1[1],
                     y_min=ext1[2],
                     y_max=ext1[3])


def epsg_from_shapefile(shapefile):
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer()
    spatialref = layer.GetSpatialRef()

    return spatialref.ExportToEPSG()


def fifteen_offset(coord):
    return (coord // 30) * 30 + 15


def geo_to_rowcol(affine, coord):
    return RowColumn(row=int(math.ceil((affine.ul_y - coord.y) / -affine.y_resolution)),
                     column=int(math.floor((coord.x - affine.ul_x) / affine.x_resolution)))


def rowcol_to_geo(affine, rowcol):
    return GeoCoordinate(x=(affine.ul_x + rowcol.column * affine.x_resolution) + affine.x_offset,
                         y=(affine.ul_y + rowcol.row * affine.y_resolution) + affine.y_offset)


def get_raster_ds(raster_file, readonly=True):
    if readonly:
        return gdal.Open(raster_file, gdal.GA_ReadOnly)
    else:
        return gdal.Open(raster_file, gdal.GA_Update)


def get_raster_geoextent(raster_file):
    ds = get_raster_ds(raster_file)

    affine = GeoAffine(ds.GetGeoTransform())
    rowcol = RowColumn(row=ds.RasterYSize, column=ds.RasterXSize)

    geo_lr = rowcol_to_geo(affine, rowcol)

    return GeoExtent(x_min=affine.ul_x, x_max=geo_lr.x,
                     y_min=geo_lr.y, y_max=affine.ul_y)


def array_from_extent(raster_file, geo_extent, band=1):
    ds = get_raster_ds(raster_file)
    affine = GeoAffine(ds.GetGeoTransform())

    ul_geo = GeoCoordinate(x=geo_extent.x_min, y=geo_extent.y_max)
    lr_geo = GeoCoordinate(x=geo_extent.x_max, y=geo_extent.y_min)

    ul_rc = geo_to_rowcol(affine, ul_geo)
    lr_rc = geo_to_rowcol(affine, lr_geo)

    return ds.GetRasterBand(band).ReadAsArray(ul_rc.column,
                                              lr_rc.column - ul_rc.column,
                                              ul_rc.row,
                                              lr_rc.row - ul_rc.row)
