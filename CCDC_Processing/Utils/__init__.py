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
