import os
import re
import urllib

import psycopg2
from psycopg2.extensions import AsIs

from CCDC_Processing.utils import get_cfg
from CCDC_Processing.db_connect import DBConnect


class LandsatMeta(object):
    def __init__(self, config=None):
        if not config:
            config = get_cfg()
        else:
            config = get_cfg(config)

        self.db_connection = {'host': config['DB'].get('host'),
                              'database': config['DB'].get('database'),
                              'user': config['DB'].get('user'),
                              'password': config['DB'].get('password'),
                              'port': config['DB'].get('port')}

        self.landsat_table = config['DB'].get('landsatmeta')
        self.shp2psql = config['DB'].get('shp2psql')
        self.psql = config['DB'].get('psql')

    @staticmethod
    def _location_to_table(location):
        """
        Translate common name to what the actual table is called in the DB

        :param location: CONUS/Alaska/Hawaii
        :return: table name
        """
        # Should update table names to be more descriptive
        if location == 'CONUS':
            table = 'weld_grid_final_shifted'
        # elif location == 'AK':
        #     table = 'something'
        # elif location == 'HI':
        #     table = 'somthing'
        else:
            raise ValueError('Location value not recognized: {}'.format(location))

        return table

    def query_tile(self, h, v, location='CONUS'):
        """
        Query the landsat data for scenes interesting a specified WELD defined tile location

        :param h: WELD horizontal location
        :param v: WELD vertical location
        :param location: CONUS/Alaska/Hawaii
        :return: list of intersecting scenes
        """

        table = self._location_to_table(location)

        # Union on the two queries seemed to give the best results
        sql = ("select distinct sceneid "
               "from landsat_meta, %s w "
               "where sensor = 'OLI_TIRS' "
               "and nadir_offnadir = 'NADIR' "
               "and acquisitiondate between '1982-01-01'::DATE and '2015-12-31'::DATE "
               "and data_type in ('L1T', 'L1GT') "
               "and dayornight = 'DAY' "
               "and w.h = %s "
               "and w.v = %s "
               "and st_intersects(landsat_meta.geom, st_transform(w.geom, 4326)) "
               "and cloudcoverfull < 100 "
               "union "
               "select distinct sceneid "
               "from landsat_meta, %s w "
               "where sensor in ('LANDSAT_ETM', 'LANDSAT_ETM_SLC_OFF', 'LANDSAT_TM') "
               "and acquisitiondate between '1982-01-01'::DATE and '2015-12-31'::DATE "
               "and data_type = 'L1T' "
               "and dayornight = 'DAY' "
               "and w.h = %s "
               "and w.v = %s "
               "and st_intersects(landsat_meta.geom, st_transform(w.geom, 4326)) "
               "and cloudcoverfull < 100")

        with DBConnect(**self.db_connection) as db:
            db.select(sql, (AsIs(table), h, v, AsIs(table), h, v))

            ret = [_[0] for _ in db.fetcharr]

        return ret

    def baecv_tile(self, h, v, location='CONUS'):
        """
        Query the landsat data for scenes interesting a specified WELD defined tile location

        Query for BAECV support

        :param h: WELD horizontal location
        :param v: WELD vertical location
        :param location: CONUS/Alaska/Hawaii
        :return: list of intersecting scenes
        """
        table = self._location_to_table(location)

        # Union on the two queries seemed to give the best results
        sql = ("select distinct sceneid "
               "from landsat_meta, %s w "
               "where sensor = 'OLI_TIRS' "
               "and nadir_offnadir = 'NADIR' "
               "and acquisitiondate >= '1984-01-01'::DATE "
               "and geometric_rmse_model <= 10 "
               "and cloudcover <= 8 "
               "and dayornight = 'DAY' "
               "and w.h = %s "
               "and w.v = %s "
               "and st_intersects(landsat_meta.geom, st_transform(w.geom, 4326)) "
               "and cloudcoverfull < 100 "
               "and l1_available = 'Y' "
               "union "
               "select distinct sceneid "
               "from landsat_meta, %s w "
               "where sensor in ('LANDSAT_ETM', 'LANDSAT_ETM_SLC_OFF', 'LANDSAT_TM') "
               "and acquisitiondate >= '1984-01-01'::DATE "
               "and geometric_rmse_model <= 10 "
               "and cloudcover <= 8 "
               "and dayornight = 'DAY' "
               "and w.h = %s "
               "and w.v = %s "
               "and st_intersects(landsat_meta.geom, st_transform(w.geom, 4326)) "
               "and cloudcoverfull < 100 "
               "and l1_available = 'Y' ")

        with DBConnect(**self.db_connection) as db:
            db.select(sql, (AsIs(table), h, v, AsIs(table), h, v))

            ret = [_[0] for _ in db.fetcharr]

        return ret

    def fetch_tile_extents(self, h, v, location='CONUS'):
        table = self._location_to_table(location)

        regex = r'BOX\((.*) (.*),(.*) (.*)\)'

        sql = ("select st_extent(geom) "
               "from %s "
               "where h = %s "
               "and v = %s")

        with DBConnect(**self.db_connection) as db:
            db.select(sql, (AsIs(table), h, v))
            xmin, ymin, xmax, ymax = re.match(regex, db[0][0]).groups()

        return round(float(xmin)), round(float(ymin)), round(float(xmax)), round(float(ymax))

    def landsat_meta(self):
        """
        Sets up a Landsat metadata table and downloads the bulk data for processing
        :param tableid: Table name to be created/updated
        :return:
        """
        tableid = self.landsat_table
        url_dict = {'LS8/OLI': 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_8.csv',
                    'LS7/ETM+ SLC-on': 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_ETM.csv',
                    'LS7/ETM+ SLC-off': 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_ETM_SLC_OFF.csv',
                    'LS4-5/TM 1980-1989': 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_TM-1980-1989.csv',
                    'LS4-5/TM 1990-1999': 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_TM-1990-1999.csv',
                    'LS4-5/TM 2000-2009': 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_TM-2000-2009.csv',
                    'LS4-5/TM 2010-2012': 'http://landsat.usgs.gov/metadata_service/bulk_metadata_files/LANDSAT_TM-2010-2012.csv'}

        with DBConnect(**self.db_connection) as db:
            try:
                db.execute(self.metatable(tableid))
            except psycopg2.Error:
                db.rollback()

        for key in url_dict:
            tmp_csv = os.path.join(os.getcwd(), 'temp.csv')
            print 'Downloading ' + url_dict[key]
            urllib.urlretrieve(url_dict[key], tmp_csv)
            print '\nUpdating Db...'
            self.update_table(tmp_csv)
            os.remove(tmp_csv)

    def metatable(self, table_id='public.landsat_meta', owner='postgres'):
        """
        Create the given table name
        :param table_id: Table to be created
        :param owner: owner of the table
        :return:
        """
        str_dict = {'table': table_id,
                    'owner': owner,
                    'pkey': table_id}

        if '.' in table_id:
            str_dict['pkey'] = table_id.split('.')[1]

        sql_string = """CREATE TABLE IF NOT EXISTS %(table)s
        (
        sceneid character varying not null,
        sensor character varying not null,
        acquisitiondate date,
        dateupdated date,
        browseavailable character varying,
        browseurl character varying,
        path smallint,
        row smallint,
        upperleftcornerlatitude double precision,
        upperleftcornerlongitude double precision,
        upperrightcornerlatitude double precision,
        upperrightcornerlongitude double precision,
        lowerleftcornerlatitude double precision,
        lowerleftcornerlongitude double precision,
        lowerrightcornerlatitude double precision,
        lowerrightcornerlongitude double precision,
        scenecenterlatitude double precision,
        scenecenterlongitude double precision,
        cloudcover smallint,
        cloudcoverfull double precision,
        full_ul_quad_ccs double precision,
        full_ur_quad_ccs double precision,
        full_ll_quad_ccs double precision,
        full_lr_quad_ccs double precision,
        dayornight character varying,
        flightpath character varying,
        sunelevation double precision,
        sunazimuth double precision,
        receivingstation character varying,
        scenestarttime character varying,
        scenestoptime character varying,
        lookangle double precision,
        imagequality1 smallint,
        imagequality2 smallint,
        gainband1 character varying,
        gainband2 character varying,
        gainband3 character varying,
        gainband4 character varying,
        gainband5 character varying,
        gainband6h character varying,
        gainband6l character varying,
        gainband7 character varying,
        gainband8 character varying,
        gainchangeband1 character varying,
        gainchangeband2 character varying,
        gainchangeband3 character varying,
        gainchangeband4 character varying,
        gainchangeband5 character varying,
        gainchangeband6h character varying,
        gainchangeband6l character varying,
        gainchangeband7 character varying,
        gainchangeband8 character varying,
        satellitenumber character varying,
        data_type character varying,
        carturl character varying,
        date_acquired_gap_fill date,
        data_type_lorp character varying,
        datum character varying(5),
        elevation_source character varying(7),
        ellipsoid character varying(5),
        ephemeris_type character varying,
        false_easting integer,
        false_northing integer,
        gap_fill double precision,
        ground_control_points_model smallint,
        ground_control_points_verify smallint,
        geometric_rmse_model double precision,
        geometric_rmse_model_x double precision,
        geometric_rmse_model_y double precision,
        geometric_rmse_verify double precision,
        grid_cell_size_panchromatic double precision,
        grid_cell_size_reflective double precision,
        grid_cell_size_thermal double precision,
        map_projection_l1 character varying,
        map_projection_lora character varying,
        orientation character varying,
        output_format character varying,
        panchromatic_lines integer,
        panchromatic_samples integer,
        l1_available character varying,
        reflective_lines integer,
        reflective_samples integer,
        resampling_option character varying,
        scan_gap_interpolation double precision,
        thermal_lines integer,
        thermal_samples integer,
        true_scale_lat double precision,
        utm_zone integer,
        vertical_lon_from_pole double precision,
        present_band_1 character varying,
        present_band_2 character varying,
        present_band_3 character varying,
        present_band_4 character varying,
        present_band_5 character varying,
        present_band_6 character varying,
        present_band_7 character varying,
        present_band_8 character varying,
        nadir_offnadir character varying,
        geom geometry(Polygon, 4326),
        CONSTRAINT %(pkey)s_pkey PRIMARY KEY (sceneid)
        )
        with (OIDS=FALSE);
        ALTER table %(table)s
        OWNER to %(owner)s;
        create index %(pkey)s_geom_gist
        on %(table)s
        using gist
        (geom);"""

        return sql_string % str_dict

    def update_table(self, csv_path, table_id='landsat_meta'):
        """
        Updates the given Landsat metadata table from a CSV file
        :param csv_path: Path to CSV to update from
        :param table_id: Table to be updated
        :return:
        """
        with DBConnect(**self.db_connection) as db:
            sql_string = "select column_name from information_schema.columns where table_name =\
             '%s' and table_schema = 'public';" % table_id
            db.select(sql_string, tuple())
            cols_ls = [x[0] for x in db]
            csv_cols = ','.join(cols_ls[:-1])
            tmp_cols = ['landsat_tmp.' + x for x in cols_ls]
            both_cols = ','.join(['%s = %s' % (x, y) for x, y in zip(cols_ls, tmp_cols)])

            sql_commands = ["CREATE TABLE landsat_tmp AS SELECT * FROM %s LIMIT 0;" % table_id,
                            "COPY landsat_tmp (%s) FROM '%s' (FORMAT CSV, HEADER TRUE, DELIMITER ',');" % (
                            csv_cols, csv_path),
                            "DELETE FROM %s USING landsat_tmp WHERE %s.dateupdated < landsat_tmp.dateupdated\
                             AND %s.acquisitiondate = landsat_tmp.acquisitiondate AND %s.path = landsat_tmp.path\
                             AND %s.row = landsat_tmp.row\
                             AND %s.sensor = landsat_tmp.sensor;" % (
                            table_id, table_id, table_id, table_id, table_id, table_id),
                            "INSERT INTO %s SELECT * FROM landsat_tmp WHERE NOT EXISTS\
                             (SELECT sceneid FROM %s WHERE sceneid = landsat_tmp.sceneid);" % (table_id, table_id),
                            "UPDATE %s SET geom = ST_GeometryFromText('Polygon((' ||\
                             upperleftcornerlongitude::text || ' ' || upperleftcornerlatitude::text || ',' ||\
                             upperrightcornerlongitude::text || ' ' || upperrightcornerlatitude::text || ',' ||\
                             lowerrightcornerlongitude::text || ' ' || lowerrightcornerlatitude::text || ',' ||\
                             lowerleftcornerlongitude::text || ' ' || lowerleftcornerlatitude::text || ',' ||\
                             upperleftcornerlongitude::text || ' ' || upperleftcornerlatitude::text || '))',\
                             4326) WHERE geom IS NULL;" % table_id,
                            "DROP TABLE IF EXISTS landsat_tmp"]

            steps = ['Creating temporary table',
                     'Importing CSV',
                     'Updating scenes',
                     'Inserting new scenes',
                     'Updating geometry',
                     'Dropping temporary table']

            # SQL commands are done sequentially for error checking purposes
            for x in range(len(sql_commands)):
                print steps[x]
                db.execute(sql_commands[x])

    # def query_shape(self, shapepath):
    #     """
    #     Queries a shape file against the Landsat metadata to determine
    #     all intersecting scenes
    #
    #     :param shapepath: input path to shape file
    #     :param listpath: output path for the scene list
    #     :return: None
    #     """
    #     spatialref = epsg_from_file(shapepath)
    #
    #     psql_in = subprocess.Popen([self.psql,
    #                                 '-h {}'.format(self.db_connection['host']),
    #                                 '-p {}'.format(self.db_connection['port']),
    #                                 '-P {}'.format(self.db_connection['password']),
    #                                 '-u {}'.format(self.db_connection['user']),
    #                                 '-d {}'.format(self.db_connection['database'])],
    #                                stdout=subprocess.PIPE)
    #
    #     psql_out = subprocess.Popen([self.shp2psql, '-s {}:4326'.format(spatialref), 'shp_tmp'], stdin=psql_in.stdout)
    #
    #     psql_in.wait()
    #
    #     pgstr = """copy (select sceneid from %s, shp_temp
    #              where st_intersects(%s.geom, shp_temp.geom)
    #              and sensor in ('LANDSAT_ETM', 'LANDSAT_ETM_SLC_OFF', 'LANDSAT_TM')
    #              and dayornight = 'DAY'
    #              and acquisitiondate between '1983-01-01'::DATE and '2014-12-31'::DATE)
    #              to '%s'"""
    #     sql = '''select distinct sceneid
    #              from landsat_meta
    #              where sensor = 'OLI_TIRS'
    #              and nadir_offnadir = 'NADIR'
    #              and acquisitiondate between '1982-01-01'::DATE and '2015-12-31'::DATE
    #              and data_type in ('L1T', 'L1GT')
    #              and dayornight = 'DAY'
    #
    #              union
    #
    #              select distinct sceneid
    #              from landsat_meta
    #              where sensor in ('LANDSAT_ETM', 'LANDSAT_ETM_SLC_OFF', 'LANDSAT_TM')
    #              and acquisitiondate between '1982-01-01'::DATE and '2015-12-31'::DATE
    #              and data_type = 'L1T'
    #              and dayornight = 'DAY';'''
    #
    #     pgrmtable = 'drop table shp_temp cascade'
    #
    #     # with DBConnect(**self.db_connection) as db:
    #     #     db.execute(pgstr % (metatbl, metatbl, listpath))
    #     #     db.execute(pgrmtable)



