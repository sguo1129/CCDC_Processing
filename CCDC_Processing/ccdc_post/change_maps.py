import os
import datetime
import multiprocessing as mp

from osgeo import gdal, osr
import numpy as np
import scipy.io as sio


indir = r'D:\lcmap\matlab_compare\WA-08\zhe\TSFitMap'
outdir = r'D:\lcmap\matlab_compare\WA-08\klsmith\changemaps'

test_image = r'D:\lcmap\matlab_compare\WA-08\LT50460271990297\LT50460271990297PAC04_MTLstack'


class ChangeMap(object):
    changemaps = ('ChangeMap', 'ChangeMagMap', 'ConditionMap',
                  'NumberMap', 'CoverMap', 'CoverQAMap', 'QAMap')

    def __init__(self, output_path, reference_file, mp=False):
        if not os.path.exists(output_path):
            os.makedirs(output_path)

        self.reference_file = reference_file
        self.output_path = output_path

        self.geo = None
        self.proj = None
        self.rows = None
        self.cols = None

        self.set_raster_info()

        self.rec_cg = None

    def set_raster_info(self):
        ds = gdal.Open(self.reference_file, gdal.GA_ReadOnly)

        self.geo = ds.GetGeoTransform()
        self.proj = ds.GetProjection()
        self.rows = ds.RasterYSize
        self.cols = ds.RasterXSize

        ds = None

    def create_changemaps(self, input_file, multi=False):
        rec_cg = self.read_matlab_record(input_file)

        offy = int(os.path.split(input_file)[-1][13:-4]) - 1

        multi_out = []

        pos = rec_cg['pos']
        t_start = rec_cg['t_start']
        t_end = rec_cg['t_end']
        t_break = rec_cg['t_break']
        coefs = rec_cg['coefs']
        change_prob = rec_cg['change_prob']
        categ = rec_cg['category']

        mag = rec_cg['magnitude']
        number = rec_cg['num_obs']

        starts = self.get_starts(t_start)

        for start in starts:
            locs = np.where(t_start == start)

            dt = self.matlab2datetime(start)
            doy = dt.timetuple().tm_yday

            out = {'y_off': offy,
                   'year': dt.year,
                   'ChangeMap': np.zeros((1, 5000), dtype=np.int32),
                   'ChangeMagMap': np.zeros((1, 5000), dtype=np.float),
                   'CoverMap': np.zeros((1, 5000), dtype=np.int32),
                   'CoverQAMap': np.zeros((1, 5000), dtype=np.int32),
                   'ConditionMap': np.zeros((1, 5000), dtype=np.float),
                   'QAMap': np.zeros((1, 5000), dtype=np.int32),
                   'NumberMap': np.zeros((1, 5000), dtype=np.int32)
                   }

            # Need to work on getting cast across the array
            for idx in locs[0]:

                b_start = coefs[idx][0, 0] + t_start[idx] * coefs[idx][0, 1]
                r_start = coefs[idx][2, 0] + t_start[idx] * coefs[idx][2, 1]
                n_start = coefs[idx][3, 0] + t_start[idx] * coefs[idx][3, 1]

                b_end = coefs[idx][0, 0] + t_end[idx] * coefs[idx][0, 1]
                r_end = coefs[idx][2, 0] + t_end[idx] * coefs[idx][2, 1]
                n_end = coefs[idx][3, 0] + t_end[idx] * coefs[idx][3, 1]

                evi_start = 2.5 * (n_start - r_start) / (n_start + 6 * r_start - 7.5 * b_start + 10000)
                evi_end = 2.5 * (n_end - r_end) / (n_end + 6 * r_end - 7.5 * b_end + 10000)

                evi_slope = 10000 * (evi_end - evi_start) / (t_end[idx] - t_start[idx])

                out['ConditionMap'][idx] = evi_slope

            out['QAMap'][locs] = categ[locs]
            out['NumberMap'][locs] = number[locs]

            # Classification
            # prod_files['CoverMap'][out_idx, year_idx] = ccdc_class[locs][0]
            # prod_files['CoverQAMap'][out_idx, year_idx] = class_qa[locs][0]

            if change_prob[locs] == 1:
                out['ChangeMap'][locs] = doy
                out['ChangeMagMap'][locs] = np.linalg.norm(mag[locs], ord=2)

            if multi:
                multi_out.append(out)
            else:
                self.write_output(out)

        if multi:
            return multi_out

    def write_output(self, data):
        year = data.pop('year')
        y_off = data.pop('y_off')

        for prod in data:
            ds = self.get_raster_ds(prod, year)
            ds.GetRasterBand(1).WriteArry(data[prod], 0, y_off)

            ds = None

    @staticmethod
    def get_starts(t_start):
        return np.unique(t_start)

    @staticmethod
    def prod_data_type(product):
        if product in ('ChangeMap', 'NumberMap'):
            return gdal.GDT_UInt16
        elif product in ('ChangeMagMap', 'ConditionMap'):
            return gdal.GDT_Float32
        elif product in ('CoverMap', 'CoverQAMap', 'QAMap'):
            return gdal.GDT_Byte
        else:
            raise ValueError

    @staticmethod
    def read_matlab_record(file_path):
        return sio.loadmat(file_path, squeeze_me=True)['rec_cg']

    @staticmethod
    def matlab2datetime(matlab_datenum):
        day = datetime.datetime.fromordinal(int(matlab_datenum))
        dayfrac = datetime.timedelta(days=matlab_datenum % 1) - datetime.timedelta(days=366)
        return day + dayfrac

    def get_raster_ds(self, product, year):
        file_path = os.path.join(self.output_path, product + '_{0}.tif'.format(year))

        if os.path.exists(file_path):
            return gdal.Open(file_path)
        else:
            return self.create_geotif(product, year)

    def create_geotif(self, product, year):
        data_type = self.prod_data_type(product)

        file_path = os.path.join(self.output_path, product + '_{0}.tif'.format(year))

        ds = (gdal
              .GetDriverByName('GTiff')
              .Create(file_path, self.cols, self.rows, data_type))

        ds.SetGeoTransform(self.geo)
        ds.SetProjection(self.proj)

        return ds


def single_run():
    for f in os.listdir(indir):
        change = ChangeMap(outdir, test_image)
        change.create_changemaps(os.path.join(indir, f))

if __name__ == '__main__':
    single_run()
