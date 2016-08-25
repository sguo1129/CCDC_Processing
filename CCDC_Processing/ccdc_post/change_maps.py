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
            pass

        if multi:
            return
        else:
            # Write outputs
            pass

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


def raster_out(output_folder, cols, rows, bands, geo, proj, write_q, kill_count, single=False, clear=False):
    files = ccdc_rasters(output_folder, cols, rows, bands, geo, proj)
    kill_total = 0
    while True:
        if kill_total >= kill_count:
            break

        prods, offy = write_q.get(block=True)

        if prods == 'kill':
            kill_total += 1
            continue

        for p in prods:
            for b in range(bands):
                files[p].GetRasterBand(b + 1).WriteArray(prods[p][:, b], 0, offy)

    for f in files:
        files[f] = None


def ccdc_raster_worker(depth, infile):
    prod_files = {'ChangeMap': np.full((5000, depth), 9999, dtype=np.int32),
                  'ChangeMagMap': np.full((5000, depth), 9999, dtype=np.float),
                  # 'CoverMap': np.full((5000, depth), 255, dtype=np.int32),
                  # 'CoverQAMap': np.full((5000, depth), 255, dtype=np.int32),
                  'ConditionMap': np.full((5000, depth), 9999, dtype=np.float),
                  'QAMap': np.full((5000, depth), 255, dtype=np.int32),
                  'NumberMap': np.full((5000, depth), 9999, dtype=np.int32)
                  }

    rec_cg = read_matlab_record(infile)
    offy = int(os.path.split(infile)[-1][13:-4]) - 1

    pos = rec_cg['pos']
    t_start = rec_cg['t_start']
    t_end = rec_cg['t_end']
    t_break = rec_cg['t_break']
    coefs = rec_cg['coefs']
    change_prob = rec_cg['change_prob']
    categ = rec_cg['category']

    mag = rec_cg['magnitude']
    number = rec_cg['num_obs']

    # Classification
    # class_qa = rec_cg['classQA']
    # ccdc_class = rec_cg['class']

    for i in range(len(pos)):
        # Matlab arrays are 1 based
        out_idx = pos[i] % 5000 - 1

        if out_idx == -1:
            out_idx = 4999

        dt = matlab2datetime(t_start[i])
        year = dt.year
        doy = dt.timetuple().tm_yday

        if not 1984 < year < 2016:
            continue
        else:
            year_idx = year - 1985

        # if i > 1:
        #     if pos[i] == pos[i - 1]:
        #         prod_files['CoverMap'][out_idx, year_idx] = ccdc_class[i]

        # EVI

        # coefs[<index position>][<band number>, <coefficient value>]
        b_start = coefs[i][0, 0] + t_start[i] * coefs[i][0, 1]
        r_start = coefs[i][2, 0] + t_start[i] * coefs[i][2, 1]
        n_start = coefs[i][3, 0] + t_start[i] * coefs[i][3, 1]

        b_end = coefs[i][0, 0] + t_end[i] * coefs[i][0, 1]
        r_end = coefs[i][2, 0] + t_end[i] * coefs[i][2, 1]
        n_end = coefs[i][3, 0] + t_end[i] * coefs[i][3, 1]

        evi_start = 2.5 * (n_start - r_start) / (n_start + 6 * r_start - 7.5 * b_start + 10000)
        evi_end = 2.5 * (n_end - r_end) / (n_end + 6 * r_end - 7.5 * b_end + 10000)

        evi_slope = 10000 * (evi_end - evi_start) / (t_end[i] - t_start[i])

        prod_files['ConditionMap'][out_idx, year_idx] = evi_slope
        prod_files['QAMap'][out_idx, year_idx] = categ[i]
        prod_files['NumberMap'][out_idx, year_idx] = number[i]

        # Classification
        # prod_files['CoverMap'][out_idx, year_idx] = ccdc_class[i][0]
        # prod_files['CoverQAMap'][out_idx, year_idx] = class_qa[i][0]

        if change_prob[i] == 1:
            prod_files['ChangeMap'][out_idx, year_idx] = doy
            prod_files['ChangeMagMap'][out_idx, year_idx] = np.linalg.norm(mag[i], ord=2)

        return prod_files, offy


def single_run():
    geo, proj = raster_info(test_image)
    files = ccdc_rasters(outdir, 5000, 5000, 30, geo, proj)

    for f in os.listdir(indir):
        print f
        prods, offy = ccdc_raster_worker(5000, os.path.join(indir, f))

        for p in prods:
            for b in range(30):
                # print p, offy, f
                files[p].GetRasterBand(b + 1).WriteArray(prods[p][:, b].reshape(1, 5000), 0, offy)

    for f in files:
        files[f] = None

if __name__ == '__main__':
    single_run()
