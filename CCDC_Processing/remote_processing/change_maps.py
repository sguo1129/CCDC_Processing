import os
import datetime
import multiprocessing as mp

from osgeo import gdal, osr
import numpy as np
import scipy.io as sio


indir = r'D:\lcmap\matlab_compare\WA-08\zhe\TSFitMap'
outdir = r'D:\lcmap\matlab_compare\WA-08\klsmith\changemaps'

min_year = 1985
max_year = 2015

num_cpus = 4

test_image = r'D:\lcmap\matlab_compare\WA-08\LT50460271990297\LT50460271990297PAC04_MTLstack'


if not os.path.exists(outdir):
    os.makedirs(outdir)


def raster_info(raster_path):
    ds = gdal.Open(test_image, gdal.GA_ReadOnly)
    return ds.GetGeoTransform(), ds.GetProjection()


def raster_out(output_folder, cols, rows, bands, geo, proj, write_q, kill_count):
    files = ccdc_raster_prods(output_folder, cols, rows, bands, geo, proj)
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


def ccdc_raster_prods(output_path, cols, rows, bands, geo, proj):
    prod_files = {'ChangeMap': None,
                  'ChangeMagMap': None,
                  'CoverMap': None,
                  'CoverQAMap': None,
                  'ConditionMap': None,
                  'QAMap': None,
                  'NumberMap': None
                  }

    params = {'file_path': '',
              'cols': cols,
              'rows': rows,
              'bands': bands,
              'geo': geo,
              'proj': proj,
              'data_type': None}

    for f in prod_files:
        params['file_path'] = os.path.join(output_path, f)
        params['data_type'] = prod_data_type(f)

        prod_files[f] = create_raster(**params)

    return prod_files


def prod_data_type(product):
    if product in ('ChangeMap', 'NumberMap'):
        return gdal.GDT_UInt16
    elif product in ('ChangeMagMap', 'ConditionMap'):
        return gdal.GDT_Float32
    elif product in ('CoverMap', 'CoverQAMap', 'QAMap'):
        return gdal.GDT_Byte
    else:
        raise ValueError


def create_raster(file_path, cols, rows, bands, geo, proj, data_type):
    ds = (gdal
          .GetDriverByName('GTiff')
          .Create(file_path + '.tif', cols, rows, bands, data_type))

    ds.SetGeoTransform(geo)
    ds.SetProjection(proj)

    return ds


def read_matlab_record(file_path):
    return sio.loadmat(file_path, squeeze_me=True)['rec_cg']


def matlab2datetime(matlab_datenum):
    day = datetime.datetime.fromordinal(int(matlab_datenum))
    dayfrac = datetime.timedelta(days=matlab_datenum%1) - datetime.timedelta(days = 366)
    return day + dayfrac


def ccdc_raster_worker(depth, read_q, write_q):
    while True:
        prod_files = {'ChangeMap': np.full((5000, depth), 9999, dtype=np.int32),
                      'ChangeMagMap': np.full((5000, depth), 9999, dtype=np.float),
                      'CoverMap': np.full((5000, depth), 255, dtype=np.int32),
                      'CoverQAMap': np.full((5000, depth), 255, dtype=np.int32),
                      'ConditionMap': np.full((5000, depth), 9999, dtype=np.float),
                      'QAMap': np.full((5000, depth), 255, dtype=np.int32),
                      'NumberMap': np.full((5000, depth), 9999, dtype=np.int32)
                      }

        infile = read_q.get()

        if infile == 'kill':
            write_q.put(('kill', None))
            break

        rec_cg = read_matlab_record(infile)
        offy = int(os.path.split(infile)[-1][13:-4])

        pos = rec_cg['pos']
        t_start = rec_cg['t_start']
        t_end = rec_cg['t_end']
        t_break = rec_cg['t_break']
        coefs = rec_cg['coefs']
        change_prob = rec_cg['change_prob']
        ccdc_class = rec_cg['class']
        categ = rec_cg['category']
        class_qa = rec_cg['classQA']
        mag = rec_cg['magnitude']
        number = rec_cg['num_obs']

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
            prod_files['CoverMap'][out_idx, year_idx] = ccdc_class[i][0]
            prod_files['CoverQAMap'][out_idx, year_idx] = class_qa[i][0]

            if change_prob[i] == 1:
                prod_files['ChangeMap'][out_idx, year_idx] = doy
                prod_files['ChangeMagMap'][out_idx, year_idx] = np.linalg.norm(mag[i], ord=2)

        write_q.put((prod_files, offy))


def build_read_queue(input_dir, read_q):
    for f in os.listdir(input_dir):
        if f[-4:] != '.mat':
            continue

        read_q.put(os.path.join(input_dir, f))


def run():
    write_q = mp.Queue()
    read_q = mp.Queue()

    build_read_queue(indir, read_q)
    geo, proj = raster_info(test_image)

    for i in range(num_cpus - 1):
        mp.Process(target=ccdc_raster_worker, args=(5000, read_q, write_q)).start()

    raster_out(output_folder=outdir, bands=30, rows=5000, cols=5000, kill_count=3, write_q=write_q, geo=geo, proj=proj)

if __name__ == '__main__':
    run()
