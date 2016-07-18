import os
import subprocess
import multiprocessing as mp
import tarfile
import shutil

from osgeo import gdal
import numpy as np

WORK_DIR = '/dev/shm'


def create_tiles(inpath, outpath, worker_num):
    if not os.path.exists(outpath):
        os.makedirs(outpath)

    file_q = mp.Queue()
    message_q = mp.Queue()

    file_enqueue(inpath, file_q, worker_num)
    work = work_paths(worker_num, WORK_DIR)

    message = mp.Process(target=progress, args=(message_q, worker_num))
    message.start()
    for i in range(worker_num):
        p_args = (file_q, message_q, outpath, work[i])
        mp.Process(target=process_tile, args=p_args).start()

    message.join()


def process_tile(file_q, prog_q, out_path, work_path):
    """Process a file from the queue"""
    def unpackage():
        with tarfile.open(file) as f:
            f.extractall(path=work_path)

    def warp():
        subprocess.call('gdalwarp -of ENVI -co "INTERLEAVE=BIP" {} {}'
                        .format(pathing['WARP']['IN'], pathing['WARP']['OUT']),
                        shell=True, stdout=devnull, stderr=devnull)

    def vrt():
        subprocess.call('gdalbuildvrt -separate {} {}'
                        .format(pathing['VRT']['OUT'], pathing['VRT']['IN']),
                        shell=True, stdout=devnull, stderr=devnull)

    def build_paths():
        base = os.path.join(out_path, tiff_base)

        if not os.path.exists(base):
            os.makedirs(base)

        phs = {'VRT': {'OUT': os.path.join(work_path, tiff_base + '.vrt'),
                       'IN': ' '.join(band_list)},
               'WARP': {'IN': os.path.join(work_path, tiff_base + '.vrt'),
                        'OUT': os.path.join(base, tiff_base + '_MTLstack.img')},
               'GCP': {'IN': os.path.join(work_path, tiff_base + '_GCP.txt'),
                       'OUT': os.path.join(base, tiff_base + '_GCP.txt')},
               'MTL': {'IN': os.path.join(work_path, tiff_base + '_MTL.txt'),
                       'OUT': os.path.join(base, tiff_base + '_MTL.txt')}}
        return phs

    def check_percent_clear():
        ds = gdal.Open(band_list[-1], gdal.GA_ReadOnly)

        arr = ds.GetRasterBand(1).ReadAsArray()
        bins = np.bincount(arr.ravel())

        if np.sum(bins[0:2]) / np.sum(bins[:-1]).astype(np.float) > 0.20:
            return True

        return False

    def build_l8_list():
        return ['{}_sr_band2.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band3.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band4.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band5.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band6.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band7.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_toa_band10.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_cfmask.tif'.format(os.path.join(work_path, tiff_base))]

    def build_tm_list():
        return ['{}_sr_band1.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band2.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band3.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band4.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band5.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_sr_band7.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_toa_band6.tif'.format(os.path.join(work_path, tiff_base)),
                '{}_cfmask.tif'.format(os.path.join(work_path, tiff_base))]

    def base_name():
        base = ''
        for tiff_file in os.listdir(work_path):
            if tiff_file[-12:] != 'sr_band1.tif':
                continue

            base = tiff_file[:21]
            break

        return base

    def clean_up():
        for f in os.listdir(work_path):
            os.remove(os.path.join(work_path, f))

    devnull = open(os.devnull, 'w')
    proc = work_path[-1]
    while True:
        try:
            file = file_q.get()

            if file == 'KILL':
                prog_q.put('Killing process %s' % proc)
                break

            # prog_q.put('Process %s: Unpacking %s' % (proc, file))
            unpackage()

            tiff_base = base_name()

            if tiff_base[2] == '8':
                band_list = build_l8_list()
            else:
                band_list = build_tm_list()

            # if not check_percent_clear():
                # prog_q.put('Process %s: %s falls below clear threshold' % (proc, tiff_base))
                # clean_up()
                # continue

            pathing = build_paths()

            if os.path.exists(pathing['WARP']['OUT']):
                clean_up()
                continue

            # prog_q.put('Process %s: Building VRT stack for %s' % (proc, tiff_base))
            vrt()

            # prog_q.put('Process %s: Calling Warp for %s' % (proc, tiff_base))
            warp()

            # prog_q.put('Process %s: Moving ancillery files for %s' % (proc, tiff_base))
            if os.path.exists(pathing['GCP']['IN']):
                shutil.copy(pathing['GCP']['IN'], pathing['GCP']['OUT'])
            if os.path.exists(pathing['MTL']['IN']):
                shutil.copy(pathing['MTL']['IN'], pathing['MTL']['OUT'])

            clean_up()
        except Exception as e:
            # prog_q.put('Process %s: Hit an exception - %s' % (proc, e))
            # prog_q.put('Killing process %s' % proc)
            break

    os.rmdir(work_path)


def file_enqueue(path, q, worker_num):
    """Builds a queue of files to be processed"""

    for gzfile in os.listdir(path):
        if gzfile[-2:] != 'gz':
            continue

        q.put(os.path.join(path, gzfile))

    for _ in range(worker_num):
        q.put('KILL')


def work_paths(worker_num, path):
    """Makes working directories for the multi-processing"""

    out = []
    for i in range(worker_num):
        new_path = os.path.join(path, 'working%s' % i)
        out.append(new_path)
        if not os.path.exists(new_path):
            os.mkdir(new_path)

    return out


def progress(prog_q, worker_num):
    count = 0
    while True:
        message = prog_q.get()

        # print(message)

        if message[:4] == 'Kill':
            count += 1
        if count >= worker_num:
            break


if __name__ == '__main__':
    # input_path = raw_input('Tarball inputs: ')
    # output_path = raw_input('Output directory: ')
    # num = raw_input('Number of workers: ')
    input_path = '/shared/users/klsmith/klsmith@usgs.gov-06012016-140616'
    output_path = '/shared/users/klsmith/AL-h06v08'
    num = 4

    create_tiles(input_path, output_path, int(num))
