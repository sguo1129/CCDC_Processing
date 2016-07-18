import os
import subprocess
import tarfile
import shutil
import logging
import argparse

from osgeo import gdal
import numpy as np

GDAL_PATH = os.environ.get('GDAL')
if not GDAL_PATH:
    raise Exception('GDAL environment variable not set')

GDAL_PATH = os.path.join(GDAL_PATH, 'bin', '{}')

LOGGER = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s in %(pathname)s:%(lineno)d')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER .setLevel(logging.DEBUG)


def parse_args():
    pass


def unpackage(infile, outpath):
    LOGGER.debug('Unpacking: {}'.format(infile))
    with tarfile.open(infile) as f:
        f.extractall(path=outpath)


def warp(inwarp, outwarp):
    LOGGER.debug('Warping to: {}'.format(outwarp))
    subprocess.call('{}/gdalwarp -of ENVI -co "INTERLEAVE=BIP" {} {}'
                    .format(GDAL_PATH, inwarp, outwarp))


def vrt(invrts, outvrt):
    LOGGER.debug('Creating VRT: {}'.format(outvrt))
    subprocess.call('{}/gdalbuildvrt -separate {} {}'
                    .format(GDAL_PATH, outvrt, ' '.join(invrts)))


def build_paths(out_path, tiff_base, band_list, work_path):
    base = os.path.join(out_path, tiff_base)

    if not os.path.exists(base):
        os.makedirs(base)

    phs = {'VRT': {'OUT': os.path.join(work_path, tiff_base + '.vrt'),
                   'IN': ' '.join(band_list)},
           'WARP': {'IN': os.path.join(work_path, tiff_base + '.vrt'),
                    'OUT': os.path.join(base, tiff_base + '_MTLstack')},
           'GCP': {'IN': os.path.join(work_path, tiff_base + '_GCP.txt'),
                   'OUT': os.path.join(base, tiff_base + '_GCP.txt')},
           'MTL': {'IN': os.path.join(work_path, tiff_base + '_MTL.txt'),
                   'OUT': os.path.join(base, tiff_base + '_MTL.txt')}}
    return phs


def check_percent_clear(cfmask):
    ds = gdal.Open(cfmask, gdal.GA_ReadOnly)

    arr = ds.GetRasterBand(1).ReadAsArray()
    bins = np.bincount(arr.ravel())

    return np.sum(bins[0:2]) / np.sum(bins[:-1]).astype(np.float)


def build_l8_list(work_path, tiff_base):
    return ['{}_sr_band2.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band3.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band4.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band5.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band6.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band7.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_toa_band10.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_cfmask.tif'.format(os.path.join(work_path, tiff_base))]


def build_tm_list(work_path, tiff_base):
    return ['{}_sr_band1.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band2.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band3.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band4.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band5.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_sr_band7.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_toa_band6.tif'.format(os.path.join(work_path, tiff_base)),
            '{}_cfmask.tif'.format(os.path.join(work_path, tiff_base))]


def base_name(work_path):
    base = ''
    for tiff_file in os.listdir(work_path):
        if tiff_file[-12:] != 'sr_band1.tif':
            continue

        base = tiff_file[:21]
        break

    return base


def clean_up(work_path):
    LOGGER.debug('Cleaning up')
    for f in os.listdir(work_path):
        os.remove(os.path.join(work_path, f))

    # os.rmdir(work_path)


def run(infile, work_path, outpath):
    unpackage(infile, work_path)

    tiff_base = base_name(work_path)

    if tiff_base[2] == '8':
        band_list = build_l8_list(work_path, tiff_base)
    else:
        band_list = build_tm_list(work_path, tiff_base)

    pathing = build_paths(outpath, tiff_base, band_list, work_path)

    if os.path.exists(pathing['WARP']['OUT']):
        LOGGER.debug('File already exists: {}'.format(tiff_base))
        clean_up(work_path)
        quit()

    clear = check_percent_clear(cfmask=band_list[-1])
    LOGGER.debug('Percent clear: {}'.format(clear))

    # if not clear < 0.20:
    #     LOGGER.debug('Threshold not met: {}'.format(tiff_base))
    #     clean_up(work_path)
    #     quit()

    vrt(pathing['VRT']['IN'], pathing['VRT']['OUT'])
    warp(pathing['WARP']['IN'], pathing['WARP']['OUT'])

    if os.path.exists(pathing['GCP']['IN']):
        shutil.copy(pathing['GCP']['IN'], pathing['GCP']['OUT'])
    if os.path.exists(pathing['MTL']['IN']):
        shutil.copy(pathing['MTL']['IN'], pathing['MTL']['OUT'])

    clean_up(work_path)

if __name__ == '__main__':
    indir = '/shared/users/klsmith/klsmith@usgs.gov-06102016-074716'
    working = '/dev/shm/ard_working'
    outpathing = '/shared/users/klsmith/AL-h07v08'

    if not os.path.exists(working):
        os.mkdir(working)

    for tarball in os.listdir(indir):
        run(os.path.join(indir, tarball), working, outpathing)

    os.rmdir(working)
