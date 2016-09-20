"""
Random Forest Classification training
"""
import os
import re
from itertools import starmap, product

import numpy as np

from CCDC_Processing import geo_utils, utils


start_time = '1999-01-01'
end_time = '2001-12-31'


def tile_standard_train(tile_dir, anc_dir, neighbors=None):
    """
    Train a RFC model for the given tile using available surrounding tiles to help with training

    This method assumes a lot of information:
    the input directory follows the convention - tile_dir/TSFitmaps  - change detection outputs
                                                         /LTXXXXXxx  - Imagery folders
                                                      ../CONUS_hxxvxx - Neighboring tiles

    anc_dir has the following files, representing all of CONUS - trends_2000.img
                                                                 dem.img
                                                                 aspect.img
                                                                 slope.img
                                                                 posidex.img
                                                                 mpw.img

    :param tile_dir:
    :param anc_dir:
    :param neighbors:
    :return:
    """
    root, tile = os.path.split(tile_dir)

    loc = tile.split('_')[0]
    h_loc, v_loc = [int(_) for _ in re.findall(r'\d+', tile)]

    if not neighbors:
        neighbors = []

        pot_matrix = starmap(lambda a, b: (h_loc + a, v_loc + b), product((0, -1, 1), (0, -1, 1)))

        for pot in pot_matrix:
            path = os.path.join(root, '{0}_h{1}v{2}'.format(loc, pot[0], pot[1]))

            if os.path.exists(path):
                neighbors.append(path)


def separate_fmask(fmask):
    """
    Generate arrays based on FMask values for stats generation

    FMask values are as noted:
    0 clear
    1 water
    2 shadow
    3 snow
    4 cloud
    255 fill

    :param fmask: numpy array
    :return: numpy array
    """
    ret = np.zeros(shape=((5,) + fmask.shape))

    ret[0][fmask < 255] = 1
    for i in range(1, len(ret)):
        ret[i][fmask == i] = 1

    return ret


def tile_fmask_stats(tile_dir):
    """
    Generate the FMask stats for a given tile area

    FMask values are as noted:
    0 clear
    1 water
    2 shadow
    3 snow
    4 cloud
    255 fill

    :param tile_dir:
    :return:
    """
    fmask_stats = np.zeros(shape=(5, 5000, 5000), dtype=np.float)

    for root, dirs, files in os.walk(tile_dir):
        for f in files:
            if f[-3:] != 'MTL':
                continue

            fmask = geo_utils.array_from_rasterband(os.path.join(root, f))
            fmask_stats += separate_fmask(fmask)

    fmask_stats[4] = 100 * fmask_stats[4] / fmask_stats[0]
    fmask_stats[3] = 100 * fmask_stats[3] / (fmask_stats[1] + fmask_stats[2] + fmask_stats[3] + 0.01)
    fmask_stats[2] = 100 * fmask_stats[2] / (fmask_stats[1] + fmask_stats[2] + 0.01)
    fmask_stats[1] = 100 * fmask_stats[1] / (fmask_stats[1] + fmask_stats[2] + 0.01)

    return fmask_stats
