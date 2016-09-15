"""
Random Forest Classification training
"""
import os

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
    :return:
    """


def tile_fmask_stats(tile_dir):
    """
    Generate the FMask stats for a given tile area

    FMask values are as noted:
    0 clear
    1 water
    2 shadow
    3 snow
    4 cloud

    :param tile_dir:
    :return:
    """

    count = 0
    fmask_stats = np.zeros(shape=(4, 5000, 5000), dtype=np.float)
    count_arr = np.zeros(shape=(5000, 5000), dtype=np.int)

    for root, dirs, files in os.walk(tile_dir):
        for f in files:
            if f[-3:] != 'MTL':
                continue

            count += 1

            fmask = geo_utils.array_from_rasterband(os.path.join(root, f))

            for x in range(len(fmask_stats)):
                count_arr[fmask < 255] += 1
                fmask_stats[x][fmask == 0] += 1
                fmask_stats[x][fmask == 1] += 1
                fmask_stats[x][fmask == 3] += 1
                fmask_stats[x][fmask == 4] += 1

    fmask_stats[3] = 100 * fmask_stats[3] / count_arr
    fmask_stats[2] = 100 * fmask_stats[2] / (fmask_stats[0] + fmask_stats[1] + fmask_stats[2] + 0.01)
    fmask_stats[1] = 100 * fmask_stats[1] / (fmask_stats[0] + fmask_stats[1] + 0.01)
    fmask_stats[0] = 100 * fmask_stats[0] / (fmask_stats[0] + fmask_stats[1] + 0.01)
