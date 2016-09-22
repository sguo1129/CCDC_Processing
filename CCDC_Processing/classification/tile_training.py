"""
File based classification methods
"""

import os
import re
from itertools import starmap, product

import numpy as np

from CCDC_Processing import geo_utils, utils
from CCDC_Processing.classification import training


def tile_standard_train(tile_dir, anc_dir, training_tiles=None):
    """
    Train a RFC model for the given tile using available surrounding tiles to help with training

    This method assumes a lot of information:
    the input directory follows the convention - tile_dir/TSFitmaps  - change detection outputs
                                                         /LTXXXXXxx  - Imagery folders
                                                      ../CONUS_hxxvxx - additional input tiles

    anc_dir has the following files, representing all of CONUS - trends_2000.img
                                                                 dem.img
                                                                 aspect.img
                                                                 slope.img
                                                                 posidex.img
                                                                 mpw.img

    :param tile_dir:
    :param anc_dir:
    :param training_tiles:
    :return:
    """
    input_tiles = tile_find_inputs(tile_dir, training_tiles)


def tile_find_inputs(tile_dir, training_tiles=None):
    """
    Determine the pathing to input ARD tiles

    :param tile_dir:
    :param training_tiles:
    :return:
    """
    input_tiles = [tile_dir]
    raise_exc = False

    root, tile = os.path.split(tile_dir)

    loc = tile.split('_')[0]
    h_loc, v_loc = [int(_) for _ in re.findall(r'\d+', tile)]

    if training_tiles:
        input_tiles += training_tiles
        raise_exc = True
    else:
        pot_matrix = starmap(lambda a, b: (h_loc + a, v_loc + b), product((0, -1, 1), (0, -1, 1)))[1:]

        for pot in pot_matrix:
            input_tiles.append(os.path.join(root, '{0}_h{1}v{2}'.format(loc, pot[0], pot[1])))

    return tile_check_inputs(input_tiles, raise_exc)


def tile_check_inputs(input_tiles, raise_exc=False):
    """
    Make sure that only tiles present are used for training

    :param input_tiles:
    :param raise_exc:
    :return:
    """
    mask = tile_check_existence(input_tiles)
    if raise_exc and False in mask:
        raise Exception('Some input tiles are missing')

    return tuple(t for t, m in zip(input_tiles, mask) if m)


def tile_check_existence(tile_dirs):
    if isinstance(tile_dirs, str):
        return os.path.exists(tile_dirs)
    else:
        return tuple(os.path.exists(d) for d in tile_dirs)


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
            fmask_stats += training.separate_fmask(fmask)

    fmask_stats[4] = 100 * fmask_stats[4] / fmask_stats[0]
    fmask_stats[3] = 100 * fmask_stats[3] / (fmask_stats[1] + fmask_stats[2] + fmask_stats[3] + 0.01)
    fmask_stats[2] = 100 * fmask_stats[2] / (fmask_stats[1] + fmask_stats[2] + 0.01)
    fmask_stats[1] = 100 * fmask_stats[1] / (fmask_stats[1] + fmask_stats[2] + 0.01)

    return fmask_stats
