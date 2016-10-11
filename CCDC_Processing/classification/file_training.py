"""
File based classification methods
"""

import os
import re
from itertools import starmap, product
from collections import namedtuple

import numpy as np
from scipy import ndimage
from scipy.ndimage.measurements import find_objects

from CCDC_Processing import geo_utils
from CCDC_Processing.classification import training


anc_file_names = ('dem.img', 'aspect.img', 'slope.img', 'posidex.img', 'mpw.img')
training_file_name = 'trends_2000.img'
conus_extent = geo_utils.GeoExtent(x_min=-2565585, y_min=14805, x_max=2384415, y_max=3314805)


def file_standard_train(tile_dir, anc_dir, training_tiles=None):
    """
    Train a RFC model for the given tile using available surrounding tiles or a provided list
    to help with training

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
    # Gather our input tiles
    input_tiles = file_find_inputs(tile_dir, training_tiles)

    # Make sure our ancillary data is available
    anc_paths = [os.path.join(anc_dir, anc) for anc in anc_file_names]
    file_check_for_inputs(anc_paths, raise_exc=True)

    # Make sure the data that is trained with is available
    trends_path = os.path.join(anc_dir, training_file_name)
    file_check_for_inputs([trends_path], raise_exc=True)

    # Now we move through the input tiles, grabbing relevant geo extents and data for all of them
    for intile in input_tiles:
        h_loc, v_loc, loc = file_hv_loc(intile)
        geo_ext, affine = file_extent_from_hv(h_loc, v_loc, loc)

        # Trends data only covers a small portion of CONUS in smaller tiles, so we can
        #   subset the data we grab from subsequent data sets
        trends_blocks, trends_exts = file_fetch_trends_extents(trends_path, geo_extent=geo_ext)

        # Grab the model information from the TSFitmaps
        changemodels = [file_fetch_changemodels(intile, ext, )]

        # Grab the information from the input data sets
        anc_data = [file_fetch_ancillery(anc_paths, ext) for ext in trends_exts]


def file_fetch_changemodels(fitmap_dir, geo_extent, begin_date, end_date, tile_affine):
    rc_ext = geo_utils.geoext_to_rowcolext(geo_extent, tile_affine)


def file_fetch_ancillery(anc_paths, geo_extent):
    return tuple(geo_utils.array_from_rasterband(f, geo_extent) for f in anc_paths)


def file_fetch_trends_extents(trends_path, geo_extent):
    """
    Retrieve the patches of trends data present inside of the given GeoExtent

    :param trends_path:
    :param geo_extent:
    :return: tuple of geo extents
    """
    affine = geo_utils.get_raster_affine(trends_path)

    block_arr = geo_utils.array_from_rasterband(trends_path, geo_extent=geo_extent)
    labels, _ = ndimage.label(block_arr)

    slices = find_objects(labels)

    geo_exts = tuple(geo_utils.rowcolext_to_geoext(affine, geo_utils.RowColumnExtent(start_row=y.start,
                                                                                     start_col=x.start,
                                                                                     end_row=y.stop,
                                                                                     end_col=x.stop))
                     for y, x in slices)

    blocks = tuple(block_arr[s] for s in slices)

    # return geo extents so that they can be used with other data sets
    return blocks, geo_exts


def file_hv_loc(tile_dir):
    """
    Determine the h, v, and location information based on tile directory name

    :param tile_dir:
    :return:
    """
    root, tile = os.path.split(tile_dir)

    loc = tile.split('_')[0]
    h_loc, v_loc = [int(_) for _ in re.findall(r'\d+', tile)]

    return h_loc, v_loc, loc


def file_find_inputs(tile_dir, training_tiles=None):
    """
    Determine the pathing to input ARD tiles

    if training_tiles is none, then it returns available neighboring tiles

    :param tile_dir:
    :param training_tiles:
    :return:
    """
    input_tiles = [tile_dir]
    raise_exc = False

    root, tile = os.path.split(tile_dir)
    h_loc, v_loc, loc = file_hv_loc(tile_dir)

    if training_tiles:
        input_tiles += training_tiles
        raise_exc = True
    else:
        pot_matrix = starmap(lambda a, b: (h_loc + a, v_loc + b), product((0, -1, 1), (0, -1, 1)))[1:]

        for pot in pot_matrix:
            input_tiles.append(os.path.join(root, '{0}_h{1}v{2}'.format(loc, pot[0], pot[1])))

    return file_check_for_inputs(input_tiles, raise_exc)


def file_check_for_inputs(inputs, raise_exc=False):
    """
    Make sure that only tiles present are used for training

    :param inputs:
    :param raise_exc:
    :return:
    """
    mask = file_check_existence(inputs)
    ret = tuple(t for t, m in zip(inputs, mask) if m)

    if raise_exc and False in mask:
        raise Exception('The following input data is missing: {0}'
                        .format(list(set(inputs) - set(ret))))

    return ret


def file_check_existence(tile_dirs):
    """
    Check the existence of a particular directory or list of directories

    :param tile_dirs:
    :return:
    """
    if isinstance(tile_dirs, str):
        return tuple(os.path.exists(tile_dirs),)
    else:
        return tuple(os.path.exists(d) for d in tile_dirs)


def file_fmask_stats(tile_dir):
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


def file_extent_from_hv(h, v, loc='conus'):
    """
    Retrieve the geospatial extent for the given tile h/v

    :param h:
    :param v:
    :param loc:
    :return:
    """
    loc = loc.lower()

    if loc == 'conus':
        xmin = conus_extent.x_min + h * 5000 * 30
        xmax = conus_extent.x_min + h * 5000 * 30 + 5000 * 30
        ymax = conus_extent.y_max - v * 5000 * 30
        ymin = conus_extent.y_max - v * 5000 * 30 - 5000 * 30
    else:
        raise Exception('Location not implemented: {0}'
                        .format(loc))

    return (geo_utils.GeoExtent(x_min=xmin, x_max=xmax, y_max=ymax, y_min=ymin),
            geo_utils.GeoAffine(ul_x=xmin, x_res=30, rot_1=0, ul_y=ymax, rot_2=0, y_res=30))
