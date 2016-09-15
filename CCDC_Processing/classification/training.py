"""
Random Forest Classification training
"""

import numpy as np

from CCDC_Processing import geo_utils, utils


start_time = '1999-01-01'
end_time = '2001-12-31'


def standard_train(tile_dir, anc_dir):
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
