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
