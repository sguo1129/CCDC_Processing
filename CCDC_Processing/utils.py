"""
General utility methods
"""

import ConfigParser
import datetime

from CCDC_Processing.db_connect import DBConnect


def get_cfg(cfgfile='ccdc.cfg'):
    cfg_info = {}

    config = ConfigParser.ConfigParser()
    config.read(cfgfile)

    for sect in config.sections():
        cfg_info[sect] = {}
        for opt in config.options(sect):
            cfg_info[sect][opt] = config.get(sect, opt)

    return cfg_info


def matlab2datetime(matlab_date):
    day = datetime.datetime.fromordinal(int(matlab_date))
    dayfrac = datetime.timedelta(days=matlab_date % 1) - datetime.timedelta(days=366)

    return day + dayfrac


def datetime2matlab(dt):
    """
    http://stackoverflow.com/questions/8776414/python-datetime-to-matlab-datenum

    :param dt:
    :return:
    """
    mdn = dt + datetime.timedelta(days=366)
    frac_seconds = (dt - datetime.datetime(dt.year, dt.month, dt.day, 0, 0, 0)).seconds / (24.0 * 60.0 * 60.0)
    frac_microseconds = dt.microsecond / (24.0 * 60.0 * 60.0 * 1000000.0)

    return mdn.toordinal() + frac_seconds + frac_microseconds


def db_instance(config_file):
    return DBConnect(**get_cfg(config_file)['DB'])
