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


def matlab2datetime(matlab_datenum):
    day = datetime.datetime.fromordinal(int(matlab_datenum))
    dayfrac = datetime.timedelta(days=matlab_datenum % 1) - datetime.timedelta(days=366)
    return day + dayfrac


def db_instance(config_file):
    return DBConnect(**get_cfg(config_file)['db'])
