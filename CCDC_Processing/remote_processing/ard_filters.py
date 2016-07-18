import abc

import numpy as np


class ARDFilter(object):
    """
    Base Class used for filtering processed image stacks
    """
    __metaclass__ = abc.ABCMeta

    @classmethod
    @abc.abstractproperty
    def required_bands(cls):
        """
        This is assuming a stack as outlined for ingestion into the CCDC algorithm
        """
        return cls.required_bands

    @classmethod
    @abc.abstractproperty
    def output_name(cls):
        """
        Used for building the symlinks
        """
        return cls.output_name

    @classmethod
    @abc.abstractmethod
    def check(cls, filename, band_arrays):
        """
        This should only return a boolean True/False
        """
        pass


class CFMASKClear(object):
    """
    Contains helper methods for dealing with the CFMASK
    """
    @staticmethod
    def _percent_clear_nofill(cfmask_arr):
        """
        Exclude fill from the calculation
        """
        bins = np.bincount(cfmask_arr.ravel())

        return np.sum(bins[0:2]) / np.sum(bins[:-1]).astype(np.float)

    @staticmethod
    def _percent_clear_fill(cfmask_arr):
        """
        Include fill in the calculation
        """
        bins = np.bincount(cfmask_arr.ravel())

        return np.sum(bins[0:2]) / np.sum(bins).astype(np.float)


class LandsatDates(object):
    """
    Contains helper methods for dealing with Landsat information derived from the file name
    """

    @staticmethod
    def _landsat_date(filename):
        return int(filename[9:13])

    @staticmethod
    def _landsat_doy(filename):
        return int(filename[13:16])

    @staticmethod
    def _landsat_path(filename):
        return int(filename[3:6])

    @staticmethod
    def _landsat_row(filename):
        return int(filename[6:9])


class NoFill_10percent(ARDFilter, CFMASKClear):
    """
    Check for 10% clear, not including the fill in the calculation
    """
    required_bands = (8,)
    output_name = 'nofill_0.10'

    thresh = 0.10

    @classmethod
    def check(cls, filename, band_arrays):
        val = cls._percent_clear_nofill(band_arrays[-1])

        if val > cls.thresh:
            return True

        return False


class NoFill_20percent(ARDFilter, CFMASKClear):
    """
    Check for 20% clear, not including the fill in the calculation
    """
    required_bands = (8,)
    output_name = 'nofill_0.20'

    thresh = 0.20

    @classmethod
    def check(cls, filename, band_arrays):
        val = cls._percent_clear_nofill(band_arrays[-1])

        if val > cls.thresh:
            return True

        return False


class Fill_20percent(ARDFilter, CFMASKClear):
    """
    Check for 20% clear, including fill in the calculation
    """
    required_bands = (8,)
    output_name = 'fill_0.20'

    thresh = 0.20

    @classmethod
    def check(cls, filename, band_arrays):
        val = cls._percent_clear_fill(band_arrays[-1])

        if val > cls.thresh:
            return True

        return False


class Fill_10percent(ARDFilter, CFMASKClear):
    """
    Check for 10% clear, including fill in the calculation
    """
    required_bands = (8,)
    output_name = 'fill_0.10'

    thresh = 0.10

    @classmethod
    def check(cls, filename, band_arrays):
        val = cls._percent_clear_fill(band_arrays[-1])

        if val > cls.thresh:
            return True

        return False


class NoFill_10Percent_1999(ARDFilter, CFMASKClear, LandsatDates):
    """
    Check for 10% clear, including fill in the calculation
    """
    required_bands = (8,)
    output_name = 'nofill_0.10_1999'

    thresh = 0.10
    year = 1999

    @classmethod
    def check(cls, filename, band_arrays):
        val = cls._percent_clear_nofill(band_arrays[-1])
        yr = cls._landsat_date(filename)

        if val > cls.thresh and yr >= cls.year:
            return True

        return False


class NoFill_20Percent_1999(ARDFilter, CFMASKClear, LandsatDates):
    """
    Check for 10% clear, including fill in the calculation
    """
    required_bands = (8,)
    output_name = 'nofill_0.20_1999'

    thresh = 0.20
    year = 1999

    @classmethod
    def check(cls, filename, band_arrays):
        val = cls._percent_clear_nofill(band_arrays[-1])
        yr = cls._landsat_date(filename)

        if val > cls.thresh and yr >= cls.year:
            return True

        return False
