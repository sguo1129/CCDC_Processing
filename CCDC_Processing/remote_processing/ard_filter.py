import os
import logging

from osgeo import gdal

from ard_filters import Fill_10percent, Fill_20percent, NoFill_10percent, NoFill_20percent
from ard_filters import NoFill_10Percent_1999, NoFill_20Percent_1999


LOGGER = logging.getLogger()
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(levelname)s]: %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)


class ARDFilteringException(Exception):
    pass


class ARDFiltering(object):
    def __init__(self, output_path, filters):
        self.file_path = None
        self.filename = None
        self.parent_dir = None
        self.ds = None

        self.filters = filters
        self.output_path = output_path

        LOGGER.info('Initialized filters: {}'.format(self.filters))
        LOGGER.info('Initialized output path: {}'.format(self.output_path))

        self._create_outpaths()

    def filter(self, file_path):
        if not os.path.exists(file_path):
            raise ARDFilteringException('File path does not exist: {}'.format(file_path))

        self.file_path = file_path
        self.filename = os.path.split(file_path)[-1]
        self.parent_dir = os.path.dirname(file_path)

        self._open_dataset()

        LOGGER.info('Filtering: {}'.format(self.filename))

        for _f in self.filters:
            arr_ls = []

            for b in _f.required_bands:
                arr_ls.append(self._fetch_bandarray(b))

            if _f.check(self.filename, arr_ls):
                self._create_folder_symlink(_f.output_name)

        self._close_dataset()

    def _open_dataset(self):
        self.ds = gdal.Open(self.file_path, gdal.GA_ReadOnly)

    def _close_dataset(self):
        self.ds = None

    def _create_folder_symlink(self, filter_name):
        parent_name = os.path.split(self.parent_dir)[-1]
        link = os.path.join(self.output_path, filter_name, parent_name)

        # os.symlink(self.parent_dir, link)
        print 'Creating symlink: {} to {}'.format(self.parent_dir, link)

    def _fetch_bandarray(self, band):
        return self.ds.GetRasterBand(band).ReadAsArray()

    def _create_outpaths(self):
        for _f in self.filters:
            out = os.path.join(self.output_path, _f.output_name)

            if not os.path.exists(out):
                os.makedirs(out)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._close_dataset()
        self.filters = None


if __name__ == '__main__':
    indir = r'D:\lcmap\kenai\0806\imagery\test'
    outpathing = r'D:\lcmap\kenai\0806\imagery\filtered'
    filters = [Fill_20percent, Fill_10percent, NoFill_20percent, NoFill_10percent,
               NoFill_10Percent_1999, NoFill_20Percent_1999]

    with ARDFiltering(outpathing, filters) as f:
        for root, dirs, files in os.walk(indir):
            for file in files:
                if file[-8:] == 'MTLstack':
                    f.filter(os.path.join(root, file))
