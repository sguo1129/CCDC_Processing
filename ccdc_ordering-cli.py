import argparse

parser = argparse.ArgumentParser(prog='LCMAP/CCDC Ordering Helper')
v_group = parser.add_mutually_exclusive_group()
parser.add_argument('--update', help='Update the Landsat Metadata before building the order', action='store_true')
parser.add_argument('--region', help='Region that the order falls in (CONUS, AL, HI)',
                    choices=('CONUS', 'AK', 'HI'))
parser.add_argument('--config', help='Config file path, defaults to working directory/ccdc.cfg')
parser.add_argument('--note', help='Note to append to the order')
# v_group.add_argument('--verbose', help='Enable more verbose output', action='store_true')
# v_group.add_argument('--quiet', help='No progress output', action='store_true')
# parser.add_argument('shapefile', help='Shape file path to build order around')


def main(args):
    pass


if __file__ == '__main__':
    args = parser.parse_args()
    main(args)
